
import json
import os
import time
import re
import logging
import io
import random
import math
import threading
import posixpath
from urllib.parse import urlparse
from typing import Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import asyncio
from arq import create_pool
from arq.connections import RedisSettings

from httpcore import TimeoutException

import boto3
import requests
from PIL import Image
try:
    import cairosvg  # type: ignore
except Exception:  # pragma: no cover
    cairosvg = None

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import subprocess
import platform
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains

#factory imports
from app.utils.product_factory import create_empty_product
from app.utils.variant_factory import create_empty_variant


# Configure logging with proper levels
_level_name = (os.getenv("LOG_LEVEL") or "INFO").upper().strip()
_level = getattr(logging, _level_name, logging.INFO)
logging.basicConfig(
    level=_level, 
    format="%(asctime)s - %(levelname)s - %(message)s", 
    force=True
)
logger = logging.getLogger(__name__)

# Silence noisy libraries
logging.getLogger("selenium").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("webdriver_manager").setLevel(logging.WARNING)

DEFAULT_BASE_URL = "https://www.primark.com/uae-en/women-clothing-dresses/"

# Import common utilities
from app.config import get_environment
from app.utils.primark_utils import (
    append_log,
    complete_task,
    post_product_to_api,
    patch_product_to_api,
)
from app.core.primark_scraper_api import _url_to_arabic, _parse_product_page_arabic_only_sync
from app.utils.image_processing import DownloadImage, ThumbnailDownloadImage

MAX_RETRIES = 3
headers = {"Content-Type": "application/json"}

# Redis configuration
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

def generate_unique_id(url: str) -> str:
    """Generate a unique numeric ID using timestamp."""
    import time
    return str(int(time.time() * 1000))


def generate_slug(name: str, color: str = "", size: str = "") -> str:
    """Generate URL-friendly slug."""
    text = f"{name} {color} {size}".strip()
    slug = re.sub(r'[^a-zA-Z0-9\s-]', '', text.lower())
    slug = re.sub(r'[\s_]+', '-', slug)
    slug = re.sub(r'-+', '-', slug).strip('-')
    return slug


# Image upload classes (DownloadImage, ThumbnailDownloadImage) now imported from app.utils.primark_utils


def kill_chrome_processes():
    """Kill any running Chrome or ChromeDriver processes to free up resources."""
    system = platform.system()
    logger.info(f"Cleaning up Chrome processes on {system}...")
    
    try:
        if system == "Windows":
            subprocess.run(["taskkill", "/F", "/IM", "chrome.exe"], capture_output=True)
            subprocess.run(["taskkill", "/F", "/IM", "chromedriver.exe"], capture_output=True)
        else:
            # Linux/MacOS
            subprocess.run(["pkill", "-9", "chrome"], capture_output=True)
            subprocess.run(["pkill", "-9", "chkrome"], capture_output=True)  # sometimes named differently
            subprocess.run(["pkill", "-9", "chromedriver"], capture_output=True)
    except Exception as e:
        logger.warning(f"Process cleanup warning: {e}")

def setup_driver():
    """Setup Chrome: Headless but configured to look EXACTLY like a Desktop Browser."""
    import tempfile
    import random
    
    options = Options()
    
    # --- 1. FORCE DESKTOP LAYOUT (Crucial for Headless) ---
    # Without this, Primark renders the Mobile View, hiding the size buttons!
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--start-maximized")
    
    # --- 2. HIDE "HEADLESS" IDENTITY ---
    # Some sites detect 'HeadlessChrome' in the User-Agent and block data
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    # Hide the "Chrome is being controlled by automated software" banner/flag
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)

    # --- 3. PERFORMANCE SETTINGS ---
    # 'normal' ensures the page structure is fully loaded (fixing "Unknown Product")
    options.page_load_strategy = 'eager' 
    
    
    # Standard options
    options.add_argument("--headless=new") 
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    
    # Unique environment for parallel safety
    temp_dir = tempfile.mkdtemp(prefix="chrome_")
    debug_port = random.randint(10000, 20000)
    
    options.add_argument(f"--user-data-dir={temp_dir}")
    options.add_argument(f"--remote-debugging-port={debug_port}")
    
    options.add_argument("--log-level=3")
    
    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )
    
    # Execute CDP command to further hide automation status
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": """
            Object.defineProperty(navigator, 'webdriver', {
            get: () => undefined
            })
        """
    })
    
    driver.set_page_load_timeout(60)
    return driver

class PrimarkScraper:
    def __init__(self, driver, task_id: str, base_sku: str = None, usd_to_aed: float = 3.67, usd_to_iqd: float = 1500, stop_event: threading.Event | None = None):
        self.driver = driver
        self.task_id = task_id
        self.base_sku = base_sku or "primark-product"
        self.sku_counter = 0
        self.all_products = []
        self._job_ids: list[str] = []  # ARQ job IDs for sentinel to await
        self.stop_event = stop_event
        self.usd_to_aed = usd_to_aed
        self.usd_to_iqd = usd_to_iqd

    def log(self, message: str, level: str = "INFO"):
        """
        Unified logging method with level support.
        level can be: DEBUG, INFO, WARNING, ERROR
        """
        log_func = getattr(logger, level.lower(), logger.info)
        log_func(f"[{self.task_id}] {message}")
        
        append_log(self.task_id, message, level.lower())

    def send_product_to_api(self, product_data: dict):
        """Sends a single product to the external API immediately."""
        success, error = post_product_to_api(product_data)
        product_name = (
            product_data.get("data", {}).get("original_name", {}).get("en")
            or product_data.get("data", {}).get("name", {}).get("en")
            or "Unknown Product"
        )
        if success:
            self.log(f"🚀 Sent product to API: {product_name}")
        else:
            self.log(f"❌ Failed to send to API: {error}")

    def extract_size_guide(self, driver) -> Dict[str, Any]:
        """Extract size guide data from Primark product page."""
        guide_data = {"table": [], "image_url": "N/A"}
        
        wait = WebDriverWait(driver, 5)
        
        # Handle cookie banner
        try:
            cookie_btn = WebDriverWait(driver, 2).until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Accept All')]"))
            )
            cookie_btn.click()
            time.sleep(1)
            self.log("Cookie Banner dismissed.")
        except:
            pass
        
        # Try to open size guide
        table = None
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # Find and click the button
                try:
                    btn = wait.until(
                        EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'View Size Guide')]"))
                    )
                    driver.execute_script("arguments[0].click();", btn)
                except:
                    if attempt == 0:
                        self.log("Size Guide button not found (skipping size guide).")
                    break
                
                # Wait for table to become visible
                table = wait.until(
                    EC.visibility_of_element_located(
                        (By.XPATH, "//div[contains(@class, 'Modal')]//table | //table")
                    )
                )
                break
            except Exception as e:
                self.log(f"Attempt {attempt+1} failed. Retrying...")
                time.sleep(1)
        
        # Scrape data from table
        if table:
            try:
                # Scrape Headers
                headers = []
                header_cells = table.find_elements(By.TAG_NAME, "th")
                for th in header_cells:
                    try:
                        # Try to get text from dropdown with single-value class first
                        special_text = th.find_element(
                            By.CSS_SELECTOR, "[class*='single-value']"
                        ).text.strip()
                        headers.append(special_text)
                    except:
                        # Fallback to direct text content
                        header_text = th.text.strip()
                        # Remove dropdown indicator if present, keep only the main text
                        if header_text:
                            headers.append(header_text)
                
                # Add headers as first row if they exist
                if headers:
                    guide_data["table"].append(headers)
                
                # Scrape Data Rows (skip header row in tbody)
                rows = table.find_elements(By.CSS_SELECTOR, "tbody tr")
                for row in rows:
                    cells = row.find_elements(By.TAG_NAME, "td")
                    if not cells:
                        continue
                    row_data = []
                    for i, cell in enumerate(cells):
                        if i < len(headers):
                            row_data.append(cell.text.strip())
                    if row_data:
                        guide_data["table"].append(row_data)
                
                # Close the modal
                try:
                    ActionChains(driver).send_keys(Keys.ESCAPE).perform()
                    time.sleep(0.5)
                except:
                    pass
            except Exception as e:
                self.log(f"Error reading table data: {e}")
        
        return guide_data

    def scrape_product_page(self, url: str):
        """Wrapper to catch and log errors during product scraping."""
        try:
            self._scrape_product_page_impl(url)
        except Exception as e:
            import traceback
            error_trace = traceback.format_exc()
            self.log(f"❌ CRITICAL ERROR scraping {url}: {e}", "ERROR")
            append_log(self.task_id, f"Traceback: {error_trace}", "error")

    def _scrape_product_page_impl(self, url: str):
        """Scrape a single Primark product page and post to API."""
        try:
            # Navigate
            self.driver.get(url)
            
            wait = WebDriverWait(self.driver, 20)
            wait.until(EC.presence_of_element_located((By.TAG_NAME, "h1")))
            
            try:
                self.driver.execute_script("window.stop();")
            except:
                pass

        except TimeoutException:
            self.log(f"Skipping: Timed out waiting for H1 on {url}")
            return
        
        except Exception as e:
            self.log(f"Skipping: Page load error {url}: {e}")
            return
        if self.stop_event and self.stop_event.is_set():
            self.log(f"Stop requested before scraping product: {url}")
            return
        
        try:
            # Wait for page load
            WebDriverWait(self.driver, 30).until(EC.presence_of_element_located((By.TAG_NAME, "h1")))
        except:
            self.log(f"Skipping: Page load timeout for {url}")
            return
        
        driver = self.driver
        
        # Generate unique ID for product
        product_id = generate_unique_id(url)

        # Extract size guide
        sg_data = self.extract_size_guide(driver)
        
        # Get product name
        try:
            p_name = driver.find_element(
                By.CSS_SELECTOR, "h1[class*='ProductConversion_productTitle']"
            ).text
        except:
            p_name = "Unknown Product"
        
        # Get brand
        try:
            p_brand = driver.find_element(
                By.CSS_SELECTOR, "h2[class*='ProductConversion_brand']"
            ).text
        except:
            p_brand = "N/A"
        
        # Get description (attributes section)
        p_desc = ""
        try:
            p_desc = driver.execute_script("""
                const descElement = document.querySelector('div[class*="CoreDetails_descText"]');
                return descElement ? descElement.textContent.trim() : '';
            """)
                
        except Exception as e:
            self.log(f"Error extracting description: {e}")
        
        # Get specifications - FAST JavaScript extraction
        p_specs = {"en": {}, "ar": {}}
        try:
            # Use JavaScript to extract all specifications at once
            specs_data = driver.execute_script("""
                const specs = [];
                const items = document.querySelectorAll('ul[class*="CoreDetails_list"] li');
                
                items.forEach(item => {
                    const spans = item.querySelectorAll('span[class*="CoreDetails_attributeColumn"]');
                    if (spans.length >= 2) {
                        const label = spans[0].textContent.trim();
                        const value = spans[1].textContent.trim();
                        if (label && value) {
                            specs.push({label: label, value: value});
                        }
                    }
                });
                
                return specs;
            """)
            
            if not specs_data:
                self.log("Warning: No specifications found")
            else:
                # Store English specs from current (en) page only
                for spec in specs_data:
                    label_en = spec["label"]
                    value_en = spec["value"]
                    if label_en and value_en:
                        p_specs["en"][label_en] = value_en
                    # Arabic specs come from visiting the ar page below (no translation)
        except Exception as e:
            self.log(f"Error extracting specifications: {e}")

        # Fetch Arabic PDP (uae-ar) to gather name, description, brand, specifications from ar page
        p_name_ar = p_desc_ar = p_brand_ar = ""
        specs_ar = {}
        url_ar = _url_to_arabic(url)
        if url_ar:
            try:
                resp = requests.get(url_ar, timeout=20, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0"})
                if resp.ok and resp.text:
                    ar_data = _parse_product_page_arabic_only_sync(resp.text)
                    p_name_ar = (ar_data.get("name_ar") or "").strip()
                    p_desc_ar = (ar_data.get("description_ar") or "").strip()
                    p_brand_ar = (ar_data.get("brand_ar") or "").strip()
                    specs_ar = ar_data.get("specifications_ar") or {}
                    if not isinstance(specs_ar, dict):
                        specs_ar = {}
                    # Use scraped Arabic specs for p_specs["ar"] (no translate_to_ar)
                    p_specs["ar"] = dict(specs_ar)
                    self.log(f"Fetched Arabic PDP: name_ar={bool(p_name_ar)}, specs_ar={len(specs_ar)}", "DEBUG")
            except Exception as e_ar:
                self.log(f"Arabic PDP fetch failed: {e_ar}", "DEBUG")
        
        # Collect all variants data - color x size combinations
        all_colors = []
        all_sizes = set()
        main_product_images = []
        all_images = []
        variants = []
        variant_index = 0
        
        main_page_sku = ""
        try:
            main_page_sku = driver.execute_script("""
                let sku = "";
                let scripts = document.querySelectorAll('script[type="application/ld+json"]');
                for (let tag of scripts) {
                    try {
                        let data = JSON.parse(tag.textContent);
                        if (data.sku) { sku = data.sku; break; }
                        if (Array.isArray(data)) {
                            for(let item of data) { if(item.sku) { sku = item.sku; break; } }
                        }
                    } catch(e) {}
                }
                return sku;
            """)
        except:
            pass
        
        # STEP 1: Extract main page price FIRST (as fallback)
        main_page_original_price = None
        main_page_discounted_price = None
        main_page_discount_percentage = 0
        
        try:
            # Handle cookie banner first
            try:
                cookie_btn = WebDriverWait(driver, 2).until(
                    EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Accept All')]"))
                )
                cookie_btn.click()
                time.sleep(1)
            except:
                pass
            
            # Extract discounted/selling price from main page
            try:
                selling_price_elem = driver.find_element(
                    By.CSS_SELECTOR, "span[class*='ProductPrice_sellingPrice'] span[class*='ProductPrice_value']"
                )
                discounted_price_text = selling_price_elem.text.strip()
                if discounted_price_text:
                    main_page_discounted_price = float(re.sub(r'[^\d.]', '', discounted_price_text))
            except Exception as e1:
                try:
                    selling_price_elem = driver.find_element(
                        By.CSS_SELECTOR, "span[class*='ProductPrice_sellingPrice']"
                    )
                    selling_price_text = selling_price_elem.text.replace("\n", " ").strip()
                    if selling_price_text:
                        main_page_discounted_price = float(re.sub(r'[^\d.]', '', selling_price_text.split()[0]))
                except Exception as e2:
                    pass
            # Extract original price from main page
            try:
                old_price_elem = driver.find_element(
                    By.CSS_SELECTOR, "div[class*='ProductPrice_preReductionPrice']"
                )
                old_price_text = old_price_elem.text.strip()
                if old_price_text:
                    main_page_original_price = float(re.sub(r'[^\d.]', '', old_price_text))
            except:
                pass 
            
            # Extract discount percentage from main page 
            try:
                discount_elem = driver.find_element(
                    By.CSS_SELECTOR, "span[class*='DiscountTag_value']"
                )
                discount_text = discount_elem.text.strip()
                if discount_text:
                    discount_value = re.sub(r'[^\d.]', '', discount_text)
                    if discount_value:
                        main_page_discount_percentage = int(discount_value)
            except:
                pass  
                
        except Exception as e:
            pass
        # Determine main page final prices
        main_page_price = main_page_original_price if main_page_original_price is not None else (main_page_discounted_price if main_page_discounted_price is not None else 0)
        main_page_final_discounted = main_page_discounted_price if main_page_discounted_price is not None else (main_page_original_price if main_page_original_price is not None else 0)
        
        # Phase 1: Find color links on main page (in horizontal wrapper)
        color_variants_data = []
        has_color_variants = False
        
        try:
            # Wait for page to be fully loaded
            time.sleep(2)
            
            # Find the horizontal wrapper containing color links - try multiple selectors
            wait = WebDriverWait(driver, 10)
            horizontal_wrapper = None
            
            # Try to find wrapper with different selectors
            try:
                horizontal_wrapper = wait.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div[class*='GroupedProducts_horizontalWrapper']"))
                )
            except:
                try:
                    # Try alternative selector
                    horizontal_wrapper = driver.find_element(By.CSS_SELECTOR, "div[class*='GroupedProducts_container'] div[class*='horizontalWrapper']")
                except:
                    # Try finding by section
                    try:
                        section = driver.find_element(By.CSS_SELECTOR, "section[class*='GroupedProducts_container']")
                        horizontal_wrapper = section.find_element(By.CSS_SELECTOR, "div[class*='horizontalWrapper']")
                    except Exception as e:
                        horizontal_wrapper = None
            
            if not horizontal_wrapper:
                has_color_variants = False
            else:
                color_info_list = []
                try:
                    color_data = driver.execute_script("""
                        const wrapper = arguments[0];
                        const links = wrapper.querySelectorAll('a');
                        const colorData = [];
                        links.forEach(link => {
                            const title = link.querySelector('div[class*="GroupedProducts_title"]');
                            if (title && title.textContent.trim()) {
                                colorData.push({
                                    name: title.textContent.trim(),
                                    href: link.href
                                });
                            }
                        });
                        return colorData;
                    """, horizontal_wrapper)
                    
                    if color_data:
                        color_info_list = color_data
                except Exception as e:
                    self.log(f"JavaScript method failed: {e}", "DEBUG")
                if color_info_list:
                    self.log(f"Collected {len(color_info_list)} color variants", "DEBUG")
                    
                    # Loop through each color and extract data
                    for color_idx, color_info in enumerate(color_info_list):
                        if self.stop_event and self.stop_event.is_set():
                            self.log("Stop requested during color processing.")
                            break
                        try:
                            color_name = color_info["name"]
                            color_href = color_info["href"]
                            
                            # Navigate to the color's URL
                            current_url = driver.current_url
                            if color_href and color_href != current_url:
                                try:
                                    driver.get(color_href)
                                    # Wait for page to load
                                    WebDriverWait(driver, 15).until(
                                        EC.presence_of_element_located((By.CSS_SELECTOR, "h1[class*='ProductConversion_productTitle']"))
                                    )
                                    time.sleep(1)  # Brief wait for dynamic content
                                except Exception as nav_err:
                                    self.log(f"⚠️ Navigation error: {nav_err}", "DEBUG")
                                    continue
                            
                            # Extract price data from main page
                            original_price = None
                            discounted_price = None
                            discount_percentage = 0
                            
                            # Extract discounted/selling price
                            try:
                                selling_price_elem = driver.find_element(
                                    By.CSS_SELECTOR, "span[class*='ProductPrice_sellingPrice'] span[class*='ProductPrice_value']"
                                )
                                discounted_price = float(re.sub(r'[^\d.]', '', selling_price_elem.text.strip()))
                            except:
                                try:
                                    selling_price_elem = driver.find_element(By.CSS_SELECTOR, "span[class*='ProductPrice_sellingPrice']")
                                    discounted_price = float(re.sub(r'[^\d.]', '', selling_price_elem.text.split()[0]))
                                except:
                                    pass
                            
                            # Extract original price
                            try:
                                old_price_elem = driver.find_element(By.CSS_SELECTOR, "div[class*='ProductPrice_preReductionPrice']")
                                original_price = float(re.sub(r'[^\d.]', '', old_price_elem.text.strip()))
                            except:
                                pass
                            
                            # Extract discount percentage
                            try:
                                discount_elem = driver.find_element(By.CSS_SELECTOR, "span[class*='DiscountTag_value']")
                                discount_value = re.sub(r'[^\d.]', '', discount_elem.text.strip())
                                if discount_value:
                                    discount_percentage = int(discount_value)
                            except:
                                pass
                            
                            # Determine final prices
                            p_price = original_price if original_price else (discounted_price if discounted_price else main_page_price)
                            p_discounted_price = discounted_price if discounted_price else (original_price if original_price else main_page_final_discounted)
                            if discount_percentage == 0 and main_page_discount_percentage > 0:
                                discount_percentage = main_page_discount_percentage
                            
                            if p_price == 0:
                                p_price = main_page_price
                                p_discounted_price = main_page_final_discounted
                                discount_percentage = main_page_discount_percentage
                                
                            color_sku = main_page_sku
                            if color_href and color_href != current_url:
                                try:
                                    color_sku = driver.execute_script("""
                                        let sku = "";
                                        let scripts = document.querySelectorAll('script[type="application/ld+json"]');
                                        for (let tag of scripts) {
                                            try {
                                                let data = JSON.parse(tag.textContent);
                                                if (data.sku) { sku = data.sku; break; }
                                                if (Array.isArray(data)) {
                                                    for(let item of data) { if(item.sku) { sku = item.sku; break; } }
                                                }
                                            } catch(e) {}
                                        }
                                        return sku;
                                    """) or main_page_sku
                                except:
                                    pass

                            sizes_data = []
                            found_sizes = False
                            
                            try:
                                wrapper_selector = "div[class*='SizePills_size_wrapper']"
                                wait_sizes = WebDriverWait(driver, 5)
                                wrapper = wait_sizes.until(EC.presence_of_element_located((By.CSS_SELECTOR, wrapper_selector)))
                                btns = wrapper.find_elements(By.TAG_NAME, "button")
                                
                                if btns:
                                    found_sizes = True
                                    for btn in btns:
                                        raw_text = btn.text.strip()
                                        size_val = raw_text.split("\n")[0].strip()
                                        
                                        btn_class = btn.get_attribute("class") or ""
                                        is_disabled = not btn.is_enabled() or "disabled" in btn_class or "oos" in btn_class.lower()
                                        is_low_stock = "lowstock" in btn_class.lower() or "low_stock" in btn_class.lower()
                                        
                                        stock_qty = 0 if is_disabled else (10 if is_low_stock else 100)
                                        stock_status = "out_of_stock" if is_disabled else ("low_stock" if is_low_stock else "in_stock")
                                        
                                        if size_val:
                                            sizes_data.append({
                                                "size": size_val, 
                                                "stock": stock_qty,
                                                "stock_status": stock_status 
                                            })
                                            all_sizes.add(size_val)
                            except Exception:
                                pass

                            if not found_sizes:
                                try:
                                    btns = driver.find_elements(By.CSS_SELECTOR, "button[class*='SizePills_size_variant']")
                                    if btns:
                                        found_sizes = True
                                        for btn in btns:
                                            size_val = btn.text.strip().split("\n")[0].strip()
                                            is_disabled = not btn.is_enabled()
                                            if size_val:
                                                sizes_data.append({"size": size_val, "stock": 0 if is_disabled else 100})
                                                all_sizes.add(size_val)
                                except:
                                    pass

                            if not found_sizes:
                                sizes_data.append({"size": "One Size", "stock": 100})
                                all_sizes.add("One Size")

                            color_images = []
                            try:
                                for img in driver.find_elements(By.CSS_SELECTOR, "div[class*='ImageGallery_imageContainer'] img"):
                                    src = img.get_attribute("src")
                                    if src and "placeholder" not in src:
                                        color_images.append(src)
                            except:
                                pass

                            # Fetch Arabic (uae-ar) for this color variant: details_ar, color_ar
                            details_ar = {}
                            color_ar = ""
                            color_url_ar = _url_to_arabic(color_href) if color_href else ""
                            if color_url_ar:
                                try:
                                    r_ar = requests.get(color_url_ar, timeout=20, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0"})
                                    if r_ar.ok and r_ar.text:
                                        ar_data = _parse_product_page_arabic_only_sync(r_ar.text)
                                        details_ar = ar_data.get("specifications_ar") or {}
                                        color_ar = (ar_data.get("color_ar") or "").strip()
                                except Exception:
                                    pass

                            color_variants_data.append({
                                "color": color_name,
                                "color_ar": color_ar,
                                "url": color_href,
                                "price": p_price,
                                "discounted_price": p_discounted_price,
                                "discount_percentage": discount_percentage,
                                "sizes": sizes_data,
                                "images": color_images,
                                "details_ar": details_ar,
                                "sku": color_sku,
                            })
                            
                            all_colors.append(color_name)
                            if color_idx == 0:
                                main_product_images = color_images.copy()
                            all_images.extend(color_images)
                            
                        except Exception as e:
                            self.log(f"❌ Error processing color {color_idx + 1}: {e}", "ERROR")
                            continue
                else:
                    has_color_variants = False
                
        except Exception as color_extract_err:
            has_color_variants = False
            pass
        
        if not has_color_variants or not color_variants_data:
            current_color = "Default"
            try:
                current_color = driver.find_element(
                    By.XPATH,
                    "//div[contains(@class, 'GroupedProducts_selected')]/parent::*/div[contains(@class, 'GroupedProducts_title')]"
                ).text
            except:
                if 'Colour' in p_specs.get("en", {}):
                    current_color = p_specs["en"]['Colour']
            
            p_price = main_page_price
            p_discounted_price = main_page_final_discounted
            discount_percentage = main_page_discount_percentage
            
            # Get sizes
            sizes_data = []
            try:
                btns = driver.find_elements(By.CSS_SELECTOR, "button[class*='SizePills_size_variant']")
                for btn in btns:
                    size_val = btn.text.strip()
                    if size_val:
                        all_sizes.add(size_val)
                        
                        # Check stock status
                        btn_class = btn.get_attribute("class") or ""
                        is_disabled = not btn.is_enabled() or "disabled" in btn_class
                        is_low = "lowstock" in btn_class.lower()
                        
                        if is_disabled:
                            stock_qty = 0
                            stock_status = "out_of_stock"
                        elif is_low:
                            stock_qty = 10
                            stock_status = "low_stock"
                        else:
                            stock_qty = 100
                            stock_status = "in_stock"
                        
                        sizes_data.append({
                            "size": size_val, 
                            "stock": stock_qty,
                            "stock_status": stock_status
                        })
            except:
                sizes_data.append({
                    "size": "One Size", 
                    "stock": 100,
                    "stock_status": "in_stock"
                })
                all_sizes.add("One Size")
            
            # Get images
            color_images = []
            try:
                elements = driver.find_elements(
                    By.CSS_SELECTOR, "div[class*='ImageGallery_imageContainer'] img"
                )
                for img in elements:
                    src = img.get_attribute("src")
                    if src and "placeholder" not in src:
                        color_images.append(src)
            except:
                pass
            
            color_variants_data.append({
                "color": current_color,
                "color_ar": "",
                "price": p_price,
                "discounted_price": p_discounted_price,
                "discount_percentage": discount_percentage,
                "sizes": sizes_data,
                "images": color_images,
                "details_ar": specs_ar,
                "sku": main_page_sku,
            })
            all_colors.append(current_color)
            main_product_images = color_images.copy()
            all_images = color_images.copy()
        
        
        # Use English specs for translation keys
        specs_en = p_specs.get("en", {})
        all_colors_ar = [str((cv.get("color_ar") or "")).strip() for cv in color_variants_data]

        # Create variants for each color x size combination matching API logic
        for color_data in color_variants_data:
            color_name = color_data["color"]
            p_price = color_data["price"]
            p_discounted_price = color_data["discounted_price"]
            discount_percentage = color_data["discount_percentage"]
            sizes_data = color_data["sizes"]
            color_images = color_data["images"]
            
            # Color slug
            color_slug = re.sub(r'[^a-zA-Z0-9]', '', color_name.lower())

            for size_data in sizes_data:
                size_val = size_data["size"]
                stock_qty = size_data["stock"]
                stock_status = size_data.get("stock_status", "in_stock")
                
                self.sku_counter += 1
                
                variant_sku = f"{self.base_sku}-{self.sku_counter}"
                size_slug = re.sub(r'[^a-zA-Z0-9]', '', size_val.lower())
                variant_id = f"PRIMARK-{color_name}-{product_id}_{color_slug}_{size_slug}_{variant_index}"
                variant_asin = f"NAM-{product_id}-{variant_index}"
                
                # Prices (AED)
                c_price_aed = float(p_price) if p_price else 0.0
                c_disc_aed = float(p_discounted_price) if p_discounted_price else 0.0
                
                # USD & IQD conversions
                usd_original = round(c_price_aed / self.usd_to_aed, 2) if c_price_aed else 0.0
                usd_discount = round(c_disc_aed / self.usd_to_aed, 2) if c_disc_aed else 0.0
                
                iqd_original = int(((usd_original * self.usd_to_iqd) / 250 + 0.999999) // 1 * 250) if usd_original else 0
                iqd_discount = int(((usd_discount * self.usd_to_iqd) / 250 + 0.999999) // 1 * 250) if usd_discount else 0

                v = create_empty_variant(
                    variant_id=variant_id,
                    asin=variant_asin,
                    task=self.task_id,
                    color=color_name,
                    color_hex='', 
                    size=size_val,
                    images=color_images.copy(),
                    base_price=c_price_aed,
                    sku=variant_sku,
                    all_colors=all_colors,
                    all_sizes=list(all_sizes),
                )

                original_color_sku = str(color_data.get("sku") or main_page_sku).strip()

                # Slugs and Names (en + ar from scraped Arabic PDP)
                v_slug = generate_slug(p_name, color_name, size_val)
                v["slug"] = {"en": v_slug, "ar": v_slug}
                v["name"] = {"en": p_name, "ar": p_name_ar}
                v["original_name"] = {"en": p_name, "ar": p_name_ar}
                v["original_short_description"] = {"en": p_desc[:200] if p_desc else "", "ar": p_desc_ar[:200] if p_desc_ar else ""}
                v["original_long_description"] = {"en": p_desc if p_desc else "", "ar": p_desc_ar if p_desc_ar else ""}

                v["brand_id"] = {"en": p_brand, "ar": p_brand_ar}
                v["link"] = color_data.get("url") or url
                v["source_data"] = {"SKU": original_color_sku}

                v["stock"] = {"quantity": stock_qty}
                if stock_status == "low_stock":
                    avail_status = "Low Stock"
                elif stock_status == "out_of_stock" or stock_qty <= 0:
                    avail_status = "Out of Stock"
                else:
                    avail_status = "In Stock"
                v["availability"] = {"status": avail_status, "quantity": stock_qty, "delivery_dates": []}

                # Price structure matching API
                v["price"] = {
                    "amazon_price_uae": c_price_aed,
                    "amazon_discount_uae": c_disc_aed if c_disc_aed else "",
                    "amazon_price": usd_original,
                    "amazon_discount": usd_discount if usd_discount else "",
                    "hanooot_price": iqd_original,
                    "hanooot_discount": iqd_discount if iqd_discount else "",
                    "commission": "",
                    "shipping_cost": "",
                    "shipping_cost_by_air": "",
                    "discount_percentage": discount_percentage,
                }
                v["price_iqd"] = {
                    "amazon_price_uae": c_price_aed,
                    "amazon_discount_uae": c_disc_aed if c_disc_aed else 0,
                    "amazon_price": usd_original,
                    "amazon_discount": usd_discount if usd_discount else "",
                    "hanooot_price": iqd_original,
                    "hanooot_discount": iqd_discount if iqd_discount else "",
                    "commission": "",
                    "shipping_cost": "",
                    "shipping_cost_by_air": "",
                    "discount_percentage": discount_percentage,
                }

                # Specs (en + ar: variant details_ar or product-level specs_ar)
                details_ar = color_data.get("details_ar") or specs_ar
                if not isinstance(details_ar, dict):
                    details_ar = specs_ar
                v["specifications"] = {
                    "en": specs_en,
                    "ar": details_ar
                }

                variants.append(v)
                variant_index += 1
        
        total_variants = sum(len(cv["sizes"]) for cv in color_variants_data)
        self.log(f"✓ Processed {len(color_variants_data)} colors × {len(all_sizes)} sizes = {total_variants} total variants")
        
        # Build final product structure
        default_color = all_colors[0] if all_colors else "Default"
        self.sku_counter += 1
        main_sku = f"{self.base_sku}-{self.sku_counter}"
        default_variant_id = variants[0]["variant_id"] if variants else f"PRIMARK-{default_color}-{product_id}_default_one size_0"
        
        final_name = p_name
        slug = generate_slug(final_name)
        
        product = create_empty_product(
            asin=f"NAM-{product_id}",
            sku=main_sku,
            brand_id=p_brand,
            link=url,
            task=self.task_id,
        ).model_dump()

        product.update({
            "source_type": "PRIMARK",
            "original_name": {
                "en": final_name,
                "ar": p_name_ar
            },
            "original_short_description": {
                "en": p_desc[:200] if p_desc else "",
                "ar": p_desc_ar[:200] if p_desc_ar else ""
            },
            "original_long_description": {
                "en": p_desc if p_desc else "",
                "ar": p_desc_ar
            },
            "name": {
                "en": final_name,
                "ar": p_name_ar
            },
            "slug": {
                "en": slug,
                "ar": slug,
            },
            "brand_id": {"en": p_brand, "ar": p_brand_ar},
            "link": url,
            "images_gallery": list(main_product_images) if main_product_images else [],
            "Product_images": list(main_product_images) if main_product_images else [],
            "description": {
                "short": {
                    "en": p_desc[:200] if p_desc else "",
                    "ar": p_desc_ar[:200] if p_desc_ar else ""
                },
                "long": {
                    "en": p_desc,
                    "ar": p_desc_ar
                }
            },
            "specifications": {"en": dict(specs_en) if isinstance(specs_en, dict) else {}, "ar": dict(specs_ar) if isinstance(specs_ar, dict) else {}},
            "attributes": {
                "colors": {"en": list(all_colors), "ar": all_colors_ar},
                "sizes": list(all_sizes),
                "size_guide_chart": sg_data.get("table", []),
                "conversion_guide_chart": sg_data.get("conversion_guide_chart", []) or [],
                "model_measurements": {
                    "size_guide_image": sg_data.get("image_url", "N/A")
                }
            },
            "variants": variants,
            "default_variant_id": default_variant_id,
            "price": variants[0].get("price") if variants else product.get("price", {}),
            "price_iqd": variants[0].get("price_iqd") if variants else product.get("price_iqd", {}),
        })

        if isinstance(product.get("price_iqd"), dict) and "hanooot_discount" in product["price_iqd"]:
            product["price_iqd"].setdefault("hanooot_discount", product["price_iqd"].get("hanooot_discount", ""))

        # Enqueue to ARQ worker immediately (pipeline parallelism)
        try:
            job_id = asyncio.run(self.enqueue_job(product_id, product, list(all_images), list(all_colors)))
            if job_id:
                self._job_ids.append(job_id)
            self.log(f"✓ Queued Product: {final_name} | {len(variants)} variants")
        except Exception as e:
            self.log(f"❌ Failed to queue product {product_id}: {e}")
            return

        # Add dummy record for counting purposes
        self.all_products.append({"data": {"name": {"en": final_name}}})

    async def enqueue_job(self, product_id, product, all_images, all_colors) -> str | None:
        """Enqueue product to ARQ worker. Returns job_id."""
        redis_pool = await create_pool(RedisSettings(host=REDIS_HOST, port=REDIS_PORT))
        try:
            job = await redis_pool.enqueue_job(
                "process_full_product",
                self.task_id,
                product_id,
                product,
                all_images,
                all_colors,
                get_environment(),
                _queue_name="arq:product",
            )
            return job.job_id if job else None
        finally:
            await redis_pool.close()


def scrape_with_retry(driver, scraper, url, max_retries=3):
    """
    Wrapper to retry scraping on the same driver instance if data is missing/unknown.
    """
    for attempt in range(max_retries):
        # Record how many products we have before starting
        initial_count = len(scraper.all_products)
        
        scraper.scrape_product_page(url)
        
        if len(scraper.all_products) > initial_count:
            # Get the product we just added
            last_product = scraper.all_products[-1]
            
            # --- QUALITY CHECK ---
            # Check if the name is valid
            product_name = last_product.get("data", {}).get("name", {}).get("en", "")
            
            if product_name and "Unknown Product" not in product_name:
                # Success! We got good data.
                if attempt > 0:
                    logger.info(f"Recovered {url} on attempt {attempt + 1}")
                return True
            else:
                # Failure: We got "Unknown Product". 
                # Remove this bad entry so we don't save garbage.
                logger.warning(f"Attempt {attempt + 1}: Extracted 'Unknown Product'. Retrying...")
                scraper.all_products.pop()
        else:
            # Failure: No product was added at all (likely a crash inside scrape_product_page)
            logger.warning(f"Attempt {attempt + 1}: No data extracted. Retrying...")

        # If we are here, we failed. Prepare for next attempt.
        if attempt < max_retries - 1:
            try:
                # FORCE REFRESH: This fixes the 'Eager' mode hydration issues
                driver.refresh()
                
                # Wait for refresh to settle (Critical!)
                time.sleep(3) 
            except Exception as e:
                logger.error(f"Error refreshing driver: {e}")
                break # If refresh fails, driver is dead. Stop.
    
    logger.error(f"Failed to scrape {url} after {max_retries} attempts.")
    return False

def process_single_product(url, task_id, sku_prefix, worker_id, usd_to_aed, usd_to_iqd, stop_event=None):
    """Returns (display_products, job_ids) tuple."""
    driver = None
    try:
        driver = setup_driver()

        worker_sku = f"{sku_prefix}-W{worker_id}"
        scraper = PrimarkScraper(driver, task_id, base_sku=worker_sku, usd_to_aed=usd_to_aed, usd_to_iqd=usd_to_iqd, stop_event=stop_event)

        logger.info(f"[{task_id}] [Worker {worker_id}] Started: {url}")

        scrape_with_retry(driver, scraper, url, max_retries=3)

        return scraper.all_products, scraper._job_ids

    except Exception as e:
        logger.error(f"[{task_id}] [Worker {worker_id}] Failed: {e}")
        return [], []
        
    finally:
        if driver:
            try:
                driver.quit()
            except:
                pass
                      
def run_scraper(
    target_url: str,
    task_id: str,
    sku: str = None,
    product_target: Optional[int] = None,
    usd_to_aed: float = 3.67,
    usd_to_iqd: float = 1500,
    stop_event: threading.Event | None = None,
):
    start_time = time.perf_counter()
    scrape_until_end = product_target is None or product_target == 0

    logger.info(f"\n{'='*60}")
    logger.info(f"[{task_id}] 🎯 PAGE-BY-PAGE SCRAPER STARTED")
    logger.info(f"[{task_id}] Target: {target_url}")
    logger.info(f"[{task_id}] Product target: {'until end' if scrape_until_end else product_target}")
    logger.info(f"{'='*60}\n")
    
    stop_event = stop_event or threading.Event()
    sku = sku or "primark-product"
    
    from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

    parsed_url = urlparse(target_url)
    query_params = parse_qs(parsed_url.query)
    # Case-insensitive: URL may have "page" or "Page"
    page_key = next((k for k in query_params if k.lower() == "page"), None)
    start_page = 1
    explicit_start_url = None
    if page_key is not None:
        try:
            start_page = int(query_params[page_key][0])
        except (ValueError, IndexError):
            start_page = 1
        explicit_start_url = target_url  # Use exact URL for first page
        query_params.pop(page_key, None)

    new_query = urlencode(query_params, doseq=True)
    base_url = urlunparse(
        (parsed_url.scheme, parsed_url.netloc, parsed_url.path,
         parsed_url.params, new_query, parsed_url.fragment)
    )
    
    MAX_PAGES = int(os.getenv("PRIMARK_MAX_PAGES", "5"))
    MAX_PARALLEL_BROWSERS = int(os.getenv("PRIMARK_PARALLEL_BROWSERS", "5"))
    
    all_results = []
    seen_urls = set()
    all_job_ids: list[str] = []  # aggregated from futures for sentinel
    page_num = start_page
    
    for page_iteration in range(MAX_PAGES):
        if stop_event.is_set():
            logger.warning(f"[{task_id}] Stop requested. Exiting.")
            break
        
        logger.info(f"\n{'='*60}")
        logger.info(f"[{task_id}] 📄 PROCESSING PAGE {page_num}")
        logger.info(f"{'='*60}")
        
        # First page: use exact URL when user provided page= so we start from that page
        if explicit_start_url and page_num == start_page:
            current_page_url = explicit_start_url
        elif "?" in base_url:
            current_page_url = f"{base_url}&page={page_num}"
        else:
            current_page_url = f"{base_url}?page={page_num}"
        
        # --- PHASE 1: Collect URLs from Current Page ---
        logger.info(f"[{task_id}] 🔍 Phase 1: Collecting product URLs from page {page_num}...")
        
        page_urls = []
        listing_driver = None
        
        try:
            listing_driver = setup_driver()
            listing_driver.get(current_page_url)
            wait = WebDriverWait(listing_driver, 30)
            
            # Handle cookie banner (only on first page)
            if page_num == start_page:
                try:
                    cookie_btn = WebDriverWait(listing_driver, 5).until(
                        EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Accept All')]"))
                    )
                    cookie_btn.click()
                    time.sleep(1)
                except:
                    pass
            
            # Wait for products to load
            try:
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "a[class*='ProductBox']")))
            except TimeoutException:
                break
            
            # Scroll to load lazy images
            for scroll_step in range(3):
                listing_driver.execute_script(f"window.scrollBy(0, {1500 * (scroll_step + 1)});")
                time.sleep(0.5)
            
            # Extract product links
            links = listing_driver.execute_script("""
                let selectors = ["a[class*='ProductBox']", "a[href*='/product/']"];
                let allLinks = [];
                for (let selector of selectors) {
                    let anchors = document.querySelectorAll(selector);
                    if (anchors.length > 0) {
                        allLinks = Array.from(anchors).map(a => a.href);
                        break;
                    }
                }
                return allLinks;
            """)
            
            # Filter and dedupe
            page_urls = [link for link in links if link and link.startswith("http")]
            new_urls = [url for url in page_urls if url not in seen_urls]
            
            if not new_urls:
                logger.info(f"[{task_id}] ⚠️  No new products on page {page_num}. Stopping pagination.")
                break

            # Check if we have enough products to reach the target (or no limit)
            current_count = len(all_results)
            if scrape_until_end:
                needed = len(new_urls)
            else:
                needed = (product_target or 0) - current_count
                if needed <= 0:
                    logger.info(f"[{task_id}] 🎯 Product target reached ({current_count}). Stopping pagination.")
                    break
                if len(new_urls) > needed:
                    logger.info(f"[{task_id}] 📉 Limiting page batch from {len(new_urls)} to {needed} to hit target of {product_target}")
                    new_urls = new_urls[:needed]
            
            # Mark as seen
            seen_urls.update(new_urls)
            page_urls = new_urls
            
            logger.info(f"[{task_id}] ✅ Found {len(page_urls)} new products on page {page_num}")
            
        except Exception as e:
            logger.error(f"[{task_id}] ❌ Error collecting URLs from page {page_num}: {e}")
            break
        finally:
            if listing_driver:
                listing_driver.quit()
        
        if not page_urls:
            logger.info(f"[{task_id}] No products to scrape on page {page_num}. Moving to next page.")
            page_num += 1
            continue
        
        # --- PHASE 2: Scrape All Products from This Page in Parallel ---
        logger.info(f"[{task_id}] 🚀 Phase 2: Scraping {len(page_urls)} products from page {page_num} with {MAX_PARALLEL_BROWSERS} workers...")
        
        page_results = []
        
        with ThreadPoolExecutor(max_workers=MAX_PARALLEL_BROWSERS) as executor:
            # Submit all tasks for this page
            future_to_url = {
                executor.submit(process_single_product, url, task_id, sku, i, usd_to_aed, usd_to_iqd, stop_event): url
                for i, url in enumerate(page_urls)
            }
            
            # Process products
            processed_count = 0
            total_items = len(page_urls)
            for future in as_completed(future_to_url):
                processed_count += 1
                if stop_event.is_set():
                    logger.warning(f"[{task_id}] Stop requested. Cancelling page {page_num}...")
                    executor.shutdown(wait=False, cancel_futures=True)
                    break
                
                url = future_to_url[future]
                try:
                    display_products, job_ids = future.result()
                    if display_products:
                        page_results.extend(display_products)
                    if job_ids:
                        all_job_ids.extend(job_ids)
                except Exception as exc:
                    logger.error(f"[{task_id}] Failed {url}: {exc}")
                
                if processed_count % 5 == 0 or processed_count == total_items:
                    logger.info(f"[{task_id}] Progress Page {page_num}: {processed_count}/{total_items} done")
        
        # Add page results to overall results
        all_results.extend(page_results)
        
        logger.info(f"[{task_id}] ✅ Page {page_num} complete: Scraped {len(page_results)} products")
        logger.info(f"[{task_id}] 📊 Total products so far: {len(all_results)}")
        
        # Check if product target reached (when not scraping until end)
        if not scrape_until_end and len(all_results) >= (product_target or 0):
            append_log(task_id, f"Product target of {product_target} reached ({len(all_results)} products). Stopping scraper.", "info")
            logger.info(f"[{task_id}] 🎯 Product target of {product_target} reached ({len(all_results)} products). Stopping scraper.")
            break
        
        # Move to next page
        page_num += 1
        
        time.sleep(1)
    
    end_time = time.perf_counter()
    duration = end_time - start_time
    minutes = int(duration // 60)
    seconds = int(duration % 60)
    
    logger.info(f"\n{'='*60}")
    logger.info(f"[{task_id}] 🎉 ALL PAGES COMPLETE!")
    logger.info(f"[{task_id}] 📄 Pages Scraped: {page_num - start_page}")
    logger.info(f"[{task_id}] 📊 Total Products: {len(all_results)}")
    logger.info(f"[{task_id}] ⏱️  Total Time: {minutes}m {seconds}s")
    logger.info(f"{'='*60}\n")

    # Enqueue sentinel job that waits for all product jobs, then signals completion
    task_status = "stopped" if stop_event.is_set() else "completed"

    async def _enqueue_sentinel():
        redis_pool = await create_pool(RedisSettings(host=REDIS_HOST, port=REDIS_PORT))
        try:
            if all_job_ids:
                await redis_pool.enqueue_job(
                    "finalize_task",
                    task_id,
                    all_job_ids,
                    task_status,
                    _queue_name="arq:product",
                )
                append_log(task_id, f"Scraped {len(all_job_ids)} products. Processing in progress.", "info")
            else:
                complete_task(task_id, task_status)
        finally:
            await redis_pool.close()

    asyncio.run(_enqueue_sentinel())

    return all_results

if __name__ == "__main__":
    run_scraper(
        target_url="https://www.primark.com/uae-en/women-clothing-tops_and_tshirts/all-sale/",
        task_id="5fad510c-7ce6-465e-890d-7e93b8b040d8",
        sku="primark-test",
        stop_event=threading.Event(),
        product_target=10
    )