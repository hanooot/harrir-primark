import asyncio
import logging
import requests
import time
import math
import json
import re
import threading
from typing import Optional, Dict, Any, List
from collections import defaultdict
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

def remove_utm_source(url: str) -> str:
    """Removes all utm_ parameters from URL."""
    if not url:
        return url
    parsed = urlparse(url)
    query_params = parse_qsl(parsed.query)
    filtered_params = [(k, v) for k, v in query_params if not k.lower().startswith("utm_")]
    new_query = urlencode(filtered_params)
    return urlunparse(parsed._replace(query=new_query))

from rnet import Client, Impersonate

RNET_CLIENT = Client(impersonate=Impersonate.Chrome128)

logger = logging.getLogger(__name__)

from app.utils.primark_utils import (
    append_log,
    patch_product_to_api,
    delete_product_from_api,
    get_product_searching_url,
)
from app.core.primark_scraper_api import (
    PrimarkScraper,
    _parse_price_number,
)

# Shipping Constants
SEA_USD_PER_CBM = 100.0
AIR_USD_PER_KG = 5.0
MIN_AIR_USD = 1.0

USD_TO_IQD = 1500.0
USD_TO_AED = 3.67

# Browser-like headers
PRIMARK_REQUEST_HEADERS = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "accept-language": "en-US,en;q=0.9",
    "cache-control": "no-cache",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
}

# Minimal projection — only fields needed for price update logic
FETCH_PROJECTION = {
    "ASIN": 1,
    "SKU": 1,
    "link": 1,
    "source_color": 1,
    "size": 1,
    "suggested_category": 1,
    "category.name.en": 1,
    "price.hanooot_price": 1,
    "price.hanooot_discount": 1,
    "availability.status": 1,
    "availability.quantity": 1,
}

# Category weight/volume data
CATEGORY_DATA = {
    "Co-ords": {
        "default": {"weight": 0.90, "cbm": 0.012},
        "subcategories": {
            "Two-piece Outfits": {"weight": 0.80, "cbm": 0.012},
            "Pant Sets": {"weight": 0.90, "cbm": 0.012},
        }
    },

    "Outerwear": {
        "default": {"weight": 0.80, "cbm": 0.010},
        "subcategories": {
            "Jackets": {"weight": 0.80, "cbm": 0.010},
            "Lightweight Jackets": {"weight": 0.65, "cbm": 0.010},
            "Coats": {"weight": 1.00, "cbm": 0.010},
            "Winter Coats": {"weight": 1.30, "cbm": 0.010},
            "Overcoats": {"weight": 1.40, "cbm": 0.010},
            "Faux Fur Coats": {"weight": 1.50, "cbm": 0.010},
            "Trench Coats": {"weight": 1.10, "cbm": 0.010},
            "Down Coats": {"weight": 1.60, "cbm": 0.010},
        }
    },

    "Hoodies & Sweatshirt": {
        "default": {"weight": 0.90, "cbm": 0.012},
        "subcategories": {
            "Hoodies": {"weight": 0.80, "cbm": 0.012},
            "Sweatshirts": {"weight": 0.70, "cbm": 0.012},
        }
    },

    "Tops": {
        "default": {"weight": 0.50, "cbm": 0.005},
        "subcategories": {
            "T-shirts": {"weight": 0.45, "cbm": 0.005},
            "Blouses": {"weight": 0.55, "cbm": 0.005},
            "Tank Tops & Camis": {"weight": 0.40, "cbm": 0.005},
            "Other Tops": {"weight": 0.55, "cbm": 0.005},
            "Customized Women Tops": {"weight": 0.60, "cbm": 0.005},
        }
    },

    "Knitwear": {
        "default": {"weight": 0.60, "cbm": 0.006},
        "subcategories": {
            "Sweaters": {"weight": 0.75, "cbm": 0.006},
            "Cardigans": {"weight": 0.70, "cbm": 0.006},
            "Lightweight Cardigans": {"weight": 0.55, "cbm": 0.006},
            "Knit Tops": {"weight": 0.55, "cbm": 0.006},
            "Sweater Vests": {"weight": 0.60, "cbm": 0.006},
            "Sweater Dresses": {"weight": 0.90, "cbm": 0.006},
            "Sweater Skirts": {"weight": 0.65, "cbm": 0.006},
            "Sweater Co-ords": {"weight": 1.00, "cbm": 0.006},
        }
    },

    "Bottoms": {
        "default": {"weight": 0.60, "cbm": 0.006},
        "subcategories": {
            "Pants": {"weight": 0.65, "cbm": 0.006},
            "Skirts": {"weight": 0.55, "cbm": 0.006},
            "Shorts": {"weight": 0.45, "cbm": 0.006},
            "Denim Shorts": {"weight": 0.65, "cbm": 0.006},
            "Sweatpants": {"weight": 0.75, "cbm": 0.006},
            "Leggings": {"weight": 0.45, "cbm": 0.006},
        }
    },

    "Jumpsuits & Bodysuits": {
        "default": {"weight": 0.70, "cbm": 0.008},
        "subcategories": {
            "Jumpsuits": {"weight": 0.75, "cbm": 0.008},
            "Body Suits": {"weight": 0.45, "cbm": 0.008},
            "Unitards": {"weight": 0.55, "cbm": 0.008},
        }
    },

    "Denim": {
        "default": {"weight": 0.80, "cbm": 0.007},
        "subcategories": {
            "Jeans": {"weight": 0.85, "cbm": 0.007},
            "Denim Shorts": {"weight": 0.65, "cbm": 0.007},
            "Denim Skirts": {"weight": 0.70, "cbm": 0.007},
            "Denim Dresses": {"weight": 0.90, "cbm": 0.007},
            "Denim Overalls & Jumpsuits": {"weight": 1.10, "cbm": 0.007},
            "Denim Jackets & Coats": {"weight": 1.20, "cbm": 0.007},
            "Denim Tops": {"weight": 0.65, "cbm": 0.007},
            "Denim Two-piece Outfits": {"weight": 1.20, "cbm": 0.007},
        }
    },

    "Wedding": {
        "default": {"weight": 1.50, "cbm": 0.050},
        "subcategories": {
            "Wedding Dresses": {"weight": 2.00, "cbm": 0.050},
            "Bridesmaid Dresses": {"weight": 1.40, "cbm": 0.050},
            "Wedding Guest Dresses": {"weight": 1.10, "cbm": 0.050},
            "Bridal Shower Dresses": {"weight": 1.00, "cbm": 0.050},
            "Mother of the Bride Dresses": {"weight": 1.60, "cbm": 0.050},
            "Engagement Dresses": {"weight": 1.25, "cbm": 0.050},
            "Ballgown Wedding Dresses": {"weight": 2.20, "cbm": 0.050},
            "Bachelorette Party Dresses": {"weight": 0.95, "cbm": 0.050},
        }
    },

    "Dresses": {
        "default": {"weight": 0.60, "cbm": 0.006},
        "subcategories": {
            "Mini Dresses": {"weight": 0.65, "cbm": 0.006},
            "Short Dresses": {"weight": 0.75, "cbm": 0.006},
            "Midi Dresses": {"weight": 0.85, "cbm": 0.006},
            "Long Dresses": {"weight": 0.95, "cbm": 0.006},
            "Maxi Dresses": {"weight": 1.00, "cbm": 0.006},
        }
    },

    "Suits": {
        "default": {"weight": 1.10, "cbm": 0.018},
        "subcategories": {
            "Blazers": {"weight": 0.80, "cbm": 0.018},
            "Lightweight Blazers": {"weight": 0.70, "cbm": 0.018},
            "Suit Pants": {"weight": 0.75, "cbm": 0.018},
            "Suit Sets": {"weight": 1.00, "cbm": 0.018},
        }
    },

    "Partywear": {
        "default": {"weight": 0.60, "cbm": 0.006},
        "subcategories": {
            "Prom Dresses": {"weight": 1.60, "cbm": 0.006},
            "Formal & Evening Dresses": {"weight": 1.30, "cbm": 0.006},
            "Cocktail Dresses": {"weight": 0.95, "cbm": 0.006},
            "Party Jumpsuits": {"weight": 0.95, "cbm": 0.006},
            "Homecoming Dresses": {"weight": 1.10, "cbm": 0.006},
            "Semi-formal Dresses": {"weight": 1.05, "cbm": 0.006},
            "Maxi Party Dresses": {"weight": 1.35, "cbm": 0.006},
            "Modest Evening Dresses": {"weight": 1.30, "cbm": 0.006},
        }
    },

    "Maternity Clothing": {
        "default": {"weight": 0.70, "cbm": 0.007},
        "subcategories": {
            "Maternity Dresses": {"weight": 0.95, "cbm": 0.007},
            "Nursing": {"weight": 0.65, "cbm": 0.007},
            "Maternity Tops": {"weight": 0.55, "cbm": 0.007},
            "Maternity Bottoms": {"weight": 0.75, "cbm": 0.007},
            "Jumpsuits & Two-pieces": {"weight": 1.05, "cbm": 0.007},
            "Maternity Denim": {"weight": 0.95, "cbm": 0.007},
            "Maternity Gowns": {"weight": 1.60, "cbm": 0.007},
            "Maternity Photoshoot": {"weight": 1.40, "cbm": 0.007},
            "Maternity Sweatshirts": {"weight": 0.85, "cbm": 0.007},
            "Maternity Sweaters": {"weight": 0.85, "cbm": 0.007},
            "Maternity Coats & Jackets": {"weight": 1.25, "cbm": 0.007},
        }
    },

    "Shoes": {
        "default": {"weight": 1.20, "cbm": 0.010},
        "subcategories": {
            "Heels": {"weight": 1.05, "cbm": 0.010},
            "Sneakers": {"weight": 1.20, "cbm": 0.010},
            "Sandals": {"weight": 0.95, "cbm": 0.010},
            "Sports Shoes": {"weight": 1.30, "cbm": 0.010},
            "Flats & Slip-Ons": {"weight": 0.95, "cbm": 0.010},
            "Slides & Flip-Flops": {"weight": 0.85, "cbm": 0.010},
            "Boots": {"weight": 1.40, "cbm": 0.010},
            "Shoe Care": {"weight": 0.50, "cbm": 0.010},
            "Bundles": {"weight": 2.20, "cbm": 0.010},
        }
    },

    "Bags": {
        "default": {"weight": 1.30, "cbm": 0.020},
        "subcategories": {
            "Shoulder Bags": {"weight": 0.70, "cbm": 0.020},
            "Backpacks": {"weight": 0.90, "cbm": 0.020},
            "Shoppers & Totes": {"weight": 0.80, "cbm": 0.020},
            "Crossbody Bags": {"weight": 0.65, "cbm": 0.020},
            "Clutches": {"weight": 0.50, "cbm": 0.020},
            "Satchels": {"weight": 0.85, "cbm": 0.020},
            "Purses": {"weight": 0.60, "cbm": 0.020},
            "Cosmetic Bags": {"weight": 0.50, "cbm": 0.020},
            "Sports Bags": {"weight": 1.00, "cbm": 0.020},
            "Duffle Bags": {"weight": 1.20, "cbm": 0.020},
            "Laptop Bags & Sleeves": {"weight": 0.50, "cbm": 0.020},
            "Belt Bags": {"weight": 0.50, "cbm": 0.020},
        }
    },

    "Accessories": {
        "default": {"weight": 0.50, "cbm": 0.004},
        "subcategories": {
            "Bracelets": {"weight": 0.20, "cbm": 0.004},
            "Necklaces": {"weight": 0.20, "cbm": 0.004},
            "Ring Sets": {"weight": 0.20, "cbm": 0.004},
            "Earrings": {"weight": 0.20, "cbm": 0.004},
            "Anklets": {"weight": 0.20, "cbm": 0.004},
            "Watches": {"weight": 0.20, "cbm": 0.004},
            "Sunglasses": {"weight": 0.20, "cbm": 0.004},
            "Hair Accessories": {"weight": 0.20, "cbm": 0.004},
            "Scarves": {"weight": 0.55, "cbm": 0.004},
            "Belts": {"weight": 0.65, "cbm": 0.004},
            "The Hat Store": {"weight": 0.60, "cbm": 0.004},
            "Sports Gear": {"weight": 0.90, "cbm": 0.004},
        }
    },

    "Nightwear": {
        "default": {"weight": 0.50, "cbm": 0.005},
        "subcategories": {
            "Pyjama Sets": {"weight": 0.75, "cbm": 0.005},
            "Pyjama Pants": {"weight": 0.55, "cbm": 0.005},
            "Nighties": {"weight": 0.55, "cbm": 0.005},
            "Tops": {"weight": 0.45, "cbm": 0.005},
            "Robes": {"weight": 0.85, "cbm": 0.005},
            "Loungewear": {"weight": 0.75, "cbm": 0.005},
            "Shorts": {"weight": 0.45, "cbm": 0.005},
            "Slips": {"weight": 0.45, "cbm": 0.005},
        }
    },
}


def get_category_data(category_name):
    """
    Retrieves weight and CBM data for a given category/subcategory.

    Args:
        category_name: String representing category or subcategory name.
                      Can be "Category" or "Category > Subcategory"

    Returns:
        Tuple of (weight, cbm)
    """
    DEFAULT = (0.5, 0.005)

    if not category_name:
        return DEFAULT

    category_name = category_name.strip()

    # Handle hierarchical format: "Category > Subcategory"
    if ">" in category_name:
        parts = [p.strip() for p in category_name.split(">")]
        category = parts[0]
        subcategory = parts[-1] if len(parts) > 1 else None

        if category in CATEGORY_DATA:
            cat_data = CATEGORY_DATA[category]

            if subcategory and "subcategories" in cat_data:
                if subcategory in cat_data["subcategories"]:
                    data = cat_data["subcategories"][subcategory]
                    return data["weight"], data["cbm"]

                # Fuzzy match on subcategory
                subcategory_lower = subcategory.lower()
                for sub_key, sub_data in cat_data["subcategories"].items():
                    if subcategory_lower in sub_key.lower() or sub_key.lower() in subcategory_lower:
                        return sub_data["weight"], sub_data["cbm"]

            return cat_data["default"]["weight"], cat_data["default"]["cbm"]

    # Exact match as main category
    if category_name in CATEGORY_DATA:
        data = CATEGORY_DATA[category_name]["default"]
        return data["weight"], data["cbm"]

    # Search as subcategory across all categories
    category_name_lower = category_name.lower()
    for cat_key, cat_data in CATEGORY_DATA.items():
        if "subcategories" in cat_data:
            if category_name in cat_data["subcategories"]:
                data = cat_data["subcategories"][category_name]
                return data["weight"], data["cbm"]

            for sub_key, sub_data in cat_data["subcategories"].items():
                if category_name_lower in sub_key.lower() or sub_key.lower() in category_name_lower:
                    return sub_data["weight"], sub_data["cbm"]

    # Fuzzy match on main category
    for cat_key, cat_data in CATEGORY_DATA.items():
        if category_name_lower in cat_key.lower() or cat_key.lower() in category_name_lower:
            return cat_data["default"]["weight"], cat_data["default"]["cbm"]

    return DEFAULT


def calculate_commission(price_usd):
    if price_usd <= 10:
        return price_usd * 0.50
    elif price_usd <= 20:
        return price_usd * 0.45
    elif price_usd <= 30:
        return price_usd * 0.40
    elif price_usd <= 40:
        return price_usd * 0.35
    elif price_usd <= 50:
        return price_usd * 0.30
    else:
        return price_usd * 0.25


def calculate_shipping_sea(cbm: float) -> float:
    """Calculate sea shipping cost with 10% buffer."""
    return round(cbm * 1.10 * SEA_USD_PER_CBM, 2)


def calculate_shipping_air(weight_kg: float) -> float:
    """Calculate air shipping cost with 10% buffer."""
    return round(max(MIN_AIR_USD, weight_kg * 1.10 * AIR_USD_PER_KG), 2)



def group_variants_by_url(variants: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Group variants by their URL (same product/color = same URL).
    Returns: {url: [variant1, variant2, ...]}
    """
    url_groups = defaultdict(list)
    for variant in variants:
        url = variant.get("link", "")
        if url:
            url = remove_utm_source(url)
            url_groups[url].append(variant)
    return dict(url_groups)


async def scrape_product_data(url: str, timeout: int = 15) -> Optional[Dict[str, Any]]:
    """
    Scrape product page once and extract all data using rnet (no fallback).
    Returns: {original_price, discounted_price, discount_percentage, size_stock, color_links}
    """
    url = remove_utm_source(url)
    if RNET_CLIENT is None:
        logger.error("RNET_CLIENT not initialized; cannot scrape.")
        return None

    try:
        # Use rnet client to fetch HTML directly without a separate sync thread
        resp = await RNET_CLIENT.get(url, headers=PRIMARK_REQUEST_HEADERS, timeout=timeout)
        status = int(resp.status) if hasattr(resp.status, "__int__") else resp.status
        if str(status).startswith("404"):
            logger.warning(f"rnet fetch failed with status 404 for {url}. Returning 404 marker.")
            return {"rnet_status_404": True}
        if not str(status).startswith("200"):
            logger.warning(f"rnet fetch failed with status {status} for {url}")
            return None

        text = await resp.text()
        if not text:
            return None

        # Delegate parsing to the static method in PrimarkScraper
        page = PrimarkScraper._parse_product_page_sync(text, url)
        if not page:
            return None

        original = _parse_price_number(page.get("original_price_text", ""))
        selling = _parse_price_number(page.get("selling_price_text", ""))
        discount_txt = str(page.get("discount_percentage_text", "")).strip()

        discount_pct = ""
        match = re.findall(r"\d+", discount_txt)
        if match:
            discount_pct = f"{match[0]}%"

        return {
            "variant_sku": page.get("variant_sku", ""),
            "original_price": original,
            "discounted_price": selling,
            "discount_percentage": discount_pct,
            "size_stock": page.get("size_stock", []),
            "color_links": page.get("color_links", []),
        }
    except Exception as e:
        logger.error(f"Error scraping {url} with rnet: {e}")
        return None


def find_stock_for_size(size_stock_list: List[Dict], size: str) -> Dict[str, Any]:
    """Find stock info for a specific size."""
    if not size_stock_list or not size:
        return {"size": size, "stock": 0, "stock_status": "out_of_stock", "sku": ""}

    size_norm = size.lower().strip()

    for stock_item in size_stock_list:
        if not isinstance(stock_item, dict):
            continue
        if str(stock_item.get("size", "")).lower().strip() == size_norm:
            return {
                "size": size,
                "stock": stock_item.get("stock", 0),
                "stock_status": stock_item.get("stock_status", "out_of_stock"),
                "sku": stock_item.get("sku", ""),
            }

    return {"size": size, "stock": 0, "stock_status": "out_of_stock", "sku": ""}


def build_price_payload(
    original_price: float,
    discounted_price: float,
    discount_percentage: str,
    category_name: str,
    usd_to_aed: float = USD_TO_AED,
    usd_to_iqd: float = USD_TO_IQD,
) -> tuple[Dict[str, Any], Dict[str, Any]]:
    """Build price/price_iqd blocks with commission and shipping."""
    base_price = original_price if original_price > 0 else discounted_price
    if base_price <= 0:
        raise ValueError("Invalid price: must be > 0")

    price_usd = round(base_price / usd_to_aed, 2)
    discount_usd = round(discounted_price / usd_to_aed, 2) if discounted_price > 0 else 0

    weight_kg, cbm = get_category_data(category_name)
    commission = calculate_commission(price_usd)
    commission_discount = calculate_commission(discount_usd)
    shipping_sea = calculate_shipping_sea(cbm)
    shipping_air = calculate_shipping_air(weight_kg)

    total_usd = price_usd + commission + shipping_air
    total_discount_usd = discount_usd + commission_discount + shipping_air

    iqd_price = int(math.ceil((total_usd * usd_to_iqd) / 250) * 250)
    iqd_discount = int(math.ceil((total_discount_usd * usd_to_iqd) / 250) * 250)

    discount_pct_value = round(((iqd_price - iqd_discount) / iqd_price) * 100, 2) if iqd_price else 0

    block = {
        "amazon_price_uae": base_price,
        "amazon_discount_uae": discounted_price if discounted_price > 0 else 0,
        "amazon_price": price_usd,
        "amazon_discount": discount_usd,
        "hanooot_price": iqd_price,
        "hanooot_discount": iqd_discount,
        "commission": commission,
        "commission_discount": commission_discount,
        "shipping_cost": shipping_sea,
        "shipping_cost_by_air": shipping_air,
        "discount_percentage": round(discount_pct_value),
    }

    # price and price_iqd have identical structure; return same block for both
    return block, block.copy()


def has_data_changed(
    current_variant: Dict[str, Any],
    new_price_block: Dict[str, Any],
    new_quantity: int,
    new_availability_status: str,
) -> bool:
    """
    Check if any meaningful data has changed.
    Compares hanooot_price/discount, availability.quantity, availability.status.
    Returns True if PATCH is needed.
    """
    current_price = current_variant.get("price", {})
    if current_price.get("hanooot_price") != new_price_block.get("hanooot_price"):
        return True
    if current_price.get("hanooot_discount") != new_price_block.get("hanooot_discount"):
        return True

    # Use availability.quantity (replaces stock.quantity)
    current_availability = current_variant.get("availability", {})
    if current_availability.get("quantity") != new_quantity:
        return True
    if current_availability.get("status") != new_availability_status:
        return True

    return False


def extract_category_name(variant: Dict[str, Any]) -> str:
    """Extract category name from variant data."""
    cat_data = variant.get("suggested_category")
    if cat_data:
        return str(cat_data)

    cat_obj = variant.get("category")
    if isinstance(cat_obj, list) and cat_obj:
        return ">".join(
            cat.get("name", {}).get("en", "")
            for cat in cat_obj
            if cat.get("name", {}).get("en")
        )
    return ""


def get_variant_color_label(variant: Dict[str, Any]) -> str:
    """Extract display color from source_color field (replaces 'color')."""
    source_color = variant.get("source_color")
    if isinstance(source_color, dict):
        return source_color.get("en") or source_color.get("ar") or "Unknown"
    if isinstance(source_color, str):
        return source_color
    return "Unknown"


async def apply_scraped_to_variants(
    url: str,
    variants: List[Dict[str, Any]],
    scraped_data: Dict[str, Any],
    task_id: str,
    start_index: int,
    usd_to_aed: float = USD_TO_AED,
    usd_to_iqd: float = USD_TO_IQD,
) -> tuple[int, int]:
    """
    Apply already-scraped data to variants and send PATCH requests.
    Returns (updated_count, skipped_count).
    """
    if not variants or not scraped_data:
        return 0, 0

    if scraped_data.get("rnet_status_404"):
        deleted_count = 0
        for idx, variant in enumerate(variants, start=start_index):
            asin = variant.get("ASIN")
            if not asin:
                continue

            def _delete_sync():
                return delete_product_from_api(asin)

            try:
                success, error = await asyncio.to_thread(_delete_sync)
            except Exception as e:
                success, error = False, str(e)

            if success:
                deleted_count += 1
                logger.info(f"[{task_id}] DELETED {asin} (404)")
                append_log(task_id, f"[{idx}] 🗑️ Deleted {asin} due to 404", "info")
            else:
                logger.warning(f"[{task_id}] DELETE FAILED {asin}: {error}")
                append_log(task_id, f"[{idx}] ❌ DELETE failed for {asin}: {error}", "error")
        logger.info(f"[{task_id}] Batch complete: 0 updated, 0 skipped, {deleted_count} deleted (404)")
        return deleted_count, 0

    # --- Handle multi-color groups: split by color and scrape each color's correct URL ---
    color_links = scraped_data.get("color_links") or []
    if color_links:
        color_url_map = {}
        for cl in color_links:
            cn = (cl.get("color") or "").strip().lower()
            cu = remove_utm_source(cl.get("url") or "")
            if cn and cu:
                color_url_map[cn] = cu

        if color_url_map:
            color_groups = defaultdict(list)
            for v in variants:
                vlabel = get_variant_color_label(v).strip().lower()
                color_groups[vlabel].append(v)

            base_url = remove_utm_source(url)
            has_different_urls = any(
                color_url_map.get(c, base_url) != base_url
                for c in color_groups
            )

            if has_different_urls:
                scraped_cache = {base_url: scraped_data}
                total_updated = 0
                total_skipped = 0
                idx = start_index

                for clabel, cvariants in color_groups.items():
                    correct_url = color_url_map.get(clabel, url)
                    clean_url = remove_utm_source(correct_url)

                    if clean_url not in scraped_cache:
                        append_log(task_id, f"[{idx}] Scraping color URL: {correct_url}", "debug")
                        cs = await scrape_product_data(correct_url)
                        scraped_cache[clean_url] = cs if (cs and not cs.get("rnet_status_404")) else None

                    cdata = scraped_cache.get(clean_url) or scraped_data
                    # Strip color_links to prevent re-splitting on recursive call
                    cdata_single = {k: v for k, v in cdata.items() if k != "color_links"}

                    u, s = await apply_scraped_to_variants(
                        correct_url, cvariants, cdata_single,
                        task_id, idx, usd_to_aed=usd_to_aed, usd_to_iqd=usd_to_iqd,
                    )
                    total_updated += u
                    total_skipped += s
                    idx += len(cvariants)

                logger.info(f"[{task_id}] Multi-color batch: {total_updated} updated, {total_skipped} skipped")
                append_log(task_id, f"[{start_index}] Multi-color batch: {total_updated} updated, {total_skipped} skipped", "info")
                return total_updated, total_skipped

    sample_variant = variants[0]
    variant_color = get_variant_color_label(sample_variant)
    original_price = scraped_data.get("original_price", 0)
    discounted_price = scraped_data.get("discounted_price", 0)
    discount_percentage = scraped_data.get("discount_percentage", 0)
    size_stock_list = scraped_data.get("size_stock") or []

    if original_price <= 0 and discounted_price <= 0:
        append_log(task_id, f"[{start_index}] ❌ Invalid price for {url}", "warning")
        return 0, 0

    category_name = extract_category_name(sample_variant)
    try:
        price_block, price_iqd_block = build_price_payload(
            original_price,
            discounted_price,
            str(discount_percentage) if discount_percentage is not None else "0",
            category_name,
            usd_to_aed=usd_to_aed,
            usd_to_iqd=usd_to_iqd,
        )
    except ValueError as e:
        append_log(task_id, f"[{start_index}] ❌ Price calculation failed: {e}", "error")
        return 0, 0

    updated_count = 0
    skipped_count = 0

    for idx, variant in enumerate(variants, start=start_index):
        asin = variant.get("ASIN")
        sku = variant.get("SKU")
        variant_size = variant.get("size", "") or ""
        if not asin:
            continue

        # Determine stock from scraped page
        stock_info = find_stock_for_size(size_stock_list, variant_size)
        stock_quantity = stock_info["stock"]
        stock_status = stock_info["stock_status"]

        if stock_status == "low_stock":
            availability_status = "Low Stock"
        elif stock_status == "out_of_stock" or stock_quantity <= 0:
            availability_status = "Out of Stock"
        else:
            availability_status = "In Stock"

        # Skip if nothing changed (uses availability.quantity now)
        if not has_data_changed(variant, price_block, stock_quantity, availability_status):
            skipped_count += 1
            logger.info(f"[{task_id}] SKIPPED {asin}")
            append_log(task_id, f"[{idx}] ⏭️  {asin} | {variant_color}/{variant_size} | No changes, skipped", "debug")
            continue

        patch_payload = {
            "data": {
                "price": price_block,
                "price_iqd": price_iqd_block,
                "stock": {
                    "quantity": stock_quantity
                },
                "availability": {
                    "status": availability_status,
                    "quantity": stock_quantity,
                },
                "SKU": sku,
            }
        }

        # with open("patch_payload.json", "a", encoding="utf-8") as f:
        #     f.write(json.dumps(patch_payload, indent=4, ensure_ascii=False))
        #     f.write("\n")

        def _patch_sync():
            # return True, None
            return patch_product_to_api(asin, patch_payload)

        try:
            success, error = await asyncio.to_thread(_patch_sync)
        except Exception as e:
            success, error = False, str(e)

        if success:
            updated_count += 1
            logger.info(f"[{task_id}] UPDATED {asin}")
            append_log(task_id, f"[{idx}] ✅ {asin} | {variant_color}/{variant_size} | Stock: {stock_quantity} ({availability_status})", "info")
        else:
            logger.warning(f"[{task_id}] PATCH FAILED {asin}: {error}")
            append_log(task_id, f"[{idx}] ❌ PATCH failed for {asin}: {error}", "error")

    logger.info(f"[{task_id}] Batch complete: {updated_count} updated, {skipped_count} skipped")
    append_log(task_id, f"[{start_index}] Batch complete: {updated_count} updated, {skipped_count} skipped (no changes)", "info")
    return updated_count, skipped_count


# ---------------------------------------------------------------------------
# Legacy helper kept for backward-compat — delegates to apply_scraped_to_variants
# after an inline scrape. Prefer the pipeline for production use.
# ---------------------------------------------------------------------------
async def update_variant_batch(
    url: str,
    variants: List[Dict[str, Any]],
    task_id: str,
    start_index: int,
    usd_to_aed: float = USD_TO_AED,
    usd_to_iqd: float = USD_TO_IQD,
) -> tuple[int, int]:
    """
    Update all variants sharing the same URL. Scrapes page once, then patches.
    Returns: (updated_count, skipped_count)
    """
    if not variants:
        return 0, 0

    variant_color = get_variant_color_label(variants[0])
    append_log(task_id, f"[{start_index}] Scraping {url} ({variant_color}) for {len(variants)} variants...", "debug")

    scraped_data = await scrape_product_data(url)
    if not scraped_data:
        append_log(task_id, f"[{start_index}] ❌ Failed to scrape {url}", "error")
        return 0, 0

    if scraped_data.get("rnet_status_404"):
        pass
    elif scraped_data.get("original_price", 0) <= 0 and scraped_data.get("discounted_price", 0) <= 0:
        append_log(task_id, f"[{start_index}] ❌ Invalid price for {url}", "warning")
        return 0, 0

    return await apply_scraped_to_variants(
        url, variants, scraped_data, task_id, start_index,
        usd_to_aed=usd_to_aed, usd_to_iqd=usd_to_iqd,
    )


# ---------------------------------------------------------------------------
# Fetch helpers
# ---------------------------------------------------------------------------

def _build_fetch_payload(filter_criteria: dict, last_seen_asin: Optional[str], batch_size: int) -> dict:
    payload = {
        "paginationMode": "cursor",
        "filters": [filter_criteria],
        "limit": batch_size,
        "projection": FETCH_PROJECTION,
        "returnFilters": False,
        # "includeDeleted": True  
    }
    if last_seen_asin:
        payload["lastSeenASIN"] = last_seen_asin
    return payload


async def fetch_all_variants(
    task_id: str,
    filter_task_id: Optional[str] = None,
    filter_sku: Optional[str] = None,
    batch_size: int = 100,
) -> List[Dict[str, Any]]:
    """Fetch ALL variants from API in batches."""
    search_url = get_product_searching_url()
    logger.info(f"[{task_id}] Fetching variants from API (configured: {bool(search_url)})")
    if not search_url:
        append_log(task_id, "ERROR: Product search URL not configured", "critical")
        return []

    filter_criteria = _build_filter_criteria(filter_task_id, filter_sku)
    all_variants: List[Dict[str, Any]] = []
    last_seen_asin = None
    append_log(task_id, "Fetching all variants from API...", "info")

    def _fetch_batch_sync(last_seen_asin: Optional[str]):
        payload = _build_fetch_payload(filter_criteria, last_seen_asin, batch_size)
        response = requests.post(search_url, json=payload, timeout=60)
        response.raise_for_status()
        data = response.json().get("data", {})
        return data.get("records", []), data.get("lastSeenASIN"), data.get("hasMore", False)

    while True:
        try:
            variants, retrieved_last_seen_asin, has_more = await asyncio.to_thread(_fetch_batch_sync, last_seen_asin)
        except Exception as e:
            logger.warning(f"[{task_id}] API request failed: {e}")
            append_log(task_id, f"API request failed: {e}", "warning")
            break

        if not variants:
            break

        all_variants.extend(variants)
        batch_asins = ", ".join(v.get("ASIN", "N/A") for v in variants)
        logger.info(f"[{task_id}] Fetched batch: {len(variants)} variants (total: {len(all_variants)}) | ASINs: {batch_asins}")
        append_log(task_id, f"Fetched {len(variants)} variants (total: {len(all_variants)}) | ASINs: {batch_asins}", "debug")

        if not has_more or not retrieved_last_seen_asin:
            break

        last_seen_asin = retrieved_last_seen_asin

    append_log(task_id, f"✅ Fetched {len(all_variants)} total variants", "info")
    return all_variants


def _build_filter_criteria(filter_task_id: Optional[str], filter_sku: Optional[str]) -> dict:
    if filter_sku:
        return {"SKU": filter_sku}
    if filter_task_id:
        return {"task": filter_task_id, "source_type": "PRIMARK"}
    return {"source_type": "PRIMARK"}


# ---------------------------------------------------------------------------
# Pipeline: Fetcher → Scraper → Patcher
# ---------------------------------------------------------------------------

async def _fetcher_into_queue(
    scrape_queue: asyncio.Queue,
    task_id: str,
    filter_task_id: Optional[str],
    filter_sku: Optional[str],
    batch_size: int,
    concurrency: int,
    stop_event: Optional[threading.Event] = None,
    total_records_ref: Optional[List[Optional[int]]] = None,
) -> int:
    """Fetch variants in batches, group by URL, put (url, variants, start_index) into scrape_queue."""
    search_url = get_product_searching_url()
    if not search_url:
        append_log(task_id, "ERROR: Product search URL not configured", "critical")
        for _ in range(concurrency):
            await scrape_queue.put(None)
        return 0

    if total_records_ref is None:
        total_records_ref = [None]

    filter_criteria = _build_filter_criteria(filter_task_id, filter_sku)

    def _fetch_batch_sync(last_seen_asin: Optional[str]):
        payload = _build_fetch_payload(filter_criteria, last_seen_asin, batch_size)
        response = requests.post(search_url, json=payload, timeout=60)
        response.raise_for_status()
        data = response.json().get("data", {})
        return data.get("records", []), data.get("total_records"), data.get("lastSeenASIN"), data.get("hasMore", False)

    MAX_FETCH_RETRIES = 3
    FETCH_RETRY_DELAY = 5

    total_enqueued = 0
    last_seen_asin = None
    next_index = 1
    consecutive_failures = 0
    append_log(task_id, "Fetcher: starting (batches → scrape queue)", "info")

    while True:
        if stop_event and stop_event.is_set():
            break

        variants = None
        total_records = None
        retrieved_last_seen_asin = None
        has_more = False

        for attempt in range(1, MAX_FETCH_RETRIES + 1):
            try:
                variants, total_records, retrieved_last_seen_asin, has_more = await asyncio.to_thread(_fetch_batch_sync, last_seen_asin)
                consecutive_failures = 0
                break
            except Exception as e:
                logger.warning(f"[{task_id}] Fetcher: attempt {attempt}/{MAX_FETCH_RETRIES} failed: {e}")
                if attempt < MAX_FETCH_RETRIES:
                    await asyncio.sleep(FETCH_RETRY_DELAY * attempt)

        if variants is None:
            consecutive_failures += 1
            if consecutive_failures >= 3:
                logger.error(f"[{task_id}] Fetcher: 3 consecutive batch failures, stopping")
                append_log(task_id, f"Fetcher: stopping after 3 consecutive batch failures", "error")
                break
            continue

        if not variants:
            break

        if total_records is not None and total_records_ref[0] is None:
            total_records_ref[0] = total_records
            append_log(task_id, f"Fetcher: total_records from API = {total_records}", "info")
            logger.info(f"[{task_id}] Fetcher: total_records from API = {total_records}")

        url_groups = group_variants_by_url(variants)
        for url, group_variants in url_groups.items():
            await scrape_queue.put((url, group_variants, next_index))
            next_index += len(group_variants)
            total_enqueued += len(group_variants)

        total_records_val = total_records_ref[0]
        progress_str = f" ({total_enqueued}/{total_records_val})" if total_records_val is not None else ""
        logger.info(f"[{task_id}] Fetcher: {len(variants)} variants, {len(url_groups)} URL groups (total: {total_enqueued}{progress_str})")
        append_log(task_id, f"Fetcher: enqueued {total_enqueued} variants{progress_str}", "debug")

        if not has_more or not retrieved_last_seen_asin:
            break

        last_seen_asin = retrieved_last_seen_asin

    for _ in range(concurrency):
        await scrape_queue.put(None)

    total_records_val = total_records_ref[0]
    progress_str = f" ({total_enqueued}/{total_records_val})" if total_records_val is not None else ""
    append_log(task_id, f"Fetcher: done. Total variants enqueued: {total_enqueued}{progress_str}", "info")
    logger.info(f"[{task_id}] Fetcher: done. Total variants enqueued: {total_enqueued}{progress_str}")
    return total_enqueued


async def _scraper_worker(
    scrape_queue: asyncio.Queue,
    patch_queue: asyncio.Queue,
    task_id: str,
    concurrency: int,
    scrape_failed_ref: Optional[List[int]] = None,
) -> None:
    """Consume (url, variants, start_index) from scrape_queue; scrape; put result into patch_queue."""
    MAX_SCRAPE_RETRIES = 2
    while True:
        item = await scrape_queue.get()
        if item is None:
            await patch_queue.put(None)
            return
        try:
            url, variants, start_index = item
            scraped_data = None
            for attempt in range(1, MAX_SCRAPE_RETRIES + 1):
                scraped_data = await scrape_product_data(url)
                if scraped_data:
                    break
                if attempt < MAX_SCRAPE_RETRIES:
                    await asyncio.sleep(2 * attempt)
            if scraped_data:
                await patch_queue.put((url, variants, scraped_data, start_index))
            else:
                if scrape_failed_ref is not None:
                    scrape_failed_ref[0] += len(variants)
                logger.warning(f"[{task_id}] Scraper: failed to scrape {url} after {MAX_SCRAPE_RETRIES} attempts, {len(variants)} variants dropped")
        except Exception as e:
            logger.exception(f"[{task_id}] Scraper worker error: {e}")
        finally:
            scrape_queue.task_done()


async def _patcher_worker(
    patch_queue: asyncio.Queue,
    task_id: str,
    counts: List[int],
    usd_to_aed: float,
    usd_to_iqd: float,
    total_records_ref: Optional[List[Optional[int]]] = None,
    variants_completed_ref: Optional[List[int]] = None,
    on_progress_cb: Optional[Any] = None,
) -> None:
    """Consume from patch_queue; call apply_scraped_to_variants; update counts and progress."""
    if variants_completed_ref is None:
        variants_completed_ref = [0]
    while True:
        item = await patch_queue.get()
        if item is None:
            patch_queue.task_done()
            return
        try:
            url, variants, scraped_data, start_index = item
            updated, skipped = await apply_scraped_to_variants(
                url, variants, scraped_data, task_id, start_index,
                usd_to_aed=usd_to_aed, usd_to_iqd=usd_to_iqd,
            )
            counts[0] += updated
            counts[1] += skipped
            variants_completed_ref[0] += len(variants)
            completed = variants_completed_ref[0]
            total_records_val = total_records_ref[0] if total_records_ref else None
            if total_records_val is not None:
                progress_str = f" {completed}/{total_records_val} completed"
                logger.info(f"[{task_id}] Patcher:{progress_str} (updated: {counts[0]}, skipped: {counts[1]})")
                append_log(task_id, progress_str.strip(), "debug")
            if on_progress_cb:
                progress = {
                    "total_records": total_records_val,
                    "updated": counts[0],
                    "skipped": counts[1],
                    "variants_completed": completed,
                }
                if asyncio.iscoroutinefunction(on_progress_cb):
                    await on_progress_cb(progress)
                else:
                    on_progress_cb(progress)
        except Exception as e:
            logger.exception(f"[{task_id}] Patcher worker error: {e}")
        finally:
            patch_queue.task_done()


async def update_primark_prices_pipeline(
    task_id: str = "primark-batch-update-pricing",
    batch_size: int = 100,
    filter_task_id: Optional[str] = None,
    filter_sku: Optional[str] = None,
    concurrency: int = 5,
    stop_event: Optional[threading.Event] = None,
    usd_to_aed: float = USD_TO_AED,
    usd_to_iqd: float = USD_TO_IQD,
    queue_max_size: int = 500,
    on_progress: Optional[Any] = None,
) -> Optional[Dict[str, Any]]:
    """
    Run price update as a pipeline: Fetcher | Scraper | Patcher running in parallel.
    """
    logger.info(f"[{task_id}] Starting pipeline (Fetcher → Scraper → Patcher). Concurrency: {concurrency}")
    append_log(
        task_id,
        f"Starting Primark price update (PIPELINE). "
        f"Filter task: {filter_task_id}, SKU: {filter_sku}, Concurrency: {concurrency}",
        "info",
    )

    scrape_queue: asyncio.Queue = asyncio.Queue(maxsize=queue_max_size)
    patch_queue: asyncio.Queue = asyncio.Queue(maxsize=queue_max_size)
    counts: List[int] = [0, 0]  # [total_updated, total_skipped]
    total_records_ref: List[Optional[int]] = [None]
    variants_completed_ref: List[int] = [0]
    scrape_failed_ref: List[int] = [0]

    fetcher_task = asyncio.create_task(
        _fetcher_into_queue(
            scrape_queue, task_id, filter_task_id, filter_sku, batch_size, concurrency,
            stop_event, total_records_ref=total_records_ref,
        )
    )
    scraper_tasks = [
        asyncio.create_task(_scraper_worker(scrape_queue, patch_queue, task_id, concurrency, scrape_failed_ref))
        for _ in range(concurrency)
    ]
    patcher_tasks = [
        asyncio.create_task(_patcher_worker(
            patch_queue, task_id, counts, usd_to_aed, usd_to_iqd,
            total_records_ref=total_records_ref,
            variants_completed_ref=variants_completed_ref,
            on_progress_cb=on_progress,
        ))
        for _ in range(concurrency)
    ]

    await fetcher_task
    await asyncio.gather(*scraper_tasks)
    for _ in range(concurrency):
        await patch_queue.put(None)
    await asyncio.gather(*patcher_tasks)

    total_updated, total_skipped = counts[0], counts[1]
    completed = variants_completed_ref[0]
    total_records_val = total_records_ref[0]
    scrape_failures = scrape_failed_ref[0]
    progress_str = f" {completed}/{total_records_val}" if total_records_val is not None else f" {completed} variants"

    append_log(task_id, f"✅ Pipeline complete.{progress_str} Updated: {total_updated}, Skipped: {total_skipped}, Scrape failed: {scrape_failures}", "info")
    logger.info(f"[{task_id}] Pipeline complete.{progress_str} Updated: {total_updated}, Skipped: {total_skipped}, Scrape failed: {scrape_failures}")

    return {
        "total_records": total_records_val,
        "updated": total_updated,
        "skipped": total_skipped,
        "variants_completed": completed,
        "scrape_failed": scrape_failures,
    }


async def update_primark_prices(
    task_id: str = "primark-batch-update-pricing",
    batch_size: int = 100,
    filter_task_id: Optional[str] = None,
    filter_sku: Optional[str] = None,
    concurrency: int = 5,
    stop_event: Optional[threading.Event] = None,
    usd_to_aed: float = USD_TO_AED,
    usd_to_iqd: float = USD_TO_IQD,
    on_progress: Optional[Any] = None,
) -> Optional[Dict[str, Any]]:
    """Main entry: run Fetcher → Scraper → Patcher pipeline."""
    return await update_primark_prices_pipeline(
        task_id=task_id,
        batch_size=batch_size,
        filter_task_id=filter_task_id,
        filter_sku=filter_sku,
        concurrency=concurrency,
        stop_event=stop_event,
        usd_to_aed=usd_to_aed,
        usd_to_iqd=usd_to_iqd,
        on_progress=on_progress,
    )


def run_update_primark_prices(
    task_id: str = "primark-batch-update-pricing",
    batch_size: int = 100,
    filter_task_id: Optional[str] = None,
    filter_sku: Optional[str] = None,
    concurrency: int = 5,
    stop_event: Optional[threading.Event] = None,
    usd_to_aed: float = USD_TO_AED,
    usd_to_iqd: float = USD_TO_IQD,
) -> None:
    """Sync wrapper to run update_primark_prices from a sync context."""
    asyncio.run(update_primark_prices(
        task_id=task_id,
        batch_size=batch_size,
        filter_task_id=filter_task_id,
        filter_sku=filter_sku,
        concurrency=concurrency,
        stop_event=stop_event,
        usd_to_aed=usd_to_aed,
        usd_to_iqd=usd_to_iqd,
    ))


if __name__ == "__main__":
    run_update_primark_prices(
        task_id="primark-batch-update-pricing",
        batch_size=100,
        filter_task_id="c914ba22-c6aa-4f20-af90-6044b7113bee",
        concurrency=8,
    )