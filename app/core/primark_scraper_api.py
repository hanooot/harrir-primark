import asyncio
import json
import logging
import os
import threading
import time
import requests
from typing import Optional
from urllib.parse import quote
from datetime import datetime, timezone

from arq import create_pool
from arq.connections import RedisSettings
from dotenv import load_dotenv

from app.config import get_environment
from app.utils.primark_utils import append_log, complete_task

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

# ============================================================
#  ENQUEUE HELPER  (ARQ integration)
# ============================================================

async def _build_and_enqueue_product(
    redis_pool,
    task_id: str,
    style_code: str,
    raw_product_data: dict,
    environment: str,
) -> Optional[str]:
    """Enqueue a single product document to the ARQ product worker."""
    all_images = raw_product_data.get("images_gallery", [])
    all_colors = raw_product_data.get("attributes", {}).get("colors", {}).get("en", [])

    logger.info("[%s] Enqueuing product %s (%d colors, %d images)",
                task_id, style_code, len(all_colors), len(all_images))
    try:
        job = await redis_pool.enqueue_job(
            "process_full_product",
            task_id,
            style_code,
            raw_product_data,
            all_images,
            all_colors,
            environment,
            _queue_name="arq:product",
        )
        return job.job_id if job else None
    except Exception as e:
        logger.error("[%s] Failed to enqueue product %s: %s", task_id, style_code, e)
        append_log(task_id, f"Failed to enqueue {style_code}: {e}", "error")
        return None

# ============================================================
#  CONFIG (same as before)
# ============================================================

BASE_URL = "https://www.primark.ae/graphql"

SEARCH_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:149.0) Gecko/20100101 Firefox/149.0",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Content-Type": "application/json",
    "Referer": "https://www.primark.ae/",
    "Magento-Website-Code": "are",
    "Magento-Store-View-Code": "are_en",
    "Magento-Store-Code": "united_arab_emirates_store",
    "Magento-Customer-Group": "0",
    "Store": "are_en",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "Connection": "keep-alive",
}

REFINE_HEADERS = {
    **SEARCH_HEADERS,
    "Magento-Environment-Id": "63fbfe28-e2fc-4d7a-bdff-70be847f56e8",
    "x-api-key": "63f82b54752045df99d62f56a33fd1c6",
}
REFINE_HEADERS.pop("Store", None)

GET_PRODUCT_SEARCH_DETAILS_QUERY = """
query getProductSearchDetails( $filter: [SearchClauseInput!] $phrase: String! $sort: [ProductSearchSortInput!] $pageSize: Int! ) {
  productSearch( phrase: $phrase filter: $filter sort: $sort page_size: $pageSize ) {
    total_count
    page_info { current_page page_size total_pages }
    items {
      productView {
        sku id name description urlKey inStock metaTitle metaDescription metaKeyword attributes { name label roles value }
        ... on SimpleProductView { price { final { amount { value currency } } regular { amount { value currency } } roles } }
        ... on ComplexProductView {
          variants {
            variants {
              selections
              product {
                id name sku inStock ... on SimpleProductView { price { final { amount { currency value } } regular { amount { currency value } } } }
                attributes { name label roles value }
              }
            }
          }
          options { id title required multi values { id title ... on ProductViewOptionValueProduct { title quantity isDefault } ... on ProductViewOptionValueSwatch { id title type value } } }
          priceRange { maximum { final { amount { value currency } } regular { amount { value currency } } roles } minimum { final { amount { value currency } } regular { amount { value currency } } roles } }
        }
      }
    }
  }
}
"""

REFINE_QUERY = """query REFINE_PRODUCT_QUERY( $optionIds: [String!]! $sku: String! ) { refineProduct( optionIds: $optionIds sku: $sku ) { ...ProductFragment } products(skus: [$sku]) { ...ProductFragment } } fragment ProductFragment on ProductView { __typename id sku name shortDescription metaDescription metaKeyword metaTitle description inStock addToCartAllowed url urlKey externalId images(roles: []) { url label roles } attributes(roles: []) { name label value roles } ... on SimpleProductView { price { roles regular { amount { value currency } } final { amount { value currency } } } } ... on ComplexProductView { options { ...ProductOptionFragment } priceRange { maximum { final { amount { value currency } } regular { amount { value currency } } roles } minimum { final { amount { value currency } } regular { amount { value currency } } roles } } } } fragment ProductOptionFragment on ProductViewOption { id title required multi values { id title inStock __typename ... on ProductViewOptionValueProduct { title quantity isDefault __typename product { sku shortDescription metaDescription metaKeyword metaTitle name price { final { amount { value currency } } regular { amount { value currency } } roles } } } ... on ProductViewOptionValueSwatch { id title type value inStock } } }"""


# ============================================================
#  CATEGORY LISTING API CONFIG (Algolia-backed)
# ============================================================
# Set CATEGORY_SLUG to crawl a whole category, or leave MANUAL_URL_KEYS
# populated to scrape specific products. If CATEGORY_SLUG is set, URL_KEYS
# will be built automatically by calling fetch_category_url_keys().
CATEGORY_SLUG = "shop-women/clothing/jeans"  # e.g. "shop-women/clothing/jeans"
HITS_PER_PAGE = 36                            # Max per page (Algolia default: 36)
ALGOLIA_INDEX  = "prm_prod_ae_product_list"
ALGOLIA_APP_ID = "K2BCZNJ60L"
ALGOLIA_API_KEY = "420f88b5ee8edc3029ce356faa3f1ded"

MANUAL_URL_KEYS = [
    # "buy-cotton-denim-high-waisted-tapered-jeans-light-blue",
]  # Override: if populated, skips category crawl

LISTING_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:149.0) Gecko/20100101 Firefox/149.0",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Content-Type": "application/json",
    "Referer": "https://www.primark.ae/en/shop-women/clothing/jeans",
    "Magento-Website-Code": "are",
    "Magento-Store-Code": "united_arab_emirates_store",
    "Magento-Store-View-Code": "are_en",
    "Magento-Environment-Id": "63fbfe28-e2fc-4d7a-bdff-70be847f56e8",
    "x-api-key": "63f82b54752045df99d62f56a33fd1c6",
    "X-Algolia-Application-Id": ALGOLIA_APP_ID,
    "X-Algolia-API-Key": ALGOLIA_API_KEY,
    "mesh_market": "ae-da",
    "mesh_locale": "en",
    "mesh_context": "live",
    "path": "/1/indexes/*/queries",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "Connection": "keep-alive",
}

GET_CATEGORY_LISTING_QUERY = """
query ($indexName1: String!, $params1: String!, $urlKey1: String, $lang1: String) {
  getProductListingWithCategory(
    input: {
      requests: [{
        indexName: $indexName1,
        params: $params1,
        urlKey: $urlKey1,
        lang: $lang1
      }]
    }
  ) {
    results {
      hits {
        sku
        in_stock
        final_price { en }
        original_price { en }
        title { en }
        url { en }
        attr_color { en }
        media { url image_type }
        gtm {
          gtm_main_sku
          gtm_product_style_code
          gtm_name
          gtm_price
          gtm_category
        }
        article_swatches {
          en { article_sku_code swatch_image swatch_type }
        }
      }
      page
      hitsPerPage
      nbHits
    }
  }
}
"""

# Load color assets mapping — resolve relative to this file so it works
# regardless of the working directory the process starts in.
COLOR_ASSETS = {}
_COLOR_ASSETS_CANDIDATES = [
    os.path.join(os.path.dirname(__file__), "..", "..", "color_assets.json"),  # workspace root
    os.path.join(os.path.dirname(__file__), "color_assets.json"),              # same dir
    "color_assets.json",                                                        # CWD fallback
]
for _ca_path in _COLOR_ASSETS_CANDIDATES:
    _ca_path = os.path.normpath(_ca_path)
    if os.path.exists(_ca_path):
        try:
            with open(_ca_path, "r", encoding="utf-8") as f:
                COLOR_ASSETS = json.load(f)
            logger.info("Loaded color_assets.json from %s", _ca_path)
        except Exception as _e:
            logger.warning("Failed to load color_assets.json from %s: %s", _ca_path, _e)
        break
else:
    logger.warning("color_assets.json not found in any search path. Using empty mapping.")

# ============================================================
#  API CALLS
# ============================================================

def build_algolia_params(page=0, hits_per_page=36):
    """Build the Algolia params string for the category listing query."""
    attributes = [
        'url.en', 'attr_product_collection.en', 'attr_collection_1.en',
        'media', 'title.en', 'article_swatches.en', 'promotions.en',
        'discount.en', 'original_price.en', 'final_price.en',
        'swatches.en', 'gtm', 'sku', 'attr_product_brand.en',
        'attr_color.en', 'member_price', 'in_stock',
    ]
    # Compact single-line JSON array (newlines in params string can cause issues)
    attr_json = '[' + ','.join(f'"{a}"' for a in attributes) + ']'
    response_json = '["hits","nbHits","page","hitsPerPage","params"]'
    params = (
        f"attributesToRetrieve={attr_json}"
        f"&responseFields={response_json}"
        f"&clickAnalytics=true"
        f"&facets=[\"*\"]"
        f"&filters=(stock > 0)"
        f"&hitsPerPage={hits_per_page}"
        f"&optionalFilters=null"
        f"&page={page}"
    )
    return params


def fetch_category_url_keys(category_slug, hits_per_page=36, max_pages=None):
    """
    Fetch all product URL keys from a Primark category page using
    the Algolia-backed getProductListingWithCategory query.
    Returns a deduplicated list of url_key strings.
    """
    all_url_keys = []
    seen_style_codes = set()
    page = 0

    while True:
        params_str = build_algolia_params(page=page, hits_per_page=hits_per_page)
        variables = {
            "indexName1": ALGOLIA_INDEX,
            "params1": params_str,
            "urlKey1": category_slug,
            "lang1": "en",
        }
        print(f"  [Listing] Fetching page {page} of '{category_slug}'...")
        try:
            data = gql_get(GET_CATEGORY_LISTING_QUERY, variables, LISTING_HEADERS)
            results = data.get("data", {}).get("getProductListingWithCategory", {}).get("results", [])
            if not results:
                print("  [Listing] No results found.")
                break

            result = results[0]
            hits = result.get("hits", [])
            nb_hits = result.get("nbHits", 0)
            total_pages = -(-nb_hits // hits_per_page)  # ceiling division

            print(f"  [Listing] Page {page}/{total_pages-1} - {len(hits)} hits (total: {nb_hits})")

            for hit in hits:
                url_obj = hit.get("url", {})
                url_en = url_obj.get("en", "") if isinstance(url_obj, dict) else ""
                # url is like "/en/<url_key>" e.g. "/en/buy-cotton-denim-..."
                url_key = url_en.rstrip("/").split("/")[-1] if url_en else ""

                # Deduplicate by style_code if available
                style_code = (hit.get("gtm") or {}).get("gtm_product_style_code", "")
                dedup_key = style_code or url_key
                if dedup_key and dedup_key not in seen_style_codes:
                    seen_style_codes.add(dedup_key)
                    if url_key:
                        all_url_keys.append(url_key)

            # Advance page
            page += 1
            if page >= total_pages:
                break
            if max_pages is not None and page >= max_pages:
                break

            time.sleep(0.5)  # polite rate-limit

        except Exception as e:
            print(f"  [Listing] Error on page {page}: {e}")
            break

    print(f"  [Listing] Collected {len(all_url_keys)} unique products from '{category_slug}'")
    return all_url_keys


def gql_get(query, variables, headers):
    url = f"{BASE_URL}?query={quote(query)}&variables={quote(json.dumps(variables, separators=(',', ':')))}"
    r = requests.get(url, headers=headers, timeout=15)
    r.raise_for_status()
    return r.json()

def get_attr(attributes, name):
    for a in (attributes or []):
        if a.get("name") == name:
            return a.get("value", "")
    return ""

def get_image_url(attributes, size="product_listing"):
    raw = get_attr(attributes, "image_styles")
    if not raw:
        return []
    try:
        styles = json.loads(raw)
        return [s["styles"].get(size, "") for s in styles if "styles" in s and s["styles"].get(size)]
    except:
        return []

# ============================================================
#  TRANSFORM → TARGET FORMAT
# ============================================================
def build_variant_v2(parent_product, variant_item, variant_index, url_key, color_images):
    """Build a single variant object from the getProductSearchDetails structure."""
    product = variant_item.get("product", {})
    attrs = product.get("attributes", [])
    
    colour_en = get_attr(attrs, "color") or get_attr(parent_product.get("attributes", []), "color")
    # Use images from color_assets.json if available, otherwise fallback to item attributes
    images = color_images.get(colour_en, [])
    if not images:
        images = get_image_url(attrs) or get_image_url(parent_product.get("attributes", []))
    
    size = get_attr(attrs, "size")
    variant_sku = product.get("sku", "")
    in_stock = product.get("inStock", False)
    
    price_obj = product.get("price", {})
    price_val = price_obj.get("final", {}).get("amount", {}).get("value")
    
    now_ts = int(datetime.now(timezone.utc).timestamp())
    colour_slug = colour_en.lower().replace(" ", "-") if colour_en else "unknown"
    parent_sku = parent_product.get("sku", "")

    variant_id = f"PRIMARK-{colour_en}-{parent_sku}_{colour_slug}_{size}_{variant_index}"

    return {
        "variant_id": variant_id,
        "ASIN": f"PRM-{parent_sku}-{variant_index}",
        "slug": {
            "en": url_key,
            "ar": url_key
        },
        "name": {
            "en": parent_product.get("name", ""),
            "ar": ""
        },
        "color": {
            "en": colour_en,
            "ar": ""
        },
        "color_hex": "",
        "source_color": colour_en,
        "size": size,
        "SKU": variant_sku,
        "stock": {
            "quantity": 1 if in_stock else 0
        },
        "price": {
            "amazon_price_uae": price_val,
            "amazon_discount_uae": None,
            "amazon_price": None,
            "amazon_discount": None,
            "hanooot_price": None,
            "hanooot_discount": None,
            "commission": "",
            "shipping_cost": "",
            "shipping_cost_by_air": "",
            "discount_percentage": None
        },
        "specifications": {
            "en": {
                "Brand": get_attr(attrs, "brand") or get_attr(parent_product.get("attributes", []), "brand"),
                "Colour": colour_en,
                "Gender": get_attr(attrs, "gender") or get_attr(parent_product.get("attributes", []), "gender"),
                "Fit": get_attr(attrs, "fit") or get_attr(parent_product.get("attributes", []), "fit"),
                "Pattern": get_attr(attrs, "pattern") or get_attr(parent_product.get("attributes", []), "pattern"),
                "Leg Length": get_attr(attrs, "leg_length") or get_attr(parent_product.get("attributes", []), "leg_length"),
                "Country of Origin": get_attr(attrs, "country_of_origin") or get_attr(parent_product.get("attributes", []), "country_of_origin"),
                "Department": get_attr(attrs, "department_name") or get_attr(parent_product.get("attributes", []), "department_name"),
                "Style Code": get_attr(attrs, "style_code") or get_attr(parent_product.get("attributes", []), "style_code"),
                "Product Type": get_attr(attrs, "prm_product_type") or get_attr(parent_product.get("attributes", []), "prm_product_type"),
            },
            "ar": {}
        },
        "availability": {
            "status": "In Stock" if in_stock else "Out of Stock",
            "quantity": 1 if in_stock else 0,
            "delivery_dates": []
        },
        "status": "DRAFT",
        "brand_id": {
            "en": get_attr(attrs, "brand") or get_attr(parent_product.get("attributes", []), "brand") or "Primark",
            "ar": ""
        },
        "link": f"https://www.primark.ae/en-ae/p/{url_key}",
        "product_images": images,
        "attributes": {
            "colors": {
                "en": [colour_en],
                "ar": [""]
            },
            "sizes": [],
            "size_guide_chart": [],
            "conversion_guide_chart": [],
            "model_measurements": {
                "size_guide_image": "N/A"
            }
        },
        "source_type": "PRIMARK",
        "images_gallery": images,
        "Product_images": images,
        "default_variant_id": variant_id,
        "updatedAt": now_ts,
        "createdAt": now_ts,
        "isWishlisted": False,
        "shipping": {
            "mode": "standard",
            "sea": {"min": 0, "max": 0},
            "air": {"min": 0, "max": 0},
            "compareOrderWith": "00:00"
        },
        "description": {
            "short": {"en": parent_product.get("shortDescription", ""), "ar": ""},
            "long": {"en": parent_product.get("description", ""), "ar": ""}
        },
        "category": [
            {
                "name": {"en": "Clothing", "ar": "ملابس"},
                "slug": {"en": "clothing", "ar": "ملابس"},
            }
        ],
        "category_name": get_attr(attrs, "prm_product_type") or "Clothing",
        "category_slug": "clothing",
        "source_data": {
            "SKU": variant_sku,
            "option_id": variant_item.get("selections", [None])[0],
        }
    }


def scrape_by_style(style_code):
    print(f"\n{'='*60}")
    print(f"Processing Style: {style_code}")

    variables = {
        "phrase": "",
        "filter": [{"attribute": "style_code", "eq": style_code}],
        "sort": [{"attribute": "price", "direction": "DESC"}],
        "pageSize": 100
    }
    
    print("[STEP 1] Fetching search details...")
    data = gql_get(GET_PRODUCT_SEARCH_DETAILS_QUERY, variables, SEARCH_HEADERS)
    items = data.get("data", {}).get("productSearch", {}).get("items", [])
    
    if not items:
        print(f"  No items found for style {style_code}")
        return None

    # Use the first item to get common product details
    first_product = items[0]["productView"]
    url_key = first_product.get("urlKey", "")
    parent_sku = style_code 
    
    print(f"  Found {len(items)} color groups for {first_product['name']}")

    all_variants = []
    all_colors = set()
    all_sizes = set()
    
    # Collect all variants from all items (colors)
    variant_idx = 0
    for color_item in items:
        prod_view = color_item["productView"]
        
        # Try to get color from parent attributes
        color_val = get_attr(prod_view.get("attributes", []), "color")
        
        # If not on parent, check the first variant
        if not color_val:
            first_v = prod_view.get("variants", {}).get("variants", [])
            if first_v:
                color_val = get_attr(first_v[0].get("product", {}).get("attributes", []), "color")
        
        if not color_val:
            color_val = "Unknown"
            
        all_colors.add(color_val)
        
        # Get variants for this color
        color_variants = prod_view.get("variants", {}).get("variants", [])
        for v_data in color_variants:
            v_obj = build_variant_v2(prod_view, v_data, variant_idx, url_key, COLOR_ASSETS)
            all_variants.append(v_obj)
            all_sizes.add(v_obj["size"])
            variant_idx += 1

    if not all_variants:
        return None

    # Sort variants for consistency
    all_variants.sort(key=lambda x: (x["color"]["en"], x["size"]))
    
    # Final document
    now_ts = int(datetime.now(timezone.utc).timestamp())
    first_attrs = first_product.get("attributes", [])
    
    # Update sizes in each variant now that we have the full list
    unique_sizes = sorted(list(all_sizes), key=lambda x: (len(x), x))
    for v in all_variants:
        v["attributes"]["sizes"] = unique_sizes

    doc = {
        "ASIN": f"PRM-{parent_sku}",
        "source_type": "PRIMARK",
        "SKU": f"primark-{parent_sku}",
        "name": {"en": first_product.get("name", ""), "ar": ""},
        "slug": {"en": url_key, "ar": url_key},
        "original_name": {"en": first_product.get("name", ""), "ar": ""},
        "original_short_description": {"en": first_product.get("description", "")[:150], "ar": ""},
        "original_long_description": {"en": first_product.get("description", ""), "ar": ""},
        "description": {
            "short": {"en": first_product.get("description", "")[:150], "ar": ""},
            "long": {"en": first_product.get("description", ""), "ar": ""}
        },
        "brand_id": {"en": get_attr(first_attrs, "brand") or "Primark", "ar": ""},
        "link": f"https://www.primark.ae/en-ae/p/{url_key}",
        "images_gallery": COLOR_ASSETS.get(list(all_colors)[0], []) if all_colors else [],
        "Product_images": COLOR_ASSETS.get(list(all_colors)[0], []) if all_colors else [],
        "specifications": {
            "en": {
                "Brand": get_attr(first_attrs, "brand") or "Primark",
                "Colour": ", ".join(sorted(list(all_colors))),
                "Gender": get_attr(first_attrs, "gender"),
                "Fit": get_attr(first_attrs, "fit"),
                "Pattern": get_attr(first_attrs, "pattern"),
                "Leg Length": get_attr(first_attrs, "leg_length"),
                "Country of Origin": get_attr(first_attrs, "country_of_origin"),
                "Department": get_attr(first_attrs, "department_name"),
                "Style Code": style_code,
                "Product Type": get_attr(first_attrs, "prm_product_type"),
                "Care": get_attr(first_attrs, "care"),
                "Express Delivery": get_attr(first_attrs, "express_delivery"),
                "Same Day Delivery": get_attr(first_attrs, "same_day_delivery"),
            },
            "ar": {},
            "images": []
        },
        "price": {
            "amazon_price_uae": all_variants[0]["price"]["amazon_price_uae"] if all_variants else 0,
            "amazon_discount_uae": None,
            "amazon_price": None,
            "amazon_discount": None,
            "hanooot_price": None,
            "hanooot_discount": None,
            "commission": "",
            "shipping_cost": "",
            "shipping_cost_by_air": "",
            "discount_percentage": None
        },
        "attributes": {
            "colors": {"en": sorted(list(all_colors)), "ar": []},
            "sizes": unique_sizes,
            "size_guide_chart": [],
            "conversion_guide_chart": [],
            "model_measurements": {"size_guide_image": "N/A"}
        },
        "variants": all_variants,
        "default_variant_id": all_variants[0]["variant_id"] if all_variants else "",
        "status": "DRAFT",
        "updatedAt": now_ts,
        "createdAt": now_ts,
        "isWishlisted": False,
        "shipping": {
            "mode": "standard",
            "sea": {"min": 0, "max": 0},
            "air": {"min": 0, "max": 0},
            "compareOrderWith": "00:00"
        },
        "category": [
            {
                "name": {"en": "Clothing", "ar": "ملابس"},
                "slug": {"en": "clothing", "ar": "ملابس"},
            }
        ],
        "category_name": get_attr(first_attrs, "prm_product_type") or "Clothing",
        "category_slug": "clothing",
        "isActive": True,
    }
    
    print(f"  Built document with {len(all_variants)} total variants across {len(all_colors)} colors")
    return doc


def get_style_code_by_url(url_key):
    """Lookup the style_code for a given url_key using ProductSearch."""
    print(f"  Looking up style_code for: {url_key}")
    variables = {
        "phrase": "",
        "filter": [{"attribute": "url_key", "eq": url_key}],
        "pageSize": 1
    }
    try:
        data = gql_get(GET_PRODUCT_SEARCH_DETAILS_QUERY, variables, SEARCH_HEADERS)
        items = data.get("data", {}).get("productSearch", {}).get("items", [])
        if not items:
            return None
        attrs = items[0]["productView"].get("attributes", [])
        return get_attr(attrs, "style_code")
    except Exception as e:
        print(f"  Style lookup failed: {e}")
        return None


def run_scraper(
    target_url: str,
    task_id: str,
    sku: Optional[str] = None,
    stop_event: Optional[threading.Event] = None,
    product_target: Optional[int] = None,
    usd_to_aed: float = 3.67,
    usd_to_iqd: float = 1500,
) -> int:
    """Entry point for the API to run the scraper."""
    
    async def _async_run():
        append_log(task_id, "Connecting to Redis...", "info")
        redis_settings = RedisSettings(host=REDIS_HOST, port=REDIS_PORT)
        try:
            redis_pool = await create_pool(redis_settings)
        except Exception as e:
            append_log(task_id, f"Failed to connect to Redis: {e}", "error")
            return 0

        environment = get_environment()
        
        # Parse target URL
        # e.g., https://www.primark.ae/en/shop-women/clothing/jeans
        # e.g., https://www.primark.ae/en/buy-cotton-denim-high-waisted-tapered-jeans-light-blue
        url_keys = []
        parsed_url = target_url.strip().rstrip("/")
        slug = parsed_url.split("/")[-1]
        
        if "shop-" in target_url:
            # It's a category
            append_log(task_id, f"Discovering products in category: '{target_url}'", "info")
            # Extract full category slug like 'shop-women/clothing/jeans'
            category_slug = parsed_url.split("/en/")[-1] if "/en/" in parsed_url else slug
            url_keys = fetch_category_url_keys(category_slug, hits_per_page=HITS_PER_PAGE)
        else:
            # Single product
            append_log(task_id, f"Scraping single product: '{slug}'", "info")
            url_keys = [slug]

        append_log(task_id, f"Total URL keys to process: {len(url_keys)}", "info")
        
        seen_style_codes = set()
        all_job_ids = []

        for i, url_key in enumerate(url_keys, 1):
            if stop_event and stop_event.is_set():
                append_log(task_id, "Stop requested, aborting.", "warning")
                break
                
            if product_target and len(all_job_ids) >= product_target:
                append_log(task_id, f"Reached target of {product_target} products.", "info")
                break

            append_log(task_id, f"[{i}/{len(url_keys)}] URL KEY: {url_key}", "debug")

            style_code = get_style_code_by_url(url_key)
            if not style_code:
                append_log(task_id, f"Could not find style_code for {url_key}, skipping.", "warning")
                continue

            if style_code in seen_style_codes:
                continue

            seen_style_codes.add(style_code)
            append_log(task_id, f"Found Style Code: {style_code}", "debug")

            doc = scrape_by_style(style_code)
            if doc:
                job_id = await _build_and_enqueue_product(
                    redis_pool, task_id, style_code, doc, environment
                )
                if job_id:
                    all_job_ids.append(job_id)

            time.sleep(0.3)

        task_status = "stopped" if (stop_event and stop_event.is_set()) else "completed"
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

        await redis_pool.aclose()
        return len(all_job_ids)

    return asyncio.run(_async_run())


# ============================================================
#  Stubs kept for compatibility with task_primark.py (price-update path).
#  The Primark refresh flow can re-use the GraphQL extractor above; these
#  stubs only exist so module imports succeed.
# ============================================================
def _parse_price_number(s):
    """Extract a numeric price from strings like 'AED 199' or '199.00'."""
    if s is None:
        return 0.0
    if isinstance(s, (int, float)):
        return float(s)
    import re as _re
    txt = str(s).replace(",", "")
    m = _re.findall(r"\d+(?:\.\d+)?", txt)
    if not m:
        return 0.0
    try:
        return float(m[0])
    except Exception:
        return 0.0


def _url_to_arabic(url: str) -> str:
    """No-op for Primark (single-locale store). Kept for namshi-parity API."""
    return url or ""


def _parse_product_page_arabic_only_sync(html_content: str) -> dict:
    """Primark serves a single locale; Arabic re-fetch is a no-op."""
    return {"name_ar": "", "description_ar": "", "brand_ar": "", "specifications_ar": {}, "color_ar": ""}


class PrimarkScraper:
    """Stub kept so task_primark.py / primark_scraper_browser.py can import it.
    The actual Primark extractor uses GraphQL via scrape_by_style/run_scraper above."""

    @staticmethod
    def _parse_product_page_sync(html_content: str, url: str | None = None) -> dict:
        """No-op parser — returns the empty shape callers expect."""
        return {
            "variant_sku": "",
            "original_price_text": "",
            "selling_price_text": "",
            "discount_percentage_text": "",
            "size_stock": [],
            "color_links": [],
        }


if __name__ == "__main__":
    import uuid
    run_scraper(
        target_url="https://www.primark.ae/en/shop-women/clothing/jeans",
        task_id=str(uuid.uuid4()),
        product_target=2
    )