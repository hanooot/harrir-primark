from __future__ import annotations

from typing import Any, Dict, List


def create_empty_variant(
    *,
    variant_id: str,
    asin: str,
    task: str,
    color: str,
    color_hex: str,
    size: str,
    images: List[str],
    base_price: float,
    sku: str = "",
    all_colors: List[str] | None = None,
    all_sizes: List[str] | None = None,
) -> Dict[str, Any]:
    """
    Factory function to create a single variant dictionary with default values.
    Matches the Harrir variant schema.
    
    Args:
        variant_id: Unique identifier for this variant
        asin: Amazon Standard Identification Number
        task: Task ID
        color: This variant's color
        color_hex: Hex code for this variant's color
        size: This variant's size
        images: Images for this variant
        base_price: Base price for this variant
        sku: Stock Keeping Unit
        all_colors: List of ALL available colors for the product (for attributes)
        all_sizes: List of ALL available sizes for the product (for attributes)
    """
    # Use all_colors and all_sizes for attributes if provided, otherwise fall back to single values
    attributes_colors = all_colors if all_colors is not None else ([color] if color else [])
    attributes_sizes = all_sizes if all_sizes is not None else ([size] if size else [])
    
    return {
        "variant_id": variant_id,
        "ASIN": asin,
        "task": task,
        "slug": {"en": "", "ar": ""},
        "name": {"en": "", "ar": ""},
        "original_name": {"en": "", "ar": ""},
        "original_short_description": {"en": "", "ar": ""},
        "original_long_description": {"en": "", "ar": ""},
        "color": {"en": color or "", "ar": ""},
        "color_hex": color_hex,
        "source_color": color or "",
        "size": size or "",
        "SKU": sku,
        "stock": {"quantity": 0},
        "price": {
            "amazon_price_uae": float(base_price) if base_price else 0.0,
            "amazon_discount_uae": "",
            "amazon_price": float(base_price) if base_price else 0.0,
            "amazon_discount": "",
            "hanooot_price": 0.0,
            "hanooot_discount": "",
            "commission": "",
            "shipping_cost": "",
            "shipping_cost_by_air": "",
            "discount_percentage": "",
        },
        "price_iqd": {
            "amazon_price_uae": float(base_price) if base_price else 0.0,
            "amazon_discount_uae": "",
            "amazon_price": float(base_price) if base_price else 0.0,
            "amazon_discount": "",
            "hanooot_price": 0.0,
            "hanooot_discount": "",
            "commission": "",
            "shipping_cost": "",
            "shipping_cost_by_air": "",
            "discount_percentage": "",
        },
        "specifications": {},
        "availability": {"status": "Out of Stock", "quantity": 0, "delivery_dates": []},
        "status": "DRAFT",
        "brand_id": {"en": "", "ar": ""},
        "link": "",
        "product_images": images,
        "attributes": {"colors": {"en": attributes_colors, "ar": []}, "sizes": attributes_sizes, "size_guide_chart": [], "conversion_guide_chart": [], "model_measurements": {}},
        "chatgpt_actions": {
            # "short_description_action": "Change the text as SEO short description and make it in <ul><li> tags for this product.",
            "long_description_action": "Create a 100-150 word e-commerce product description using the provided information. Elaborate on standard features typical for this product category while staying factual and relevant. DO NOT include warranty, delivery, returns, discounts, brand history, or specific technical specs unless mentioned. Focus on product use, styling, and versatility. Output: plain text paragraph only, no formatting.",
            "product_name_action": "Rewrite the product title into a short optimized product title and write the color at the end, and if there is no color leave the end blank.",
            "product_tag_action": "Act as an SEO expert. Generate relevant SEO tags only for the given product data. Return tags only. Do not generate meta descriptions, summaries, sentences, or character-limited text (e.g., 51-character outputs). No explanations or extra text.",
            "product_keyword_action": "You are an AI assistant that helps generate product keywords. Given the product name and description, provide relevant keywords that capture the product's key attributes. Start with the core/main term from the product name, then progress from general to specific keywords. Exclude any warranty or shipping information. Return only the most contextual keywords - avoid generating unnecessary or redundant terms. Format the output as a comma-separated list.",
            # "alt_description_action": "Act as an SEO expert and write a simple short alt tag for the first image of Product (White Background)",
            # "meta_description_action": "Act as an SEO expert and write a short simple meta description consisting of less than 100 characters",
        },
        "product_seo": {
            # "short_description": {"en": "", "ar": ""},
            "long_description": {"en": "", "ar": ""},
            "keywords": {"en": [], "ar": []},
            "product_tag": {"en": "", "ar": ""},
            # "meta_description": {"en": "", "ar": ""},
            # "alt_description": {"en": "", "ar": ""},
            # "search_keywords": {"en": "", "ar": ""},
        },
    }


def generate_variants_list(
    *,
    product_id: str,
    task: str,
    colors: List[str],
    sizes: List[str],
    color_images_dict: Dict[str, List[str]],
    base_price: float,
) -> List[Dict[str, Any]]:
    """
    Generate the full list of variants by combining colors and sizes (color × size).
    Each variant will have attributes containing ALL colors and ALL sizes available.
    """
    variants: List[Dict[str, Any]] = []
    variant_index = 0

    colors = colors or ["Default"]
    sizes = sizes or ["One Size"]

    for color in colors:
        # color_hex = get_color_hex(color)
        for size in sizes:
            safe_color = (color or "default").lower().replace(" ", "_")
            safe_size = (size or "one_size").lower().replace(" ", "_")

            variant_id = f"PRIMARK-{color}-{product_id}_{safe_color}_{safe_size}_{variant_index}"
            variant_asin = f"NAM-{product_id}-{variant_index}"
            images = color_images_dict.get(color, [])

            variants.append(
                create_empty_variant(
                    variant_id=variant_id,
                    asin=variant_asin,
                    task=task,
                    color=color,
                    color_hex="",
                    size=size,
                    images=images,
                    base_price=base_price,
                    all_colors=colors,
                    all_sizes=sizes,
                )
            )
            variant_index += 1

    return variants

