from app.schemas.product import Product


def create_empty_product(
    *,
    asin: str,
    sku: str = "",
    brand_id: str | dict | None = None,
    status: str = "DRAFT",
    link: str = "",
    task: str = "",
) -> Product:
    """
    Create a new Product with defaults, matching Harrir product schema.
    brand_id: string or dict with en/ar (defaults to en="Primark", ar="").
    """
    if brand_id is None:
        brand_id = {"en": "Primark", "ar": ""}
    elif isinstance(brand_id, str):
        brand_id = {"en": brand_id.strip(), "ar": ""}
    return Product(
        ASIN=asin,
        source_type="PRIMARK",
        original_name={"en": "", "ar": ""},
        task=task,
        original_short_description={"en": "", "ar": ""},
        original_long_description={"en": "", "ar": ""},
        name={"en": "", "ar": ""},
        slug={"en": "", "ar": ""},
        brand_id=brand_id,
        link=link,
        SKU=sku,
        images_gallery=[],
        Product_images=[],
        description={"short": {"en": "", "ar": ""}, "long": {"en": "", "ar": ""}},
        specifications={},
        price={
            "amazon_price_uae": 0.0,
            "amazon_discount_uae": "",
            "amazon_price": 0.0,
            "amazon_discount": "",
            "hanooot_price": 0.0,
            "hanooot_discount": "",
            "commission": "",
            "shipping_cost": "",
            "shipping_cost_by_air": "",
            "discount_percentage": "",
        },
        price_iqd={
            "amazon_price_uae": 0.0,
            "amazon_discount_uae": "",
            "amazon_price": 0.0,
            "amazon_discount": "",
            "hanooot_price": 0.0,
            "hanooot_discount": "",
            "commission": "",
            "shipping_cost": "",
            "shipping_cost_by_air": "",
            "discount_percentage": "",
        },
        attributes={"colors": {"en": [], "ar": []}, "sizes": [], "size_guide_chart": [], "conversion_guide_chart": [], "model_measurements": {}},
        variants=[],
        default_variant_id="",
        chatgpt_actions={
            # "short_description_action": "Change the text as SEO short description and make it in <ul><li> tags for this product.",
            "long_description_action": "Create a 100-150 word e-commerce product description using the provided information. Elaborate on standard features typical for this product category while staying factual and relevant. DO NOT include warranty, delivery, returns, discounts, brand history, or specific technical specs unless mentioned. Focus on product use, styling, and versatility. Output: plain text paragraph only, no formatting.",
            "product_name_action": "Rewrite the product title into a short optimized product title and write the color at the end, and if there is no color leave the end blank.",
            "product_tag_action": "Act as an SEO expert. Generate relevant SEO tags only for the given product data. Return tags only. Do not generate meta descriptions, summaries, sentences, or character-limited text (e.g., 51-character outputs). No explanations or extra text.",
            "product_keyword_action": "You are an AI assistant that helps generate product keywords. Given the product name and description, provide relevant keywords that capture the product's key attributes. Start with the core/main term from the product name, then progress from general to specific keywords. Exclude any warranty or shipping information. Return only the most contextual keywords - avoid generating unnecessary or redundant terms. Format the output as a comma-separated list.",
            # "alt_description_action": "Act as an SEO expert and write a simple short alt tag for the first image of Product (White Background)",
            # "meta_description_action": "Act as an SEO expert and write a short simple meta description consisting of less than 100 characters",
        },
        product_seo={
            # "short_description": {"en": "", "ar": ""},
            "long_description": {"en": "", "ar": ""},
            "keywords": {"en": [], "ar": []},
            "product_tag": {"en": "", "ar": ""},
            # "meta_description": {"en": "", "ar": ""},
            # "alt_description": {"en": "", "ar": ""},
            # "search_keywords": {"en": "", "ar": ""},
        },
        status=status,
    )

