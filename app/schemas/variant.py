from typing import Dict, List

from pydantic import BaseModel, Field

from app.schemas.attributes import ProductAttributes
from app.schemas.common import LocalizedText
from app.schemas.pricing import ProductPrice
from app.schemas.seo import ChatGPTActions, ProductSEO


class VariantStock(BaseModel):
    quantity: int = 0


class VariantAvailability(BaseModel):
    status: str = "Out of Stock"
    quantity: int = 0
    delivery_dates: List[str] = Field(default_factory=list)


class ProductVariant(BaseModel):
    variant_id: str
    ASIN: str
    task: str = ""

    slug: LocalizedText = Field(default_factory=LocalizedText)
    name: LocalizedText = Field(default_factory=LocalizedText)

    original_name: LocalizedText = Field(default_factory=LocalizedText)
    original_short_description: LocalizedText = Field(default_factory=LocalizedText)
    original_long_description: LocalizedText = Field(default_factory=LocalizedText)

    color: LocalizedText = Field(default_factory=LocalizedText)
    color_hex: str = ""
    source_color: str = ""  # Scraped value, no translation
    size: str = ""  # Scraped size, no translation
    SKU: str = ""

    stock: VariantStock = Field(default_factory=VariantStock)

    price: ProductPrice = Field(default_factory=ProductPrice)
    price_iqd: ProductPrice = Field(default_factory=ProductPrice)

    specifications: Dict[str, str] = Field(default_factory=dict)

    availability: VariantAvailability = Field(default_factory=VariantAvailability)

    status: str = "DRAFT"
    brand_id: LocalizedText = Field(default_factory=LocalizedText)

    link: str = ""
    product_images: List[str] = Field(default_factory=list)

    attributes: ProductAttributes = Field(default_factory=ProductAttributes)

    chatgpt_actions: ChatGPTActions = Field(default_factory=ChatGPTActions)
    product_seo: ProductSEO = Field(default_factory=ProductSEO)

    default_variant_id: str = ""

