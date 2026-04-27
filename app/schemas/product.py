from typing import Dict, List, Optional

from pydantic import BaseModel, Field

from app.schemas.attributes import ProductAttributes
from app.schemas.common import LocalizedText
from app.schemas.pricing import ProductPrice
from app.schemas.seo import ChatGPTActions, ProductSEO
from app.schemas.variant import ProductVariant


class ProductDescription(BaseModel):
    short: LocalizedText = Field(default_factory=LocalizedText)
    long: LocalizedText = Field(default_factory=LocalizedText)


class Product(BaseModel):
    _id: Optional[str] = None
    ASIN: str
    source_type: str = "PRIMARK"
    SKU: str

    task: str = ""

    name: LocalizedText = Field(default_factory=LocalizedText)
    slug: LocalizedText = Field(default_factory=LocalizedText)

    original_name: LocalizedText = Field(default_factory=LocalizedText)
    original_short_description: LocalizedText = Field(default_factory=LocalizedText)
    original_long_description: LocalizedText = Field(default_factory=LocalizedText)

    description: ProductDescription = Field(default_factory=ProductDescription)

    brand_id: LocalizedText = Field(default_factory=lambda: LocalizedText(en="Primark", ar=""))

    link: str = ""
    images_gallery: List[str] = Field(default_factory=list)
    Product_images: List[str] = Field(default_factory=list)

    specifications: Dict[str, str] = Field(default_factory=dict)

    price: ProductPrice = Field(default_factory=ProductPrice)
    price_iqd: ProductPrice = Field(default_factory=ProductPrice)

    attributes: ProductAttributes = Field(default_factory=ProductAttributes)

    variants: List[ProductVariant] = Field(default_factory=list)
    default_variant_id: str = ""

    chatgpt_actions: ChatGPTActions = Field(default_factory=ChatGPTActions)
    product_seo: ProductSEO = Field(default_factory=ProductSEO)

    status: str = "DRAFT"

