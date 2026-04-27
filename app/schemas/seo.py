from typing import List

from pydantic import BaseModel, Field

from app.schemas.common import LocalizedText


class ProductSearchKeywords(BaseModel):
    en: List[str] = Field(default_factory=list)
    ar: List[str] = Field(default_factory=list)


class ProductSEO(BaseModel):
    # short_description: LocalizedText = Field(default_factory=LocalizedText)
    long_description: LocalizedText = Field(default_factory=LocalizedText)
    keywords: ProductSearchKeywords = Field(default_factory=ProductSearchKeywords)
    product_tag: LocalizedText = Field(default_factory=LocalizedText)
    # meta_description: LocalizedText = Field(default_factory=LocalizedText)
    # alt_description: LocalizedText = Field(default_factory=LocalizedText)
    # search_keywords: LocalizedText = Field(default_factory=LocalizedText)


class ChatGPTActions(BaseModel):
    # short_description_action: str = ""
    long_description_action: str = ""
    product_name_action: str = ""
    product_tag_action: str = ""
    product_keyword_action: str = ""
    # alt_description_action: str = ""
    # meta_description_action: str = ""

