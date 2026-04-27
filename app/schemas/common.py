from typing import List

from pydantic import BaseModel, Field


class LocalizedText(BaseModel):
    en: str = ""
    ar: str = ""


class LocalizedList(BaseModel):
    """Object of arrays per language (en, ar)."""
    en: List[str] = Field(default_factory=list)
    ar: List[str] = Field(default_factory=list)

