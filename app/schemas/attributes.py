from typing import Any, List

from pydantic import BaseModel, Field

from app.schemas.common import LocalizedList


class ProductAttributes(BaseModel):
    colors: LocalizedList = Field(default_factory=LocalizedList)
    sizes: List[str] = Field(default_factory=list)  # No translation
    size_guide_chart: List[Any] = Field(default_factory=list)
    conversion_guide_chart: List[Any] = Field(default_factory=list)
    model_measurements: dict = Field(default_factory=dict)

