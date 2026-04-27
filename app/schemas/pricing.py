from pydantic import BaseModel


class ProductPrice(BaseModel):
    amazon_price_uae: float = 0.0
    amazon_discount_uae: str = ""
    amazon_price: float = 0.0
    amazon_discount: str = ""
    hanooot_price: float = 0.0
    hanooot_discount: str = ""
    commission: str = ""
    shipping_cost: str = ""
    shipping_cost_by_air: str = ""
    discount_percentage: str = ""

