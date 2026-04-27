from fastapi import APIRouter

from .primark_scraper import router as primark_router
from .price_updater import router as price_updater_router

api_router = APIRouter()
api_router.include_router(
    primark_router,
)
api_router.include_router(
    price_updater_router,
)

