import logging

from fastapi import APIRouter

from .home_scraper import router as home_router
from .primark_scraper import router as primark_router
from .price_updater import router as price_updater_router

api_router = APIRouter()
api_router.include_router(
    primark_router,
)
api_router.include_router(
    price_updater_router,
)
api_router.include_router(home_router)

# The cross-source hub (`/api/v1/home/*`) lives in harrir_core so every service
# answers the same registry, latest and report paths. Older images do not carry
# it; the per-source /home-* routes above work either way.
try:
    from harrir_core.utils.home_hub import router as home_hub_router
except ImportError:  # pragma: no cover
    home_hub_router = None
    logging.getLogger(__name__).warning(
        "harrir_core.utils.home_hub missing — /api/v1/home/* not mounted; rebuild the image."
    )

if home_hub_router is not None:
    api_router.include_router(home_hub_router)
