from contextlib import asynccontextmanager
from fastapi import FastAPI

from app.api.v1 import api_router
from app.api.v1.price_updater import auto_start_price_updater


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events"""
    await auto_start_price_updater()
    yield


app = FastAPI(
    title="Harrir Primark Scraping",
    description="Primark scraping API",
    version="1.0.0",
    openapi_url="/openapi.json",
    redirect_slashes=False,
    lifespan=lifespan,
)

app.include_router(api_router, prefix="/api/v1")


@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "service": "harrir-primark",
        "version": app.version,
    }

