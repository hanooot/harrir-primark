"""
Shared async MongoDB client (PyMongo Async driver).
URI / DB / collection are resolved from app.config.get_config() so the right
cluster is used for stage vs. prod.

Usage:
    from app.utils.mongo import get_products_collection_safe

    col = await get_products_collection_safe()
    total = await col.count_documents({"source_type": "PRIMARK"})
"""

import logging
from typing import Optional

from pymongo import AsyncMongoClient
from pymongo.asynchronous.collection import AsyncCollection
from pymongo.asynchronous.database import AsyncDatabase

from app.config import get_config

logger = logging.getLogger(__name__)

_client: Optional[AsyncMongoClient] = None
_connected_uri: Optional[str] = None


async def connect() -> None:
    """Initialize the PyMongo Async client singleton (idempotent).
    Re-connects if the configured URI changed (e.g. environment switched)."""
    global _client, _connected_uri

    cfg = get_config()
    uri = cfg.MONGO_URI
    if not uri:
        raise EnvironmentError(
            "MONGO_URI not configured (set MONGO_URI_STAGE / MONGO_URI_PROD or MONGO_URI)."
        )

    if _client is not None and _connected_uri == uri:
        return

    if _client is not None:
        try:
            await _client.aclose()
        except Exception:
            pass

    _client = AsyncMongoClient(
        uri,
        serverSelectionTimeoutMS=10_000,
        connectTimeoutMS=10_000,
        socketTimeoutMS=30_000,
        maxPoolSize=20,
        minPoolSize=2,
    )
    _connected_uri = uri

    try:
        logger.info("[MongoDB] Connecting to %s ...", uri.split("@")[-1])
        await _client.admin.command("ping")
        logger.info("[MongoDB] Connected — db='%s'", cfg.MONGO_DB_NAME)
    except Exception as e:
        logger.error("[MongoDB] Connection failed: %s", e)
        raise


async def disconnect() -> None:
    global _client, _connected_uri
    if _client is not None:
        await _client.aclose()
        _client = None
        _connected_uri = None
        logger.info("[MongoDB] Connection closed.")


def get_db(db_name: Optional[str] = None) -> AsyncDatabase:
    if _client is None:
        raise RuntimeError("MongoDB client not initialized. Call await connect() first.")
    return _client[db_name or get_config().MONGO_DB_NAME]


def get_collection(collection_name: str, db_name: Optional[str] = None) -> AsyncCollection:
    return get_db(db_name)[collection_name]


def get_products_collection() -> AsyncCollection:
    return get_collection(get_config().MONGO_PRODUCTS_COLLECTION)


async def get_products_collection_safe() -> AsyncCollection:
    """Auto-connects if not yet initialized, then returns the products collection."""
    if _client is None or _connected_uri != get_config().MONGO_URI:
        await connect()
    return get_products_collection()
