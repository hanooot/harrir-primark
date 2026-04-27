"""Common utility functions for Primark scrapers."""
import logging
import requests
from typing import Tuple

from app.config import get_config

logger = logging.getLogger(__name__)


def append_log(task_id: str, message: str, level: str = "debug") -> None:
    """Send log message to job log API; continue silently on failure."""
    cfg = get_config()
    if not cfg.AUTOMATION_BACKEND_URL:
        return
    try:
        url = f"{cfg.AUTOMATION_BACKEND_URL}/logs/ingest"
        payload = {"task_id": task_id, "message": message, "level": level}
        headers = {"Content-Type": "application/json", "X-API-KEY": cfg.API_KEY}

        logger.info("Sending POST request")
        logger.info("URL: %s", url)
        logger.info("Headers: %s", headers)
        logger.info("Payload: %s", payload)

        response = requests.post(url, json=payload, headers=headers, timeout=10)

        logger.info("Response status: %s", response.status_code)
        logger.info("Response body: %s", response.text)
    except Exception as e:
        logger.debug(f"[{task_id}] [LOG_ERROR] {e}")


def complete_task(task_id: str, status: str) -> None:
    """Signal task completion to Express API; continue silently on failure."""
    cfg = get_config()
    if not cfg.AUTOMATION_BACKEND_URL:
        return
    try:
        requests.post(
            f"{cfg.AUTOMATION_BACKEND_URL}/tasks/complete",
            json={"task_id": task_id, "status": status},
            headers={"Content-Type": "application/json", "X-API-KEY": cfg.API_KEY},
            timeout=10,
        )
    except Exception as e:
        logger.debug(f"[{task_id}] [COMPLETE_ERROR] {e}")


def _product_url() -> str:
    return f"{get_config().BASE_URL}/api/v1/product"


def get_product_searching_url() -> str:
    cfg = get_config()
    return f"{cfg.BASE_URL}/api/v1/products/search" if cfg.BASE_URL else ""


def post_product_to_api(product_data: dict) -> Tuple[bool, str]:
    """Post product to API. Returns (success, error_message)."""
    url = _product_url()
    if not url or not get_config().BASE_URL:
        return False, "BASE_URL is not set"
    payload = product_data.get("data", product_data)
    logger.info("📡 POST %s (ASIN=%s)", url, payload.get("ASIN", "?"))
    try:
        response = requests.post(
            url, json=payload,
            headers={"Content-Type": "application/json"}, timeout=60,
        )
        body = response.text[:500]
        logger.info("📡 Response %s: %s", response.status_code, body)
        if response.status_code in (200, 201):
            try:
                resp_json = response.json()
                statuses = resp_json.get("data", {}).get("product_status", [])
                if statuses and statuses[0].get("status") == "failed":
                    msg = statuses[0].get("message", "Unknown API error")
                    return False, f"API rejected: {msg}"
            except Exception:
                pass
            return True, ""
        return False, f"{response.status_code}: {body}"
    except Exception as e:
        return False, str(e)


def patch_product_to_api(product_id: str, patch_data: dict) -> Tuple[bool, str]:
    """Update part of a product via PATCH. Returns tuple(success, error_message)."""
    url = _product_url()
    if not url or not get_config().BASE_URL:
        return False, "BASE_URL is not set"
    try:
        response = requests.patch(
            url, json=patch_data,
            headers={"Content-Type": "application/json"}, timeout=60,
        )
        if response.status_code in [200, 204]:
            return True, ""
        return False, f"{response.status_code}: {response.text}"
    except Exception as e:
        return False, str(e)


def delete_product_from_api(asin: str) -> Tuple[bool, str]:
    """Delete a product from the API by its ASIN. Returns tuple(success, error_message)."""
    cfg = get_config()
    if not cfg.BASE_URL:
        return False, "BASE_URL is not set"
    try:
        url = f"{cfg.BASE_URL}/api/v1/product?ASIN={asin}"
        response = requests.delete(
            url, headers={"Content-Type": "application/json"}, timeout=60,
        )
        if response.status_code in [200, 204]:
            return True, ""
        return False, f"{response.status_code}: {response.text}"
    except Exception as e:
        return False, str(e)
