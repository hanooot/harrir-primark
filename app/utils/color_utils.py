import asyncio
import io
import logging
import math
import os
import re
from pathlib import Path
from urllib.parse import quote
from typing import Dict, Tuple

import httpx
import joblib
import numpy as np
from PIL import Image
from dotenv import load_dotenv
from sklearn.neighbors import KNeighborsRegressor
from tenacity import (
    AsyncRetrying,
    RetryError,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    before_sleep_log,
)

from app.config import get_config
from app.utils.gemini import get_gemini_client

load_dotenv()
logger = logging.getLogger(__name__)

# Config

def _colors_api_base() -> str:
    cfg = get_config()
    return cfg.EXPRESS_API_URL.rstrip("/") + "/colors" if cfg.EXPRESS_API_URL else ""
DEFAULT_HEX_FALLBACK = "#000000"
GEMINI_COLOR_MODEL = "gemini-2.5-flash-lite"
GEMINI_HEX_SYSTEM_PROMPT = (
    "Analyze the product image."
    "If there are more then two colors in main product as described in title, then return the #xxxxxx."
    "Return only the primary color as a hex code in the "
    "form #RRGGBB. No other text."
)
GEMINI_NAMES_SYSTEM_PROMPT = (
    "You are a precise color naming expert. Given a hex color code, generate ONE accurate, "
    "descriptive color name in English and Arabic that matches the exact shade. "
    "Be specific with shades (e.g., 'Dark Gray' not just 'Gray', 'Light Blue' not just 'Blue', "
    "'Charcoal' for very dark grays #4a4a4a, 'Taupe' for brownish grays #483c32). "
    "Distinguish between similar shades with descriptive modifiers (Dark/Light/Pale/Deep/Soft/Bright). "
    "Output exactly one line: EnglishName | ArabicName. Use standard color terminology. No other text."
)
MAX_RETRIES = 3
RETRY_WAIT = wait_exponential(multiplier=1, min=1, max=10)
HTTP_TIMEOUT = 20.0
KNN_SIMILARITY_THRESHOLD = 95.0  # Minimum similarity % to use KNN match
# Pretrained KNN cache: load from here if present, save after train/retrain
COLOR_KNN_CACHE_PATH = os.getenv("COLOR_KNN_CACHE_PATH") or str(
    Path(__file__).resolve().parent / "color_knn_cache.joblib"
)


async def _with_retry(exceptions: Tuple[type, ...], coro_fn):
    """Run coroutine with exponential backoff; raises RetryError if all attempts fail."""
    async for attempt in AsyncRetrying(
        retry=retry_if_exception_type(exceptions),
        stop=stop_after_attempt(MAX_RETRIES),
        wait=RETRY_WAIT,
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    ):
        with attempt:
            return await coro_fn()


# ============================================================================
# KNN Color Matching Setup
# ============================================================================

async def _fetch_all_colors_from_api() -> list[dict] | None:
    """Fetch all colors from API endpoint."""
    if not _colors_api_base() or _colors_api_base() == "/colors":
        logger.error("[color_utils] EXPRESS_API_URL not configured")
        return None
    
    url = _colors_api_base()
    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            logger.info("[color_utils] Fetching colors from API: %s", url)
            response = await client.get(url)
            response.raise_for_status()
            if not response.text or not response.text.strip():
                logger.warning("[color_utils] API returned empty body")
                return None
            try:
                data = response.json()
            except ValueError as e:
                logger.error("[color_utils] API response is not valid JSON: %s", e)
                return None
            if not data.get("success"):
                logger.warning("[color_utils] API returned success=false")
                return None
            
            colors = data.get("data", [])
            if isinstance(colors, list):
                logger.info("[color_utils] Fetched %d colors from API", len(colors))
                return colors
            else:
                logger.warning("[color_utils] Unexpected data format from API")
                return None
    except Exception as e:
        logger.error("[color_utils] Failed to fetch colors from API: %s", e, exc_info=True)
        return None


def _load_knn_cache() -> tuple[KNeighborsRegressor | None, list[dict] | None]:
    """Load pretrained KNN model and colors_data from disk. Returns (None, None) on failure."""
    try:
        if not os.path.isfile(COLOR_KNN_CACHE_PATH):
            return None, None
        data = joblib.load(COLOR_KNN_CACHE_PATH)
        if not isinstance(data, dict) or "knn" not in data or "colors_data" not in data:
            return None, None
        knn, colors_data = data["knn"], data["colors_data"]
        if not isinstance(colors_data, list) or not colors_data or not hasattr(knn, "kneighbors"):
            return None, None
        logger.info("[color_utils] Loaded pretrained KNN from cache (%d colors)", len(colors_data))
        return knn, colors_data
    except Exception as e:
        logger.warning("[color_utils] Failed to load KNN cache: %s", e)
        return None, None


def _save_knn_cache(knn: KNeighborsRegressor, colors_data: list[dict]) -> None:
    """Save KNN model and colors_data to disk."""
    try:
        Path(COLOR_KNN_CACHE_PATH).parent.mkdir(parents=True, exist_ok=True)
        joblib.dump({"knn": knn, "colors_data": colors_data}, COLOR_KNN_CACHE_PATH)
        logger.info("[color_utils] Saved KNN cache (%d colors) to %s", len(colors_data), COLOR_KNN_CACHE_PATH)
    except Exception as e:
        logger.warning("[color_utils] Failed to save KNN cache: %s", e)


def _train_knn_from_colors(valid_colors: list[dict]) -> tuple[KNeighborsRegressor, list[dict]]:
    """Train KNN regressor from list of color dicts with r,g,b. Returns (knn, valid_colors)."""
    X = np.array([[c["r"], c["g"], c["b"]] for c in valid_colors])
    knn = KNeighborsRegressor(n_neighbors=1, metric="euclidean")
    knn.fit(X, X)
    return knn, valid_colors


async def _get_trained_knn():
    """Use pretrained KNN from cache if present; else fetch from API, train, and save."""
    knn, colors_data = _load_knn_cache()
    if knn is not None and colors_data is not None:
        return knn, colors_data

    try:
        colors_data = await _fetch_all_colors_from_api()
        if not colors_data:
            logger.warning("[color_utils] No color data available for KNN training")
            return None, None

        valid_colors = [
            c for c in colors_data
            if isinstance(c.get("r"), (int, float))
            and isinstance(c.get("g"), (int, float))
            and isinstance(c.get("b"), (int, float))
        ]
        if not valid_colors:
            logger.warning("[color_utils] No valid RGB color data for KNN training")
            return None, None

        knn, valid_colors = _train_knn_from_colors(valid_colors)
        logger.info("[color_utils] KNN trained with %d colors", len(valid_colors))
        _save_knn_cache(knn, valid_colors)
        return knn, valid_colors
    except Exception as e:
        logger.error("[color_utils] KNN training failed: %s", e, exc_info=True)
        return None, None


async def _retrain_and_save_knn() -> None:
    """Refetch colors from API, retrain KNN, and save cache. Call after every new color insert."""
    try:
        colors_data = await _fetch_all_colors_from_api()
        if not colors_data:
            return
        valid_colors = [
            c for c in colors_data
            if isinstance(c.get("r"), (int, float))
            and isinstance(c.get("g"), (int, float))
            and isinstance(c.get("b"), (int, float))
        ]
        if not valid_colors:
            return
        knn, valid_colors = _train_knn_from_colors(valid_colors)
        _save_knn_cache(knn, valid_colors)
        logger.info("[color_utils] Retrained and saved KNN after new color insert (%d colors)", len(valid_colors))
    except Exception as e:
        logger.warning("[color_utils] Retrain KNN after insert failed: %s", e)


def _normalize_hex(hex_code: str) -> str:
    """Normalize hex to #rrggbb lowercase for comparison."""
    try:
        h = (hex_code or "").strip().lstrip("#")
        if len(h) == 3:
            h = "".join(c * 2 for c in h)
        if len(h) != 6:
            return ""
        return "#" + h.lower()
    except (ValueError, TypeError):
        return ""


def _hex_to_rgb(hex_code: str) -> tuple[int, int, int]:
    """Convert hex color (#RRGGBB, RRGGBB, or #RGB) to RGB tuple.
    Returns (260, 260, 260) for invalid hex.
    """
    try:
        hex_code = hex_code.lstrip('#')

        if len(hex_code) == 3:
            hex_code = ''.join(c * 2 for c in hex_code)

        if len(hex_code) != 6:
            return (260, 260, 260)

        return tuple(int(hex_code[i:i+2], 16) for i in (0, 2, 4))
    except (ValueError, TypeError):
        return (260, 260, 260)



def _rgb_to_hex(r: int, g: int, b: int) -> str:
    """Convert RGB values to hex string."""
    if r > 255 or g > 255 or b > 255:
        return "#xxxxxx"
    return f"#{r:02x}{g:02x}{b:02x}"


async def _find_nearest_color_knn(hex_code: str, knn_model, colors_data: list[dict]) -> dict | None:
    """
    Find nearest color using KNN. Returns API color object { id, name: {en, ar}, hex, ... } if similarity >= threshold, else None.
    """
    try:
        rgb = _hex_to_rgb(hex_code)
        distances, indices = knn_model.kneighbors([list(rgb)], n_neighbors=1)
        dist_val = float(distances[0][0])
        idx = int(indices[0][0])
        matched_color = colors_data[idx] if 0 <= idx < len(colors_data) else None
        if not matched_color:
            return None
        name = matched_color.get("name", {}).get("en", "")
        matched_hex = matched_color.get("hex", hex_code)
        max_dist = math.sqrt(3 * (255**2))
        similarity = (1 - (dist_val / max_dist)) * 100
        logger.info("[color_utils] KNN match for %s: %s (%.2f%% similarity)", hex_code, name, similarity)
        if similarity >= KNN_SIMILARITY_THRESHOLD:
            return matched_color
        logger.info("[color_utils] KNN similarity %.2f%% below threshold %.2f%%, will create new entry",
                   similarity, KNN_SIMILARITY_THRESHOLD)
        return None
    except Exception as e:
        logger.error("[color_utils] KNN matching failed: %s", e, exc_info=True)
        return None


async def _create_color_in_db(name_en: str, name_ar: str, hex_code: str) -> dict | None:
    """
    Create new color via API POST. Returns API color object { id, name: {en, ar}, hex, r, g, b } or None.
    If color already exists (409), fetches and returns the existing color data.
    """
    if not _colors_api_base() or _colors_api_base() == "/colors":
        logger.error("[color_utils] EXPRESS_API_URL not configured")
        return None
    try:
        rgb = _hex_to_rgb(hex_code)
        url = _colors_api_base()
        payload = {
            "name": {"en": (name_en or "").strip(), "ar": (name_ar or "").strip()},
            "hex": hex_code,
            "r": rgb[0],
            "g": rgb[1],
            "b": rgb[2],
        }
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            response = await client.post(url, json=payload)
            response.raise_for_status()
            data = response.json()
            if data.get("success"):
                created_color = data.get("data")
                if created_color and isinstance(created_color, dict):
                    logger.info("[color_utils] Created new color via API: %s / %s -> %s", name_en, name_ar, hex_code)
                    return created_color
            logger.warning("[color_utils] API POST returned success=false for %s", name_en)
            return None
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 409:
            logger.info("[color_utils] Color already exists (409 Conflict), fetching existing color: %s", name_en)
            # Fetch the existing color by the name we just tried to create
            existing_color = await _get_color_from_api(name_en)
            if existing_color:
                logger.info(
                    "[color_utils] Using existing color: %s (hex: %s)",
                    name_en,
                    existing_color.get("hex")
                )
                return existing_color
            logger.warning("[color_utils] Could not fetch existing color after 409: %s", name_en)
            return None
        logger.error("[color_utils] HTTP error creating color: %s", e)
        return None
    except Exception as e:
        logger.error("[color_utils] Failed to create color via API: %s", e, exc_info=True)
        return None

def _api_color_to_result(c: dict, source_color: str) -> dict:
    """Build result from API color object: { id, name: {en, ar}, hex, r, g, b }."""
    name = c.get("name") or {}
    en = (name.get("en") or "").strip() or source_color
    ar = (name.get("ar") or "").strip()
    hex_val = c.get("hex") or ""
    if isinstance(hex_val, str) and hex_val and not hex_val.startswith("#"):
        hex_val = "#" + hex_val
    return {"color": en, "color_ar": ar, "color_hex": hex_val or DEFAULT_HEX_FALLBACK, "source_color": source_color}


async def _get_color_from_api(color_name: str) -> dict | None:
    """Fetch color from API by slug. Returns API color object { id, name: {en, ar}, hex, r, g, b } or None."""
    if not _colors_api_base() or _colors_api_base() == "/colors":
        logger.error("[color_utils] EXPRESS_API_URL not configured")
        return None
    slug = re.sub(r"[^a-z0-9_-]", "", re.sub(r"\s+", "_", (color_name or "").strip().lower())) or "default"
    url = f"{_colors_api_base()}/{quote(slug, safe='')}"

    async def fetch():
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as c:
            r = await c.get(url)
            if r.status_code == 404:
                return None
            r.raise_for_status()
            if not r.text or not r.text.strip():
                return None
            try:
                data = r.json()
            except ValueError:
                return None
            if not data.get("success"):
                return None
            field = data.get("data")
            d = field[0] if isinstance(field, list) and field else (field if isinstance(field, dict) else None)
            if not d or not isinstance(d, dict):
                return None
            raw = d.get("hex") or d.get("hex_id")
            if not raw or not isinstance(raw, str):
                return None
            d["hex"] = raw.strip() if raw.strip().startswith("#") else f"#{raw.strip()}"
            return d

    try:
        result = await _with_retry((httpx.HTTPError, httpx.TimeoutException), fetch)
        if result:
            logger.info("[color_utils] API found color by slug '%s': %s", color_name, result.get("hex"))
            return result
        logger.info("[color_utils] Color '%s' not found in API", color_name)
    except RetryError as e:
        logger.error("[color_utils] API fetch for '%s' failed after %d retries: %s", color_name, MAX_RETRIES, e)
    except Exception as e:
        logger.error("[color_utils] API fetch for '%s' error: %s", color_name, e)
    return None


async def _fetch_and_resize_image_for_gemini(
    image_url: str,
    max_width: int = 200,
    max_height: int = 200,
) -> tuple[bytes, str] | None:
    """Fetch and resize image from URL for Gemini usage."""

    async def fetch_image():
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as c:
            r = await c.get(image_url, follow_redirects=True)
            r.raise_for_status()
            if not r.content:
                raise ValueError(f"Empty image: {image_url}")
            ct = (r.headers.get("content-type") or "").split(";")[0].strip()
            if ct not in ("image/jpeg", "image/jpg", "image/webp", "image/png", "image/gif"):
                ct = "image/jpeg"
            return r.content, ct

    try:
        image_bytes, content_type = await _with_retry(
            (httpx.HTTPError, httpx.TimeoutException, httpx.ConnectError),
            fetch_image,
        )
    except RetryError as e:
        logger.error("[color_utils] Image fetch for Gemini failed after %d retries: %s", MAX_RETRIES, e)
        return None

    # Resize image
    try:
        img = Image.open(io.BytesIO(image_bytes))
        img = img.convert("RGB")
        original_width, original_height = img.size
        aspect_ratio = original_width / original_height

        if aspect_ratio > 1:
            if original_width > max_width:
                new_width = max_width
                new_height = int(max_width / aspect_ratio)
            else:
                new_width, new_height = original_width, original_height
        else:
            if original_height > max_height:
                new_height = max_height
                new_width = int(max_height * aspect_ratio)
            else:
                new_width, new_height = original_width, original_height

        resized_image = img.resize((new_width, new_height), Image.LANCZOS)
        final_image = Image.new("RGB", (max_width, max_height), (255, 255, 255))
        paste_position = ((max_width - new_width) // 2, (max_height - new_height) // 2)
        final_image.paste(resized_image, paste_position)

        buf = io.BytesIO()
        final_image.save(buf, format="JPEG", quality=85)
        return buf.getvalue(), "image/jpeg"
    except Exception as e:
        logger.warning("[color_utils] Image resize for Gemini failed, using original: %s", e)
        return image_bytes, content_type


def _parse_gemini_hex_response(text: str) -> str | None:
    """Parse Gemini response: extract single hex code #RRGGBB or #RGB. Returns None on failure."""
    if not text or not text.strip():
        return None
    s = text.strip()
    match = re.search(r"#([0-9A-Fa-f]{3}|[0-9A-Fa-f]{6})\b", s)
    if match:
        hex_part = match.group(0)
        return _normalize_hex(hex_part) or hex_part
    return None


async def _generate_hex_with_gemini(product_title: str, image_url: str, source_color: str) -> str | None:
    """Generate primary color hex only using Gemini. Returns #RRGGBB or None."""
    if not image_url:
        return None

    try:
        client = get_gemini_client()
        image_data = await _fetch_and_resize_image_for_gemini(image_url)
        if not image_data:
            return None

        image_bytes, content_type = image_data
        text = await client.generate_content(
            prompt="Return only the primary color as #RRGGBB.",
            system_instruction=GEMINI_HEX_SYSTEM_PROMPT,
            model=GEMINI_COLOR_MODEL,
            temperature=0.1,
            image_data=(image_bytes, content_type),
        )
        if not text:
            return None

        parsed = _parse_gemini_hex_response(text)
        if not parsed:
            return None

        logger.info("[color_utils] Gemini hex: %s", parsed)
        return parsed
    except Exception as e:
        logger.error("[color_utils] Gemini hex generation failed: %s", e, exc_info=True)
        return None


async def _generate_color_names_with_gemini(hex_code: str) -> tuple[str, str] | None:
    """Generate common color name (en) and name_ar from hex. Returns (en, ar) or None."""
    if not hex_code:
        return None

    try:
        client = get_gemini_client()
        text = await client.generate_content(
            prompt=f"Hex color: {hex_code}. Output one line: EnglishName | ArabicName. Common names only.",
            system_instruction=GEMINI_NAMES_SYSTEM_PROMPT,
            model=GEMINI_COLOR_MODEL,
            temperature=0.1,
        )
        if not text or "|" not in text:
            return None

        en, ar = text.split("|", 1)
        en, ar = en.strip(), ar.strip()
        if not en or not ar:
            return None

        logger.info("[color_utils] Gemini names for %s: en=%s ar=%s", hex_code, en, ar)
        return en, ar
    except Exception as e:
        logger.error("[color_utils] Gemini names generation failed: %s", e)
        return None


def _color_result(name_en: str, name_ar: str, color_hex: str, source_color: str) -> dict:
    """Build result dict: color (en), color_ar (ar), color_hex, source_color."""
    return {
        "color": (name_en or source_color).strip(),
        "color_ar": (name_ar or "").strip(),
        "color_hex": color_hex or DEFAULT_HEX_FALLBACK,
        "source_color": source_color,
    }


async def _resolve_single_color(
    source_color: str,
    image_url: str,
    product_title: str,
    knn_model,
    colors_data: list[dict] | None,
) -> dict:
    """
    Resolve color from API or generate. source_color = exact extracted label (unchanged).
    Returns { color, color_ar, color_hex, source_color } with name.en/ar and hex from API or created.
    """
    # Step 1: API lookup by source_color slug
    api_result = await _get_color_from_api(source_color)
    if api_result is not None:
        return _api_color_to_result(api_result, source_color)

    # Step 2: Gemini — hex only
    gemini_hex = await _generate_hex_with_gemini(product_title, image_url, source_color)
    if not gemini_hex:
        logger.warning("[color_utils] Gemini hex failed for '%s', using fallback", source_color)
        return _color_result(source_color, "", DEFAULT_HEX_FALLBACK, source_color)

    # Step 3: KNN — closest color from API (name.en, name.ar, hex)
    if knn_model is not None and colors_data is not None:
        knn_color = await _find_nearest_color_knn(gemini_hex, knn_model, colors_data)
        if knn_color is not None:
            logger.info("[color_utils] Using KNN match for '%s'", source_color)
            return _api_color_to_result(knn_color, source_color)

    # Step 4: Generate name (en) and name_ar with Gemini, then create in DB
    names = await _generate_color_names_with_gemini(gemini_hex)
    if names:
        name_en, name_ar = names
        created_color = await _create_color_in_db(name_en, name_ar, gemini_hex)
        if created_color:
            await _retrain_and_save_knn()
            # Use the actual color data from DB (handles 409 case where existing color is returned)
            return _api_color_to_result(created_color, source_color)
        # Fallback if creation failed
        return _color_result(name_en, name_ar, gemini_hex, source_color)
    
    # Fallback: use source_color as name
    created_color = await _create_color_in_db(source_color, "", gemini_hex)
    if created_color:
        await _retrain_and_save_knn()
        return _api_color_to_result(created_color, source_color)
    return _color_result(source_color, "", gemini_hex, source_color)


async def get_color_hexes_from_variants(
    product_title: str, color_to_image: Dict[str, str]
) -> Dict[str, dict]:
    """
    Resolve color per source_color. source_color = exact extracted label (unchanged).
    Returns source_color -> { color (en), color_ar (ar), color_hex, source_color } from API or created.
    """
    if not color_to_image:
        logger.warning("[color_utils] No color->image map provided")
        return {}

    title = (product_title or "").strip() or "Product"
    knn_model, colors_data = await _get_trained_knn()

    tasks = [
        _resolve_single_color(source_color, image_url, title, knn_model, colors_data)
        for source_color, image_url in color_to_image.items()
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    out: Dict[str, dict] = {}
    keys = list(color_to_image.keys())
    for i, source_color in enumerate(keys):
        r = results[i] if i < len(results) else None
        if isinstance(r, dict) and "color" in r and "color_hex" in r and "source_color" in r:
            out[source_color] = r
        else:
            if isinstance(r, Exception):
                logger.error("[color_utils] Color '%s' failed: %s", source_color, r)
            out[source_color] = {
                "color": source_color,
                "color_ar": "",
                "color_hex": DEFAULT_HEX_FALLBACK,
                "source_color": source_color,
            }

    ok = sum(1 for v in out.values() if v.get("color_hex") != DEFAULT_HEX_FALLBACK)
    logger.info("[color_utils] Completed: %d/%d colors successfully resolved", ok, len(out))
    return out

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    asyncio.run(get_color_hexes_from_variants("Angelica Floral Applique Short Sleeve Top & Skirt Set", {"Color1": "https://f.nooncdn.com/p/pzsku/Z945DE41B94ED003F3C21Z/45/1752666297/3d7ab47f-91c7-40fc-80f5-adb80e4ff38f.jpg?format=webp&width=800"}))