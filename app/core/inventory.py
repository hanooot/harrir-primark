"""Inventory counts must come from the source, never from availability flags."""
import asyncio
import logging
from decimal import Decimal, InvalidOperation

logger = logging.getLogger(__name__)


def source_quantity(value, context="source inventory"):
    """Keep a supplied nonnegative integer count; warn and use zero otherwise."""
    try:
        if value is None or isinstance(value, bool) or str(value).strip() == "":
            raise ValueError("missing count")
        number = Decimal(str(value))
        if not number.is_finite() or number < 0 or number != number.to_integral_value():
            raise ValueError("invalid count")
        return int(number)
    except (ValueError, TypeError, InvalidOperation):
        logger.warning("%s: inventory quantity missing or invalid; defaulting to 0", context)
        return 0


async def mark_missing_variants(variants, patch_product, task_id, queue_update=None):
    """A missing size is out of stock, retaining its publication status and price."""
    updated = 0
    for variant in variants:
        asin = variant.get("ASIN")
        if not asin:
            continue
        if ((variant.get("stock") or {}).get("quantity") == 0
                and (variant.get("availability") or {}).get("quantity") == 0
                and (variant.get("availability") or {}).get("status") == "Out of Stock"):
            continue
        payload = {"data": {"SKU": variant.get("SKU"), "stock": {"quantity": 0},
                            "availability": {"status": "Out of Stock", "quantity": 0}}}
        try:
            success, error = await asyncio.to_thread(patch_product, asin, payload)
        except Exception as exc:
            success, error = False, str(exc)
        if success:
            updated += 1
            if queue_update:
                queue_update(asin.rsplit("-", 1)[0])
            logger.info("[%s] %s: missing source variant marked Out of Stock", task_id, asin)
        else:
            logger.error("[%s] %s: missing variant update failed: %s", task_id, asin, error)
    return updated


class SourceNotFound(Exception):
    """A product request returned a confirmed HTTP 404."""
