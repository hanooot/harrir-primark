import asyncio
import logging
import os
from typing import Tuple

import httpx
from dotenv import load_dotenv
from google import genai
from google.genai import types as genai_types
from tenacity import (
    AsyncRetrying,
    RetryError,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    before_sleep_log,
)


load_dotenv()
logger = logging.getLogger(__name__)


# ============================================================================
# Configuration
# ============================================================================

MAX_RETRIES = 5
RETRY_WAIT = wait_exponential(multiplier=1, min=1, max=8)
RETRYABLE_EXCEPTIONS: Tuple[type, ...] = (
    httpx.HTTPError,
    httpx.TimeoutException,
    httpx.ConnectError,
    OSError,
    ConnectionError,
)
DEFAULT_MODEL = "gemini-2.5-flash-lite"


# ============================================================================
# Generic Gemini Client with Multi-Key Support and Auto-Fallback
# ============================================================================

class GeminiClient:
    """
    Generic Gemini client with multiple API key support and automatic fallback.
    
    When one API key gets throttled or fails, automatically tries the next available key.
    Supports text-only and image+text generation with customizable prompts and models.
    """
    
    def __init__(self):
        """
        Initialize Gemini client with multiple API keys loaded from environment.

        Reads a single comma-separated env var GEMINI_KEYS, e.g.:
        GEMINI_KEYS="key1,key2,key3"
        """
        raw = (os.getenv("GEMINI_KEYS") or "").strip()
        self.api_keys = [k.strip() for k in raw.split(",") if k.strip()]
        # Start with the first configured key as primary; this will change
        # dynamically to the last successful key.
        self.current_key_index: int = 0

        if not self.api_keys:
            raise RuntimeError("At least one Gemini API key must be provided")
        
        logger.info("[gemini] Initialized with %d API key(s)", len(self.api_keys))
    
    async def _execute_with_fallback(self, coro_fn):
        """
        Try available API keys in a round-robin fashion until one succeeds
        or we exhaust MAX_RETRIES attempts in total.

        The key that last successfully served a request becomes the new
        "primary" and will be tried first for subsequent calls.
        """
        if not self.api_keys:
            raise RuntimeError("No Gemini API keys configured")

        key_count = len(self.api_keys)
        # Snapshot the primary index at the start of this call so concurrent
        # calls don't interfere with each other's key ordering.
        start_index = self.current_key_index
        last_error: Exception | None = None

        async for attempt in AsyncRetrying(
            retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS),
            stop=stop_after_attempt(MAX_RETRIES),
            wait=RETRY_WAIT,
            before_sleep=before_sleep_log(logger, logging.WARNING),
            reraise=True,
        ):
            with attempt:
                # attempt_number is 1-based (1=first try, 2=first retry, ...).
                # offset 0=primary, 1=next key, etc. — first retry uses next key, not same key.
                attempt_number = attempt.retry_state.attempt_number
                offset = attempt_number - 1
                key_index = (start_index + offset) % key_count
                api_key = self.api_keys[key_index]
                key_number = key_index + 1

                logger.info(
                    "[gemini] Attempt %d using key %d/%d",
                    attempt_number,
                    key_number,
                    key_count,
                )
                try:
                    result = await coro_fn(api_key)
                    # Promote this key to be primary for future calls.
                    self.current_key_index = key_index
                    return result
                except Exception as e:  # pragma: no cover - handled by tenacity
                    last_error = e
                    raise

        # If we somehow exit the retry loop without returning, raise the last error
        raise last_error or Exception("All Gemini attempts failed")
    
    async def generate_content(
        self,
        prompt: str,
        system_instruction: str | None = None,
        model: str = DEFAULT_MODEL,
        temperature: float = 0.1,
        image_data: tuple[bytes, str] | None = None,
    ) -> str | None:
        """
        Generate content using Gemini API with automatic key fallback.
        
        Args:
            prompt: User prompt/question
            system_instruction: Optional system instruction to guide model behavior
            model: Gemini model to use
            temperature: Generation temperature (0.0-1.0)
            image_data: Optional tuple of (image_bytes, mime_type) for vision tasks
        
        Returns:
            Generated text or None on failure
        """
        async def call_gemini(api_key: str) -> str:
            client = genai.Client(api_key=api_key)
            
            # Build contents
            contents = []
            if image_data:
                image_bytes, mime_type = image_data
                part = genai_types.Part.from_bytes(data=image_bytes, mime_type=mime_type)
                contents.append(part)
            contents.append(prompt)
            
            # Build config
            config = genai_types.GenerateContentConfig(temperature=temperature)
            if system_instruction:
                config.system_instruction = system_instruction
            
            # Call API
            resp = await asyncio.to_thread(
                client.models.generate_content,
                model=model,
                contents=contents,
                config=config,
            )
            
            if not resp or not resp.text:
                raise ValueError("Empty Gemini response")
            
            return resp.text.strip()
        
        try:
            return await self._execute_with_fallback(call_gemini)
        except Exception as e:
            logger.error("[gemini] Content generation failed: %s", e)
            return None


# ============================================================================
# Global Singleton Instance
# ============================================================================

_gemini_client: GeminiClient | None = None


def get_gemini_client() -> GeminiClient:
    """Get or create the global Gemini client instance."""
    global _gemini_client
    if _gemini_client is None:
        _gemini_client = GeminiClient()
    return _gemini_client

