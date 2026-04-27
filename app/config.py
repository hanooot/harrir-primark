"""
Centralized configuration with stage/prod environment switching.

Uses contextvars so the environment is set once at the request/job entry point
and automatically available everywhere downstream — no parameter threading needed.

Usage:
    from app.config import get_config, set_environment

    # At entry point (API handler, worker job):
    set_environment("stage")

    # Anywhere downstream:
    cfg = get_config()
    cfg.BASE_URL, cfg.AWS_STORAGE_BUCKET_NAME, etc.
"""

import contextvars
import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Context variable — holds the active environment for the current request/task
# ---------------------------------------------------------------------------
_current_env: contextvars.ContextVar[str] = contextvars.ContextVar(
    "environment", default=os.getenv("DEFAULT_ENVIRONMENT", "prod").strip().lower(),
)


def set_environment(env: str) -> None:
    """Set the active environment for the current context (request/thread/task)."""
    _current_env.set(env.strip().lower())


def get_environment() -> str:
    """Get the active environment for the current context."""
    return _current_env.get()


# ---------------------------------------------------------------------------
# Config dataclass
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class EnvConfig:
    BASE_URL: str
    EXPRESS_API_URL: str
    AWS_ACCESS_KEY_ID: str
    AWS_SECRET_ACCESS_KEY: str
    AWS_STORAGE_BUCKET_NAME: str
    AWS_REGION: str
    GEMINI_KEYS: str
    AUTOMATION_BACKEND_URL: str
    API_KEY: str


_SHARED = dict(
    GEMINI_KEYS=os.getenv("GEMINI_KEYS", ""),
    AUTOMATION_BACKEND_URL=os.getenv("AUTOMATION_BACKEND_URL", ""),
    API_KEY=os.getenv("API_KEY", ""),
)

_CONFIGS = {
    "stage": EnvConfig(
        BASE_URL=os.getenv("BASE_URL_STAGE", ""),
        EXPRESS_API_URL=os.getenv("EXPRESS_API_URL_STAGE", ""),
        AWS_ACCESS_KEY_ID=os.getenv("AWS_ACCESS_KEY_ID_STAGE", ""),
        AWS_SECRET_ACCESS_KEY=os.getenv("AWS_SECRET_ACCESS_KEY_STAGE", ""),
        AWS_STORAGE_BUCKET_NAME=os.getenv("AWS_STORAGE_BUCKET_NAME_STAGE", ""),
        AWS_REGION=os.getenv("AWS_REGION_STAGE", ""),
        **_SHARED,
    ),
    "prod": EnvConfig(
        BASE_URL=os.getenv("BASE_URL_PROD", ""),
        EXPRESS_API_URL=os.getenv("EXPRESS_API_URL_PROD", ""),
        AWS_ACCESS_KEY_ID=os.getenv("AWS_ACCESS_KEY_ID_PROD", ""),
        AWS_SECRET_ACCESS_KEY=os.getenv("AWS_SECRET_ACCESS_KEY_PROD", ""),
        AWS_STORAGE_BUCKET_NAME=os.getenv("AWS_STORAGE_BUCKET_NAME_PROD", ""),
        AWS_REGION=os.getenv("AWS_REGION_PROD", ""),
        **_SHARED,
    ),
}


def get_config() -> EnvConfig:
    """Return the config for the current environment (from context)."""
    env = _current_env.get()
    if env not in _CONFIGS:
        raise ValueError(f"Unknown environment '{env}'. Must be 'stage' or 'prod'.")
    return _CONFIGS[env]
