"""Django settings for the Notify Queue project.

Deliberately lean: this is a pure JSON API (no templates, sessions, auth, or
admin UI needed for the exercise), so INSTALLED_APPS / MIDDLEWARE only carry
what rest_framework and the jobs app actually need.
"""
import os
from pathlib import Path

import dj_database_url
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR.parent / ".env")

SECRET_KEY = os.environ.get("SECRET_KEY", "django-insecure-notify-queue-dev-key-not-for-production")
DEBUG = os.environ.get("DEBUG", "true").lower() == "true"
ALLOWED_HOSTS = os.environ.get("ALLOWED_HOSTS", "*").split(",")

INSTALLED_APPS = [
    "django.contrib.contenttypes",
    "django.contrib.auth",  # rest_framework imports auth.models.Permission at load time
    "django.contrib.staticfiles",  # required by DRF's browsable API renderer
    "rest_framework",
    "jobs",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.middleware.common.CommonMiddleware",
]

ROOT_URLCONF = "config.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {"context_processors": ["django.template.context_processors.request"]},
    },
]

WSGI_APPLICATION = "config.wsgi.application"

DATABASES = {
    "default": dj_database_url.parse(
        os.environ.get("DATABASE_URL", "postgresql://notify:notify@localhost:5432/notify_queue"),
        conn_max_age=0,
    )
}

USE_I18N = False
USE_TZ = True
TIME_ZONE = "UTC"

STATIC_URL = "static/"
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

REST_FRAMEWORK = {
    "DEFAULT_RENDERER_CLASSES": ["rest_framework.renderers.JSONRenderer"],
    "DEFAULT_PARSER_CLASSES": ["rest_framework.parsers.JSONParser"],
    "DEFAULT_PAGINATION_CLASS": None,
}

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "handlers": {"console": {"class": "logging.StreamHandler"}},
    "formatters": {"simple": {"format": "%(asctime)s %(levelname)s %(name)s: %(message)s"}},
    "root": {"handlers": ["console"], "level": "INFO"},
    "loggers": {
        "django": {"handlers": ["console"], "level": "WARNING", "propagate": False},
        "notify_queue": {"handlers": ["console"], "level": "INFO", "propagate": False},
    },
}
for _handler in LOGGING["handlers"].values():
    _handler["formatter"] = "simple"


def _int(name: str, default: int) -> int:
    return int(os.environ.get(name, default))


def _float(name: str, default: float) -> float:
    return float(os.environ.get(name, default))


# --- Notify Queue application settings -------------------------------------

# Worker
POLL_INTERVAL_SECONDS = _float("POLL_INTERVAL_SECONDS", 1.0)
CLAIM_BATCH_SIZE = _int("CLAIM_BATCH_SIZE", 10)
VISIBILITY_TIMEOUT_SECONDS = _int("VISIBILITY_TIMEOUT_SECONDS", 30)

# Mock delivery
FAILURE_RATE = _float("FAILURE_RATE", 0.3)
SEND_LATENCY_MIN_MS = _int("SEND_LATENCY_MIN_MS", 20)
SEND_LATENCY_MAX_MS = _int("SEND_LATENCY_MAX_MS", 150)

# Retry / backoff
DEFAULT_MAX_ATTEMPTS = _int("DEFAULT_MAX_ATTEMPTS", 5)
BACKOFF_BASE_SECONDS = _float("BACKOFF_BASE_SECONDS", 2.0)
BACKOFF_CAP_SECONDS = _float("BACKOFF_CAP_SECONDS", 300.0)
BACKOFF_JITTER = _float("BACKOFF_JITTER", 0.2)

# Rate limiting
RATE_LIMIT_PER_RECIPIENT_PER_HOUR = _int("RATE_LIMIT_PER_RECIPIENT_PER_HOUR", 5)
RATE_LIMIT_RECHECK_SECONDS = _float("RATE_LIMIT_RECHECK_SECONDS", 15.0)

# Webhooks
# NOTE: trailing slash matters -- Django's APPEND_SLASH redirect turns a
# POST without it into a GET on redirect, which would silently no-op the
# webhook call instead of hitting MockWebhookView.post().
DEFAULT_WEBHOOK_URL = os.environ.get("DEFAULT_WEBHOOK_URL", "http://localhost:8000/webhooks/mock/")
WEBHOOK_TIMEOUT_SECONDS = _float("WEBHOOK_TIMEOUT_SECONDS", 5.0)

PRIORITY_MAP = {"low": 1, "normal": 5, "high": 8, "critical": 10}
PRIORITY_NAMES = {v: k for k, v in PRIORITY_MAP.items()}
