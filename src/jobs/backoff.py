import random

from django.conf import settings


def compute_backoff_seconds(attempt_number: int) -> float:
    """Exponential backoff with full jitter, capped.

    attempt_number is the count of attempts made so far (1 = first failure).
    delay = min(cap, base * 2^(attempt_number - 1)), then +/- jitter% applied
    to avoid a thundering herd of retries all landing on the same instant.
    """
    raw = settings.BACKOFF_BASE_SECONDS * (2 ** (attempt_number - 1))
    delay = min(settings.BACKOFF_CAP_SECONDS, raw)
    jitter = delay * settings.BACKOFF_JITTER
    return max(0.0, delay + random.uniform(-jitter, jitter))
