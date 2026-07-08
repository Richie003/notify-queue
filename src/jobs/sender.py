"""Stub notification sender. Simulates a real email/SMS/push provider call
with latency and a configurable random failure rate (FAILURE_RATE)."""
import random
import time

from django.conf import settings


class SendError(Exception):
    pass


def mock_send(*, channel: str, recipient: str, payload: dict) -> None:
    latency_ms = random.uniform(settings.SEND_LATENCY_MIN_MS, settings.SEND_LATENCY_MAX_MS)
    time.sleep(latency_ms / 1000.0)

    if random.random() < settings.FAILURE_RATE:
        raise SendError(f"simulated {channel} provider failure for {recipient}")
