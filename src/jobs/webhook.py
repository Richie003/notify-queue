"""Outbound webhook notifications. Fired by the worker whenever a job's
status changes to sent / failed / dead_letter, per the spec.

Kept deliberately simple: one attempt, short timeout, failure is logged
(via WebhookDelivery) but never raised -- a flaky webhook receiver must not
be able to stall job processing or cause a job to be retried/re-sent just
because the callback didn't land. See DESIGN.md for how this would be
hardened (its own retry queue) in production.
"""
import logging

import requests
from django.conf import settings

from .models import Job, WebhookDelivery

logger = logging.getLogger("notify_queue.webhook")


def fire_webhook(job: Job, status: str) -> None:
    url = job.webhook_url or settings.DEFAULT_WEBHOOK_URL
    body = {
        "job_id": str(job.id),
        "status": status,
        "recipient": job.recipient,
        "channel": job.channel,
        "attempts": job.attempts,
        "last_error": job.last_error,
    }
    http_status = None
    success = False
    error = None
    try:
        resp = requests.post(url, json=body, timeout=settings.WEBHOOK_TIMEOUT_SECONDS)
        http_status = resp.status_code
        success = resp.ok
    except requests.RequestException as exc:
        error = str(exc)
        logger.warning("webhook delivery to %s failed for job %s: %s", url, job.id, exc)

    WebhookDelivery.objects.create(
        job=job, reported_status=status, url=url,
        http_status=http_status, success=success, error=error,
    )
