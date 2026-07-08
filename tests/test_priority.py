from datetime import timedelta

import pytest
from django.conf import settings
from django.utils import timezone

from jobs import repository

pytestmark = pytest.mark.django_db


def _due_job(recipient, priority_name):
    job, _ = repository.insert_job(
        recipient=recipient, channel="email", payload={},
        priority=settings.PRIORITY_MAP[priority_name],
        send_at=timezone.now(),
        max_attempts=5, idempotency_key=None, webhook_url=None,
    )
    return job


def test_high_priority_claimed_before_low_priority_even_if_older():
    low = _due_job("low@example.com", "low")
    high = _due_job("high@example.com", "high")  # created after `low`, but should still win

    claimed = repository.claim_due_jobs(worker_id="w1", batch_size=1, visibility_timeout_seconds=30)

    assert len(claimed) == 1
    assert claimed[0].id == high.id


def test_full_priority_ordering():
    ids_in_creation_order = []
    for name in ["low", "normal", "critical", "high"]:
        job = _due_job(f"{name}@example.com", name)
        ids_in_creation_order.append((name, job.id))

    claimed_order = []
    while True:
        batch = repository.claim_due_jobs(worker_id="w1", batch_size=1, visibility_timeout_seconds=30)
        if not batch:
            break
        claimed_order.append(batch[0].id)

    expected = [job_id for name, job_id in sorted(
        ids_in_creation_order, key=lambda t: settings.PRIORITY_MAP[t[0]], reverse=True
    )]
    assert claimed_order == expected


def test_same_priority_falls_back_to_earliest_due_first():
    older, _ = repository.insert_job(
        recipient="a@example.com", channel="email", payload={}, priority=settings.PRIORITY_MAP["normal"],
        send_at=timezone.now() - timedelta(seconds=5),
        max_attempts=5, idempotency_key=None, webhook_url=None,
    )
    repository.insert_job(
        recipient="b@example.com", channel="email", payload={}, priority=settings.PRIORITY_MAP["normal"],
        send_at=timezone.now(),
        max_attempts=5, idempotency_key=None, webhook_url=None,
    )

    claimed = repository.claim_due_jobs(worker_id="w1", batch_size=1, visibility_timeout_seconds=30)
    assert claimed[0].id == older.id
