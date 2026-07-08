import pytest
from django.conf import settings
from django.utils import timezone

from jobs import repository
from jobs.management.commands.worker import Worker
from jobs.models import Job

pytestmark = pytest.mark.django_db


@pytest.fixture
def no_webhooks(monkeypatch):
    """Webhook delivery isn't under test here; stub it out so we don't make
    real HTTP calls to the (not running, in this test process) API."""
    monkeypatch.setattr("jobs.management.commands.worker.fire_webhook", lambda job, status: None)


def _insert_due_job(recipient):
    job, _ = repository.insert_job(
        recipient=recipient, channel="email", payload={}, priority=settings.PRIORITY_MAP["normal"],
        send_at=timezone.now(), max_attempts=5, idempotency_key=None, webhook_url=None,
    )
    return job


def test_jobs_over_the_hourly_cap_are_requeued_not_failed(monkeypatch, no_webhooks):
    monkeypatch.setattr(settings, "RATE_LIMIT_PER_RECIPIENT_PER_HOUR", 2)
    recipient = "capped@example.com"

    # simulate 2 jobs already sent to this recipient within the last hour
    for _ in range(2):
        job = _insert_due_job(recipient)
        repository.mark_sent(job, worker_id="setup")

    third = _insert_due_job(recipient)
    claimed = repository.claim_due_jobs(worker_id="w1", batch_size=1, visibility_timeout_seconds=30)
    assert claimed[0].id == third.id

    send_was_called = False

    def _fake_send(**kwargs):
        nonlocal send_was_called
        send_was_called = True

    monkeypatch.setattr("jobs.management.commands.worker.mock_send", _fake_send)

    Worker("w1").process(claimed[0])

    assert send_was_called is False, "must not attempt delivery once the recipient's cap is hit"

    refreshed = Job.objects.get(id=third.id)
    assert refreshed.status == "pending"   # requeued, not failed
    assert refreshed.attempts == 0         # not counted as a failed attempt
    assert refreshed.next_attempt_at > refreshed.send_at

    events = [e.event_type for e in refreshed.events.all()]
    assert "rate_limited" in events
    assert "failed" not in events


def test_jobs_under_the_cap_are_delivered_normally(monkeypatch, no_webhooks):
    monkeypatch.setattr(settings, "RATE_LIMIT_PER_RECIPIENT_PER_HOUR", 5)
    monkeypatch.setattr("jobs.management.commands.worker.mock_send", lambda **kwargs: None)  # always succeeds

    job = _insert_due_job("plenty-of-room@example.com")
    claimed = repository.claim_due_jobs(worker_id="w1", batch_size=1, visibility_timeout_seconds=30)

    Worker("w1").process(claimed[0])

    refreshed = Job.objects.get(id=job.id)
    assert refreshed.status == "sent"
