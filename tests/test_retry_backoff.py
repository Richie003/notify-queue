import pytest
from django.conf import settings
from django.utils import timezone

from jobs import repository
from jobs.backoff import compute_backoff_seconds
from jobs.management.commands.worker import Worker
from jobs.models import Job
from jobs.sender import SendError

pytestmark = pytest.mark.django_db


@pytest.fixture
def no_webhooks(monkeypatch):
    monkeypatch.setattr("jobs.management.commands.worker.fire_webhook", lambda job, status: None)


def _always_fails(**kwargs):
    raise SendError("simulated provider outage")


def _insert_due_job(max_attempts=3):
    job, _ = repository.insert_job(
        recipient="flaky@example.com", channel="email", payload={}, priority=settings.PRIORITY_MAP["normal"],
        send_at=timezone.now(), max_attempts=max_attempts,
        idempotency_key=None, webhook_url=None,
    )
    return job


def test_failed_attempt_schedules_retry_with_backoff(monkeypatch, no_webhooks):
    monkeypatch.setattr("jobs.management.commands.worker.mock_send", _always_fails)
    job = _insert_due_job(max_attempts=5)

    claimed = repository.claim_due_jobs(worker_id="w1", batch_size=1, visibility_timeout_seconds=30)[0]
    before = timezone.now()
    Worker("w1").process(claimed)

    refreshed = Job.objects.get(id=job.id)
    assert refreshed.status == "pending"        # back in the queue
    assert refreshed.attempts == 1
    assert refreshed.last_error == "simulated provider outage"
    assert refreshed.next_attempt_at > before    # pushed into the future, not immediately due

    events = [e.event_type for e in refreshed.events.all()]
    assert events == ["created", "claimed", "failed"]


def test_backoff_grows_between_successive_retries(monkeypatch, no_webhooks):
    monkeypatch.setattr("jobs.management.commands.worker.mock_send", _always_fails)
    job = _insert_due_job(max_attempts=10)

    delays = []
    for _ in range(3):
        # force the job due immediately for this test iteration
        Job.objects.filter(id=job.id).update(next_attempt_at=timezone.now())

        before = timezone.now()
        claimed = repository.claim_due_jobs(worker_id="w1", batch_size=1, visibility_timeout_seconds=30)[0]
        Worker("w1").process(claimed)
        refreshed = Job.objects.get(id=job.id)
        delays.append((refreshed.next_attempt_at - before).total_seconds())

    # exponential backoff (base=2s by default) means each successive delay
    # should trend upward; allow for jitter by comparing against the
    # unjittered theoretical floor instead of a strict a < b < c chain.
    assert delays[1] > delays[0] * 0.5
    assert delays[2] > delays[1]


def test_exceeding_max_attempts_moves_job_to_dead_letter(monkeypatch, no_webhooks):
    monkeypatch.setattr("jobs.management.commands.worker.mock_send", _always_fails)
    monkeypatch.setattr(settings, "BACKOFF_BASE_SECONDS", 0.0)  # keep the test fast
    monkeypatch.setattr(settings, "BACKOFF_CAP_SECONDS", 0.0)

    job = _insert_due_job(max_attempts=3)

    for attempt in range(1, 4):
        Job.objects.filter(id=job.id).update(next_attempt_at=timezone.now())
        claimed = repository.claim_due_jobs(worker_id="w1", batch_size=1, visibility_timeout_seconds=30)[0]
        Worker("w1").process(claimed)

        refreshed = Job.objects.get(id=job.id)
        if attempt < 3:
            assert refreshed.status == "pending"
        else:
            assert refreshed.status == "dead_letter"

    final = Job.objects.get(id=job.id)
    assert final.status == "dead_letter"
    assert final.attempts == 3

    # a dead-lettered job must never be picked up again
    claimed_again = repository.claim_due_jobs(worker_id="w2", batch_size=10, visibility_timeout_seconds=30)
    assert all(j.id != job.id for j in claimed_again)

    events = [e.event_type for e in final.events.all()]
    assert events == ["created", "claimed", "failed", "claimed", "failed", "claimed", "dead_letter"]


def test_compute_backoff_respects_cap():
    for attempt in range(1, 30):
        delay = compute_backoff_seconds(attempt)
        assert 0 <= delay <= settings.BACKOFF_CAP_SECONDS * (1 + settings.BACKOFF_JITTER) + 0.01
