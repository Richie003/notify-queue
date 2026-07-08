"""The centerpiece test: proves no duplicate delivery under concurrency.

Each "worker" here is a real OS thread. Django gives every thread its own
DB connection automatically (connections are thread-local), so the claim
SQL genuinely runs as concurrent, independent Postgres transactions --
this is exercising the database's `FOR UPDATE SKIP LOCKED` locking, not
just Python-level thread-safety. A `threading.Barrier` lines every worker
up so they all start polling at the same instant, maximizing the chance of
a race actually occurring if the locking were broken.

`transaction=True` (TransactionTestCase semantics, via
`pytest.mark.django_db(transaction=True)`) is required: the default
`django_db` marker wraps a test in one outer transaction on the main
thread's connection that is rolled back at the end and never actually
committed, so other threads' independent connections would never see the
inserted jobs at all. Real concurrent worker threads need real,
independently committed transactions -- exactly what this project's
locking design has to be correct under in production.
"""
import threading
from collections import defaultdict

import pytest
from django.conf import settings
from django.db import connections
from django.utils import timezone

from jobs import repository
from jobs.management.commands.worker import Worker
from jobs.models import Job

NUM_JOBS = 60
NUM_WORKERS = 8


def _insert_jobs(n):
    ids = []
    for i in range(n):
        job, _ = repository.insert_job(
            recipient=f"user{i}@example.com", channel="email", payload={"i": i},
            priority=settings.PRIORITY_MAP["normal"], send_at=timezone.now(),
            max_attempts=5, idempotency_key=None, webhook_url=None,
        )
        ids.append(job.id)
    return ids


@pytest.mark.django_db(transaction=True)
def test_no_job_is_claimed_by_more_than_one_worker():
    job_ids = set(_insert_jobs(NUM_JOBS))

    claims_by_worker: dict[str, list] = defaultdict(list)
    lock = threading.Lock()
    barrier = threading.Barrier(NUM_WORKERS)

    def run(worker_index: int):
        worker_id = f"worker-{worker_index}"
        try:
            barrier.wait()  # everyone starts claiming at the same instant
            while True:
                batch = repository.claim_due_jobs(
                    worker_id=worker_id, batch_size=3, visibility_timeout_seconds=30
                )
                if not batch:
                    break
                with lock:
                    claims_by_worker[worker_id].extend(j.id for j in batch)
        finally:
            connections.close_all()  # this thread is done with its connection

    threads = [threading.Thread(target=run, args=(i,)) for i in range(NUM_WORKERS)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)

    all_claims = [jid for claims in claims_by_worker.values() for jid in claims]

    assert len(all_claims) == NUM_JOBS, "every inserted job must be claimed exactly once, in total"
    assert len(set(all_claims)) == NUM_JOBS, "no job id should appear twice across all workers combined"
    assert set(all_claims) == job_ids

    # independent DB-side check: nothing left pending, nothing double-booked
    for jid in job_ids:
        row = Job.objects.get(id=jid)
        assert row.status == "processing"
        assert row.locked_by is not None


@pytest.mark.django_db(transaction=True)
def test_no_duplicate_delivery_under_concurrent_workers(monkeypatch):
    """End-to-end version: run the real claim -> deliver -> mark_sent
    pipeline from many concurrent worker threads and prove each job is
    delivered exactly once -- never zero times, never more than once."""
    monkeypatch.setattr("jobs.management.commands.worker.mock_send", lambda **kwargs: None)  # always "succeeds"
    monkeypatch.setattr("jobs.management.commands.worker.fire_webhook", lambda job, status: None)

    job_ids = _insert_jobs(NUM_JOBS)

    send_attempts: dict = defaultdict(int)
    lock = threading.Lock()
    barrier = threading.Barrier(NUM_WORKERS)

    def run(worker_index: int):
        worker = Worker(f"worker-{worker_index}")
        try:
            barrier.wait()
            while True:
                batch = repository.claim_due_jobs(
                    worker_id=worker.worker_id, batch_size=3, visibility_timeout_seconds=30
                )
                if not batch:
                    break
                for job in batch:
                    worker.process(job)
                    with lock:
                        send_attempts[job.id] += 1
        finally:
            connections.close_all()

    threads = [threading.Thread(target=run, args=(i,)) for i in range(NUM_WORKERS)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)

    assert set(send_attempts.keys()) == set(job_ids)
    assert all(count == 1 for count in send_attempts.values()), (
        f"every job must be processed exactly once; got counts {dict(send_attempts)}"
    )

    for jid in job_ids:
        row = Job.objects.get(id=jid)
        assert row.status == "sent"
        assert row.attempts == 1  # exactly one delivery attempt was ever recorded
        assert row.events.filter(event_type="sent").count() == 1
