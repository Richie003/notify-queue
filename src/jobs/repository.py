"""Data access layer. Every state transition on a job lives here as one
short, explicit operation executed in its own transaction.

Deliberate rule: we never hold a DB transaction open across the mock-send
call in the worker. Claiming a batch of jobs is committed immediately (so
the row is visible as 'processing' to other workers right away); the
delivery attempt happens with no transaction open; the outcome (sent/retry/
dead letter) is then written in its own, separate transaction. Holding a
transaction across slow/unreliable external I/O is a classic way job queues
end up starving their connection pool or deadlocking, so we avoid it
entirely.
"""
from datetime import timedelta
from typing import Optional

from django.db import IntegrityError, connection, transaction
from django.db.models import Avg, Count
from django.utils import timezone

from .models import Job, JobEvent, ReceivedWebhook

# ---------------------------------------------------------------- creation --


def insert_job(
    *,
    recipient: str,
    channel: str,
    payload: dict,
    priority: int,
    send_at,
    max_attempts: int,
    idempotency_key: Optional[str],
    webhook_url: Optional[str],
) -> tuple[Job, bool]:
    """Insert a new job. If idempotency_key collides with an existing job,
    that existing job is returned instead (duplicate=True) and nothing new
    is created. The uniqueness is enforced by a DB constraint, so this is
    race-safe even if two identical requests land at the same instant on
    two different API processes.
    """
    try:
        with transaction.atomic():
            job = Job.objects.create(
                recipient=recipient,
                channel=channel,
                payload=payload,
                priority=priority,
                send_at=send_at,
                next_attempt_at=send_at,
                max_attempts=max_attempts,
                idempotency_key=idempotency_key,
                webhook_url=webhook_url,
            )
            JobEvent.objects.create(job=job, event_type="created", detail={"send_at": send_at.isoformat()})
        return job, False
    except IntegrityError:
        existing = Job.objects.get(idempotency_key=idempotency_key)
        return existing, True


# --------------------------------------------------------- claim / assign --


def claim_due_jobs(*, worker_id: str, batch_size: int, visibility_timeout_seconds: int) -> list[Job]:
    """Atomically claim up to `batch_size` jobs, highest priority (and
    earliest-due within a priority tier) first.

    The SELECT + UPDATE happen as one raw SQL statement so no other worker
    can observe an in-between state:
      1. `FOR UPDATE SKIP LOCKED` picks rows and locks them for this
         transaction only; any row already locked by a concurrent claim is
         silently skipped rather than waited on, so N workers polling at
         once partition the queue instead of contending for it.
      2. The UPDATE ... FROM CTE flips status to 'processing' and stamps
         locked_by / lock_expires_at before the transaction commits.

    This is raw SQL (rather than Django's `select_for_update(skip_locked=
    True)` + a separate `.update()`) specifically so the claim is a single
    round trip and there is no daylight between "select the rows to claim"
    and "mark them claimed" for another worker to land in -- see DESIGN.md
    §3 for the full race-condition discussion.

    The same statement also reclaims jobs stuck in 'processing' whose
    lock_expires_at has passed -- i.e. a worker that claimed a job and then
    crashed (or was killed) before finishing it. This is the same
    visibility-timeout idea SQS uses.

    Uses `clock_timestamp()` rather than `now()`: Postgres's `now()` is
    frozen at transaction start, not real wall-clock time. A worker's claim
    transaction is normally short-lived so the difference is negligible,
    but `clock_timestamp()` is the correct choice for "is this due right
    now" and avoids surprising staleness if the enclosing transaction is
    ever held open longer (this bit a test that inserts multiple jobs and
    then claims within one wrapping transaction, per Django's TestCase-style
    test isolation -- a frozen `now()` from before a later insert made that
    job look "not due yet" even though it plainly was).
    """
    with transaction.atomic():
        with connection.cursor() as cursor:
            cursor.execute(
                """
                WITH due AS (
                    SELECT id
                    FROM jobs
                    WHERE (status = 'pending' AND next_attempt_at <= clock_timestamp())
                       OR (status = 'processing' AND lock_expires_at < clock_timestamp())
                    ORDER BY priority DESC, next_attempt_at ASC
                    LIMIT %s
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE jobs
                SET status = 'processing',
                    locked_by = %s,
                    locked_at = clock_timestamp(),
                    lock_expires_at = clock_timestamp() + (%s || ' seconds')::interval
                FROM due
                WHERE jobs.id = due.id
                RETURNING jobs.id
                """,
                [batch_size, worker_id, visibility_timeout_seconds],
            )
            claimed_ids = [row[0] for row in cursor.fetchall()]

        if not claimed_ids:
            return []

        jobs_by_id = {job.id: job for job in Job.objects.filter(id__in=claimed_ids)}
        claimed = [jobs_by_id[i] for i in claimed_ids]  # preserve priority/due ordering

        JobEvent.objects.bulk_create(
            [JobEvent(job=job, event_type="claimed", worker_id=worker_id) for job in claimed]
        )
        return claimed


# --------------------------------------------------------------- outcomes --


def mark_sent(job: Job, *, worker_id: str) -> Job:
    with transaction.atomic():
        job.status = Job.Status.SENT
        job.sent_at = timezone.now()
        job.attempts += 1
        job.last_error = None
        job.locked_by = None
        job.locked_at = None
        job.lock_expires_at = None
        job.save()
        JobEvent.objects.create(job=job, event_type="sent", worker_id=worker_id)
    return job


def schedule_retry(job: Job, *, worker_id: str, error: str, next_attempt_at) -> Job:
    with transaction.atomic():
        job.status = Job.Status.PENDING
        job.attempts += 1
        job.next_attempt_at = next_attempt_at
        job.last_error = error
        job.locked_by = None
        job.locked_at = None
        job.lock_expires_at = None
        job.save()
        JobEvent.objects.create(
            job=job, event_type="failed", worker_id=worker_id,
            detail={"error": error, "next_attempt_at": next_attempt_at.isoformat(), "attempts": job.attempts},
        )
    return job


def mark_dead_letter(job: Job, *, worker_id: str, error: str) -> Job:
    with transaction.atomic():
        job.status = Job.Status.DEAD_LETTER
        job.attempts += 1
        job.last_error = error
        job.locked_by = None
        job.locked_at = None
        job.lock_expires_at = None
        job.save()
        JobEvent.objects.create(
            job=job, event_type="dead_letter", worker_id=worker_id,
            detail={"error": error, "attempts": job.attempts},
        )
    return job


def requeue_rate_limited(job: Job, *, worker_id: str, next_attempt_at) -> Job:
    """Put a claimed job back to pending without counting it as a failed
    attempt -- used when the recipient's hourly cap is currently exhausted.
    Per the spec, over-limit jobs queue rather than fail.
    """
    with transaction.atomic():
        job.status = Job.Status.PENDING
        job.next_attempt_at = next_attempt_at
        job.locked_by = None
        job.locked_at = None
        job.lock_expires_at = None
        job.save()
        JobEvent.objects.create(
            job=job, event_type="rate_limited", worker_id=worker_id,
            detail={"next_attempt_at": next_attempt_at.isoformat()},
        )
    return job


def cancel_job(job_id) -> Optional[Job]:
    with transaction.atomic():
        updated = Job.objects.filter(
            id=job_id, status__in=[Job.Status.PENDING, Job.Status.PROCESSING]
        ).update(status=Job.Status.CANCELLED)
        if not updated:
            return None
        job = Job.objects.get(id=job_id)
        JobEvent.objects.create(job=job, event_type="cancelled")
        return job


# ---------------------------------------------------------- rate limiting --


def count_sent_last_hour(recipient: str) -> int:
    cutoff = timezone.now() - timedelta(hours=1)
    return Job.objects.filter(recipient=recipient, status=Job.Status.SENT, sent_at__gt=cutoff).count()


# --------------------------------------------------------------- webhooks --


def record_received_webhook(*, job_id, status, body: dict) -> None:
    ReceivedWebhook.objects.create(job_id=job_id, status=status, body=body)


def list_received_webhooks(limit: int = 50) -> list[ReceivedWebhook]:
    return list(ReceivedWebhook.objects.order_by("-received_at")[:limit])


# ---------------------------------------------------------------- metrics --


def metrics() -> dict:
    counts_by_status = dict(
        Job.objects.values_list("status").annotate(n=Count("id")).values_list("status", "n")
    )
    cutoff = timezone.now() - timedelta(hours=1)
    avg_attempts = Job.objects.filter(status=Job.Status.SENT).aggregate(a=Avg("attempts"))["a"] or 0.0
    return {
        "counts_by_status": counts_by_status,
        "total_jobs": Job.objects.count(),
        "sent_last_hour": Job.objects.filter(status=Job.Status.SENT, sent_at__gt=cutoff).count(),
        "dead_letter_last_hour": Job.objects.filter(status=Job.Status.DEAD_LETTER, updated_at__gt=cutoff).count(),
        "avg_attempts_for_sent": float(avg_attempts),
    }
