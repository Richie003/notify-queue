"""Worker process. Run as many instances of this as you like -- concurrently,
on one machine or many -- pointed at the same DATABASE_URL; the claim query
in repository.claim_due_jobs() guarantees they never process the same job at
the same time. See DESIGN.md for why.

Usage:
    python manage.py worker --id worker-1
"""
import logging
import signal
import socket
import time
import uuid
from datetime import timedelta

from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils import timezone

from jobs import repository
from jobs.backoff import compute_backoff_seconds
from jobs.models import Job
from jobs.sender import SendError, mock_send
from jobs.webhook import fire_webhook

logger = logging.getLogger("notify_queue.worker")


class Worker:
    def __init__(self, worker_id: str):
        self.worker_id = worker_id
        self._stop = False

    def request_stop(self, *_args):
        logger.info("[%s] shutdown requested, finishing current batch then exiting", self.worker_id)
        self._stop = True

    def run(self) -> None:
        logger.info(
            "[%s] starting: poll_interval=%ss batch_size=%s failure_rate=%s rate_limit=%s/hr",
            self.worker_id, settings.POLL_INTERVAL_SECONDS, settings.CLAIM_BATCH_SIZE,
            settings.FAILURE_RATE, settings.RATE_LIMIT_PER_RECIPIENT_PER_HOUR,
        )
        while not self._stop:
            claimed = repository.claim_due_jobs(
                worker_id=self.worker_id,
                batch_size=settings.CLAIM_BATCH_SIZE,
                visibility_timeout_seconds=settings.VISIBILITY_TIMEOUT_SECONDS,
            )
            if not claimed:
                time.sleep(settings.POLL_INTERVAL_SECONDS)
                continue

            # Jobs in this batch are already claimed (status='processing' in
            # the DB) -- finish them even if a stop was just requested,
            # rather than abandoning them to sit until the visibility
            # timeout expires and another worker has to reclaim them.
            for job in claimed:
                self.process(job)

    def process(self, job: Job) -> None:
        if job.attempts >= job.max_attempts:
            # defensive: shouldn't happen (dead_letter is terminal) but avoids
            # ever sending a job that's already exhausted its budget
            repository.mark_dead_letter(job, worker_id=self.worker_id, error="max attempts already reached")
            return

        sent_count = repository.count_sent_last_hour(job.recipient)
        if sent_count >= settings.RATE_LIMIT_PER_RECIPIENT_PER_HOUR:
            next_attempt = timezone.now() + timedelta(seconds=settings.RATE_LIMIT_RECHECK_SECONDS)
            repository.requeue_rate_limited(job, worker_id=self.worker_id, next_attempt_at=next_attempt)
            logger.info(
                "[%s] job=%s recipient=%s rate-limited (%s/%s sent this hour), requeued",
                self.worker_id, job.id, job.recipient, sent_count, settings.RATE_LIMIT_PER_RECIPIENT_PER_HOUR,
            )
            return

        try:
            mock_send(channel=job.channel, recipient=job.recipient, payload=job.payload)
        except SendError as exc:
            self._handle_failure(job, str(exc))
            return

        updated = repository.mark_sent(job, worker_id=self.worker_id)
        logger.info(
            "[%s] job=%s sent to %s via %s (attempt %s)",
            self.worker_id, job.id, job.recipient, job.channel, updated.attempts,
        )
        fire_webhook(updated, "sent")

    def _handle_failure(self, job: Job, error: str) -> None:
        next_attempt_number = job.attempts + 1

        if next_attempt_number >= job.max_attempts:
            updated = repository.mark_dead_letter(job, worker_id=self.worker_id, error=error)
            logger.warning(
                "[%s] job=%s DEAD-LETTERED after %s attempts: %s",
                self.worker_id, job.id, updated.attempts, error,
            )
            fire_webhook(updated, "dead_letter")
            return

        delay = compute_backoff_seconds(next_attempt_number)
        next_attempt_at = timezone.now() + timedelta(seconds=delay)
        updated = repository.schedule_retry(
            job, worker_id=self.worker_id, error=error, next_attempt_at=next_attempt_at,
        )
        logger.info(
            "[%s] job=%s attempt %s/%s failed (%s), retrying in %.1fs",
            self.worker_id, job.id, updated.attempts, job.max_attempts, error, delay,
        )
        fire_webhook(updated, "failed")


class Command(BaseCommand):
    help = "Run a Notify Queue worker that claims and delivers due jobs. Run multiple instances for concurrency."

    def add_arguments(self, parser):
        parser.add_argument(
            "--id", dest="worker_id", default=None,
            help="unique worker id (defaults to hostname-<random>)",
        )

    def handle(self, *args, **options):
        worker_id = options["worker_id"] or f"{socket.gethostname()}-{uuid.uuid4().hex[:6]}"
        worker = Worker(worker_id)
        signal.signal(signal.SIGINT, worker.request_stop)
        signal.signal(signal.SIGTERM, worker.request_stop)
        worker.run()
