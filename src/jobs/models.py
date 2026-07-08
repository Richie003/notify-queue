import uuid

from django.db import models
from django.db.models import Q


class Job(models.Model):
    class Status(models.TextChoices):
        PENDING = "pending"
        PROCESSING = "processing"
        SENT = "sent"
        DEAD_LETTER = "dead_letter"
        CANCELLED = "cancelled"

    class Channel(models.TextChoices):
        EMAIL = "email"
        SMS = "sms"
        PUSH = "push"

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    # De-duplication: a second POST /jobs with the same key returns the
    # original job instead of creating a new one. Enforced by this DB
    # constraint, not just app logic, so it's race-safe under concurrent
    # submits (see DESIGN.md §4).
    idempotency_key = models.CharField(max_length=255, unique=True, null=True, blank=True)

    recipient = models.CharField(max_length=255)
    channel = models.CharField(max_length=10, choices=Channel.choices)
    payload = models.JSONField(default=dict, blank=True)

    # higher number = more urgent. Exposed over the API as low/normal/high/critical.
    priority = models.SmallIntegerField(default=5)

    status = models.CharField(max_length=20, choices=Status.choices, default=Status.PENDING)

    # send_at is the caller's original request ("don't send before this").
    # next_attempt_at is the scheduler's working field: starts equal to
    # send_at, pushed forward on retry backoff or rate-limit defers. The
    # claim query only ever looks at next_attempt_at.
    send_at = models.DateTimeField()
    next_attempt_at = models.DateTimeField()

    attempts = models.IntegerField(default=0)
    max_attempts = models.IntegerField(default=5)
    last_error = models.TextField(null=True, blank=True)

    # Claim / visibility-timeout fields. A worker that dies mid-delivery
    # leaves a row in 'processing' with a lock_expires_at in the past; the
    # claim query treats that as reclaimable, same as SQS's visibility timeout.
    locked_by = models.CharField(max_length=255, null=True, blank=True)
    locked_at = models.DateTimeField(null=True, blank=True)
    lock_expires_at = models.DateTimeField(null=True, blank=True)

    webhook_url = models.URLField(max_length=2048, null=True, blank=True)
    sent_at = models.DateTimeField(null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "jobs"
        indexes = [
            # Matches the claim query's WHERE/ORDER BY exactly, so claiming
            # stays an index scan even with millions of historical
            # (sent/dead_letter) rows in the table.
            models.Index(
                fields=["-priority", "next_attempt_at"],
                name="idx_jobs_claimable",
                condition=Q(status="pending"),
            ),
            models.Index(
                fields=["lock_expires_at"],
                name="idx_jobs_stale_lock",
                condition=Q(status="processing"),
            ),
            models.Index(
                fields=["recipient", "sent_at"],
                name="idx_jobs_recipient_sent",
                condition=Q(status="sent"),
            ),
            models.Index(fields=["status"], name="idx_jobs_status"),
        ]

    def __str__(self):
        return f"Job({self.id}, {self.status}, {self.recipient})"


class JobEvent(models.Model):
    """Append-only audit trail: one row per state transition. Backs
    GET /jobs/{id}'s history and is what the concurrency tests assert
    against independently of the row's current state."""

    job = models.ForeignKey(Job, on_delete=models.CASCADE, related_name="events")
    event_type = models.CharField(max_length=30)
    worker_id = models.CharField(max_length=255, null=True, blank=True)
    detail = models.JSONField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "job_events"
        ordering = ["created_at"]
        indexes = [models.Index(fields=["job", "created_at"], name="idx_job_events_job_id")]


class WebhookDelivery(models.Model):
    """Outbound webhook attempts the system made."""

    job = models.ForeignKey(Job, on_delete=models.CASCADE, related_name="webhook_deliveries")
    reported_status = models.CharField(max_length=30)
    url = models.URLField(max_length=2048)
    http_status = models.IntegerField(null=True, blank=True)
    success = models.BooleanField()
    error = models.TextField(null=True, blank=True)
    attempted_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "webhook_deliveries"


class ReceivedWebhook(models.Model):
    """Inbound log for the mock webhook receiver endpoint, so the callback
    can be demonstrated end-to-end without a real 3rd party."""

    job_id = models.CharField(max_length=64, null=True, blank=True)
    status = models.CharField(max_length=30, null=True, blank=True)
    body = models.JSONField()
    received_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "received_webhooks"
