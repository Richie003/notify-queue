from datetime import timedelta

from django.conf import settings
from django.utils import timezone
from drf_spectacular.types import OpenApiTypes
from drf_spectacular.utils import extend_schema_field
from rest_framework import serializers

from .models import Job, JobEvent, ReceivedWebhook

PRIORITY_CHOICES = list(settings.PRIORITY_MAP.keys())


class ScheduleJobSerializer(serializers.Serializer):
    recipient = serializers.CharField(min_length=1)
    channel = serializers.ChoiceField(choices=Job.Channel.choices)
    payload = serializers.JSONField(required=False, default=dict)
    send_at = serializers.DateTimeField(required=False, allow_null=True, default=None)
    delay_seconds = serializers.FloatField(required=False, allow_null=True, default=None, min_value=0)
    priority = serializers.ChoiceField(choices=PRIORITY_CHOICES, default="normal")
    idempotency_key = serializers.CharField(required=False, allow_null=True, default=None)
    max_attempts = serializers.IntegerField(
        required=False, default=settings.DEFAULT_MAX_ATTEMPTS, min_value=1, max_value=20
    )
    webhook_url = serializers.URLField(required=False, allow_null=True, default=None)

    def validate(self, data):
        if data.get("send_at") and data.get("delay_seconds") is not None:
            raise serializers.ValidationError("provide either send_at or delay_seconds, not both")
        return data

    def resolved_send_at(self):
        data = self.validated_data
        if data.get("send_at"):
            return data["send_at"]
        if data.get("delay_seconds") is not None:
            return timezone.now() + timedelta(seconds=data["delay_seconds"])
        return timezone.now()

    def priority_value(self) -> int:
        return settings.PRIORITY_MAP[self.validated_data["priority"]]


class JobEventSerializer(serializers.ModelSerializer):
    class Meta:
        model = JobEvent
        fields = ["event_type", "worker_id", "detail", "created_at"]


class JobSerializer(serializers.ModelSerializer):
    priority = serializers.SerializerMethodField()

    class Meta:
        model = Job
        fields = [
            "id", "idempotency_key", "recipient", "channel", "payload", "priority", "status",
            "send_at", "next_attempt_at", "attempts", "max_attempts", "last_error",
            "locked_by", "sent_at", "created_at", "updated_at",
        ]

    @extend_schema_field(OpenApiTypes.STR)
    def get_priority(self, obj):
        name = settings.PRIORITY_NAMES.get(obj.priority)
        if name:
            return name
        return min(settings.PRIORITY_MAP.items(), key=lambda kv: abs(kv[1] - obj.priority))[0]


class JobDetailSerializer(JobSerializer):
    events = JobEventSerializer(many=True, read_only=True)

    class Meta(JobSerializer.Meta):
        fields = JobSerializer.Meta.fields + ["events"]


class ScheduleJobResponseSerializer(serializers.Serializer):
    duplicate = serializers.BooleanField(
        help_text="True if idempotency_key matched an existing job (no new job was created)."
    )
    job = JobSerializer()


class ReceivedWebhookSerializer(serializers.ModelSerializer):
    class Meta:
        model = ReceivedWebhook
        fields = ["id", "job_id", "status", "body", "received_at"]


class WebhookCallbackSerializer(serializers.Serializer):
    """Shape of the payload the system POSTs on a job status change --
    documents jobs.webhook.fire_webhook()'s body, which the mock receiver
    below accepts."""

    job_id = serializers.CharField()
    status = serializers.ChoiceField(choices=["sent", "failed", "dead_letter"])
    recipient = serializers.CharField(required=False)
    channel = serializers.CharField(required=False)
    attempts = serializers.IntegerField(required=False)
    last_error = serializers.CharField(required=False, allow_null=True)


class WebhookAckSerializer(serializers.Serializer):
    received = serializers.BooleanField()


class HealthSerializer(serializers.Serializer):
    status = serializers.CharField()


class MetricsResponseSerializer(serializers.Serializer):
    counts_by_status = serializers.DictField(
        child=serializers.IntegerField(), help_text="e.g. {\"pending\": 3, \"sent\": 12, \"dead_letter\": 1}"
    )
    total_jobs = serializers.IntegerField()
    sent_last_hour = serializers.IntegerField()
    dead_letter_last_hour = serializers.IntegerField()
    avg_attempts_for_sent = serializers.FloatField()


class ErrorDetailSerializer(serializers.Serializer):
    detail = serializers.CharField()
