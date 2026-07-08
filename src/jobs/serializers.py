from datetime import timedelta

from django.conf import settings
from django.utils import timezone
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

    def get_priority(self, obj):
        name = settings.PRIORITY_NAMES.get(obj.priority)
        if name:
            return name
        return min(settings.PRIORITY_MAP.items(), key=lambda kv: abs(kv[1] - obj.priority))[0]


class JobDetailSerializer(JobSerializer):
    events = JobEventSerializer(many=True, read_only=True)

    class Meta(JobSerializer.Meta):
        fields = JobSerializer.Meta.fields + ["events"]


class ReceivedWebhookSerializer(serializers.ModelSerializer):
    class Meta:
        model = ReceivedWebhook
        fields = ["id", "job_id", "status", "body", "received_at"]
