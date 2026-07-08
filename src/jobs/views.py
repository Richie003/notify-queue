import logging

from django.shortcuts import get_object_or_404
from rest_framework import status as http_status
from rest_framework.response import Response
from rest_framework.views import APIView

from . import repository
from .models import Job
from .serializers import (
    JobDetailSerializer,
    JobSerializer,
    ReceivedWebhookSerializer,
    ScheduleJobSerializer,
)

logger = logging.getLogger("notify_queue.api")


class HealthView(APIView):
    def get(self, request):
        return Response({"status": "ok"})


class JobsView(APIView):
    """GET  /jobs  -- list/filter jobs
    POST /jobs  -- schedule a new notification job
    """

    def get(self, request):
        qs = Job.objects.all().order_by("-created_at")
        status_param = request.query_params.get("status")
        recipient = request.query_params.get("recipient")
        if status_param:
            qs = qs.filter(status=status_param)
        if recipient:
            qs = qs.filter(recipient=recipient)

        limit = min(int(request.query_params.get("limit", 50)), 500)
        offset = int(request.query_params.get("offset", 0))
        jobs = qs[offset:offset + limit]
        return Response(JobSerializer(jobs, many=True).data)

    def post(self, request):
        serializer = ScheduleJobSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        job, duplicate = repository.insert_job(
            recipient=serializer.validated_data["recipient"],
            channel=serializer.validated_data["channel"],
            payload=serializer.validated_data.get("payload") or {},
            priority=serializer.priority_value(),
            send_at=serializer.resolved_send_at(),
            max_attempts=serializer.validated_data["max_attempts"],
            idempotency_key=serializer.validated_data.get("idempotency_key"),
            webhook_url=serializer.validated_data.get("webhook_url"),
        )
        body = {"duplicate": duplicate, "job": JobSerializer(job).data}
        code = http_status.HTTP_200_OK if duplicate else http_status.HTTP_201_CREATED
        return Response(body, status=code)


class JobDetailView(APIView):
    def get(self, request, job_id):
        job = get_object_or_404(Job, id=job_id)
        return Response(JobDetailSerializer(job).data)


class CancelJobView(APIView):
    def post(self, request, job_id):
        existing = get_object_or_404(Job, id=job_id)
        job = repository.cancel_job(job_id)
        if job is None:
            return Response(
                {"detail": f"job already {existing.status}, cannot cancel"},
                status=http_status.HTTP_409_CONFLICT,
            )
        return Response(JobSerializer(job).data)


class MetricsView(APIView):
    def get(self, request):
        return Response(repository.metrics())


class MockWebhookView(APIView):
    """Mocked webhook receiver: the system calls out to this (or a per-job
    override URL) whenever a job's status changes. Logs everything it
    receives so the callback can be demonstrated end-to-end."""

    def post(self, request):
        body = request.data
        logger.info("mock webhook received: %s", body)
        repository.record_received_webhook(
            job_id=body.get("job_id"), status=body.get("status"), body=body
        )
        return Response({"received": True})


class ReceivedWebhooksView(APIView):
    def get(self, request):
        limit = int(request.query_params.get("limit", 50))
        items = repository.list_received_webhooks(limit=limit)
        return Response(ReceivedWebhookSerializer(items, many=True).data)
