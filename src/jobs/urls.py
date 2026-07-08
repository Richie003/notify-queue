from django.urls import path

from . import views

urlpatterns = [
    path("health/", views.HealthView.as_view(), name="health"),
    path("jobs/", views.JobsView.as_view(), name="jobs"),
    path("jobs/<uuid:job_id>/", views.JobDetailView.as_view(), name="job-detail"),
    path("jobs/<uuid:job_id>/cancel/", views.CancelJobView.as_view(), name="job-cancel"),
    path("metrics/", views.MetricsView.as_view(), name="metrics"),
    path("webhooks/mock/", views.MockWebhookView.as_view(), name="webhooks-mock"),
    path("webhooks/received/", views.ReceivedWebhooksView.as_view(), name="webhooks-received"),
]
