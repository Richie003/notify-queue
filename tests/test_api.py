import pytest

pytestmark = pytest.mark.django_db


def test_schedule_job_returns_pending_job(api_client):
    resp = api_client.post("/jobs/", {
        "recipient": "alice@example.com",
        "channel": "email",
        "payload": {"subject": "hi"},
        "delay_seconds": 60,
        "priority": "high",
    }, format="json")
    assert resp.status_code == 201
    body = resp.json()
    assert body["duplicate"] is False
    job = body["job"]
    assert job["status"] == "pending"
    assert job["priority"] == "high"
    assert job["attempts"] == 0
    assert job["recipient"] == "alice@example.com"


def test_get_job_status(api_client):
    created = api_client.post("/jobs/", {
        "recipient": "bob@example.com", "channel": "sms", "payload": {"body": "hi"},
    }, format="json").json()["job"]

    resp = api_client.get(f"/jobs/{created['id']}/")
    assert resp.status_code == 200
    body = resp.json()
    assert body["id"] == created["id"]
    assert any(e["event_type"] == "created" for e in body["events"])


def test_get_unknown_job_404(api_client):
    resp = api_client.get("/jobs/00000000-0000-0000-0000-000000000000/")
    assert resp.status_code == 404


def test_send_at_and_delay_seconds_mutually_exclusive(api_client):
    resp = api_client.post("/jobs/", {
        "recipient": "a@b.com", "channel": "email", "payload": {},
        "send_at": "2099-01-01T00:00:00Z", "delay_seconds": 10,
    }, format="json")
    assert resp.status_code == 400


def test_invalid_channel_rejected(api_client):
    resp = api_client.post("/jobs/", {
        "recipient": "a@b.com", "channel": "carrier_pigeon", "payload": {},
    }, format="json")
    assert resp.status_code == 400


def test_metrics_endpoint_reports_counts(api_client):
    api_client.post("/jobs/", {"recipient": "a@b.com", "channel": "email", "payload": {}}, format="json")
    resp = api_client.get("/metrics/")
    assert resp.status_code == 200
    body = resp.json()
    assert body["counts_by_status"]["pending"] >= 1
    assert body["total_jobs"] >= 1


def test_list_jobs_filters_by_status(api_client):
    api_client.post("/jobs/", {"recipient": "a@b.com", "channel": "email", "payload": {}}, format="json")
    resp = api_client.get("/jobs/", {"status": "pending"})
    assert resp.status_code == 200
    jobs = resp.json()
    assert all(j["status"] == "pending" for j in jobs)


def test_cancel_pending_job(api_client):
    created = api_client.post("/jobs/", {
        "recipient": "a@b.com", "channel": "email", "payload": {}, "delay_seconds": 3600,
    }, format="json").json()["job"]

    resp = api_client.post(f"/jobs/{created['id']}/cancel/")
    assert resp.status_code == 200
    assert resp.json()["status"] == "cancelled"

    # cancelling again should fail -- it's already terminal
    resp2 = api_client.post(f"/jobs/{created['id']}/cancel/")
    assert resp2.status_code == 409


def test_mock_webhook_receiver_logs_payload(api_client):
    resp = api_client.post("/webhooks/mock/", {"job_id": "abc", "status": "sent"}, format="json")
    assert resp.status_code == 200
    received = api_client.get("/webhooks/received/").json()
    assert any(r["job_id"] == "abc" for r in received)
