from concurrent.futures import ThreadPoolExecutor

import pytest


def _schedule(client, idem_key):
    return client.post("/jobs/", {
        "recipient": "dup@example.com",
        "channel": "email",
        "payload": {"subject": "one-time"},
        "idempotency_key": idem_key,
    }, format="json")


@pytest.mark.django_db
def test_duplicate_idempotency_key_returns_existing_job(api_client):
    first = _schedule(api_client, "order-42").json()["job"]
    second_resp = _schedule(api_client, "order-42")
    second = second_resp.json()

    assert second_resp.status_code == 200  # not 201 -- nothing new created
    assert second["duplicate"] is True
    assert second["job"]["id"] == first["id"]

    listing = api_client.get("/jobs/", {"recipient": "dup@example.com"}).json()
    assert len(listing) == 1


@pytest.mark.django_db(transaction=True)
def test_concurrent_duplicate_submissions_create_exactly_one_job():
    """Two clients race to submit the same idempotency key at the same
    instant. The DB unique constraint (not an app-level check-then-insert)
    must ensure only one row is ever created.

    `transaction=True` (TransactionTestCase semantics) is required here: the
    default django_db marker wraps the whole test in one outer transaction
    on the main thread's connection, which other threads' connections would
    never see. Real concurrent worker threads need real, independently
    committed transactions.
    """
    from django.db import connections
    from rest_framework.test import APIClient

    idem_key = "race-condition-key"

    def _submit(_):
        try:
            return _schedule(APIClient(), idem_key)
        finally:
            connections.close_all()  # this pool thread is done with its connection

    with ThreadPoolExecutor(max_workers=10) as pool:
        responses = list(pool.map(_submit, range(10)))

    job_ids = {r.json()["job"]["id"] for r in responses}
    assert len(job_ids) == 1, "all concurrent submissions must resolve to the same job"

    duplicates_flagged = sum(1 for r in responses if r.json()["duplicate"] is True)
    assert duplicates_flagged == 9  # exactly one request created it, the other 9 saw the duplicate

    all_jobs = APIClient().get("/jobs/", {"limit": 500}).json()
    matching = [j for j in all_jobs if j["idempotency_key"] == idem_key]
    assert len(matching) == 1
