"""pytest-django fixtures.

Tests run against a real Postgres database (pytest-django creates and
migrates a disposable `test_<db name>` database automatically, using the
same DATABASE_URL as normal), rather than mocks -- because the thing under
test in the concurrency suite is the actual row-locking behaviour of
`SELECT ... FOR UPDATE SKIP LOCKED`, which cannot be faithfully exercised
against an in-memory fake.

Requires a reachable Postgres (see README.md) before running pytest.
"""
import os

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql://notify:notify@localhost:5432/notify_queue")


@pytest.fixture
def api_client():
    from rest_framework.test import APIClient

    return APIClient()
