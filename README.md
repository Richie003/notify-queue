# Notify Queue

A distributed delayed job & notification delivery system built on Django +
Django REST Framework + PostgreSQL. Schedule a notification for now or later;
any number of concurrent worker processes will deliver it exactly once, in
priority order, with retries, backoff, dead-lettering, per-recipient rate
limiting, idempotent scheduling, and webhook status callbacks.

See [DESIGN.md](DESIGN.md) for architecture, the exactly-once reasoning, and
where simplifying assumptions were made. This file is just setup/run
instructions.

## Requirements

- Python 3.11+
- A PostgreSQL server (16 recommended). Two ways to get one, pick either:
  - **Docker**: `docker compose` (see below), or
  - **Native/local Postgres**: any Postgres 13+ install reachable at the
    connection string in `DATABASE_URL`.

## 1. Setup

```bash
python -m venv venv
# Windows:
venv\Scripts\activate
# macOS/Linux:
source venv/bin/activate

pip install -r requirements.txt
cp .env.example .env   # adjust DATABASE_URL etc. if needed
```

## 2. Start Postgres

**Option A -- Docker Compose** (starts Postgres only; see §5 for running the
whole stack in Docker):

```bash
docker compose up -d db
```

**Option B -- a Postgres you already have running.** Create the database and
user referenced by `DATABASE_URL` in `.env` (defaults to
`postgresql://notify:notify@localhost:5432/notify_queue`):

```sql
CREATE USER notify WITH PASSWORD 'notify';
CREATE DATABASE notify_queue OWNER notify;
```

## 3. Migrate and run the API

```bash
cd src
python manage.py migrate
python manage.py runserver 0.0.0.0:8000
```

The API is now at `http://localhost:8000/`. Quick check:

```bash
curl http://localhost:8000/health/
```

## 4. Run workers -- multiple concurrent instances

Each worker is a normal Django management command that polls Postgres in a
loop. **Run as many as you like, in separate terminals, against the same
database** -- this is exactly the "multiple concurrent worker instances"
scenario the exactly-once guarantee is designed for (see DESIGN.md §3):

```bash
# terminal 2
cd src
python manage.py worker --id worker-1

# terminal 3
cd src
python manage.py worker --id worker-2

# terminal 4
cd src
python manage.py worker --id worker-3
```

`--id` is optional (defaults to `<hostname>-<random>`); give each instance a
distinct one so their log lines and `locked_by` values are easy to tell apart
when watching them race for jobs. You'll see log lines like:

```
[worker-1] job=... sent to alice@example.com via email (attempt 1)
[worker-2] job=... recipient=rate.limited@example.com rate-limited (5/5 sent this hour), requeued
[worker-1] job=... attempt 1/5 failed (simulated email provider failure for ...), retrying in 1.7s
```

Tunable via environment variables (see `.env.example` for the full list):
`FAILURE_RATE` (mock send failure rate, default 0.3), `POLL_INTERVAL_SECONDS`,
`CLAIM_BATCH_SIZE`, `VISIBILITY_TIMEOUT_SECONDS`, `RATE_LIMIT_PER_RECIPIENT_PER_HOUR`,
`DEFAULT_MAX_ATTEMPTS`, `BACKOFF_BASE_SECONDS` / `BACKOFF_CAP_SECONDS`.

## 5. Run everything in Docker (API + N workers + Postgres)

```bash
docker compose up --build --scale worker=3
```

This starts Postgres, the API (migrating automatically on boot), and 3
concurrent worker containers. Change `--scale worker=N` for more/fewer.

## 6. Seed sample data

With the API running (either from §3 or §5):

```bash
python seed.py
# or: python seed.py --base-url http://localhost:8000
```

This creates ~17 jobs covering every feature worth demoing: mixed priorities,
a couple of future-scheduled (not-yet-due) jobs, a duplicate `idempotency_key`
submission (to show it's recognized instead of creating a second job), and a
burst of 8 jobs to the same recipient (to show rate limiting queue instead of
fail once the hourly cap is hit). Start a worker afterward and watch it work
through the queue.

## 7. Run the tests

Tests run against a real Postgres (not mocks) -- `pytest-django` creates and
migrates a disposable `test_<db name>` database automatically. Needs Postgres
reachable per §2.

```bash
pytest
```

`tests/test_concurrency.py` is the one demonstrating no duplicate delivery
under concurrency: it runs 8 real worker threads (each with its own DB
connection) against a shared batch of due jobs and asserts every job is
claimed and delivered by exactly one of them.

## API reference

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/jobs/` | Schedule a job. Body: `recipient`, `channel` (`email`\|`sms`\|`push`), `payload`, one of `send_at` (ISO datetime) or `delay_seconds`, `priority` (`low`\|`normal`\|`high`\|`critical`, default `normal`), optional `idempotency_key`, `max_attempts` (default 5), `webhook_url`. Returns `{"duplicate": bool, "job": {...}}`, HTTP 201 (new) or 200 (duplicate key). |
| `GET` | `/jobs/` | List jobs. Filters: `?status=`, `?recipient=`, `?limit=`, `?offset=`. |
| `GET` | `/jobs/{id}/` | Job status + full event history. |
| `POST` | `/jobs/{id}/cancel/` | Cancel a pending/processing job. 409 if already terminal. |
| `GET` | `/metrics/` | `{counts_by_status, total_jobs, sent_last_hour, dead_letter_last_hour, avg_attempts_for_sent}`. |
| `POST` | `/webhooks/mock/` | Mocked receiver the system calls on status changes (also usable as a per-job `webhook_url` override target). |
| `GET` | `/webhooks/received/` | What the mock receiver has logged -- lets you verify callbacks landed. |

Example:

```bash
curl -X POST http://localhost:8000/jobs/ \
  -H "Content-Type: application/json" \
  -d '{
    "recipient": "alice@example.com",
    "channel": "email",
    "payload": {"subject": "Welcome!", "body": "Thanks for signing up."},
    "priority": "high",
    "delay_seconds": 30,
    "idempotency_key": "welcome-alice-2026"
  }'
```

## Project layout

```
DESIGN.md              architecture, exactly-once reasoning, scaling notes
README.md              this file
seed.py                sample data script (talks to the running API)
docker-compose.yml      Postgres + API + scalable worker
Dockerfile
requirements.txt
src/
  manage.py
  config/               Django project (settings, urls)
  jobs/                 the app
    models.py           Job, JobEvent, WebhookDelivery, ReceivedWebhook
    repository.py       claim query (raw SQL, SKIP LOCKED) + all state transitions
    serializers.py       DRF request/response (de)serialization
    views.py             DRF API views
    urls.py
    backoff.py           exponential backoff with jitter
    sender.py             mock notification sender (configurable failure rate)
    webhook.py             outbound webhook delivery
    management/commands/worker.py   the worker process
tests/
  test_concurrency.py    no-duplicate-delivery-under-concurrency (the key test)
  test_idempotency.py
  test_priority.py
  test_rate_limit.py
  test_retry_backoff.py
  test_api.py
```
