# Design: Notify Queue

A distributed delayed job & notification delivery system: schedule a notification
for now/later, have any number of concurrent worker processes deliver it exactly
once, in priority order, with retries, backoff, dead-lettering, per-recipient
rate limiting, idempotent scheduling, and webhook status callbacks.

Stack: **Django + Django REST Framework** for the API, the **Django ORM**
(models + migrations) for schema and everyday reads/writes, one **raw SQL**
statement for the claim itself (see §3.2 for why), and **PostgreSQL** as the
only piece of infrastructure the system depends on.

## 1. High-level architecture

```
                 POST /jobs/                   GET /jobs/{id}/, /metrics/
                     |                                   ^
                     v                                   |
              +----------------+                 +----------------+
   client --> |  API (Django +   | <-----------------|   same API     |
              |  DRF, manage.py  |                 |  process       |
              |  runserver)      |                 +----------------+
              +----------------+
                     |
                     v
              +----------------------------------------+
              |   PostgreSQL   (jobs, job_events,       |
              |   webhook_deliveries, received_webhooks)|
              +----------------------------------------+
                     ^        ^        ^
                     |        |        |               all pollers hit the
                claim/update via        same table; row-level locking
                SKIP LOCKED             (FOR UPDATE SKIP LOCKED) is what
                     |        |        |               keeps them from
              +---------+ +---------+ +---------+       colliding
              | worker  | | worker  | | worker  |  ...  (N instances --
              | (manage.| | (manage.| | (manage.|        manage.py worker)
              |  py     | |  py     | |  py     |
              |  worker)| |  worker)| |  worker)|
              +---------+ +---------+ +---------+
                     |
                     v
              mock sender (simulated channel, random failure rate)
                     |
                     v
              webhook callback --> POST /webhooks/mock/ (mocked receiver)
```

Two independent process types share one database and nothing else:

- **API** (`src/jobs/views.py`, served by `manage.py runserver` / any WSGI
  server) -- stateless. Accepts new jobs, answers status/metrics/list queries,
  and hosts the mock webhook receiver. Horizontally scalable trivially (no
  in-memory state).
- **Worker** (`src/jobs/management/commands/worker.py`, run via `manage.py
  worker`) -- a poll loop, implemented as a Django management command so it
  gets Django's settings/ORM/migrations wiring for free. Run it N times (N
  terminals, or `docker-compose up --scale worker=N`) to get N concurrent
  workers. Workers do not talk to each other or to the API; they only talk to
  Postgres and to the mock sender/webhook.

Postgres is the only source of truth and the only coordination point. There is no
separate message broker (SQS/Kafka/Redis) and no external lock service
(Zookeeper/etcd/Redis-Redlock) -- Postgres's MVCC + row locking does that job. See
[§9](#9-why-postgres-as-the-queue-not-a-dedicated-broker) for why that trade-off was made deliberately, and where it stops
being the right choice.

## 2. Data model

`src/jobs/models.py` defines four tables. `Job` (`db_table="jobs"`) is the queue
itself -- there's no separate "queue" data structure; a job's row *is* its queue
position. Key fields:

| field | purpose |
|---|---|
| `status` | `pending` \| `processing` \| `sent` \| `dead_letter` \| `cancelled` |
| `priority` | smallint, higher = more urgent (`low`=1, `normal`=5, `high`=8, `critical`=10) |
| `send_at` | caller's original "don't send before this" request |
| `next_attempt_at` | scheduler's working field -- starts equal to `send_at`, pushed forward on retry backoff or rate-limit defers. **This is the only timestamp the claim query looks at.** |
| `attempts` / `max_attempts` | retry budget |
| `locked_by` / `locked_at` / `lock_expires_at` | claim ownership + visibility timeout |
| `idempotency_key` | `unique=True`, nullable -- enforces dedup at the DB level |

`JobEvent` is an append-only audit log (one row per state transition:
`created`, `claimed`, `sent`, `failed`, `dead_letter`, `rate_limited`,
`cancelled`) -- it backs `GET /jobs/{id}/`'s history and is what the concurrency
tests assert against independently of the row's current state.

`WebhookDelivery` / `ReceivedWebhook` log outbound and (for the mock receiver)
inbound webhook traffic, for observability and testing.

Partial indexes (`Meta.indexes` with `condition=Q(...)`) match the claim query's
exact `WHERE`/`ORDER BY`, so claiming stays an index scan (not a sort over the
whole table) even with millions of historical `sent`/`dead_letter` rows sitting
in the same table.

## 3. Exactly-once delivery under concurrent workers

This is the core correctness requirement, so it gets the most detail.

### 3.1 Where the race condition would occur, naively

The obvious-but-wrong implementation:

```python
# Worker A and Worker B, running at the same moment:
due = Job.objects.filter(status="pending", next_attempt_at__lte=now())  # (1) both read the same rows
# ... both think they own job X ...
job.status = "processing"; job.save()                                   # (2) both send it
```

Between the read and the write, there is a window where two workers can both
observe job X as unclaimed and both proceed to send it. This is the classic
"check-then-act" race, and it's exactly what "no relying on a single-process-only
assumption" in the brief is calling out. It gets worse with more workers and more
throughput, not better.

### 3.2 The fix: a single atomic claim statement

`repository.claim_due_jobs()` does the read and the write in **one** raw SQL
statement, executed in one transaction:

```sql
WITH due AS (
    SELECT id
    FROM jobs
    WHERE (status = 'pending' AND next_attempt_at <= clock_timestamp())
       OR (status = 'processing' AND lock_expires_at < clock_timestamp())
    ORDER BY priority DESC, next_attempt_at ASC
    LIMIT :batch_size
    FOR UPDATE SKIP LOCKED
)
UPDATE jobs
SET status = 'processing', locked_by = :worker_id, locked_at = clock_timestamp(),
    lock_expires_at = clock_timestamp() + :visibility_timeout * interval '1 second'
FROM due
WHERE jobs.id = due.id
RETURNING jobs.id;
```

Why this closes the race:

- `FOR UPDATE` on the CTE takes a row-level lock on every candidate row as part
  of selecting it, **inside the same statement/transaction that will update it**.
  There's no gap between "decide to claim" and "mark as claimed" for another
  worker to land in.
- `SKIP LOCKED` means a worker that finds a row already locked by a concurrent
  claim simply **skips it and moves to the next one**, instead of blocking. This
  is what makes it scale: N workers polling simultaneously partition the queue
  rather than queueing up behind a lock.
- The transaction commits immediately after claiming (before any delivery
  attempt), so the claim is durable and visible to other workers right away. We
  deliberately do **not** hold the transaction open across the mock-send call --
  see §3.3.

**Why raw SQL instead of `Job.objects.select_for_update(skip_locked=True)`:**
Django does support `select_for_update(skip_locked=True)`, and the idiomatic
Django pattern would be a `SELECT ... FOR UPDATE SKIP LOCKED` queryset followed
by a separate `.update()` inside the same `transaction.atomic()` block. That
works too (the row locks taken by the `SELECT` are held until the transaction
ends, so the subsequent `UPDATE` in the *same* transaction is still safe --
nothing else can touch those rows in between). I used one raw `UPDATE ... FROM
(SELECT ... FOR UPDATE SKIP LOCKED) RETURNING` statement instead specifically so
the claim is a **single round trip** with **RETURNING** giving back the exact
claimed rows in one shot, rather than two queries. Only this one query is raw
SQL; everything else in the app is plain Django ORM.

Priority ordering falls out of the same statement for free: `ORDER BY priority
DESC, next_attempt_at ASC` before `LIMIT` means high-priority due jobs are
always claimed first. Note that Postgres does **not** guarantee `RETURNING`
preserves the CTE's `ORDER BY` when a batch >1 is claimed (the `ORDER BY`
+`LIMIT` correctly determines *which* rows get locked and claimed, but not
necessarily the order they come back in `RETURNING`) -- `claim_due_jobs()`
re-fetches the claimed rows by id and re-sorts them into the original
`claimed_ids` order before returning, so priority order is preserved end to end
for the caller.

### 3.3 Never hold a DB transaction across the external send call

A tempting-but-wrong variant of the above is to keep the claim transaction open,
call the (slow, unreliable) provider, and only commit `status='sent'`
afterward. Don't do this: the transaction holds a row lock (and a pooled
connection) for as long as the external call takes, which under load starves the
connection pool and turns provider latency into queue-wide throughput loss, and
risks statement/transaction timeouts aborting an otherwise-successful send.

Instead, claiming (`processing`) and recording the outcome (`sent` /
retry-`pending` / `dead_letter`) are **two separate, short transactions**
(`repository.mark_sent` / `schedule_retry` / `mark_dead_letter`, each its own
`transaction.atomic()`), with the actual send happening in between with no
transaction open. `locked_by` / `lock_expires_at` are what keep the row
"reserved" during that gap, not a held lock.

### 3.4 Crash recovery: the visibility timeout

If a worker claims a job and then crashes (killed, OOM, network partition) before
recording an outcome, the job would be stuck in `processing` forever under a
naive design. `lock_expires_at` (default 30s, `VISIBILITY_TIMEOUT_SECONDS`) is a
lease: the same claim statement above also matches rows where
`status='processing' AND lock_expires_at < clock_timestamp()`, so a stalled claim
is automatically eligible for another worker to pick up again. This is the same
idea as SQS's visibility timeout. No separate reaper process is needed -- it's
folded into the hot path of the claim query itself.

### 3.5 A real bug this surfaced: `now()` vs `clock_timestamp()`

Worth including because it's a genuine, non-obvious correctness detail (and
because it's exactly the kind of thing that's easy to get wrong and only shows
up under specific conditions): the claim query originally used Postgres's
`now()`. `now()` (= `CURRENT_TIMESTAMP` = `transaction_timestamp()`) is **frozen
at transaction start**, not real wall-clock time -- it returns the same value no
matter how many statements you run or how much real time elapses inside one
transaction. For a worker's normal claim loop this is harmless (each claim opens
its own short-lived transaction, so `now()` there is always ~current time). It
broke, very visibly, in a test that inserted two jobs and then claimed within one
enclosing transaction (Django's `TestCase`-style test isolation wraps a whole
test in one outer transaction): the second job's `next_attempt_at`, computed from
Python's real wall clock a few milliseconds *after* the transaction began, ended
up *later* than Postgres's frozen `now()` -- making a job that was plainly due
look like it wasn't yet. Switched to `clock_timestamp()`, which re-evaluates real
time on every call, and the bug (and the class of bug) disappeared. This is
called out explicitly rather than quietly fixed because it's a good illustration
of the kind of assumption ("`now()` means *now*") that's easy to carry over from
application-level thinking and that a claim-based queue specifically needs to get
right.

### 3.6 The one gap that's fundamentally unavoidable, and how it's mitigated

If a worker successfully calls the (external) send, and then crashes **before**
committing the `mark_sent` transaction, another worker will later see
`lock_expires_at` expired, reclaim the job, and send it again -- a real duplicate
*external* send. No amount of database locking can close this specific window,
because the failure happens *outside* the database's transaction boundary. This
is a general truth about distributed systems, not a bug in this design: you
cannot get exactly-once delivery to an external system purely from your own
locking, only *at-least-once delivery* + *an idempotent operation at the
receiver*.

What this system does about it:
- The claim mechanism guarantees exactly-once **processing ownership** at any
  given moment (§3.2) -- this is what the concurrency tests verify, and it's the
  part that's fully solvable.
- `VISIBILITY_TIMEOUT_SECONDS` is set well above the expected p99 send latency,
  so a healthy worker virtually never has its claim reclaimed out from under it;
  this crash-window duplicate only happens on an actual worker crash mid-send,
  which is rare.
- In production, the real fix is to pass a stable idempotency/dedup key to the
  downstream provider (SES message dedup, Twilio idempotency headers, etc.), the
  same way this system dedupes *scheduling* via `idempotency_key` (§4). The mock
  sender here doesn't model a provider-side idempotency API, since building a
  fake one wouldn't demonstrate anything beyond what §4 already proves about the
  pattern -- this is called out explicitly as a simplifying assumption rather
  than quietly ignored.

### 3.7 Proof

`tests/test_concurrency.py` spins up 8 real threads (Django gives each thread
its own DB connection automatically -- connections are thread-local -- so these
are genuinely concurrent, independent Postgres transactions, not just concurrent
Python bytecode), lines them up on a `threading.Barrier` so they all start
polling the same 60 due jobs at the same instant, and asserts:
- every job is claimed, and claimed by **exactly one** worker (no duplicates, no
  misses) -- `test_no_job_is_claimed_by_more_than_one_worker`
- running the full claim -> mock-send -> mark-sent pipeline concurrently results
  in exactly one `sent` event and `attempts == 1` per job, never zero, never more
  than one -- `test_no_duplicate_delivery_under_concurrent_workers`

These use `@pytest.mark.django_db(transaction=True)` specifically (Django
`TransactionTestCase` semantics) rather than the default `django_db` marker:
the default wraps a whole test in one outer transaction on the *main* thread's
connection, which other threads' independent connections would never see
(nothing is actually committed until a rollback at teardown). Real concurrent
worker threads need real, independently committed transactions -- exactly what
the implementation has to be correct under in production, so the test has to
create that same condition to mean anything.

## 4. Idempotency

`POST /jobs/` accepts an optional `idempotency_key`. `Job.idempotency_key` has
`unique=True`. `repository.insert_job()` tries the `Job.objects.create(...)`;
if Django raises `IntegrityError` (unique violation), it re-fetches and returns
the *existing* job instead (`duplicate: true` in the response body, HTTP 200
instead of 201).

This is deliberately a DB constraint, not an app-level "check if it exists, then
insert if not" -- the latter has the exact same check-then-act race as §3.1 if
two identical requests land on two different API processes at the same instant.
`tests/test_idempotency.py::test_concurrent_duplicate_submissions_create_exactly_one_job`
fires 10 concurrent requests (real threads, real DRF `APIClient` instances, each
with its own Django DB connection) with the same key and asserts exactly one job
is ever created.

## 5. Priority queueing

`priority` is a plain `SmallIntegerField` (exposed over the API as
`low`/`normal`/`high`/`critical`, mapped to `1`/`5`/`8`/`10` in
`settings.PRIORITY_MAP` -- kept as an int rather than a Django `TextChoices`/DB
enum so new tiers can be added without a migration). The claim query's `ORDER BY
priority DESC, next_attempt_at ASC` is the entire implementation: among all
currently-due jobs, higher priority always wins, and ties break FIFO by due
time.

One consequence worth being explicit about: priority is a *preference*, not a
hard reservation. A large batch of high-priority jobs will starve low-priority
ones for as long as high-priority jobs keep arriving faster than workers can
drain them -- there's no separate low-priority-only worker pool carved out. This
was a deliberate simplification for this scope; §11 discusses splitting into
per-priority worker pools as a scaling response.

## 6. Rate limiting per recipient

Before attempting delivery of a claimed job, `Worker.process()` calls
`repository.count_sent_last_hour(recipient)` (`Job.objects.filter(recipient=...,
status="sent", sent_at__gt=now()-1h).count()`, backed by a partial index on
`(recipient, sent_at) WHERE status='sent'`). If the recipient is already at
`RATE_LIMIT_PER_RECIPIENT_PER_HOUR`, the job is **not** sent, **not** counted
against its retry budget, and **not** failed -- `requeue_rate_limited()` puts it
back to `pending` with `next_attempt_at` pushed forward by
`RATE_LIMIT_RECHECK_SECONDS`, and logs a `rate_limited` event (distinct from
`failed`). This directly satisfies "excess jobs should queue, not fail" -- see
the live worker output in README.md's demo walkthrough, where jobs 6-8 to the
same recipient visibly get `rate-limited (5/5 sent this hour), requeued` instead
of being sent or failed.

This is a fixed-window counter derived from the jobs table itself (no separate
counter table), which keeps it trivially consistent with the source of truth at
the cost of a query per check. §11 covers the Redis-token-bucket replacement for
this at higher scale.

## 7. Retry, backoff, and dead-lettering

On a failed send, `Worker._handle_failure`:
1. Computes `next_attempt_number = attempts + 1`.
2. If `next_attempt_number >= max_attempts`: `mark_dead_letter` -- terminal,
   fires a `dead_letter` webhook. A dead-lettered job is never claimed again
   (its `status` no longer matches the claim query's `pending`/stale-`processing`
   conditions).
3. Otherwise: `schedule_retry` with `next_attempt_at = now() +
   compute_backoff_seconds(next_attempt_number)`, back to `status='pending'` so
   it re-enters the normal claim queue (with its original priority intact) once
   due. Fires a `failed` webhook (this is a *transient*-failure notification, not
   the terminal state).

Backoff (`src/jobs/backoff.py`) is textbook exponential-with-full-jitter:
`delay = min(cap, base * 2^(attempt-1))`, then +/-20% jitter. Jitter matters
under real concurrency: without it, a burst of jobs that all fail at once (e.g. a
provider outage) would all retry at exactly the same instant, over and over,
instead of spreading back out.

The dead-letter "queue" is not a separate table/topic -- it's
`status='dead_letter'` on the same `jobs` row. That keeps the audit trail
(`JobEvent`) and the row's history attached to one identity, which is simpler
to query and present (`GET /jobs/{id}/`, `GET /jobs/?status=dead_letter`) than
reconstructing it by joining across a separate DLQ table. In a message-broker
architecture (§9) this would naturally become a real separate topic/queue
instead.

## 8. Webhook callbacks

`fire_webhook(job, status)` (`src/jobs/webhook.py`) POSTs `{job_id, status,
recipient, channel, attempts, last_error}` to `job.webhook_url` (per-job
override) or `DEFAULT_WEBHOOK_URL` (a mock receiver at `POST /webhooks/mock/`,
which logs to `ReceivedWebhook` so delivery can be verified end-to-end without a
real 3rd party). Fired synchronously from the worker on `sent`, `failed` (each
failed attempt), and `dead_letter`.

Simplifying assumption, stated explicitly: this is one attempt with a short
timeout: if the receiver is down, the failure is logged
(`WebhookDelivery.success=False`) but **not** retried, and critically does
**not** affect the job's own status or retry count -- a flaky webhook receiver
must never cause a notification to be re-sent or lost. Production hardening
would give webhook delivery its own retry queue (itself just another row in a
job-like table with the same claim mechanism) rather than best-effort fire and
forget.

One Django-specific gotcha worth flagging since it cost real debugging time: the
default webhook URL needs its trailing slash (`/webhooks/mock/`, not
`/webhooks/mock`). Django's `CommonMiddleware` (`APPEND_SLASH`) 301-redirects a
POST to the slash-suffixed URL, and `requests` (like most HTTP clients) follows
a 301 by converting the retried request to a GET and dropping the body --
silently turning the webhook call into a no-op `GET /webhooks/mock/` (405, since
that view only defines `post()`) instead of raising anything visible. Fixed by
always including the trailing slash in configured webhook URLs.

## 9. Why Postgres as the queue, not a dedicated broker

A "real" system at large scale would likely put a message broker (SQS, Kafka +
a claim-check pattern, RabbitMQ) in front of delivery, with Postgres (or
similar) only for the durable job record and status API. For this assessment's
scope, one Postgres database is deliberately sufficient and simpler to reason
about, run locally, and demonstrate correctness in:
- `FOR UPDATE SKIP LOCKED` gives genuine multi-consumer claim semantics without
  operating a second piece of infrastructure.
- Priority ordering, rate-limit lookups, retry state, and the audit trail are
  all just queries against one table (via the ORM), joinable and query-able ad
  hoc (`GET /jobs/`) without a separate analytics pipeline.
- It keeps `docker-compose up` (or a single native Postgres) sufficient to run
  the whole system and its test suite.

§11 covers exactly where this stops scaling and what would need to change.

## 10. Simplifying assumptions (explicit)

- **Single Postgres instance, no read replicas / sharding.** Fine up to roughly
  the point discussed in §11; a sharded or broker-fronted design would be a
  bigger rewrite than the scope here warrants.
- **Delivery is genuinely mocked** (`src/jobs/sender.py`): random latency + a
  configurable failure rate, no real email/SMS/push provider integration, and no
  provider-side idempotency key modeled (§3.6).
- **Rate limiting is a fixed hourly window measured off `Job.sent_at`**, not a
  sliding window or token bucket, and is recomputed by a `count()` per claimed
  job rather than a maintained counter. Simple and exactly consistent with the
  source of truth; not the cheapest at scale (§11).
- **Webhook delivery is one attempt, not retried** (§8), and is fired
  synchronously from the worker's processing loop rather than handed off to a
  separate dispatcher -- keeps the worker loop simple, at the cost of a slow
  webhook receiver adding latency to that worker's throughput.
- **Priority is a soft preference, not a reserved-capacity guarantee** (§5) --
  no separate high-priority-only worker pool.
- **No authentication/authorization on the API.** Out of scope for the exercise;
  would need DRF authentication/permission classes (or API keys / mTLS) between
  callers and the API in any real deployment.
- **`max_attempts` is caller-settable per job** (bounded 1-20 by the serializer)
  rather than fixed system-wide, to demonstrate the cap is a real per-job field,
  not a hardcoded constant.
- **`CONN_MAX_AGE=0`** (Django closes each request's DB connection rather than
  pooling it) -- adequate at this scale; a real deployment would put PgBouncer
  (or similar) in front of Postgres rather than rely on Django's built-in
  per-process connection reuse (see §11.4).
- **Django's migration framework is the schema story** (`manage.py
  makemigrations` / `migrate`), which is the standard, production-appropriate
  choice here (unlike a hand-rolled schema-application script) -- no simplifying
  assumption needed on this point, noted for completeness.

## 11. Scaling to millions of jobs / thousands of workers -- what breaks first

Roughly in the order it would start to hurt:

1. **The claim query's `UPDATE ... FOR UPDATE SKIP LOCKED` on one table becomes a
   contention point.** Thousands of workers all hammering the same partial index
   with `LIMIT :batch_size` will still correctly avoid double-claiming, but
   throughput is bounded by single-table write contention and WAL generation on
   one primary. This is the first real ceiling. Mitigations, roughly in order of
   effort: bigger claim batches per poll (fewer round trips), sharding the jobs
   table by a hash of `recipient` or tenant across multiple Postgres
   instances/schemas so claims spread across independently-lockable tables, or
   graduating to a broker (SQS/Kafka) purpose-built for exactly this fan-out and
   letting Postgres hold only the durable record + final status.
2. **The rate-limit check (`count()` on `sent_at > now() - 1h` per recipient)
   does one query per claimed job.** At millions of sends/hour this index still
   scales reasonably (it's a narrow, well-targeted partial index), but it's extra
   round-trip latency per job on the hot path. Replace with a Redis `INCR` +
   `EXPIRE` sliding-window counter per recipient -- O(1), no DB round trip, and
   naturally shardable by recipient across a Redis cluster.
3. **The jobs table itself grows unbounded** (sent/dead_letter rows never
   deleted). Index bloat and autovacuum pressure grow with it, even though the
   claim query's partial index only *indexes* pending rows, and would eventually
   slow down `GET /jobs/`, `GET /metrics/`, and background vacuum. Fix: partition
   `jobs` by `created_at` (monthly/weekly) and archive/drop old partitions, or
   move terminal (`sent`/`dead_letter`/`cancelled`) rows to a separate
   history table on a schedule.
4. **Per-process DB connections become the ceiling on how many worker instances
   can poll concurrently.** `CONN_MAX_AGE=0` means each Django process opens
   connections as needed rather than pooling internally; at thousands of workers,
   `max_connections` on Postgres itself becomes the limit. Put PgBouncer/PgCat in
   front of Postgres in transaction-pooling mode so worker/API connection counts
   don't map 1:1 onto real Postgres backend connections.
5. **The synchronous webhook POST inside the worker's processing loop** becomes
   a throughput drag if the receiver is slow, even with a short timeout, at high
   enough job volume. Move webhook dispatch to its own queue/table + dedicated
   dispatcher pool, decoupled from delivery workers.
6. **A single logical `next_attempt_at` ORDER BY across the whole table** stops
   being the cheapest way to express priority once priority tiers need *hard*
   isolation (e.g. "critical alerts must never wait behind a backlog of low
   priority marketing sends," not just "usually go first") -- at that point,
   physically separate queues/topics per priority tier (separate tables, or
   separate broker queues) with dedicated worker pools per tier is the correct
   answer, not a bigger shared table.

In short: the claim mechanism (§3) is designed to *stay correct* at any of these
scales -- `FOR UPDATE SKIP LOCKED` doesn't degrade into double-sends under more
load, it degrades into *contention* (workers doing more skipped-lock retries,
lower throughput per worker). Correctness holds; the thing that erodes first is
raw throughput on a single Postgres primary, which is why §11's fixes are mostly
about spreading load (sharding, Redis, brokers, pooling) rather than fixing a
correctness bug.
