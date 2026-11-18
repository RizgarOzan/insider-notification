# Notification service

A small Go service that accepts SMS, email and push notifications over HTTP, queues them in Postgres and delivers them through an external provider with retries, per-channel rate limiting and priorities.

I built it as the take-home case for the Insider One Golang role. The brief asks for something that survives a flash sale, so most of the design is about what happens when things go wrong: a worker dies mid-send, the provider has a bad minute, a client retries a request.

## Run it

```
docker compose up --build
```

That starts Postgres, a mock provider (it answers `202` with a `messageId`, like the webhook.site setup in the case) and the service on `http://localhost:8080`. Migrations run on startup. Swagger UI is at `/docs`, the raw spec at `/openapi.yaml`.

To send through your own webhook.site URL instead:

```
PROVIDER_URL=https://webhook.site/<your-id> docker compose up --build
```

To watch retries happen, make the mock flaky: `MOCK_FAIL_RATE=0.3 docker compose up --build`.

Tests (they need a Postgres; `make test` starts one in docker):

```
make test
# or, with your own database:
TEST_DATABASE_URL=postgres://user:pass@localhost:5432/notif_test go test -race ./...
```

## Try it

```
curl -i -X POST localhost:8080/api/v1/notifications \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: order-1042-sms' \
  -d '{"to":"+905551234567","channel":"sms","content":"Your code is 4821","priority":"high"}'

curl localhost:8080/api/v1/notifications/<id>
curl -X POST localhost:8080/api/v1/notifications/<id>/cancel
curl 'localhost:8080/api/v1/notifications?status=sent&channel=sms&from=2026-11-18T00:00:00Z&limit=20'
curl localhost:8080/api/v1/batches/<batchId>
curl localhost:8080/metrics
```

| Endpoint | What it does |
|---|---|
| `POST /api/v1/notifications` | create one (202) |
| `POST /api/v1/notifications/batch` | create up to 1000, all or nothing (202) |
| `GET /api/v1/notifications/{id}` | status of one |
| `GET /api/v1/batches/{id}` | count per status for a batch |
| `POST /api/v1/notifications/{id}/cancel` | cancel while `queued` or `retrying`, otherwise 409 |
| `GET /api/v1/notifications` | list; filters `status`, `channel`, `batchId`, `from`, `to`; `limit` and `cursor` for paging |
| `GET /healthz`, `GET /readyz`, `GET /metrics` | liveness, readiness (pings Postgres), Prometheus |

`scheduledAt` on a create request holds the message back until that time.

## How it works

```
client -> API -> INSERT (status queued) -> Postgres <- worker loop per channel -> provider
                                              ^                  |
                                              +---- result ------+  (sent / retrying / failed)
```

The API only writes a row. Workers (one loop per channel, in the same process) react to rows that are due. A status change is the event; there is no second system to keep in sync with the table.

**Why a Postgres queue and not Kafka or Redis.** The state we must serve anyway (cancel, filtered list, batch progress) lives in a table. With a separate broker the queue and the table can disagree. With `SELECT ... FOR UPDATE SKIP LOCKED`, claiming a message and changing its status is one statement. The case's own ceiling is 100 messages per second per channel, which Postgres handles comfortably. I measured it instead of guessing (see below). If this ever needs tens of thousands per second, the move is an outbox table feeding Kafka while Postgres stays the source of truth.

**Claiming.** Each channel loop claims up to 20 due rows. The claim sets `status = 'sending'`, bumps `attempts`, and writes a 30 second lease (`locked_until` plus a random `lease_token`). Priorities share a claim 60/30/10 (high/normal/low) and spare slots flow down, so low is slowed but never starved.

**Crashes.** If a worker dies, its rows stay `sending` until the lease runs out. A reaper then puts them back to `retrying`, or to `failed` when they are out of attempts. Because `attempts` is counted at claim time, a message that kills its worker every time cannot loop forever. A worker that comes back late cannot overwrite the new owner: results are written with `WHERE lease_token = ...` and dropped if it no longer matches.

**Rate limit.** One token bucket per channel, 100 per second, burst of 1, applied right before the provider call. A burst larger than 1 would let one closed second hold more than 100 sends. A 429 from the provider pauses that whole channel for `Retry-After` and does not use up an attempt.

**Retries.** `5s * 2^n`, capped at 5 minutes, half of it random ("equal jitter") so retries do not arrive as a herd. 8 attempts, which is roughly 10 minutes of provider outage. 4xx (except 408 and 429) is permanent and fails at once. 5xx, 408, network errors and timeouts are retried. After the last attempt the row is `failed` with `last_error`, and every try is in `notification_attempts`. That table plus `status = 'failed'` is the dead-letter store; I did not add a second one.

**Idempotency.** Optional `Idempotency-Key` header. The key is inserted into a table inside the same transaction as the notification; the unique index makes concurrent requests with one key wait for each other, so exactly one creates. Same key and same body returns the original response with `Idempotent-Replayed: true`. Same key with another body is a 422. Replays return the response as it was when created, not the current status.

**Status names.** `queued -> sending -> sent`, or `retrying -> sending ...` and `failed`, or `cancelled`. It is `sent`, not `delivered`, because the provider only says "accepted"; we do not know it reached a phone. The legal transitions are one table in `internal/domain`, checked in tests for every pair.

**Cancel vs claim.** Cancel is one `UPDATE ... WHERE status IN ('queued','retrying')`. If a worker claimed the row first the update matches nothing and the caller gets a 409. One window remains by design: a message already handed to the provider cannot be pulled back.

**Limits I chose** (the PDF gives none): SMS 160 characters and an E.164 number, push 256, email 10 000 and a plain address. Counted in characters, not bytes.

## Observability

Logs are JSON through `log/slog`. Each request gets an `X-Correlation-ID` (yours, or a generated one). It is stored on the notification and sent to the provider, so one id follows a message from the API call to the worker log line to the provider request.

`/metrics` has `notification_queue_depth{channel,status}` (read from Postgres at scrape time), `notifications_processed_total{channel,result}` (sent, retry, failed, dropped), `provider_request_duration_seconds`, and request counters and latency. Success rate is `rate(...{result="sent"}) / rate(...)` in PromQL.

## Known limits

- **At least once, not exactly once.** If the provider accepts a message but the response is lost, the retry sends it again. The notification id goes out as `Idempotency-Key` so a provider that supports it can drop the duplicate.
- **One process.** The 100 per second limit is in memory. Two replicas would allow 200. Running more than one needs a shared limiter (Redis or a token table). The claim query itself is safe with many workers; the tests run two competing pools.
- **Polling**, every 200 ms when idle. `LISTEN/NOTIFY` would cut idle latency; I did not need it.
- **Tracing, WebSocket and templates** are not built. Polling `GET /notifications/{id}` is the status view.
- The Swagger page loads its JavaScript from a CDN, so `/docs` needs internet; `/openapi.yaml` does not.
- `docker compose` could not be run on the machine this was written on (no Docker there). The CI workflow runs `docker compose up --build`, the curl examples, `make test` and the benchmark on every push, so that flow is checked there.

## I measured it

1000 SMS in one batch call took 0.12 s to accept. With a healthy mock provider all 1000 reached `sent` in 10.3 s, one provider call each, which is the channel limit (100 per second) doing its job. Against a mock that failed 20% of requests with a 503, all 1000 also reached `sent`, in about 50 s, mostly the retry waits, with 1236 provider calls (earlier run). Both on one machine with a local Postgres, no Docker.

To reproduce the healthy-provider number against a running `docker compose up --build`: `scripts/bench.sh` (needs `curl`, `jq`, `bc`). It prints the accept time and the time until all 1000 are `sent`; the channel limit means it cannot be under 10 s. `scripts/smoke.sh` runs the curl examples above and checks the answers. CI runs both.

## Layout

```
cmd/server         the service (API + workers)
cmd/mockprovider   stand-in for webhook.site
internal/domain    types, validation, state transitions, backoff
internal/store     all SQL: create, claim, finish, reap, list
internal/worker    per-channel loops, rate limiter, result handling
internal/provider  the HTTP client for the provider
internal/api       handlers and openapi.yaml
internal/observ    logging, correlation id, metrics
internal/itest     integration tests against real Postgres
migrations         versioned .sql files, applied at startup
```
