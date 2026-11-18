#!/usr/bin/env bash
# Runs the README curl examples against a running service and checks the answers.
# Usage: scripts/smoke.sh [base-url]
set -euo pipefail
BASE=${1:-http://localhost:8080}
fail() { echo "FAIL: $*" >&2; exit 1; }

curl -fsS "$BASE/healthz" >/dev/null || fail healthz
curl -fsS "$BASE/readyz" >/dev/null || fail readyz

# create (with an idempotency key, sent twice)
KEY="smoke-$(date +%s%N)"
BODY='{"to":"+905551234567","channel":"sms","content":"Your code is 4821","priority":"high"}'
R1=$(curl -fsS -X POST "$BASE/api/v1/notifications" -H 'Content-Type: application/json' -H "Idempotency-Key: $KEY" -d "$BODY")
ID=$(echo "$R1" | jq -er .id)
HDRS=$(curl -fsS -D - -o /dev/null -X POST "$BASE/api/v1/notifications" -H 'Content-Type: application/json' -H "Idempotency-Key: $KEY" -d "$BODY")
echo "$HDRS" | grep -qi '^idempotent-replayed: true' || fail "replay header missing"

# get: it should reach "sent" through the mock provider
for _ in $(seq 1 30); do
  ST=$(curl -fsS "$BASE/api/v1/notifications/$ID" | jq -er .status)
  [ "$ST" = sent ] && break
  sleep 1
done
[ "$ST" = sent ] || fail "notification $ID is $ST, expected sent"

# cancel: a scheduled message is still queued, so cancel works; a second cancel is a 409
FUTURE=$(date -u -d '+1 hour' +%Y-%m-%dT%H:%M:%SZ)
CID=$(curl -fsS -X POST "$BASE/api/v1/notifications" -H 'Content-Type: application/json' \
  -d "{\"to\":\"a@example.com\",\"channel\":\"email\",\"content\":\"later\",\"scheduledAt\":\"$FUTURE\"}" | jq -er .id)
curl -fsS -X POST "$BASE/api/v1/notifications/$CID/cancel" >/dev/null || fail cancel
[ "$(curl -fsS "$BASE/api/v1/notifications/$CID" | jq -er .status)" = cancelled ] || fail "not cancelled"
CODE=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$BASE/api/v1/notifications/$CID/cancel")
[ "$CODE" = 409 ] || fail "second cancel gave $CODE, expected 409"

# list
curl -fsS "$BASE/api/v1/notifications?status=sent&channel=sms&limit=20" | jq -e . >/dev/null || fail list

# batch
BATCH=$(curl -fsS -X POST "$BASE/api/v1/notifications/batch" -H 'Content-Type: application/json' \
  -d '{"notifications":[{"to":"+905551234567","channel":"sms","content":"one"},{"to":"b@example.com","channel":"email","content":"two"},{"to":"device-token-1","channel":"push","content":"three"}]}')
BID=$(echo "$BATCH" | jq -er .batchId)
for _ in $(seq 1 30); do
  SENT=$(curl -fsS "$BASE/api/v1/batches/$BID" | jq -r '.statuses.sent // 0')
  [ "$SENT" = 3 ] && break
  sleep 1
done
[ "$SENT" = 3 ] || fail "batch $BID has $SENT sent, expected 3"

METRICS=$(curl -fsS "$BASE/metrics")
grep -q notifications_processed_total <<<"$METRICS" || fail metrics
echo "smoke ok"
