#!/usr/bin/env bash
# Reproduces the "Measured" numbers: 1000 SMS in one batch call, time to accept and time until all are sent.
# Usage: scripts/bench.sh [base-url]
# The channel limit is 100/s, so a healthy run cannot finish in under 10 s.
set -euo pipefail
BASE=${1:-http://localhost:8080}
N=1000
BODY=$(jq -nc --argjson n $N '{notifications: [range($n) | {to:"+905551234567", channel:"sms", content:("bench " + tostring)}]}')
T0=$(date +%s.%N)
RESP=$(curl -fsS -X POST "$BASE/api/v1/notifications/batch" -H 'Content-Type: application/json' -d "$BODY")
T1=$(date +%s.%N)
BID=$(echo "$RESP" | jq -er .batchId)
while :; do
  SENT=$(curl -fsS "$BASE/api/v1/batches/$BID" | jq -r '.statuses.sent // 0')
  [ "$SENT" -ge $N ] && break
  [ "$(echo "$(date +%s.%N) - $T0 > 120" | bc)" = 1 ] && { echo "FAIL: only $SENT of $N sent after 120 s" >&2; exit 1; }
  sleep 0.5
done
T2=$(date +%s.%N)
printf 'accepted %d messages in %.2f s, all sent after %.1f s\n' $N "$(echo "$T1 - $T0" | bc)" "$(echo "$T2 - $T0" | bc)"
