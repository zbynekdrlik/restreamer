#!/usr/bin/env bash
# Phase 1 mini-soak (issue #176, spec section 5.2).
# 30 min sample loop asserting per-endpoint chunk_delay and FB death-rate.
# Manual run:  EVENT_ID=9289 ./scripts/soak-mini.sh
# CI invokes via .github/workflows/soak-mini.yml.

set -euo pipefail

HOST="${HOST:-http://10.77.9.204:8910}"
EVENT_ID="${EVENT_ID:?EVENT_ID env var required}"
DELIVERY_DELAY_SECS="${DELIVERY_DELAY_SECS:-120}"
SAMPLES="${SAMPLES:-60}"
INTERVAL_SECS="${INTERVAL_SECS:-30}"
MAX_DEATHS_PER_ENDPOINT="${MAX_DEATHS_PER_ENDPOINT:-50}"

START_TS="$(date -u +%Y-%m-%dT%H:%M:%S.000Z)"
echo "soak-mini start: host=$HOST event=$EVENT_ID samples=$SAMPLES interval=${INTERVAL_SECS}s start_ts=$START_TS"

threshold_for_alias() {
  local alias_in="$1"
  case "$alias_in" in
    FB-*) echo "1.3" ;;
    *)    echo "1.1" ;;
  esac
}

FAIL_FILE="${FAIL_FILE:-/tmp/soak-mini-fails.txt}"
: > "$FAIL_FILE"

for i in $(seq 1 "$SAMPLES"); do
  sleep "$INTERVAL_SECS"
  status_json="$(curl -fsS --max-time 10 "$HOST/api/v1/delivery/status?event_id=$EVENT_ID")"
  echo "[sample $i] $status_json"

  while read -r ep_json; do
    [ -z "$ep_json" ] && continue
    alias_val="$(echo "$ep_json" | jq -r '.alias')"
    delay="$(echo "$ep_json" | jq -r '.chunk_delay_secs')"
    threshold="$(threshold_for_alias "$alias_val")"
    limit="$(awk -v d="$DELIVERY_DELAY_SECS" -v t="$threshold" 'BEGIN { printf "%.3f", d * t }')"
    over="$(awk -v dly="$delay" -v lim="$limit" 'BEGIN { print (dly > lim) ? 1 : 0 }')"
    if [ "$over" = "1" ]; then
      msg="[sample $i] alias='$alias_val' delay=${delay}s exceeds threshold ${limit}s (target=${DELIVERY_DELAY_SECS}s, mult=${threshold})"
      echo "FAIL: $msg" >&2
      echo "$msg" >> "$FAIL_FILE"
    fi
  done < <(echo "$status_json" | jq -c '.endpoint_details[]?')

  if [ -s "$FAIL_FILE" ]; then
    echo "==== soak-mini FAIL detail ===="
    cat "$FAIL_FILE"
    exit 1
  fi
done

# Cumulative death-rate check at end of window.
audit_json="$(curl -fsS --max-time 30 "$HOST/api/v1/audit?event_id=$EVENT_ID&action=endpoint_rtmp_push_died&since=$START_TS&limit=10000")"
echo "audit_json size: $(echo "$audit_json" | wc -c)"
deaths_per_endpoint="$(echo "$audit_json" | jq -r '[.rows // .items // [] | group_by(.endpoint) | map({endpoint: (.[0].endpoint // "<unknown>"), count: length})][0]')"
echo "deaths_per_endpoint=$deaths_per_endpoint"

failed=0
while read -r row; do
  [ -z "$row" ] && continue
  ep="$(echo "$row" | jq -r '.endpoint')"
  cnt="$(echo "$row" | jq -r '.count')"
  if [ "$cnt" -gt "$MAX_DEATHS_PER_ENDPOINT" ]; then
    echo "FAIL: endpoint=$ep had $cnt rtmp_push_died audit rows (limit $MAX_DEATHS_PER_ENDPOINT)" >&2
    failed=1
  fi
done < <(echo "$deaths_per_endpoint" | jq -c '.[]?')

if [ "$failed" = "1" ]; then
  exit 1
fi

echo "soak-mini PASS: $SAMPLES samples * ${INTERVAL_SECS}s = $((SAMPLES * INTERVAL_SECS / 60)) min, all endpoints under thresholds."
