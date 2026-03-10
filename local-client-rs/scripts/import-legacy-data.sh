#!/usr/bin/env bash
# import-legacy-data.sh - One-time migration from legacy Django manager to local Rust API.
#
# Imports endpoint configurations and streaming events (with M2M assignments)
# from the production Django manager server into the new Rust local-client API.
#
# Usage:
#   ./import-legacy-data.sh [API_BASE]
#
# Defaults:
#   API_BASE=http://127.0.0.1:8910/api/v1
#
# Prerequisites:
#   - SSH access to the manager server (see CLAUDE.md for host details)
#   - curl and jq installed locally
#   - Local Rust API running at API_BASE

set -euo pipefail

API_BASE="${1:-http://127.0.0.1:8910/api/v1}"
# Manager server connection details - see CLAUDE.md deployment targets
MANAGER_HOST="${MANAGER_HOST:-root@172.105.95.118}"
MANAGER_DIR="${MANAGER_DIR:-/root/kristian/manager-server/restreamer-manager}"
VENV="${MANAGER_VENV:-/root/.virtualenvs/venv/bin/python}"
CLIENT_UUID="${CLIENT_UUID:-95da874e-6b06-41e5-99db-6f47a459c48b}"

echo "=== Restreamer Legacy Data Import ==="
echo "API: $API_BASE"
echo ""

# Verify local API is running
if ! curl -sf "$API_BASE/health" > /dev/null 2>&1; then
    echo "ERROR: Local API not responding at $API_BASE/health"
    exit 1
fi
echo "Local API is healthy."

# --- Extract data from legacy manager ---

echo ""
echo "--- Extracting endpoint data from legacy manager ---"

ENDPOINTS_JSON=$(ssh "$MANAGER_HOST" "cd $MANAGER_DIR && DJANGO_SETTINGS_MODULE=nl_restreamer.settings $VENV manage.py shell -c \"
import json
from restreamer.models import EndPointCfg
from accounts.models import RestreamerUser

user = RestreamerUser.objects.filter(api_key='$CLIENT_UUID').first()
if not user:
    print('[]')
else:
    eps = EndPointCfg.objects.filter(user=user)
    data = []
    for ep in eps:
        data.append({
            'legacy_id': ep.id,
            'alias': ep.alias,
            'service_type': ep.service_type,
            'stream_key': ep.stream_key or '',
            'is_fast': ep.is_fast,
        })
    print(json.dumps(data))
\"")

EP_COUNT=$(echo "$ENDPOINTS_JSON" | jq length)
echo "Found $EP_COUNT endpoints on legacy manager."

echo ""
echo "--- Extracting event data from legacy manager ---"

EVENTS_JSON=$(ssh "$MANAGER_HOST" "cd $MANAGER_DIR && DJANGO_SETTINGS_MODULE=nl_restreamer.settings $VENV manage.py shell -c \"
import json
from restreamer.models import StreamingEvent
from accounts.models import RestreamerUser

user = RestreamerUser.objects.filter(api_key='$CLIENT_UUID').first()
if not user:
    print('[]')
else:
    events = StreamingEvent.objects.filter(user=user)
    data = []
    for ev in events:
        ep_ids = list(ev.end_points.values_list('id', flat=True))
        data.append({
            'legacy_id': ev.id,
            'name': ev.short_description or ev.identifier,
            'endpoint_ids': ep_ids,
        })
    print(json.dumps(data))
\"")

EV_COUNT=$(echo "$EVENTS_JSON" | jq length)
echo "Found $EV_COUNT events on legacy manager."

# --- Import endpoints ---

echo ""
echo "--- Importing endpoints ---"

declare -A EP_MAP

for i in $(seq 0 $((EP_COUNT - 1))); do
    ALIAS=$(echo "$ENDPOINTS_JSON" | jq -r ".[$i].alias")
    SERVICE=$(echo "$ENDPOINTS_JSON" | jq -r ".[$i].service_type")
    STREAM_KEY=$(echo "$ENDPOINTS_JSON" | jq -r ".[$i].stream_key")
    IS_FAST=$(echo "$ENDPOINTS_JSON" | jq -r ".[$i].is_fast")
    LEGACY_ID=$(echo "$ENDPOINTS_JSON" | jq -r ".[$i].legacy_id")

    RESP=$(curl -sf -X POST "$API_BASE/endpoints" \
        -H "Content-Type: application/json" \
        -d "$(jq -n \
            --arg alias "$ALIAS" \
            --arg service "$SERVICE" \
            --arg key "$STREAM_KEY" \
            --argjson fast "$IS_FAST" \
            '{alias: $alias, service_type: $service, stream_key: $key, is_fast: $fast}')" \
        2>&1) || {
        echo "  WARN: Failed to import endpoint '$ALIAS' (may already exist)"
        continue
    }

    NEW_ID=$(echo "$RESP" | jq -r '.id // empty')
    if [ -n "$NEW_ID" ]; then
        EP_MAP[$LEGACY_ID]=$NEW_ID
        echo "  Imported: $ALIAS (legacy=$LEGACY_ID -> new=$NEW_ID)"
    else
        echo "  WARN: No ID returned for endpoint '$ALIAS'"
    fi
done

echo "Imported ${#EP_MAP[@]} endpoints."

# --- Import events ---

echo ""
echo "--- Importing events ---"

declare -A EV_MAP

for i in $(seq 0 $((EV_COUNT - 1))); do
    NAME=$(echo "$EVENTS_JSON" | jq -r ".[$i].name")
    LEGACY_ID=$(echo "$EVENTS_JSON" | jq -r ".[$i].legacy_id")

    RESP=$(curl -sf -X POST "$API_BASE/events" \
        -H "Content-Type: application/json" \
        -d "$(jq -n --arg name "$NAME" '{name: $name}')" \
        2>&1) || {
        echo "  WARN: Failed to import event '$NAME' (may already exist)"
        continue
    }

    NEW_ID=$(echo "$RESP" | jq -r '.id // empty')
    if [ -n "$NEW_ID" ]; then
        EV_MAP[$LEGACY_ID]=$NEW_ID
        echo "  Imported: $NAME (legacy=$LEGACY_ID -> new=$NEW_ID)"
    else
        echo "  WARN: No ID returned for event '$NAME'"
    fi
done

echo "Imported ${#EV_MAP[@]} events."

# --- Assign endpoints to events ---

echo ""
echo "--- Assigning endpoints to events ---"

for i in $(seq 0 $((EV_COUNT - 1))); do
    NAME=$(echo "$EVENTS_JSON" | jq -r ".[$i].name")
    LEGACY_EV_ID=$(echo "$EVENTS_JSON" | jq -r ".[$i].legacy_id")
    NEW_EV_ID="${EV_MAP[$LEGACY_EV_ID]:-}"

    if [ -z "$NEW_EV_ID" ]; then
        echo "  SKIP: Event '$NAME' was not imported, skipping assignments"
        continue
    fi

    EP_IDS=$(echo "$EVENTS_JSON" | jq -r ".[$i].endpoint_ids[]")
    for LEG_EP_ID in $EP_IDS; do
        NEW_EP_ID="${EP_MAP[$LEG_EP_ID]:-}"
        if [ -z "$NEW_EP_ID" ]; then
            echo "  SKIP: Endpoint legacy=$LEG_EP_ID not imported, skipping"
            continue
        fi

        curl -sf -X POST "$API_BASE/events/$NEW_EV_ID/endpoints/$NEW_EP_ID" > /dev/null 2>&1 && \
            echo "  Assigned endpoint $NEW_EP_ID to event '$NAME'" || \
            echo "  WARN: Failed to assign endpoint $NEW_EP_ID to event '$NAME'"
    done
done

echo ""
echo "=== Import complete ==="
echo "Endpoints: ${#EP_MAP[@]} imported"
echo "Events: ${#EV_MAP[@]} imported"
echo ""
echo "Verify at: $API_BASE/events"
