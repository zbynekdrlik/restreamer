# Cache-Metric Reform + Start Delivering Reset — Design Spec

**Date:** 2026-05-11
**Issue:** to-be-filed
**Status:** Approved for implementation
**Goal:** Eliminate three operator-reported regressions in one coherent change:
1. Cache bar "112s → 1s" drop at first-push transition
2. `received_bytes` counter accumulating across sessions (57 GB for a 3-min test stream)
3. Fast endpoints (Kiko) ending up ~50 s behind live edge after VPS creation completes

---

## 1. Problem

### 1.1 Cache bar drop at first-push (operator-reported, verified live 2026-05-11)

Live capture on streamsnv v0.8.0-dev, scraping dashboard `.endpoint-cache-label` every 5 s through a Stop → Start Delivering cycle:

| t (s) | Label (non-fast) | Source |
|---:|---|---|
| 0-5 | empty | VPS creating |
| 10 | 45s / 120s | `ps.cache_duration_secs` growing |
| 75 | 112s / 120s | still growing |
| **80** | **1s / 120s** | switched to per-endpoint `chunk_delay_secs` |
| 85-200 | 7s → 120s | regrowing, second prefill |
| 240 | 121s / 120s | finally steady |

The 112 → 1 drop happens at the moment any endpoint first pushes (`chunks_processed > 0`), because the dashboard switches from `ps.cache_duration_secs` (host-side S3-buffer total) to per-endpoint `chunk_delay_secs` (currently computed as "buffer ABOVE the endpoint's current chunk"). At first push, the endpoint sits at the chunk it just pushed and there is nothing newer in S3 yet from THIS delivery's perspective → 1 s.

### 1.2 `received_bytes` accumulates across all sessions

`streaming_events.received_bytes` is a cumulative counter incremented by the RTMP ingest as long as `receiving_activated = true`. There is no reset on `Start Delivering`. An event that has been streamed intermittently over multiple days accumulates 57+ GB; the dashboard shows that total, confusing operators who expect the displayed value to reflect the current Start Delivering cycle.

S3 chunks are already wiped on Start (`delivery.rs::wipe_event_s3_chunks`, #174), but the byte counter is independent and persists.

### 1.3 Fast endpoint stuck 50 s behind live edge after VPS creation

`start_chunk_id` is computed once in `delivery_init_sent` (≈t=2 s on the host clock). VPS creation takes 30-50 s. By the time the VPS is `delivering` and endpoints start pushing, `start_chunk_id` is 30-50 s stale.

For non-fast endpoints (delivery_delay_ms = 120 000) this is fine: they wait for 120 s of buffer above `start_chunk_id` anyway, so a 50 s head start on buffering only shortens the warmup.

For fast endpoints (Kiko, is_fast = true, delivery_delay_ms = 0) this is broken:
- Kiko's design intent is "push at live edge with minimum buffer"
- Kiko starts pushing chunk `start_chunk_id`, which is 50 s behind live edge by the time VPS is ready
- The lag-probe in `producer_lag.rs::maybe_jump` is short-circuited for `delivery_delay_ms == 0` (line 63), so Kiko never catches up
- Result: Kiko streams content 50 s behind reality, indefinitely. Operator sees both Kiko and the 120 s endpoints display roughly the same timestamp in their downstream video, defeating the "fast" purpose.

---

## 2. Design

### 2.1 Change A — `chunk_delay_secs` semantics: lag from live edge

Today's `db::get_cache_duration_secs(event_id, delivered_up_to)` returns `SUM(duration_ms) FROM chunk_records WHERE sequence_number > delivered_up_to`. That is "buffer above consumer".

Replace per-endpoint use of this metric with **lag-from-live-edge**:

```rust
pub async fn get_endpoint_lag_secs(
    pool: &SqlitePool,
    event_id: i64,
    endpoint_current_chunk_id: i64,
) -> Result<f64> {
    // SUM of durations from endpoint's current chunk up to the live edge.
    // Equivalent to: how many seconds of stream content is between this
    // endpoint's read position and the most recently uploaded chunk.
    let row = sqlx::query(
        "SELECT COALESCE(SUM(duration_ms), 0) as total_ms FROM chunk_records
         WHERE streaming_event_id = ?1
           AND sent = 1
           AND sequence_number > ?2
           AND sequence_number <= (
             SELECT COALESCE(MAX(sequence_number), 0) FROM chunk_records
             WHERE streaming_event_id = ?1 AND sent = 1
           )",
    )
    .bind(event_id)
    .bind(endpoint_current_chunk_id)
    .fetch_one(pool)
    .await?;
    Ok(row.get::<i64, _>("total_ms") as f64 / 1000.0)
}
```

Wire `delivery_status.rs` per-endpoint `chunk_delay_secs` to this new helper, passing `ep.current_chunk_id`.

**Resulting transition with new metric:**

| t (s) | ps.cache_duration_secs | per-endpoint lag | Dashboard reads |
|---:|---:|---:|---:|
| 10 | 45 | (no endpoint yet) | 45 (fallback) |
| 75 | 112 | (no endpoint yet) | 112 (fallback) |
| 80 | 120 | **120** (just pushed chunk 51, live edge at chunk 110) | **120** ✓ no jump |
| 200 | 120 | 120 | 120 ✓ steady |

**For fast endpoints (Kiko, is_fast = true) the same metric reads ~2 s steady** (since the endpoint is at live_edge-1, the lag is one chunk duration).

### 2.2 Change B — `received_bytes` reset on Start Delivering

In `delivery.rs::start_delivery`, after the existing `wipe_event_s3_chunks` call (line 261) and before spawning the VPS:

```rust
// Reset received_bytes counter so dashboard reflects only current
// delivery cycle, not cumulative bytes since event creation.
if let Err(e) = sqlx::query(
    "UPDATE streaming_events SET received_bytes = 0 WHERE id = ?1",
)
.bind(event_id)
.execute(&self.pool)
.await
{
    warn!(event_id, "received_bytes reset failed: {e}");
}
```

Best-effort like the S3 wipe — a DB error logs warn but doesn't abort Start Delivering.

### 2.3 Change C — Fast endpoint start_chunk_id recompute on VPS-ready

In `delivery.rs`, the existing monitor loop polls the VPS status. When status transitions from `creating`/`booting`/`initializing` → `delivering`, the host has the freshest `MAX(sent)+1` and the fast endpoints are about to start pushing from the stale `start_chunk_id`.

Add a transition handler:

```rust
async fn on_vps_ready(&self, event_id: i64, instance: &DeliveryInstance) -> Result<()> {
    let fresh_live_edge = db::compute_target_start_chunk(&self.pool, event_id).await?;
    let endpoints = db::get_event_endpoints(&self.pool, event_id).await?;
    let fast_eps: Vec<_> = endpoints.iter().filter(|e| e.is_fast).collect();
    if fast_eps.is_empty() {
        return Ok(());
    }
    let url = format!("http://{}:8000/api/endpoints/update_start", instance.ipv4);
    let client = reqwest::Client::new();
    for ep in &fast_eps {
        let body = serde_json::json!({
            "alias": ep.alias,
            "new_start_chunk_id": fresh_live_edge,
        });
        let _ = client.post(&url)
            .bearer_auth(&instance.auth_token)
            .json(&body)
            .timeout(Duration::from_secs(5))
            .send().await;
        // Audit: fast endpoint jumped to fresh live edge.
        if let Some(tx) = &self.audit_tx {
            rs_core::audit::record(tx, AuditRow {
                severity: Severity::Info,
                source: Source::Delivery,
                event_id: Some(event_id),
                instance_id: Some(instance.id),
                endpoint: Some(ep.alias.clone()),
                action: Action::FastEndpointJumpedToLiveEdge,
                detail: serde_json::json!({
                    "from_chunk_id": existing_start,
                    "to_chunk_id": fresh_live_edge,
                    "gap_chunks": fresh_live_edge - existing_start,
                }),
                ts_override: None,
            });
        }
    }
    Ok(())
}
```

VPS-side `/api/endpoints/update_start` handler:
- Look up endpoint by alias
- Replace `start_chunk_id` in its `EndpointHandle` (requires a watch channel or restart of the consumer)
- New audit row on the VPS: `endpoint_start_chunk_updated`

Trigger: in the existing `metrics_poll` task in `delivery.rs` (the same loop that emits `PipelineState`), on the tick where `instance_status` transitions to `delivering`. Idempotent: only fires once per VPS lifecycle.

### 2.4 Bonus — Fast endpoint cache bar UX

In `operator_dashboard.rs` cache-bar render, branch on `ep.is_fast`:

```rust
let (cache_secs, target_label, threshold_critical_above, threshold_healthy_below) = if ep.is_fast {
    (ep.chunk_delay_secs, "live".to_string(), 8.0, 5.0)
} else {
    (cache_secs_for_non_fast, format!("{}s", target), target as f64 * threshold_mult, target as f64 * 0.75)
};
let label = format!("{}s / {} cache", cache_secs as u64, target_label);
let bar_class = if cache_secs > threshold_critical_above {
    "buffer-bar-fill critical"
} else if cache_secs <= threshold_healthy_below {
    "buffer-bar-fill healthy"
} else {
    "buffer-bar-fill warning"
};
```

Result for Kiko (is_fast, lag = 2 s): "2s / live cache", green. Above 8 s = red.

---

## 3. Data flow

### 3.1 Happy path (single Start Delivering cycle, multi-endpoint event)

```
t=0  operator clicks "Start Delivering"
     - delivery.rs::start_delivery() runs:
       - wipe_event_s3_chunks ✓ (#174 existing)
       - UPDATE streaming_events SET received_bytes=0 (new B)
       - spawn VPS (50s)
       - compute initial start_chunk_id from MAX(sent)+1 at t=0
t=2  PipelineState broadcast: cache_duration_secs grows from raw measure,
     dashboard bar shows ps.cache_duration_secs since per-endpoint
     chunks_processed = 0
t=10 cache_duration_secs = 45 → bar shows 45/120
t=50 VPS status → "delivering"
     - on_vps_ready() runs:
       - compute fresh_live_edge = MAX(sent)+1 at t=50 = original_start + 25 chunks
       - For Kiko: POST /api/endpoints/update_start { alias, new_start = fresh_live_edge }
       - For FB/YT/non-fast: no change
       - Audit: FastEndpointJumpedToLiveEdge
t=50 Non-fast endpoints begin first push from original start_chunk_id (51)
     - per-endpoint lag-from-live-edge = ~120s (60 chunks behind)
     - dashboard switches to per-endpoint metric → reads 120s
     - bar stays at 120s ✓ no jump
     Kiko starts pushing from fresh_live_edge
     - per-endpoint lag = ~2s
     - dashboard bar (fast UX) shows "2s / live", green
t=200 steady state: non-fast 120s/120s, Kiko 2s/live
```

### 3.2 Stop Delivering, Start Delivering again

```
operator clicks Stop
     - VPS destroyed
     - delivery_instance row marked deleted
     - received_bytes UNCHANGED (Stop doesn't reset; only Start does)
     - dashboard endpoints disappear
     - cache_duration_secs falls to 0 within the 1.5x cap window

operator clicks Start again
     - wipe_event_s3_chunks (existing S3 cleanup)
     - UPDATE received_bytes = 0 (B kicks in here, NOT on stop — per operator preference)
     - flow continues as 3.1
```

### 3.3 Edge cases

- **No fast endpoints** in event: `on_vps_ready` is a no-op for the C branch.
- **VPS-ready handler fails** (network blip on update_start call): warn-log, audit row records the failure class, Kiko stays at original (50 s stale) start. Operator sees this in audit feed and can fix manually with Stop+Start. Not auto-retry because the issue is rare and a stale Kiko is visible immediately on the wall display.
- **Mid-stream add of fast endpoint** (existing `add_endpoint` flow): already computes a fresh `start_chunk_id` at add-time, so no change needed there.
- **chunk_records pruned** (the 1-hour cleanup in `migrations.rs`): the lag-from-live-edge metric only counts `sent = 1` rows. Pruned rows are gone. If a long-running endpoint falls 1+ hour behind live, the metric undercounts. Acceptable — that endpoint is already in deep trouble.

---

## 4. Error handling

- `received_bytes` reset SQL fails → warn, continue Start
- VPS-ready transition handler fails → warn, audit row, no retry
- `update_start` REST call to VPS fails → warn, audit row, no retry
- Per-endpoint lag query returns 0 with no sent chunks → bar reads 0s (matches "buffer empty" state, correct)

---

## 5. Testing

### 5.1 Unit tests

- `get_endpoint_lag_secs` returns 0 when endpoint at live edge
- `get_endpoint_lag_secs` returns 120s when endpoint is 60 chunks behind live edge (chunk_dur=2000ms)
- `get_endpoint_lag_secs` ignores chunks with `sent=0`
- `start_delivery` resets `received_bytes` to 0
- `start_delivery` reset survives transient SQL error (warn-only, doesn't abort)
- `on_vps_ready` builds correct update payload for fast endpoints, skips non-fast
- `on_vps_ready` audit row carries from_chunk_id, to_chunk_id, gap_chunks

### 5.2 Integration tests

- Stop+Start cycle: dashboard cache bar stays under 1.5x target throughout (already covered by #187 cap, this spec preserves that)
- Stop+Start cycle: per-endpoint cache bar transitions smoothly through first-push moment — NO drop below previous reading (NEW test)
- After Start: `received_bytes` is 0 (NEW test against the `/api/v1/status` endpoint)
- After VPS-ready: fast endpoint's reported `current_chunk_id` is within 2 chunks of live edge, NOT at original start (NEW test)

### 5.3 E2E (Playwright)

- Stop+Start delivery, scrape `.endpoint-cache-label` every 5s for 4 min — no value > 130s (= target × 1.08), no drop > 10s between adjacent samples after first-push fires
- After Start, scrape RTMP-bytes counter on dashboard — reads near 0 (< 100 MB) within first 30 s post-Start
- Kiko cache label shows "Xs / live" format (NOT "Xs / 120s")

---

## 6. Operator validation

Post-deploy on streamsnv, operator:

1. Starts OBS streaming, presses Start Delivering
2. Watches the dashboard cache bar through prefill phase (~120s)
3. Confirms bar grows 0 → 120 smoothly with NO drop at any point
4. Confirms `received_bytes` shows current-session value (< few GB), not 57GB
5. Confirms Kiko cache label reads "Xs / live"
6. Compares Kiko's downstream video preview to a non-fast endpoint's preview — Kiko should be 120 seconds AHEAD in baked-in timestamps (the original "fast" intent restored)

---

## 7. Out of scope

- Replacing the producer/consumer pipeline with EndpointReader+PrefetchReader+PrefetchQueue (issue #188)
- Auto-reset of `received_bytes` on Stop Delivering (operator confirmed: only on Start)
- Lifecycle telemetry wiring (issue #184 / #185 follow-up)

---

## 8. Risks

- Changing `chunk_delay_secs` semantics may break existing dashboards that interpret the old "buffer above consumer" meaning. Audit shows only the leptos cache bar consumes this; safe.
- `update_start` REST endpoint requires VPS code changes. Old VPS binaries (pre-this-PR) won't have it. Backwards-compat: host treats 404 from `update_start` as no-op + audit row. Forwards-compat: new VPS handles missing endpoint alias gracefully.
- `received_bytes` reset on Start changes existing data semantics. Anyone querying historical bytes-per-event will see only the latest cycle's bytes. Acceptable per operator preference.

---

## 9. Acceptance

PR is mergeable when:
- All unit, integration, E2E tests pass in CI
- Operator soak: Stop+Start cycle on streamsnv shows smooth cache bar 0 → 120 with no drops
- Operator confirms Kiko visibly leads non-fast endpoints by 120s in downstream video
- `received_bytes` reads near 0 after each Start Delivering
