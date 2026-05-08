# Soak gate + RTMP/cache telemetry — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land Phase 1 of the three-phase 4h-green-dashboard recovery: hardened YT health gate, mini-soak workflow, RTMP-push + disk_cache telemetry, and a diagnostic-dump endpoint. No behavior fix to data path — telemetry data feeds Phase 2/3.

**Architecture:** Five additive components per spec §4. Tasks 1-17 are subagent-dispatched. Task 18 is orchestrator-only (push, CI, manual soak-mini dispatch as gate-validation, follow-up issues, completion report).

**Tech Stack:** Rust 2024, Axum, sqlx, tokio, dashmap, hdrhistogram (or simple bucketing), Playwright, GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-05-06-soak-gate-and-telemetry-design.md` (commit 144afe9 on dev).

**Issue:** #176

---

## Context

Branch: `dev` at 144afe9 (= 9cb5ac4 + new spec). Version: 0.5.0. PR #170 (disk_cache) is OPEN and STAYS OPEN through this work. Phase 1 lands separately on top of dev.

Local checks: `cargo fmt --all --check` only. NO `cargo build/test/clippy` locally per `ci-push-discipline`. TDD strict — failing-test commit BEFORE implementation commit. One commit per task. Subagents do NOT push, compile, or run tests locally. Every new `.rs` file <1000 lines. ASCII-only PowerShell strings in CI YAML. Mutation testing must NOT exclude new files. All commits reference `(#176)`.

---

### Task 1: Version bump 0.5.0 → 0.6.0

**Files:**
- Modify: `Cargo.toml`
- Modify: `src-tauri/Cargo.toml`
- Modify: `src-tauri/tauri.conf.json`
- Modify: `leptos-ui/Cargo.toml`

- [ ] **Step 1:** In `Cargo.toml` (workspace at repo root), change `version = "0.5.0"` → `version = "0.6.0"`.

- [ ] **Step 2:** In `src-tauri/Cargo.toml`, change `version = "0.5.0"` → `version = "0.6.0"`.

- [ ] **Step 3:** In `src-tauri/tauri.conf.json`, change `"version": "0.5.0"` → `"version": "0.6.0"`.

- [ ] **Step 4:** In `leptos-ui/Cargo.toml`, change `version = "0.5.0"` → `version = "0.6.0"`.

- [ ] **Step 5:** `cargo fmt --all --check` (must pass).

- [ ] **Step 6:** Commit:

```bash
git add Cargo.toml src-tauri/Cargo.toml src-tauri/tauri.conf.json leptos-ui/Cargo.toml
git commit -m "chore: bump version to 0.6.0 (#176)"
```

---

### Task 2: TDD — failing test for `Action::DiskCachePushSample` rate-limiter keying

**Files:**
- Modify: `crates/rs-core/src/audit.rs` (test only — Action variant added in Task 3)
- Test: `crates/rs-core/src/audit.rs::tests`

- [ ] **Step 1:** Add a failing test under the existing `#[cfg(test)] mod tests {}` block in `crates/rs-core/src/audit.rs`:

```rust
#[test]
fn rate_limiter_keys_disk_cache_push_sample_per_endpoint() {
    let rl = RateLimiter::new(std::time::Duration::from_secs(60));
    assert!(rl.allow(Action::DiskCachePushSample, "FB-NewLevel"));
    assert!(!rl.allow(Action::DiskCachePushSample, "FB-NewLevel"));
    // Different endpoint key -> separate slot, must allow.
    assert!(rl.allow(Action::DiskCachePushSample, "YT NLCH 4K"));
    assert!(!rl.allow(Action::DiskCachePushSample, "YT NLCH 4K"));
}
```

- [ ] **Step 2:** Confirm test fails: `Action::DiskCachePushSample` does not exist yet → compile-time error proves the test is "RED" (per TDD it counts as failing).

- [ ] **Step 3:** Commit:

```bash
git add crates/rs-core/src/audit.rs
git commit -m "test(audit): failing rate-limiter keying test for DiskCachePushSample (#176)"
```

---

### Task 3: Add `Action::DiskCachePushSample` variant

**Files:**
- Modify: `crates/rs-core/src/audit.rs`

- [ ] **Step 1:** Add the new variant at the end of the disk-cache action group, immediately after `DiskCacheReaderRecovered`:

```rust
    /// Per-endpoint push sample emitted by EndpointReader on chunk push.
    /// Rate-limited 1/min/endpoint via RateLimiter keyed by
    /// (DiskCachePushSample, endpoint_alias). Carries chunk_supply_lag_ms,
    /// inter_chunk_gap_ms, burst_factor, current_chunk_delay_secs, and
    /// delivery_delay_secs target. Issue #176.
    DiskCachePushSample,
```

- [ ] **Step 2:** Run `cargo fmt --all --check`.

- [ ] **Step 3:** Commit:

```bash
git add crates/rs-core/src/audit.rs
git commit -m "feat(audit): add Action::DiskCachePushSample (#176)"
```

---

### Task 4: TDD — failing tests for `RtmpPushTelemetry::snapshot`

**Files:**
- Create: `crates/rs-delivery/src/rtmp_push_telemetry.rs` (test module only — impl in Task 5)
- Modify: `crates/rs-delivery/src/lib.rs` (declare module)

- [ ] **Step 1:** Create `crates/rs-delivery/src/rtmp_push_telemetry.rs` with the test module ONLY. The struct + impl are Task 5; the module compiles to empty + tests until Task 5.

```rust
// Phase 1 telemetry struct for the rust_rtmp_push backend.
// See docs/superpowers/specs/2026-05-06-soak-gate-and-telemetry-design.md §5.3.
// Issue #176.

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn snapshot_with_no_ack_reports_null_time_since_ack() {
        let mut t = RtmpPushTelemetry::new_for_test_at(0);
        t.advance_clock_for_test(Duration::from_millis(1500));
        t.note_send("Audio", 1024);
        t.note_chunk_pushed();
        let v = t.snapshot(&[0u8; 8]);
        assert_eq!(v["bytes_sent_since_connect"], 1024);
        assert_eq!(v["time_since_connect_ms"], 1500);
        assert_eq!(v["time_since_last_upstream_ack_ms"], serde_json::Value::Null);
        assert_eq!(v["last_rtmp_message_type_sent"], "Audio");
        assert_eq!(v["chunks_pushed"], 1);
    }

    #[test]
    fn snapshot_after_ack_reports_age_since_ack() {
        let mut t = RtmpPushTelemetry::new_for_test_at(0);
        t.advance_clock_for_test(Duration::from_millis(500));
        t.note_upstream_ack();
        t.advance_clock_for_test(Duration::from_millis(750));
        let v = t.snapshot(&[]);
        assert_eq!(v["time_since_last_upstream_ack_ms"], 750);
    }

    #[test]
    fn snapshot_hex_encodes_close_buffer() {
        let mut t = RtmpPushTelemetry::new_for_test_at(0);
        let v = t.snapshot(&[0xDE, 0xAD, 0xBE, 0xEF]);
        assert_eq!(v["upstream_close_first_bytes_hex"], "deadbeef");
    }

    #[test]
    fn snapshot_truncates_close_buffer_to_64_bytes() {
        let mut t = RtmpPushTelemetry::new_for_test_at(0);
        let buf = vec![0xAA; 200];
        let v = t.snapshot(&buf);
        let hex = v["upstream_close_first_bytes_hex"].as_str().unwrap();
        assert_eq!(hex.len(), 128); // 64 bytes * 2 hex chars
    }
}
```

- [ ] **Step 2:** In `crates/rs-delivery/src/lib.rs`, add `pub mod rtmp_push_telemetry;` (alongside existing `pub mod` declarations — pick the section with similar utility modules).

- [ ] **Step 3:** Confirm RED: compile fails because `RtmpPushTelemetry` does not exist.

- [ ] **Step 4:** Commit:

```bash
git add crates/rs-delivery/src/rtmp_push_telemetry.rs crates/rs-delivery/src/lib.rs
git commit -m "test(rtmp-push-telemetry): failing snapshot tests (#176)"
```

---

### Task 5: Implement `RtmpPushTelemetry`

**Files:**
- Modify: `crates/rs-delivery/src/rtmp_push_telemetry.rs`

- [ ] **Step 1:** Replace the file body (above the test module) with the full implementation:

```rust
// Phase 1 telemetry struct for the rust_rtmp_push backend.
// See docs/superpowers/specs/2026-05-06-soak-gate-and-telemetry-design.md §5.3.
// Issue #176.

use std::time::{Duration, Instant};

/// Per-session telemetry counters for one RTMP push connection.
/// Reset on each connect by constructing a fresh value.
pub struct RtmpPushTelemetry {
    connect_at: Instant,
    bytes_sent: u64,
    last_upstream_ack_at: Option<Instant>,
    last_message_type_sent: Option<&'static str>,
    chunks_pushed: u32,
    /// Test-only override: when Some, overrides Instant::now() for snapshot math.
    test_clock: Option<Instant>,
}

impl RtmpPushTelemetry {
    pub fn new() -> Self {
        Self {
            connect_at: Instant::now(),
            bytes_sent: 0,
            last_upstream_ack_at: None,
            last_message_type_sent: None,
            chunks_pushed: 0,
            test_clock: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn new_for_test_at(_offset_ms: u64) -> Self {
        let now = Instant::now();
        Self {
            connect_at: now,
            bytes_sent: 0,
            last_upstream_ack_at: None,
            last_message_type_sent: None,
            chunks_pushed: 0,
            test_clock: Some(now),
        }
    }

    #[cfg(test)]
    pub(crate) fn advance_clock_for_test(&mut self, by: Duration) {
        let cur = self.test_clock.expect("test clock not set");
        self.test_clock = Some(cur + by);
    }

    fn now(&self) -> Instant {
        self.test_clock.unwrap_or_else(Instant::now)
    }

    pub fn note_send(&mut self, msg_type: &'static str, n_bytes: u64) {
        self.last_message_type_sent = Some(msg_type);
        self.bytes_sent = self.bytes_sent.saturating_add(n_bytes);
    }

    pub fn note_upstream_ack(&mut self) {
        self.last_upstream_ack_at = Some(self.now());
    }

    pub fn note_chunk_pushed(&mut self) {
        self.chunks_pushed = self.chunks_pushed.saturating_add(1);
    }

    pub fn snapshot(&self, close_buf: &[u8]) -> serde_json::Value {
        let now = self.now();
        let time_since_connect_ms =
            now.saturating_duration_since(self.connect_at).as_millis() as u64;
        let time_since_last_upstream_ack_ms = self
            .last_upstream_ack_at
            .map(|t| now.saturating_duration_since(t).as_millis() as u64);

        let truncated = if close_buf.len() > 64 { &close_buf[..64] } else { close_buf };
        let mut hex = String::with_capacity(truncated.len() * 2);
        for b in truncated {
            hex.push_str(&format!("{:02x}", b));
        }

        serde_json::json!({
            "bytes_sent_since_connect": self.bytes_sent,
            "time_since_connect_ms": time_since_connect_ms,
            "time_since_last_upstream_ack_ms": time_since_last_upstream_ack_ms,
            "last_rtmp_message_type_sent": self.last_message_type_sent,
            "chunks_pushed": self.chunks_pushed,
            "upstream_close_first_bytes_hex": hex,
        })
    }
}

impl Default for RtmpPushTelemetry {
    fn default() -> Self {
        Self::new()
    }
}
```

- [ ] **Step 2:** `cargo fmt --all --check`.

- [ ] **Step 3:** Commit:

```bash
git add crates/rs-delivery/src/rtmp_push_telemetry.rs
git commit -m "feat(rtmp-push-telemetry): RtmpPushTelemetry with test clock (#176)"
```

---

### Task 6: TDD — failing test for `emit_rtmp_push_died_detailed`

**Files:**
- Modify: `crates/rs-delivery/src/endpoint_audit.rs`

- [ ] **Step 1:** Add a failing test in `crates/rs-delivery/src/endpoint_audit.rs` test module (or create one if missing):

```rust
#[test]
fn emit_rtmp_push_died_detailed_includes_telemetry_fields() {
    use crate::rtmp_push_telemetry::RtmpPushTelemetry;
    use rs_core::audit::AuditRing;
    use std::sync::Arc;

    let ring = Arc::new(AuditRing::new(64));
    let mut tel = RtmpPushTelemetry::new();
    tel.note_send("Audio", 100);
    tel.note_chunk_pushed();
    let close_buf = [0x00, 0xC0, 0x00, 0x03];

    emit_rtmp_push_died_detailed(
        &Some(Arc::clone(&ring)),
        "FB-NewLevel",
        "upstream closed connection mid-stream: unexpected end of file",
        3000,
        2840,
        &tel,
        &close_buf,
        0, // chunks_buffered_in_pipeline
    );

    let rows = ring.snapshot();
    assert_eq!(rows.len(), 1);
    let detail = &rows[0].detail;
    assert_eq!(detail["backend"], "rust_rtmp_push");
    assert_eq!(detail["reconnect_count"], 2840);
    assert_eq!(detail["bytes_sent_since_connect"], 100);
    assert_eq!(detail["chunks_pushed"], 1);
    assert_eq!(detail["last_rtmp_message_type_sent"], "Audio");
    assert_eq!(detail["upstream_close_first_bytes_hex"], "00c00003");
    assert_eq!(detail["chunks_buffered_in_pipeline"], 0);
}
```

- [ ] **Step 2:** Confirm RED — `emit_rtmp_push_died_detailed` does not exist.

- [ ] **Step 3:** Commit:

```bash
git add crates/rs-delivery/src/endpoint_audit.rs
git commit -m "test(endpoint-audit): failing test for emit_rtmp_push_died_detailed (#176)"
```

---

### Task 7: Implement `emit_rtmp_push_died_detailed` and migrate call sites

**Files:**
- Modify: `crates/rs-delivery/src/endpoint_audit.rs`
- Modify: `crates/rs-delivery/src/endpoint_consumer_helpers.rs` (call sites)
- Modify: `crates/rs-delivery/src/endpoint_task_rust_push_tests.rs` (existing tests update if needed)

- [ ] **Step 1:** In `endpoint_audit.rs`, add the new helper alongside `emit_rtmp_push_died`:

```rust
/// Detailed variant of `emit_rtmp_push_died` that merges
/// `RtmpPushTelemetry` + close-buffer + pipeline depth into the audit
/// detail JSON. Phase 1 telemetry — see spec §5.3 and issue #176.
pub fn emit_rtmp_push_died_detailed(
    audit_ring: &Option<Arc<AuditRing>>,
    alias: &str,
    error_display: &str,
    backoff_ms: u64,
    reconnect_count: u32,
    telemetry: &crate::rtmp_push_telemetry::RtmpPushTelemetry,
    close_first_bytes: &[u8],
    chunks_buffered_in_pipeline: u32,
) {
    let Some(ring) = audit_ring else { return };
    let mut detail = telemetry.snapshot(close_first_bytes);
    if let Some(obj) = detail.as_object_mut() {
        obj.insert("backend".into(), serde_json::json!("rust_rtmp_push"));
        obj.insert("error".into(), serde_json::json!(error_display));
        obj.insert("backoff_ms".into(), serde_json::json!(backoff_ms));
        obj.insert("reconnect_count".into(), serde_json::json!(reconnect_count));
        obj.insert(
            "chunks_buffered_in_pipeline".into(),
            serde_json::json!(chunks_buffered_in_pipeline),
        );
    }
    ring.push(
        Severity::Warn,
        Source::Vps,
        Some(alias.to_string()),
        Action::EndpointRtmpPushDied,
        detail,
    );
}
```

- [ ] **Step 2:** Migrate call sites in `crates/rs-delivery/src/endpoint_consumer_helpers.rs` (lines around 138 and 195 per `grep`). Each call must:

  1. Receive the per-session `RtmpPushTelemetry` (added to the consumer state struct in this task — wire as a field of the existing per-session struct; if the struct doesn't carry one, store it in the local fn scope and pass through).
  2. Capture last 64 bytes read from the upstream socket into a small `Vec<u8>`. The read loop already buffers — extend it to keep the most-recent read into a `last_read: Vec<u8>` field on the session state, capped at 64 bytes.
  3. Call `emit_rtmp_push_died_detailed(..)` instead of `emit_rtmp_push_died(..)`.

  Do NOT delete `emit_rtmp_push_died` — it stays as a fallback for sites that have no telemetry context (e.g. early connect-failure paths). Keep both.

- [ ] **Step 3:** In `endpoint_task_rust_push_tests.rs`, update the existing `emit_rtmp_push_died_appends_row_to_ring` test (line ~250) to keep its current assertion (legacy helper still callable) AND add a new test that exercises `emit_rtmp_push_died_detailed` end-to-end through the consumer harness. If the consumer harness is too heavy for unit tests, restrict to verifying the detailed helper appends one row with the merged fields (already covered by Task 6's test — leave it alone).

- [ ] **Step 4:** `cargo fmt --all --check`.

- [ ] **Step 5:** Commit:

```bash
git add crates/rs-delivery/src/endpoint_audit.rs crates/rs-delivery/src/endpoint_consumer_helpers.rs crates/rs-delivery/src/endpoint_task_rust_push_tests.rs
git commit -m "feat(rtmp-push): wire emit_rtmp_push_died_detailed with telemetry (#176)"
```

---

### Task 8: TDD — failing test for `EndpointReader` push-sample math

**Files:**
- Modify: `crates/rs-delivery/src/disk_cache/endpoint_reader.rs` (test module)

- [ ] **Step 1:** Add a failing test (place in the existing `#[cfg(test)] mod tests {}` block):

```rust
#[test]
fn push_sample_payload_math() {
    // Given a known event start and a chunk_id 100 with chunk_duration_ms 1000,
    // expected wall-clock = 100_000 ms after start. Cache made the chunk
    // available 320 ms after that. Inter-chunk-gap from the previous push is
    // 850 ms. chunk_duration_ms / inter_chunk_gap_ms = 1000 / 850 = ~1.176.
    let payload = build_push_sample_payload(
        "FB-NewLevel",
        100,
        /* chunk_supply_lag_ms = */ 320,
        /* inter_chunk_gap_ms = */ 850,
        /* chunk_duration_ms = */ 1000,
        /* delivery_delay_secs = */ 120,
        /* current_chunk_delay_secs = */ 151.3,
    );
    assert_eq!(payload["endpoint"], "FB-NewLevel");
    assert_eq!(payload["chunk_id"], 100);
    assert_eq!(payload["chunk_supply_lag_ms"], 320);
    assert_eq!(payload["inter_chunk_gap_ms"], 850);
    let burst = payload["burst_factor"].as_f64().unwrap();
    assert!((burst - (1000.0 / 850.0)).abs() < 1e-6);
    assert_eq!(payload["delivery_delay_secs"], 120);
    let cd = payload["current_chunk_delay_secs"].as_f64().unwrap();
    assert!((cd - 151.3).abs() < 1e-6);
}

#[test]
fn push_sample_burst_factor_is_zero_when_gap_is_zero() {
    // Edge case: first push, no previous chunk → inter_chunk_gap_ms = 0.
    // Avoid div-by-zero; report burst_factor = 0.0 and let the consumer treat
    // it as "no signal yet".
    let payload = build_push_sample_payload(
        "YT NLCH 4K",
        1,
        0,
        0,
        1000,
        120,
        0.0,
    );
    assert_eq!(payload["burst_factor"].as_f64().unwrap(), 0.0);
}
```

- [ ] **Step 2:** Confirm RED: `build_push_sample_payload` does not exist.

- [ ] **Step 3:** Commit:

```bash
git add crates/rs-delivery/src/disk_cache/endpoint_reader.rs
git commit -m "test(disk_cache): failing tests for push_sample math (#176)"
```

---

### Task 9: Implement push-sample emission in `EndpointReader`

**Files:**
- Modify: `crates/rs-delivery/src/disk_cache/endpoint_reader.rs`

- [ ] **Step 1:** Add the helper above the test module:

```rust
fn build_push_sample_payload(
    endpoint: &str,
    chunk_id: u64,
    chunk_supply_lag_ms: i64,
    inter_chunk_gap_ms: u64,
    chunk_duration_ms: u64,
    delivery_delay_secs: u64,
    current_chunk_delay_secs: f64,
) -> serde_json::Value {
    let burst_factor = if inter_chunk_gap_ms == 0 {
        0.0
    } else {
        chunk_duration_ms as f64 / inter_chunk_gap_ms as f64
    };
    serde_json::json!({
        "endpoint": endpoint,
        "chunk_id": chunk_id,
        "chunk_supply_lag_ms": chunk_supply_lag_ms,
        "inter_chunk_gap_ms": inter_chunk_gap_ms,
        "burst_factor": burst_factor,
        "delivery_delay_secs": delivery_delay_secs,
        "current_chunk_delay_secs": current_chunk_delay_secs,
    })
}
```

- [ ] **Step 2:** In the `EndpointReader` state struct, add fields:

```rust
last_push_at: Option<std::time::Instant>,
last_chunk_id_pushed: Option<u64>,
event_start_at: std::time::Instant, // captured at construction
chunk_duration_ms: u64,             // inherited from event config; default 1000
```

- [ ] **Step 3:** In the existing chunk-push path (the function that calls into `RtmpPusher::push_chunk` or equivalent), AFTER a successful push, compute and emit the sample, gated by the existing `AuditRateLimiter` keyed by `(Action::DiskCachePushSample, endpoint_alias)`:

```rust
let now = std::time::Instant::now();
let inter_chunk_gap_ms = self
    .last_push_at
    .map(|t| now.saturating_duration_since(t).as_millis() as u64)
    .unwrap_or(0);
let expected_wallclock_ms = chunk_id.saturating_mul(self.chunk_duration_ms);
let actual_wallclock_ms = now
    .saturating_duration_since(self.event_start_at)
    .as_millis() as i64;
let chunk_supply_lag_ms = actual_wallclock_ms.saturating_sub(expected_wallclock_ms as i64);

if self.audit_rl.allow(Action::DiskCachePushSample, &self.alias) {
    let payload = build_push_sample_payload(
        &self.alias,
        chunk_id,
        chunk_supply_lag_ms,
        inter_chunk_gap_ms,
        self.chunk_duration_ms,
        self.delivery_delay_secs,
        self.current_chunk_delay_secs,
    );
    if let Some(ring) = &self.audit_ring {
        ring.push(
            Severity::Info,
            Source::Vps,
            Some(self.alias.clone()),
            Action::DiskCachePushSample,
            payload,
        );
    }
}

self.last_push_at = Some(now);
self.last_chunk_id_pushed = Some(chunk_id);
```

If `self.audit_rl` does not exist on the struct yet, add an `Arc<AuditRateLimiter>` field initialized in `EndpointReader::new` from the shared `DiskCache` facade (the facade already owns one per spec §5.4). Use the existing rate-limiter (per-event), do NOT create a new one.

- [ ] **Step 4:** `cargo fmt --all --check`.

- [ ] **Step 5:** Commit:

```bash
git add crates/rs-delivery/src/disk_cache/endpoint_reader.rs
git commit -m "feat(disk_cache): emit DiskCachePushSample in EndpointReader (#176)"
```

---

### Task 10: TDD — failing tests for `S3FetchProfile`

**Files:**
- Create: `crates/rs-delivery/src/s3_fetch/profile.rs` (test module + module declaration)
- Modify: `crates/rs-delivery/src/s3_fetch.rs` OR `crates/rs-delivery/src/s3_fetch/mod.rs` to declare the new submodule. (If `s3_fetch.rs` is a single-file module today, convert to `s3_fetch/mod.rs` + `s3_fetch/profile.rs`. Implementer must check the actual layout and pick the smaller-diff option.)

- [ ] **Step 1:** Create `crates/rs-delivery/src/s3_fetch/profile.rs` with tests only (impl in Task 11):

```rust
// S3 fetch quantile + bucket profile for diag dump.
// See docs/superpowers/specs/2026-05-06-soak-gate-and-telemetry-design.md §5.5.
// Issue #176.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profile_records_count_bytes_and_buckets_latency() {
        let mut p = S3FetchProfile::new();
        p.record_success(45, 1024);
        p.record_success(50, 2048);
        p.record_success(320, 4096);
        let snap = p.snapshot();
        assert_eq!(snap.count, 3);
        assert_eq!(snap.bytes_total, 1024 + 2048 + 4096);
        assert!(snap.p50_latency_ms >= 45 && snap.p50_latency_ms <= 50);
        assert!(snap.p99_latency_ms >= 320);
    }

    #[test]
    fn profile_classifies_failures() {
        let mut p = S3FetchProfile::new();
        p.record_failure("504");
        p.record_failure("504");
        p.record_failure("timeout");
        let snap = p.snapshot();
        assert_eq!(*snap.fail_count_by_class.get("504").unwrap(), 2);
        assert_eq!(*snap.fail_count_by_class.get("timeout").unwrap(), 1);
        assert_eq!(snap.fail_count_by_class.get("503"), None);
    }
}
```

- [ ] **Step 2:** Wire the module: `pub mod profile;` in the parent.

- [ ] **Step 3:** Confirm RED.

- [ ] **Step 4:** Commit:

```bash
git add crates/rs-delivery/src/s3_fetch* 
git commit -m "test(s3_fetch): failing tests for S3FetchProfile (#176)"
```

---

### Task 11: Implement `S3FetchProfile` and integrate

**Files:**
- Modify: `crates/rs-delivery/src/s3_fetch/profile.rs`
- Modify: `crates/rs-delivery/src/disk_cache/download_service.rs` (call sites)

- [ ] **Step 1:** Implement above the tests:

```rust
use std::collections::BTreeMap;
use std::sync::Mutex;

pub struct S3FetchProfile {
    inner: Mutex<Inner>,
}

struct Inner {
    count: u64,
    bytes_total: u64,
    latency_buckets: Vec<u64>, // 0..=64 buckets, log-spaced
    fail_count_by_class: BTreeMap<String, u64>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct S3FetchProfileSnapshot {
    pub count: u64,
    pub bytes_total: u64,
    pub p50_latency_ms: u64,
    pub p99_latency_ms: u64,
    pub fail_count_by_class: BTreeMap<String, u64>,
}

impl S3FetchProfile {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner {
                count: 0,
                bytes_total: 0,
                latency_buckets: vec![0u64; 65],
                fail_count_by_class: BTreeMap::new(),
            }),
        }
    }

    pub fn record_success(&self, latency_ms: u64, bytes: u64) {
        let mut g = self.inner.lock().unwrap();
        g.count += 1;
        g.bytes_total += bytes;
        let bucket = bucket_index(latency_ms);
        g.latency_buckets[bucket] += 1;
    }

    pub fn record_failure(&self, class: &str) {
        let mut g = self.inner.lock().unwrap();
        *g.fail_count_by_class.entry(class.to_string()).or_insert(0) += 1;
    }

    pub fn snapshot(&self) -> S3FetchProfileSnapshot {
        let g = self.inner.lock().unwrap();
        let p50 = quantile_from_buckets(&g.latency_buckets, 0.50);
        let p99 = quantile_from_buckets(&g.latency_buckets, 0.99);
        S3FetchProfileSnapshot {
            count: g.count,
            bytes_total: g.bytes_total,
            p50_latency_ms: p50,
            p99_latency_ms: p99,
            fail_count_by_class: g.fail_count_by_class.clone(),
        }
    }
}

impl Default for S3FetchProfile {
    fn default() -> Self {
        Self::new()
    }
}

fn bucket_index(latency_ms: u64) -> usize {
    // Log-spaced: bucket i covers [2^i, 2^(i+1)) ms; bucket 64 = "very large".
    let mut i = 0usize;
    let mut threshold = 1u64;
    while i < 64 && latency_ms >= threshold * 2 {
        threshold *= 2;
        i += 1;
    }
    i
}

fn quantile_from_buckets(buckets: &[u64], q: f64) -> u64 {
    let total: u64 = buckets.iter().sum();
    if total == 0 {
        return 0;
    }
    let target = ((total as f64) * q).ceil() as u64;
    let mut acc = 0u64;
    for (i, &c) in buckets.iter().enumerate() {
        acc += c;
        if acc >= target {
            // Bucket i covers [2^i .. 2^(i+1)-1]; report the upper edge.
            return (1u64 << i).saturating_mul(2).saturating_sub(1);
        }
    }
    u64::MAX
}
```

- [ ] **Step 2:** In `crates/rs-delivery/src/disk_cache/download_service.rs`, add a `profile: Arc<S3FetchProfile>` field on `DownloadService`, initialize in `new()`, and on every fetch path:
  - Wrap the S3 fetch with `Instant::now()` measurement.
  - On success: `profile.record_success(elapsed_ms, bytes_len)`.
  - On error: classify via existing `classify_s3_fetch_error` (already in `endpoint_audit.rs`); call `profile.record_failure(class)`.
  - Add `pub fn profile_snapshot(&self) -> S3FetchProfileSnapshot { self.profile.snapshot() }`.

- [ ] **Step 3:** `cargo fmt --all --check`.

- [ ] **Step 4:** Commit:

```bash
git add crates/rs-delivery/src/s3_fetch/profile.rs crates/rs-delivery/src/disk_cache/download_service.rs
git commit -m "feat(s3_fetch): S3FetchProfile + integration in DownloadService (#176)"
```

---

### Task 12: Surface `S3FetchProfileSnapshot` through VPS `/api/v1/delivery/status`

**Files:**
- Modify: `crates/rs-delivery/src/api.rs` (existing endpoint)
- Modify: `crates/rs-delivery/src/disk_cache/mod.rs` (DiskCache facade exposes profile snapshot)

- [ ] **Step 1:** In `DiskCache` facade (`crates/rs-delivery/src/disk_cache/mod.rs`), add:

```rust
pub fn s3_fetch_profile_snapshot(&self) -> crate::s3_fetch::profile::S3FetchProfileSnapshot {
    self.download_service.profile_snapshot()
}
```

- [ ] **Step 2:** In `crates/rs-delivery/src/api.rs` `delivery_status` handler (search for `endpoint_details` JSON construction), extend the response with a top-level `s3_fetch_profile` field populated from `disk_cache.s3_fetch_profile_snapshot()`. Add the field to the existing response struct (next to `endpoint_details`), ensuring serde_json serializes correctly.

- [ ] **Step 3:** `cargo fmt --all --check`.

- [ ] **Step 4:** Commit:

```bash
git add crates/rs-delivery/src/api.rs crates/rs-delivery/src/disk_cache/mod.rs
git commit -m "feat(api): expose s3_fetch_profile in delivery/status (#176)"
```

---

### Task 13: TDD — failing tests for `diag::build_dump`

**Files:**
- Create: `crates/rs-api/src/diag.rs` (tests + module declaration)
- Modify: `crates/rs-api/src/lib.rs`

- [ ] **Step 1:** Create `crates/rs-api/src/diag.rs` with the test module only (impl in Task 14):

```rust
// Diagnostic dump endpoint for stream.snv.
// See docs/superpowers/specs/2026-05-06-soak-gate-and-telemetry-design.md §5.5.
// Issue #176.

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn build_dump_with_full_sources_returns_complete_json() {
        let sources = MockSources::full().await;
        let dump = build_dump(&sources).await;
        assert!(dump["generated_at"].is_string());
        assert!(dump["audit_60min"].is_array());
        assert!(dump["endpoint_timeline"].is_object());
        assert!(dump["disk_cache_stats"].is_object());
        assert!(dump["s3_fetch_profile"].is_object());
        assert_eq!(dump["event_id"], 9289);
    }

    #[tokio::test]
    async fn build_dump_with_vps_unreachable_returns_partial() {
        let sources = MockSources::vps_unreachable().await;
        let dump = build_dump(&sources).await;
        // Failed sub-section replaced with { "error": "..." } per spec §7.
        assert!(dump["disk_cache_stats"]["error"].is_string());
        assert!(dump["s3_fetch_profile"]["error"].is_string());
        // Other sections still populated.
        assert!(dump["audit_60min"].is_array());
    }
}
```

- [ ] **Step 2:** In `crates/rs-api/src/lib.rs`, add `pub(crate) mod diag;`.

- [ ] **Step 3:** Confirm RED — `build_dump`, `MockSources` do not exist.

- [ ] **Step 4:** Commit:

```bash
git add crates/rs-api/src/diag.rs crates/rs-api/src/lib.rs
git commit -m "test(diag): failing tests for build_dump (#176)"
```

---

### Task 14: Implement `diag::build_dump` and `POST /api/v1/diag/dump`

**Files:**
- Modify: `crates/rs-api/src/diag.rs`
- Modify: `crates/rs-api/src/lib.rs` or wherever Axum routes are registered (likely `crates/rs-api/src/api_router.rs`; implementer to verify)

- [ ] **Step 1:** Implement above the tests, ~300-380 lines:

```rust
use serde_json::{json, Value};
use std::sync::Arc;
use sqlx::SqlitePool;

pub trait DumpSources: Send + Sync {
    fn pool(&self) -> &SqlitePool;
    fn current_event_id(&self) -> Option<i64>;
    /// Returns endpoint_timeline JSON for the active event (last 60 min, 30 s samples).
    fn endpoint_timeline(&self) -> Value;
    /// Returns DiskCacheStats + S3FetchProfileSnapshot, or { "error": "..." } each on failure.
    fn vps_state(&self) -> futures::future::BoxFuture<'_, (Value, Value)>;
}

pub struct ProductionSources {
    pub pool: SqlitePool,
    pub event_id: Option<i64>,
    pub timeline_ring: Arc<crate::EndpointTimelineRing>, // see Task 14 step 2
    pub vps_url: Option<String>,
}

impl DumpSources for ProductionSources {
    fn pool(&self) -> &SqlitePool { &self.pool }
    fn current_event_id(&self) -> Option<i64> { self.event_id }
    fn endpoint_timeline(&self) -> Value { self.timeline_ring.snapshot_as_json() }
    fn vps_state(&self) -> futures::future::BoxFuture<'_, (Value, Value)> {
        Box::pin(async move {
            let Some(url) = &self.vps_url else {
                return (
                    json!({ "error": "no VPS configured" }),
                    json!({ "error": "no VPS configured" }),
                );
            };
            let client = reqwest::Client::new();
            match client.get(format!("{url}/api/v1/delivery/status")).send().await {
                Ok(r) => match r.json::<Value>().await {
                    Ok(v) => (
                        v.get("disk_cache_stats").cloned().unwrap_or(json!({"error":"missing"})),
                        v.get("s3_fetch_profile").cloned().unwrap_or(json!({"error":"missing"})),
                    ),
                    Err(e) => (
                        json!({ "error": format!("decode: {e}") }),
                        json!({ "error": format!("decode: {e}") }),
                    ),
                },
                Err(e) => (
                    json!({ "error": format!("vps unreachable: {e}") }),
                    json!({ "error": format!("vps unreachable: {e}") }),
                ),
            }
        })
    }
}

pub async fn build_dump<S: DumpSources>(sources: &S) -> Value {
    let event_id = sources.current_event_id();
    let pool = sources.pool();
    let audit_60min = sqlx::query_as::<_, (i64, String, String, String, String)>(
        "SELECT id, ts, severity, action, detail FROM audit \
         WHERE ts > strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '-60 minutes') \
         ORDER BY id DESC LIMIT 5000"
    )
    .fetch_all(pool)
    .await
    .map(|rows| {
        rows.into_iter()
            .map(|(id, ts, sev, action, detail)| json!({
                "id": id, "ts": ts, "severity": sev, "action": action,
                "detail": serde_json::from_str::<Value>(&detail).unwrap_or(json!(detail))
            }))
            .collect::<Vec<_>>()
    })
    .unwrap_or_else(|_| Vec::new());

    let timeline = sources.endpoint_timeline();
    let (disk_cache_stats, s3_fetch_profile) = sources.vps_state().await;

    json!({
        "generated_at": chrono::Utc::now().to_rfc3339(),
        "version": env!("CARGO_PKG_VERSION"),
        "event_id": event_id,
        "audit_60min": audit_60min,
        "endpoint_timeline": timeline,
        "disk_cache_stats": disk_cache_stats,
        "s3_fetch_profile": s3_fetch_profile,
    })
}

#[cfg(test)]
pub(crate) struct MockSources {
    pool: sqlx::SqlitePool,
    event_id: Option<i64>,
    timeline: serde_json::Value,
    vps_unreachable: bool,
}

#[cfg(test)]
impl MockSources {
    pub async fn full() -> Self {
        let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::query(
            "CREATE TABLE audit (
                id INTEGER PRIMARY KEY,
                ts TEXT NOT NULL,
                severity TEXT NOT NULL,
                action TEXT NOT NULL,
                detail TEXT NOT NULL
            )"
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO audit (ts, severity, action, detail) VALUES \
             (strftime('%Y-%m-%dT%H:%M:%fZ','now','-5 minutes'), 'info', 'EndpointStarted', '{\"alias\":\"YT\"}')"
        )
        .execute(&pool)
        .await
        .unwrap();
        Self {
            pool,
            event_id: Some(9289),
            timeline: serde_json::json!({ "FB-NewLevel": [] }),
            vps_unreachable: false,
        }
    }
    pub async fn vps_unreachable() -> Self {
        let mut s = Self::full().await;
        s.vps_unreachable = true;
        s
    }
}

#[cfg(test)]
impl DumpSources for MockSources {
    fn pool(&self) -> &sqlx::SqlitePool { &self.pool }
    fn current_event_id(&self) -> Option<i64> { self.event_id }
    fn endpoint_timeline(&self) -> Value { self.timeline.clone() }
    fn vps_state(&self) -> futures::future::BoxFuture<'_, (Value, Value)> {
        let unreachable = self.vps_unreachable;
        Box::pin(async move {
            if unreachable {
                (
                    json!({ "error": "vps unreachable: simulated" }),
                    json!({ "error": "vps unreachable: simulated" }),
                )
            } else {
                (
                    json!({ "in_flight": 0, "cached_chunks": 120 }),
                    json!({ "count": 1234, "bytes_total": 1_000_000_000, "p50_latency_ms": 45, "p99_latency_ms": 320, "fail_count_by_class": {} }),
                )
            }
        })
    }
}
```

Update Task 13 test to call `MockSources::full().await` and `MockSources::vps_unreachable().await` — both are async constructors.

- [ ] **Step 2:** Add an in-memory ring `EndpointTimelineRing` (small struct, ~50 lines, in `crates/rs-api/src/endpoint_timeline.rs` — new file). The existing delivery monitor (the one extracted to `delivery_monitor.rs` already) writes one entry per 30 s into this ring per endpoint. Cap: 120 entries per endpoint (= 60 min). Wire write-side into the monitor loop.

- [ ] **Step 3:** Register the Axum route. Find the existing API router (likely in `crates/rs-api/src/lib.rs` or a `routes.rs`) and add:

```rust
.route("/api/v1/diag/dump", post(diag_dump_handler))
```

Handler implementation:

```rust
async fn diag_dump_handler(State(state): State<AppState>) -> Json<Value> {
    let sources = ProductionSources {
        pool: state.pool.clone(),
        event_id: state.current_event_id().await,
        timeline_ring: Arc::clone(&state.endpoint_timeline),
        vps_url: state.current_vps_url().await,
    };
    Json(build_dump(&sources).await)
}
```

`AppState` must already expose `pool` and have a way to resolve current event + VPS URL — implementer adapts to existing patterns (use what `delivery_monitor.rs` uses).

- [ ] **Step 4:** `cargo fmt --all --check`.

- [ ] **Step 5:** Commit:

```bash
git add crates/rs-api/src/diag.rs crates/rs-api/src/endpoint_timeline.rs crates/rs-api/src/lib.rs crates/rs-api/src/delivery_monitor.rs
git commit -m "feat(diag): POST /api/v1/diag/dump returns last-60min snapshot (#176)"
```

---

### Task 15: TDD — failing E2E test for hardened YT health check

**Files:**
- Modify: `e2e/frontend.spec.ts` (add a new test using the existing fixture-stub style)

- [ ] **Step 1:** Append a new test to `e2e/frontend.spec.ts` (existing file uses `setupRoute` fixtures — match the style of nearby tests around line 1684):

```typescript
test("YT studio gate fails when /api/v1/youtube/status reports health=bad", async ({
  page,
}) => {
  // This test runs against a STUBBED /api/v1/youtube/status response, not
  // live YT. It exists to lock in the gate logic. The live youtube-studio-check
  // E2E uses the same assertion; this is its unit-style guard.
  await page.route("**/api/v1/youtube/status", async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        authenticated: true,
        stream_receiving: true,
        broadcast_testing: true,
        broadcast_statuses: [],
        stream_count: 1,
        streams: [
          {
            title: "e2e rtmp",
            stream_status: "active",
            health_status: "bad",
            configuration_issues: [
              "videoIngestionFasterThanRealtime: Check video settings (error)",
            ],
            cdn_resolution: "2160p",
            cdn_frame_rate: "30fps",
            cdn_ingestion_type: "rtmp",
          },
        ],
        error: null,
      }),
    });
  });

  // Run the gate function in isolation. The youtube-studio-check spec
  // exposes the assertion as `assertYtHealthGood` (added in Task 16).
  const { assertYtHealthGood } = await import("./youtube-studio-check.spec");
  await expect(assertYtHealthGood(page)).rejects.toThrow(
    /YT health must be 'good'/,
  );
});

test("YT studio gate passes when health=good and configuration_issues empty", async ({
  page,
}) => {
  await page.route("**/api/v1/youtube/status", async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        authenticated: true,
        stream_receiving: true,
        streams: [
          {
            title: "e2e rtmp",
            stream_status: "active",
            health_status: "good",
            configuration_issues: [],
          },
        ],
      }),
    });
  });

  const { assertYtHealthGood } = await import("./youtube-studio-check.spec");
  await expect(assertYtHealthGood(page)).resolves.toBeUndefined();
});
```

- [ ] **Step 2:** Confirm RED: `assertYtHealthGood` is not exported from `youtube-studio-check.spec.ts` yet.

- [ ] **Step 3:** Commit:

```bash
git add e2e/frontend.spec.ts
git commit -m "test(e2e): failing fixture tests for assertYtHealthGood (#176)"
```

---

### Task 16: Update `youtube-studio-check.spec.ts` — drop `bad`, add `assertYtHealthGood`

**Files:**
- Modify: `e2e/youtube-studio-check.spec.ts`

- [ ] **Step 1:** Drop `bad` from the receiving regex:

```diff
       const receivingPatterns = [
         /\d+\s*kbps/i,
         /\d+p\s+\d+\s*fps/i,
-        /stream\s*health.*(?:excellent|good|ok|bad)/i,
+        /stream\s*health.*(?:excellent|good|ok)/i,
         /Výborn/i,
         /Stav\s*streamu/i,
         /Kvalita\s*streamu/i,
       ];
```

- [ ] **Step 2:** Above the existing `test(...)` definition, export the new helper (so the fixture tests in Task 15 can import it):

```typescript
export async function assertYtHealthGood(page: import("@playwright/test").Page) {
  const ytStatus = await page.evaluate(async () => {
    const res = await fetch("http://10.77.9.204:8910/api/v1/youtube/status");
    return res.json();
  });
  const activeStreams = (ytStatus.streams || []).filter(
    (s: any) => s.stream_status === "active",
  );
  if (activeStreams.length === 0) {
    throw new Error("no active YT stream observed");
  }
  for (const s of activeStreams) {
    if (s.health_status !== "good") {
      throw new Error(
        `YT health must be 'good' (got '${s.health_status}' on stream '${s.title}')`,
      );
    }
    if (
      Array.isArray(s.configuration_issues) &&
      s.configuration_issues.length > 0
    ) {
      throw new Error(
        `YT configuration_issues must be empty (got ${JSON.stringify(s.configuration_issues)} on '${s.title}')`,
      );
    }
  }
}
```

- [ ] **Step 3:** In the existing NORMAL-mode branch (after the "Preparing" check passes, search for `console.log("  YOUTUBE STREAM VERIFICATION PASSED")`), call the helper BEFORE the success log:

```typescript
await assertYtHealthGood(page);
console.log("==========================================");
console.log("  YOUTUBE STREAM VERIFICATION PASSED");
```

- [ ] **Step 4:** Commit:

```bash
git add e2e/youtube-studio-check.spec.ts
git commit -m "feat(e2e): drop 'bad' regex; add assertYtHealthGood helper (#176)"
```

---

### Task 17: Add `scripts/soak-mini.sh` + `.github/workflows/soak-mini.yml`

**Files:**
- Create: `scripts/soak-mini.sh`
- Create: `.github/workflows/soak-mini.yml`

- [ ] **Step 1:** Create `scripts/soak-mini.sh`:

```bash
#!/usr/bin/env bash
# Phase 1 mini-soak — see issue #176, spec §5.2.
# 30 min sample loop asserting per-endpoint chunk_delay and FB death-rate.
# Run locally:  EVENT_ID=9289 ./scripts/soak-mini.sh
# Run in CI: invoked from .github/workflows/soak-mini.yml.

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
  local alias="$1"
  if [[ "$alias" == FB-* ]]; then
    echo "1.3"
  else
    echo "1.1"
  fi
}

declare -A endpoint_history
fail_msgs=()

for i in $(seq 1 "$SAMPLES"); do
  sleep "$INTERVAL_SECS"
  status_json="$(curl -fsS --max-time 10 "$HOST/api/v1/delivery/status?event_id=$EVENT_ID")"
  echo "[sample $i] $status_json"

  echo "$status_json" | jq -c '.endpoint_details[]' | while read -r ep; do
    alias="$(echo "$ep" | jq -r '.alias')"
    delay="$(echo "$ep" | jq -r '.chunk_delay_secs')"
    threshold="$(threshold_for_alias "$alias")"
    limit="$(awk "BEGIN { printf \"%.3f\", $DELIVERY_DELAY_SECS * $threshold }")"
    over="$(awk "BEGIN { print ($delay > $limit) ? 1 : 0 }")"
    if [[ "$over" == "1" ]]; then
      msg="[sample $i] alias='$alias' delay=${delay}s exceeds threshold ${limit}s (target=${DELIVERY_DELAY_SECS}s, mult=${threshold})"
      echo "FAIL: $msg" >&2
      echo "$msg" >> /tmp/soak-mini-fails.txt
    fi
  done

  if [[ -s /tmp/soak-mini-fails.txt ]]; then
    cat /tmp/soak-mini-fails.txt
    exit 1
  fi
done

# Cumulative death-rate check at end of window.
audit_json="$(curl -fsS --max-time 30 "$HOST/api/v1/audit?event_id=$EVENT_ID&action=endpoint_rtmp_push_died&since=$START_TS&limit=10000")"
deaths_per_endpoint="$(echo "$audit_json" | jq -r '.rows | group_by(.endpoint) | map({endpoint: .[0].endpoint, count: length})')"
echo "deaths_per_endpoint=$deaths_per_endpoint"

failed=0
echo "$deaths_per_endpoint" | jq -c '.[]' | while read -r row; do
  ep="$(echo "$row" | jq -r '.endpoint')"
  cnt="$(echo "$row" | jq -r '.count')"
  if [[ "$cnt" -gt "$MAX_DEATHS_PER_ENDPOINT" ]]; then
    echo "FAIL: endpoint=$ep had $cnt rtmp_push_died audit rows (limit $MAX_DEATHS_PER_ENDPOINT)" >&2
    failed=1
  fi
done

if [[ "$failed" == "1" ]]; then
  exit 1
fi

echo "soak-mini PASS: $SAMPLES samples × ${INTERVAL_SECS}s = $((SAMPLES * INTERVAL_SECS / 60)) min, all endpoints under thresholds."
```

- [ ] **Step 2:** Create `.github/workflows/soak-mini.yml`:

```yaml
name: Soak mini (30 min)

on:
  workflow_dispatch:
    inputs:
      event_id:
        description: "Streaming event ID (must be already created on stream.snv)"
        required: true
        default: "9289"
  schedule:
    # Nightly 03:00 Slovak (= 02:00 UTC during DST)
    - cron: "0 2 * * *"

concurrency:
  group: soak-mini
  cancel-in-progress: false

jobs:
  soak:
    runs-on: [self-hosted, stream-lan]
    timeout-minutes: 45
    steps:
      - uses: actions/checkout@v4

      - name: Run soak-mini
        env:
          EVENT_ID: ${{ github.event.inputs.event_id || '9289' }}
        run: |
          chmod +x scripts/soak-mini.sh
          bash scripts/soak-mini.sh
```

ASCII-only — verified, no em-dashes, no curly quotes.

- [ ] **Step 3:** Commit:

```bash
git add scripts/soak-mini.sh .github/workflows/soak-mini.yml
git commit -m "feat(ci): mini-soak workflow + script (#176)"
```

---

### Task 18: Orchestrator-only — push, CI, follow-up issues, completion report

**Performed by the orchestrator (NOT a subagent).** This task is the only one not dispatched.

- [ ] **Step 1:** `git push origin dev`. Monitor CI per `ci-monitoring.md`.
- [ ] **Step 2:** Once CI is green on dev, file Phase 2 issue:

  ```
  gh issue create --title "Phase 2: FB rust_rtmp_push protocol root-cause fix (consumes #176 telemetry)" \
    --body "..."
  ```

  And Phase 3:

  ```
  gh issue create --title "Phase 3: disk_cache pacing fix + 4h soak (consumes #176 telemetry)" \
    --body "..."
  ```

- [ ] **Step 3:** Manually dispatch `soak-mini` workflow on dev with `event_id=9289`. EXPECTED outcome: workflow FAILS loud (drift over threshold; FB death cascade detected). This proves the gate works on real production-state regressions. Capture failure URL.

- [ ] **Step 4:** Open PR `dev → main`. Body references issue #176. PR will be UNMERGEABLE because new `youtube-studio-check.spec.ts` assertion fails against live production (health=bad). That is the explicit acceptance per spec §10.7. PR stays open as the merge-gate.

  Do NOT bypass the failing gate. Do NOT propose admin-merge. The PR sits red until Phase 2 + 3 land green on dev.

- [ ] **Step 5:** Send completion report per `completion-report.md` template. Include:
  - `✅ /plan-check: 18/18 fulfilled`
  - `✅ /review: clean`
  - `❌ Deploy: gate fails by design — Phase 2/3 must land before merge to main`
  - `🌐 Dev: http://10.77.9.204:8910/` and `🌐 Prod: <main URL>` (BOTH lines required by hook)
  - PR URL with full title.
  - Phase 2 and Phase 3 issue numbers + titles.

---

## Verification

1. CI green on dev push (Phase 1 PR's own per-push CI).
2. Manual `soak-mini` dispatch on dev FAILS loud as expected.
3. New audit rows on stream.snv (event 9289) show extended `endpoint_rtmp_push_died` payload with all 7 telemetry fields.
4. New `DiskCachePushSample` rows appear ~1/min/endpoint in audit feed.
5. `curl -X POST http://10.77.9.204:8910/api/v1/diag/dump` returns the §5.5 schema.
6. PR `dev → main` open, mergeable: false, mergeable_state: blocked (because new YT health assertion fails against live state). PR sits open as gate.
7. Phase 2 issue and Phase 3 issue filed.
