# Self-Healing Fast Stream Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the `is_fast` delivery endpoint survive local S3-upload latency spikes without ever crashing/looping — by adding an adaptive read-delay controller (grow on starvation, shrink on health, always 1× push) and a never-crash keepalive (freeze last chunk, then rescue loop) that holds the RTMP connection alive through gaps.

**Architecture:** All new behaviour lives VPS-side in `crates/rs-delivery`. A new pure controller (`fast_delay.rs`) decides how far behind live the fast endpoint reads; the producer feeds that into the existing live-edge lag-probe so jumps leave a buffer instead of yanking to the edge. A new consumer keepalive layer (rust-pusher path only — the fast endpoint always uses the rust pusher) keeps the *existing* session fed with the last chunk (freeze) then `DEFAULT_RESCUE_FLV`, so a trickle/gap never leaves the socket idle long enough for YouTube to reset it. New audit events make grow/shrink/keepalive observable. A latency-injecting mock fetcher provides the regression gate that was missing.

**Tech Stack:** Rust 2024, tokio, `rs-rtmp-push` (RtmpPusher), `tracing`, sqlx audit, `cargo test -p rs-delivery`. Tier-2 fast-iterate is ON (local builds + `cargo xwin` allowed); run the full local pre-push gate before any push.

**Root-cause evidence:** see `docs/superpowers/specs/2026-06-07-fast-stream-self-healing-design.md`. Summary: fast endpoint reads at delay 0 (live edge); streampp upload lag spikes to 90 s (vs 2.2 s on stream.lan, same S3 bucket) → producer "no chunks found in probe range" → push idles → YouTube `connection reset` → restart loop; operator removed the endpoint and switched OBS direct.

**Key facts the implementer must respect:**
- RTMP push is **always strictly 1×** (never speed up to catch up — corrupts quality, YT/FB kill it). The controller only moves the READ pointer / sets keepalive, never the push rate. (memory: `feedback_rtmp_push_always_1x`)
- The fast endpoint (`KS-PP-TEST`) uses `pusher = Rust` → keepalive is implemented on the **rust-pusher path only**.
- `RtmpPusher::push_flv_bytes(&bytes)` re-anchors timestamps across repeated FLV blobs internally (per-track `*_base_ms` roll-forward + `MAX_TAG_TS_JUMP_MS=30_000` re-anchor). Re-pushing the same FLV blob = a working freeze/loop with advancing timestamps. Each S3 chunk is itself a complete, self-contained FLV.
- The rs-delivery `EndpointConfig` (`crates/rs-delivery/src/api.rs`) is a SEPARATE struct from the rs-core one. Add `#[serde(default)]` fields so old/new binaries stay cross-compatible.
- New audit `Action` variants serialize via `#[serde(rename_all="snake_case")]` automatically; no central match to update. VPS emits through `AuditRing::push_parts`.
- File-size CI cap: **1000 lines per .rs file**. `endpoint_task.rs` is already large — put new logic in new modules/helpers, not inline.

---

## Task 0: Version bump (FIRST commit, before any code)

**Files:**
- Modify: `Cargo.toml` (workspace `version`)
- Modify: `src-tauri/Cargo.toml`
- Modify: `src-tauri/tauri.conf.json`
- Modify: `leptos-ui/Cargo.toml`

- [ ] **Step 1: Bump all four version fields `0.22.5` → `0.22.6`**

Edit each file's `version = "0.22.5"` / `"version": "0.22.5"` to `0.22.6`. Confirm:

```bash
grep -rn '0\.22\.5' Cargo.toml src-tauri/Cargo.toml src-tauri/tauri.conf.json leptos-ui/Cargo.toml
```
Expected: no matches remain (all now `0.22.6`).

- [ ] **Step 2: Commit**

```bash
git add Cargo.toml src-tauri/Cargo.toml src-tauri/tauri.conf.json leptos-ui/Cargo.toml
git commit -m "chore: bump version 0.22.5 -> 0.22.6 (fast-stream self-healing)"
```

---

## Task 1: Adaptive delay controller (pure logic + unit tests)

**Files:**
- Create: `crates/rs-delivery/src/fast_delay.rs`
- Modify: `crates/rs-delivery/src/lib.rs` (add `mod fast_delay;` — find the existing `mod` list and add it alphabetically)

This is pure logic with no I/O — fully unit-testable. `now: Instant` is passed in so tests are deterministic (no tokio time needed).

- [ ] **Step 1: Write the failing tests** in `crates/rs-delivery/src/fast_delay.rs`

```rust
//! Adaptive read-delay controller for fast endpoints.
//!
//! A fast endpoint normally reads at the live edge (delay 0). That has zero
//! tolerance for local S3-upload latency spikes: when the live-edge chunk is
//! not yet in S3 the push starves and YouTube resets the idle connection.
//!
//! This controller makes the fast endpoint's read-delay ADAPTIVE: it grows
//! when the producer starves (so the live-edge lag-probe jumps to
//! `live_edge - delay` and leaves a buffer instead of yanking back to the
//! edge) and shrinks slowly when healthy (chasing the lowest working
//! latency). It NEVER speeds up the push — it only changes which chunk the
//! producer reads next. See the design doc for the full rationale.

use std::time::Instant;

/// Lowest fast-stream read-delay when healthy (seconds).
pub const FAST_DELAY_FLOOR_SECS: u64 = 5;
/// Maximum read-delay (seconds) = same safety as the normal stream.
pub const FAST_DELAY_CEILING_SECS: u64 = 120;
/// Headroom added above the observed deficit when growing (seconds).
pub const FAST_DELAY_MARGIN_SECS: u64 = 5;
/// Step size when shrinking back toward the floor (seconds).
pub const FAST_DELAY_SHRINK_STEP_SECS: u64 = 5;
/// Healthy window (seconds) with no starvation before one shrink step.
pub const FAST_HEALTHY_SHRINK_SECS: u64 = 180;

#[derive(Debug, Clone)]
pub struct FastDelayController {
    target_secs: u64,
    floor: u64,
    ceiling: u64,
    margin: u64,
    shrink_step: u64,
    healthy_shrink_secs: u64,
    /// Wall-clock of the last grow OR shrink; gates the next shrink.
    last_change: Instant,
}

impl FastDelayController {
    /// Production constructor: floor/ceiling/margin/step from the consts above.
    pub fn new(now: Instant) -> Self {
        Self::with_params(
            FAST_DELAY_FLOOR_SECS,
            FAST_DELAY_CEILING_SECS,
            FAST_DELAY_MARGIN_SECS,
            FAST_DELAY_SHRINK_STEP_SECS,
            FAST_HEALTHY_SHRINK_SECS,
            now,
        )
    }

    /// Test/explicit constructor.
    pub fn with_params(
        floor: u64,
        ceiling: u64,
        margin: u64,
        shrink_step: u64,
        healthy_shrink_secs: u64,
        now: Instant,
    ) -> Self {
        Self {
            target_secs: floor,
            floor,
            ceiling,
            margin,
            shrink_step,
            healthy_shrink_secs,
            last_change: now,
        }
    }

    pub fn target_secs(&self) -> u64 {
        self.target_secs
    }

    /// Producer starved: the chunk it needs is not in S3 yet. `deficit_secs`
    /// is how far the needed chunk trails the newest chunk available in S3
    /// (0 when unknown). Grows the target to `max(target, deficit + margin)`,
    /// clamped to the ceiling. Returns `Some((from, to))` when the target
    /// actually changed.
    pub fn on_starvation(&mut self, deficit_secs: u64, now: Instant) -> Option<(u64, u64)> {
        let want = deficit_secs
            .saturating_add(self.margin)
            .clamp(self.floor, self.ceiling);
        let next = self.target_secs.max(want);
        if next != self.target_secs {
            let from = self.target_secs;
            self.target_secs = next;
            self.last_change = now;
            Some((from, next))
        } else {
            None
        }
    }

    /// Called while chunks are flowing normally. After `healthy_shrink_secs`
    /// with no change, shrink one step toward the floor. Returns
    /// `Some((from, to))` when the target changed.
    pub fn on_healthy(&mut self, now: Instant) -> Option<(u64, u64)> {
        if self.target_secs <= self.floor {
            return None;
        }
        if now.duration_since(self.last_change).as_secs() < self.healthy_shrink_secs {
            return None;
        }
        let from = self.target_secs;
        let next = from.saturating_sub(self.shrink_step).max(self.floor);
        self.target_secs = next;
        self.last_change = now;
        Some((from, next))
    }

    /// Current target expressed in chunks, for the live-edge lag-probe.
    /// Always >= 1 so a fast endpoint never re-pins to the absolute edge.
    pub fn delay_chunks(&self, typical_chunk_dur_ms: u64) -> i64 {
        let dur = typical_chunk_dur_ms.max(1);
        ((self.target_secs.saturating_mul(1000) / dur) as i64).max(1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn ctrl(now: Instant) -> FastDelayController {
        // floor 5, ceiling 120, margin 5, step 5, healthy-window 180
        FastDelayController::with_params(5, 120, 5, 5, 180, now)
    }

    #[test]
    fn starts_at_floor() {
        let now = Instant::now();
        assert_eq!(ctrl(now).target_secs(), 5);
    }

    #[test]
    fn grows_to_deficit_plus_margin() {
        let now = Instant::now();
        let mut c = ctrl(now);
        // deficit 20s -> target 25s
        assert_eq!(c.on_starvation(20, now), Some((5, 25)));
        assert_eq!(c.target_secs(), 25);
    }

    #[test]
    fn grow_is_monotonic_until_shrink() {
        let now = Instant::now();
        let mut c = ctrl(now);
        c.on_starvation(20, now); // -> 25
        // smaller deficit does not lower the target
        assert_eq!(c.on_starvation(5, now), None);
        assert_eq!(c.target_secs(), 25);
    }

    #[test]
    fn grow_clamps_to_ceiling() {
        let now = Instant::now();
        let mut c = ctrl(now);
        // deficit 200s + margin would be 205 -> clamp to 120
        assert_eq!(c.on_starvation(200, now), Some((5, 120)));
        assert_eq!(c.target_secs(), 120);
    }

    #[test]
    fn unknown_deficit_grows_by_margin_floor() {
        let now = Instant::now();
        let mut c = ctrl(now);
        // deficit 0 -> want = max(floor, margin)=5 == floor -> no change at floor
        assert_eq!(c.on_starvation(0, now), None);
        // after a grow to 25, deficit-0 still cannot lower
        c.on_starvation(20, now);
        assert_eq!(c.on_starvation(0, now), None);
        assert_eq!(c.target_secs(), 25);
    }

    #[test]
    fn shrink_only_after_healthy_window() {
        let base = Instant::now();
        let mut c = ctrl(base);
        c.on_starvation(40, base); // -> 45 at t=0
        // before window: no shrink
        assert_eq!(c.on_healthy(base + Duration::from_secs(179)), None);
        // at window: one step down (45 -> 40)
        assert_eq!(c.on_healthy(base + Duration::from_secs(180)), Some((45, 40)));
        assert_eq!(c.target_secs(), 40);
    }

    #[test]
    fn shrink_floors_at_floor() {
        let base = Instant::now();
        let mut c = ctrl(base);
        c.on_starvation(2, base); // -> max(target, 5+? ) ; deficit2+margin5=7 -> 7
        assert_eq!(c.target_secs(), 7);
        let t = base + Duration::from_secs(180);
        assert_eq!(c.on_healthy(t), Some((7, 5))); // 7-5=2 -> max(floor)=5
        // already at floor -> no further shrink
        assert_eq!(c.on_healthy(t + Duration::from_secs(180)), None);
    }

    #[test]
    fn delay_chunks_uses_chunk_duration() {
        let now = Instant::now();
        let mut c = ctrl(now);
        c.on_starvation(20, now); // 25s
        // 2000ms chunks -> 25000/2000 = 12 chunks
        assert_eq!(c.delay_chunks(2000), 12);
        // 1000ms chunks -> 25 chunks
        assert_eq!(c.delay_chunks(1000), 25);
        // never below 1 even at floor with huge chunks
        let edge = ctrl(now);
        assert_eq!(edge.delay_chunks(60_000), 1);
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail (module not yet wired)**

Run: `cargo test -p rs-delivery fast_delay`
Expected: FAIL — `fast_delay` not declared in `lib.rs` (compile error) until the implementation file + `mod fast_delay;` exist. (If you wrote the file in Step 1, the failure is the missing `mod` line.)

- [ ] **Step 3: Declare the module**

In `crates/rs-delivery/src/lib.rs`, add `mod fast_delay;` to the module list (keep alphabetical with the neighbouring `mod` lines). If other modules need it later, use `pub(crate) mod fast_delay;`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p rs-delivery fast_delay`
Expected: PASS — all 8 tests green.

- [ ] **Step 5: Commit**

```bash
git add crates/rs-delivery/src/fast_delay.rs crates/rs-delivery/src/lib.rs
git commit -m "feat(delivery): adaptive read-delay controller for fast endpoints"
```

---

## Task 2: New audit Action variants

**Files:**
- Modify: `crates/rs-core/src/audit.rs` (add variants + serde round-trip tests)

- [ ] **Step 1: Write failing serde round-trip tests**

In the `#[cfg(test)] mod tests` block of `crates/rs-core/src/audit.rs`, alongside the existing `action_*_serdes` tests, add:

```rust
#[test]
fn action_fast_delay_grown_serdes() {
    let a = Action::FastDelayGrown;
    let s = serde_json::to_string(&a).unwrap();
    assert_eq!(s, "\"fast_delay_grown\"");
    assert_eq!(serde_json::from_str::<Action>(&s).unwrap(), a);
}

#[test]
fn action_fast_delay_shrank_serdes() {
    let a = Action::FastDelayShrank;
    let s = serde_json::to_string(&a).unwrap();
    assert_eq!(s, "\"fast_delay_shrank\"");
    assert_eq!(serde_json::from_str::<Action>(&s).unwrap(), a);
}

#[test]
fn action_fast_keepalive_started_serdes() {
    let a = Action::FastKeepaliveStarted;
    let s = serde_json::to_string(&a).unwrap();
    assert_eq!(s, "\"fast_keepalive_started\"");
    assert_eq!(serde_json::from_str::<Action>(&s).unwrap(), a);
}

#[test]
fn action_fast_keepalive_ended_serdes() {
    let a = Action::FastKeepaliveEnded;
    let s = serde_json::to_string(&a).unwrap();
    assert_eq!(s, "\"fast_keepalive_ended\"");
    assert_eq!(serde_json::from_str::<Action>(&s).unwrap(), a);
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p rs-core audit::tests::action_fast`
Expected: FAIL — `no variant named FastDelayGrown` (compile error).

- [ ] **Step 3: Add the variants**

In the `Action` enum in `crates/rs-core/src/audit.rs`, add (next to `FastEndpointJumpedToLiveEdge, EndpointStartChunkUpdated,`):

```rust
    FastDelayGrown,
    FastDelayShrank,
    FastKeepaliveStarted,
    FastKeepaliveEnded,
```

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p rs-core audit::tests::action_fast`
Expected: PASS — 4 tests green.

- [ ] **Step 5: Commit**

```bash
git add crates/rs-core/src/audit.rs
git commit -m "feat(audit): fast-delay + fast-keepalive action variants"
```

---

## Task 3: Audit emit helpers (VPS-side AuditRing)

**Files:**
- Create: `crates/rs-delivery/src/fast_delay_audit.rs`
- Modify: `crates/rs-delivery/src/lib.rs` (add `mod fast_delay_audit;`)

Mirror the pattern in `rescue_audit.rs` (which emits via `AuditRing::push_parts`).

- [ ] **Step 1: Write the helper module with tests**

```rust
//! Audit emit helpers for the fast-endpoint self-healing path. Mirrors
//! `rescue_audit.rs`: VPS-side events go through the `AuditRing`.
use std::sync::Arc;

use rs_core::audit::{Action, Severity, Source};

use crate::audit_ring::{AuditRing, RingRowParts};

fn push(
    audit_ring: &Option<Arc<AuditRing>>,
    severity: Severity,
    action: Action,
    alias: &str,
    detail: serde_json::Value,
) {
    if let Some(ring) = audit_ring {
        ring.push_parts(RingRowParts {
            severity,
            source: Source::Vps,
            endpoint: Some(alias.to_string()),
            action,
            detail,
        });
    }
}

pub fn emit_delay_grown(
    audit_ring: &Option<Arc<AuditRing>>,
    alias: &str,
    from_secs: u64,
    to_secs: u64,
    deficit_secs: u64,
) {
    push(
        audit_ring,
        Severity::Warn,
        Action::FastDelayGrown,
        alias,
        serde_json::json!({
            "alias": alias,
            "from_secs": from_secs,
            "to_secs": to_secs,
            "observed_deficit_secs": deficit_secs,
        }),
    );
}

pub fn emit_delay_shrank(
    audit_ring: &Option<Arc<AuditRing>>,
    alias: &str,
    from_secs: u64,
    to_secs: u64,
) {
    push(
        audit_ring,
        Severity::Info,
        Action::FastDelayShrank,
        alias,
        serde_json::json!({ "alias": alias, "from_secs": from_secs, "to_secs": to_secs }),
    );
}

pub fn emit_keepalive_started(audit_ring: &Option<Arc<AuditRing>>, alias: &str, mode: &str) {
    push(
        audit_ring,
        Severity::Warn,
        Action::FastKeepaliveStarted,
        alias,
        serde_json::json!({ "alias": alias, "mode": mode }),
    );
}

pub fn emit_keepalive_ended(audit_ring: &Option<Arc<AuditRing>>, alias: &str, gap_secs: u64) {
    push(
        audit_ring,
        Severity::Info,
        Action::FastKeepaliveEnded,
        alias,
        serde_json::json!({ "alias": alias, "gap_secs": gap_secs }),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn emit_with_none_ring_is_noop() {
        // Must not panic when there is no audit ring (e.g. tests / no DB).
        emit_delay_grown(&None, "ep", 5, 25, 20);
        emit_delay_shrank(&None, "ep", 25, 20);
        emit_keepalive_started(&None, "ep", "freeze");
        emit_keepalive_ended(&None, "ep", 12);
    }
}
```

> NOTE for implementer: confirm `RingRowParts`' exact field set by reading `crates/rs-delivery/src/audit_ring.rs`. The canonical call site is `rescue.rs::run_warmup_loop` (`ring.push_parts(RingRowParts{ severity, source, endpoint, action, detail })`). Match field names exactly.

- [ ] **Step 2: Declare module + run test (expect fail then pass)**

Add `mod fast_delay_audit;` to `crates/rs-delivery/src/lib.rs`.
Run: `cargo test -p rs-delivery fast_delay_audit`
Expected: PASS (the no-op test).

- [ ] **Step 3: Commit**

```bash
git add crates/rs-delivery/src/fast_delay_audit.rs crates/rs-delivery/src/lib.rs
git commit -m "feat(delivery): fast-delay/keepalive audit emit helpers"
```

---

## Task 4: Wire the adaptive controller into the producer

**Files:**
- Modify: `crates/rs-delivery/src/endpoint_task.rs` (producer_task + EndpointHandle::spawn call site)
- Test: `crates/rs-delivery/src/endpoint_task_tests.rs` (extend `TimedMockFetcher` with injected latency + a producer-grows test)

The producer owns `chunk_id` and the miss loop — the controller lives here for fast endpoints. For non-fast endpoints behaviour is unchanged.

- [ ] **Step 1: Extend `TimedMockFetcher` with injected per-fetch latency**

In `crates/rs-delivery/src/endpoint_task_tests.rs`, add a latency field to `TimedMockFetcher` (currently fields `chunks`, `available_up_to: Arc<AtomicI64>`, `duration_ms_per_chunk: 2000`). Add:

```rust
    /// Optional artificial delay applied inside each fetch/HEAD, so tests
    /// can simulate a slow live edge under `tokio::time::pause()`.
    fetch_latency: std::time::Duration,
```

Set it to `Duration::ZERO` in the existing constructor, add a `with_latency(self, d: Duration) -> Self` builder, and at the top of both `fetch_chunk_with_meta` and `chunk_duration_ms` add:

```rust
        if !self.fetch_latency.is_zero() {
            tokio::time::sleep(self.fetch_latency).await;
        }
```

(Works deterministically under `tokio::time::pause()` + `tokio::time::advance()`.)

- [ ] **Step 2: Write the failing producer test**

Add to `endpoint_task_tests.rs`. It drives `producer_task` for a FAST endpoint where the live edge is briefly unavailable, and asserts the read pointer ends up BEHIND the live edge (a buffer was built), not pinned to it.

```rust
#[tokio::test(start_paused = true)]
async fn fast_producer_builds_buffer_after_starvation() {
    use std::sync::atomic::Ordering;
    // available_up_to starts low; chunks 0..=4 exist, edge will advance.
    let fetcher = TimedMockFetcher::new(/* chunks 0..=4, dur 2000 */);
    fetcher.set_available_up_to(4);

    let (tx, mut rx) = tokio::sync::mpsc::channel(10);
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let stats = make_stats();
    let buffer_state = std::sync::Arc::new(crate::buffer_state::BufferState::new());

    let handle = tokio::spawn(producer_task_fast(
        fetcher.clone(),
        tx,
        0,            // start_chunk_id
        stop_rx,
        stats.clone(),
        "fast-ep".to_string(),
        buffer_state.clone(),
        None,         // audit_ring
    ));

    // Drain a few chunks; advance the live edge far ahead while the producer
    // is busy, then induce a miss window so the controller grows the delay.
    tokio::time::advance(std::time::Duration::from_secs(2)).await;
    let _ = rx.recv().await;
    fetcher.set_available_up_to(100);             // edge jumps far ahead
    tokio::time::advance(std::time::Duration::from_secs(120)).await;

    stop_tx.send(true).unwrap();
    let _ = handle.await;

    // After the lag-probe, a FAST endpoint must read at `edge - delay`, i.e.
    // strictly behind 100, never AT 100. (delay_chunks >= floor/dur >= 1)
    let read_pos = stats.lock().await.current_chunk_id;
    assert!(read_pos < 100, "fast endpoint must keep a buffer, got {read_pos}");
}
```

> NOTE: `producer_task` is currently private and not fast-aware. Step 3 introduces a thin fast-aware entry. If a `make_stats()` helper / `TimedMockFetcher::new` shape differs, match the existing test helpers in this file (read the top of `endpoint_task_tests.rs`).

- [ ] **Step 3: Run to verify failure**

Run: `cargo test -p rs-delivery fast_producer_builds_buffer_after_starvation`
Expected: FAIL — `producer_task_fast` not defined.

- [ ] **Step 4: Make `producer_task` fast-aware**

In `crates/rs-delivery/src/endpoint_task.rs`:

1. Add `is_fast: bool` to the `producer_task` parameter list (thread it from `EndpointHandle::spawn`, which already has `ep_cfg.is_fast`). At the `EndpointHandle::spawn` producer-spawn call site, pass `ep_cfg.is_fast`.

2. At the top of `producer_task`, construct the controller for fast endpoints:

```rust
    let mut fast_delay = if is_fast {
        Some(crate::fast_delay::FastDelayController::new(tokio::time::Instant::now().into_std()))
    } else {
        None
    };
```

   (Use `std::time::Instant::now()` directly if simpler — the controller takes `std::time::Instant`. Under `start_paused`, prefer `tokio::time::Instant::now().into_std()` so `advance()` is honoured; verify which compiles cleanly and is deterministic in the test.)

3. Replace the fixed delay-chunks computation (current lines ~219-223):

```rust
                let delivery_delay_chunks: i64 = if delivery_delay_ms == 0 {
                    0
                } else {
                    ((delivery_delay_ms / typical_chunk_dur_ms.max(1)) as i64).max(1)
                };
```

   with a controller-aware version:

```rust
                let delivery_delay_chunks: i64 = match fast_delay.as_mut() {
                    Some(ctrl) => {
                        // Healthy fetch: opportunistically shrink toward the
                        // floor after a sustained healthy window.
                        if let Some((from, to)) =
                            ctrl.on_healthy(std::time::Instant::now())
                        {
                            crate::fast_delay_audit::emit_delay_shrank(
                                &audit_ring, &alias, from, to,
                            );
                        }
                        ctrl.delay_chunks(typical_chunk_dur_ms)
                    }
                    None if delivery_delay_ms == 0 => 0,
                    None => ((delivery_delay_ms / typical_chunk_dur_ms.max(1)) as i64).max(1),
                };
```

4. In the `Ok(None)` arm, after the skip-ahead probe determines the gap, grow the controller. When `found_ahead` is true the gap is `probe_id - chunk_id_before`; when not found pass `0`. Add — inside the `if consecutive_chunk_misses >= MAX_CHUNK_MISS_COUNT` block, capture `let stuck_at = chunk_id;` before the probe loop, and after the block:

```rust
                    if let Some(ctrl) = fast_delay.as_mut() {
                        let deficit_secs = if found_ahead {
                            ((chunk_id - stuck_at).max(0) as u64)
                                .saturating_mul(typical_chunk_dur_ms)
                                / 1000
                        } else {
                            0
                        };
                        if let Some((from, to)) =
                            ctrl.on_starvation(deficit_secs, std::time::Instant::now())
                        {
                            crate::fast_delay_audit::emit_delay_grown(
                                &audit_ring, &alias, from, to, deficit_secs,
                            );
                        }
                    }
```

   (`audit_ring` is already a `producer_task` param — confirm its exact type matches the helper signature `&Option<Arc<AuditRing>>`; pass `&audit_ring`.)

5. The lag-probe already targets `live_edge - delivery_delay_chunks`; with the controller returning `>= 1` for fast endpoints, fast now keeps a buffer. No change needed in `producer_lag.rs`.

- [ ] **Step 5: Run the producer test + the existing producer_lag/endpoint_task suites**

Run: `cargo test -p rs-delivery`
Expected: PASS — new test green; all existing `producer_lag`, `endpoint_task_tests`, `buffer_state` tests still green (non-fast path unchanged).

- [ ] **Step 6: Commit**

```bash
git add crates/rs-delivery/src/endpoint_task.rs crates/rs-delivery/src/endpoint_task_tests.rs
git commit -m "feat(delivery): drive fast-endpoint read-delay from adaptive controller"
```

---

## Task 5: Keepalive — hold the connection through short gaps (freeze → rescue)

**Files:**
- Create: `crates/rs-delivery/src/fast_keepalive.rs`
- Modify: `crates/rs-delivery/src/lib.rs` (add `mod fast_keepalive;`)
- Modify: `crates/rs-delivery/src/endpoint_task.rs` (consumer rust-pusher path: capture last chunk; run keepalive instead of idling on a short gap)

The fast endpoint always uses the rust pusher. The fix keeps that SAME pusher session fed during a gap, so YouTube never sees an idle socket. This is distinct from the heavyweight 8 s outage rescue (which drops+recreates the session) — keepalive is the light, fast (sub-second-trigger) gap filler.

- [ ] **Step 1: Write the keepalive helper with a unit test**

```rust
//! Fast-endpoint keepalive: hold the EXISTING rtmp session alive during a
//! short producer gap by re-pushing the last delivered chunk (freeze), then
//! the default rescue loop after `FAST_KEEPALIVE_RESCUE_AFTER_SECS`. Re-using
//! the same `Pushable` means the RTMP connection is never closed by
//! starvation — only a real socket error reconnects. `push_flv_bytes`
//! re-anchors timestamps across the repeated blob internally.
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Wait this long for a real chunk before starting keepalive frames. Far
/// below the 8s full-stall rescue threshold so the trickle regime (chunks
/// arriving late but often) is covered, not just total outages.
pub const FAST_KEEPALIVE_TRIGGER_SECS: u64 = 2;
/// After this much continuous gap, switch the keepalive content from the
/// frozen last chunk to the default rescue loop.
pub const FAST_KEEPALIVE_RESCUE_AFTER_SECS: u64 = 10;

/// Which content the keepalive is currently pushing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeepaliveMode {
    Freeze,
    Rescue,
}

/// Pure decision: given how long the gap has lasted, what to push.
pub fn keepalive_mode(gap_secs: u64, have_freeze: bool) -> KeepaliveMode {
    if have_freeze && gap_secs < FAST_KEEPALIVE_RESCUE_AFTER_SECS {
        KeepaliveMode::Freeze
    } else {
        KeepaliveMode::Rescue
    }
}

/// Select the FLV bytes to push for a keepalive tick.
pub fn keepalive_bytes<'a>(
    mode: KeepaliveMode,
    last_chunk: &'a Option<Arc<Vec<u8>>>,
) -> std::borrow::Cow<'a, [u8]> {
    match mode {
        KeepaliveMode::Freeze => match last_chunk {
            Some(b) => std::borrow::Cow::Borrowed(b.as_slice()),
            None => std::borrow::Cow::Borrowed(crate::rescue_default::DEFAULT_RESCUE_FLV),
        },
        KeepaliveMode::Rescue => {
            std::borrow::Cow::Borrowed(crate::rescue_default::DEFAULT_RESCUE_FLV)
        }
    }
}

#[allow(dead_code)]
pub(crate) fn elapsed_secs(since: Instant) -> u64 {
    since.elapsed().as_secs()
}

/// Test seam: a duration-free "did we exceed the rescue switch?" check.
#[allow(dead_code)]
pub(crate) fn should_switch_to_rescue(gap: Duration) -> bool {
    gap.as_secs() >= FAST_KEEPALIVE_RESCUE_AFTER_SECS
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn freeze_then_rescue_by_gap() {
        assert_eq!(keepalive_mode(0, true), KeepaliveMode::Freeze);
        assert_eq!(keepalive_mode(9, true), KeepaliveMode::Freeze);
        assert_eq!(keepalive_mode(10, true), KeepaliveMode::Rescue);
        // No freeze bytes available -> straight to rescue.
        assert_eq!(keepalive_mode(0, false), KeepaliveMode::Rescue);
    }

    #[test]
    fn bytes_fall_back_to_default_when_no_freeze() {
        let none: Option<Arc<Vec<u8>>> = None;
        let b = keepalive_bytes(KeepaliveMode::Freeze, &none);
        assert_eq!(&*b, crate::rescue_default::DEFAULT_RESCUE_FLV);
    }

    #[test]
    fn freeze_uses_last_chunk_bytes() {
        let last = Some(Arc::new(vec![1u8, 2, 3]));
        let b = keepalive_bytes(KeepaliveMode::Freeze, &last);
        assert_eq!(&*b, &[1u8, 2, 3]);
    }
}
```

- [ ] **Step 2: Declare module, run tests (expect pass)**

Add `mod fast_keepalive;` to `lib.rs`.
Run: `cargo test -p rs-delivery fast_keepalive`
Expected: PASS — 3 unit tests green.

- [ ] **Step 3: Capture the last delivered chunk in the consumer (rust path)**

In `consumer_task` (`endpoint_task.rs`), add near the other `let mut` state (e.g. by `last_delivered_chunk_id`):

```rust
    // Last full FLV chunk pushed — replayed as a freeze during keepalive.
    let mut last_chunk_bytes: Option<std::sync::Arc<Vec<u8>>> = None;
```

In the rust-pusher write branch, after `handle_rust_push` returns `RustPushAction::Continue`, set:

```rust
                if matches!(action, RustPushAction::Continue) {
                    last_chunk_bytes = Some(std::sync::Arc::new(chunk.data.clone()));
                    // ... existing emit_push_sample(...) call stays ...
                }
```

(The `chunk.data` clone is one chunk (~few MB) held for freeze; acceptable.)

- [ ] **Step 4: Run keepalive on a short gap instead of idling (rust path only)**

Replace the consumer's chunk-pull `tokio::select!` so that, on the rust path, a short gap triggers keepalive on the existing pusher rather than waiting silently up to 8 s.

Concretely, change the `_ = tokio::time::sleep(RESCUE_STALL_THRESHOLD_SECS) => { ... }` arm: for the rust path, lower the first reaction to `FAST_KEEPALIVE_TRIGGER_SECS` and, when fired, enter a keepalive loop that keeps the SAME `rust_pusher` fed until a real chunk arrives, then returns it. Implementation (new private async fn in `endpoint_task.rs`, kept short to respect the 1000-line cap — or place the loop body in `fast_keepalive.rs` taking `&mut impl Pushable`):

```rust
/// Keep the existing rust session alive during a producer gap. Returns the
/// next real chunk when one arrives, or None on stop/closed channel.
async fn keepalive_until_chunk(
    pusher: &mut rs_rtmp_push::RtmpPusher,
    rx: &mut tokio::sync::mpsc::Receiver<PrefetchedChunk>,
    last_chunk_bytes: &Option<std::sync::Arc<Vec<u8>>>,
    alias: &str,
    audit_ring: &Option<std::sync::Arc<crate::audit_ring::AuditRing>>,
    stop_rx: &mut tokio::sync::watch::Receiver<bool>,
) -> Option<PrefetchedChunk> {
    use crate::fast_keepalive::{keepalive_bytes, keepalive_mode};
    let started = std::time::Instant::now();
    crate::fast_delay_audit::emit_keepalive_started(audit_ring, alias, "freeze");
    let mut last_mode = crate::fast_keepalive::KeepaliveMode::Freeze;
    loop {
        let gap = started.elapsed().as_secs();
        let mode = keepalive_mode(gap, last_chunk_bytes.is_some());
        if mode != last_mode {
            crate::fast_delay_audit::emit_keepalive_started(
                audit_ring, alias,
                if mode == crate::fast_keepalive::KeepaliveMode::Rescue { "rescue" } else { "freeze" },
            );
            last_mode = mode;
        }
        let bytes = keepalive_bytes(mode, last_chunk_bytes).into_owned();
        tokio::select! {
            maybe = rx.recv() => {
                crate::fast_delay_audit::emit_keepalive_ended(audit_ring, alias, started.elapsed().as_secs());
                return maybe; // Some(chunk) resumes; None -> caller handles teardown
            }
            res = pusher.push_flv_bytes(&bytes) => {
                if let Err(e) = res {
                    // A REAL socket error: log + short backoff, but DO NOT
                    // tear down — the pusher lazy-reconnects on next push.
                    tracing::warn!(alias = %alias, "keepalive push error: {e}; reconnecting");
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
                // push_flv_bytes self-paces ~1x; loop pushes the next tick.
            }
            _ = stop_rx.changed() => {
                if *stop_rx.borrow() {
                    crate::fast_delay_audit::emit_keepalive_ended(audit_ring, alias, started.elapsed().as_secs());
                    return None;
                }
            }
        }
    }
}
```

Then in the consumer chunk-pull, on the rust path, gate entry into keepalive: when `rx.recv()` hasn't produced a chunk within `FAST_KEEPALIVE_TRIGGER_SECS`, call `keepalive_until_chunk` and treat its `Some(chunk)` return exactly like a normal `rx.recv()` chunk (process it), or `None` like the channel-closed/stop path (defensive teardown). Keep the existing 8 s `run_outage_rescue` path for the **non-rust** (ffmpeg) endpoints unchanged. Minimal restructure:

```rust
        // rust path: short-gap keepalive on the SAME session.
        let chunk = if use_rust_pusher {
            tokio::select! {
                maybe = rx.recv() => match maybe {
                    Some(c) => { /* update buffer_duration_ms, last_delivered_chunk_id */ c }
                    None => { /* existing defensive-rescue None branch */ break }
                },
                _ = tokio::time::sleep(std::time::Duration::from_secs(
                        crate::fast_keepalive::FAST_KEEPALIVE_TRIGGER_SECS)) => {
                    if let Some(ref mut pusher) = rust_pusher {
                        match keepalive_until_chunk(pusher, &mut rx, &last_chunk_bytes, &alias, &audit_ring, &mut stop_rx).await {
                            Some(c) => { /* update buffer_duration_ms, last_delivered_chunk_id */ c }
                            None => break,
                        }
                    } else { continue }
                }
                _ = stop_rx.changed() => { if *stop_rx.borrow() { break } else { continue } }
            }
        } else {
            // EXISTING ffmpeg-path select! (rx.recv / RESCUE_STALL_THRESHOLD_SECS / stop) unchanged
            tokio::select! { /* ...existing code... */ }
        };
```

> IMPLEMENTER NOTE: this is the riskiest edit. Keep the buffer-duration bookkeeping (`buffer_state.buffer_duration_ms` decrement + `last_delivered_chunk_id = c.chunk_id`) identical to the current `Some(c)` branch in BOTH the direct-recv and post-keepalive paths. Do not remove the existing 8 s `run_outage_rescue` arm for the ffmpeg path. `rust_pusher` is `Option`; the `if let Some(ref mut pusher)` borrow must end before the chunk is processed in the write section below — restructure so the keepalive borrow is scoped to the select arm only (return the chunk value out of the arm, then process it after the `select!`). Iterate against the compiler (Tier-2 fast-iterate is ON).

- [ ] **Step 5: Run the full rs-delivery suite**

Run: `cargo test -p rs-delivery`
Expected: PASS — keepalive unit tests + all existing tests green.

- [ ] **Step 6: Commit**

```bash
git add crates/rs-delivery/src/fast_keepalive.rs crates/rs-delivery/src/lib.rs crates/rs-delivery/src/endpoint_task.rs
git commit -m "feat(delivery): never-crash keepalive (freeze->rescue) for fast endpoints"
```

---

## Task 6: Integration regression test — jitter never crashes the fast stream

**Files:**
- Modify: `crates/rs-delivery/src/endpoint_task_tests.rs` (or a new `fast_self_healing_tests.rs` module — match how integration tests are organized; declare in `lib.rs` if new)

This is the gate that was missing: a deterministic test proving a fast endpoint survives an injected upload-latency gap without the push ever erroring/closing, with keepalive frames emitted and the stream resuming.

- [ ] **Step 1: Write the failing integration test**

Use a recording mock `Pushable` (push never errors; counts pushes + records byte-lengths) and the latency-extended `TimedMockFetcher`. Drive `consumer_task` (rust path) with a producer that stops supplying chunks for 30 s of simulated time, then resumes.

```rust
#[tokio::test(start_paused = true)]
async fn fast_endpoint_survives_upload_gap_without_dying() {
    // A Pushable that NEVER errors and records each push.
    #[derive(Clone, Default)]
    struct RecordingPusher { pushes: std::sync::Arc<std::sync::atomic::AtomicU32>, closed: std::sync::Arc<std::sync::atomic::AtomicBool> }
    impl crate::endpoint_consumer_helpers::Pushable for RecordingPusher {
        async fn push_flv_bytes(&mut self, _d: &[u8]) -> Result<(), rs_rtmp_push::PushError> {
            self.pushes.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            tokio::time::sleep(std::time::Duration::from_millis(200)).await; // 1x-ish pacing
            Ok(())
        }
        async fn close(&mut self) { self.closed.store(true, std::sync::atomic::Ordering::Relaxed); }
        fn reconnect_count(&self) -> u32 { 0 }
    }

    // ... build consumer with: a channel that yields 3 chunks, then a 30s
    // gap (no chunks), then 3 more chunks; last_chunk_bytes populated after
    // the first real chunk; stop after resume.

    // Assertions:
    // 1. RecordingPusher.closed == false the whole time (connection never
    //    torn down by starvation).
    // 2. push count during the gap > 0 (keepalive frames were emitted).
    // 3. after resume, real chunks were pushed again.
    // 4. no panic / no early return.
}
```

> IMPLEMENTER NOTE: `consumer_task` is generic over `OutputProcessFactory` and constructs `RtmpPusher` internally for the rust path — it does not currently accept an injected `Pushable`. To test deterministically, EITHER (a) refactor the keepalive + per-chunk push to go through a small `&mut impl Pushable` seam that the test can supply (preferred — also de-risks the consumer), OR (b) test `keepalive_until_chunk` directly with `RecordingPusher` + a hand-driven `mpsc` (lighter, still proves the never-close + keepalive-frames + resume guarantees). Choose (b) if the consumer refactor is too invasive for one task; it still locks the core guarantee. Use `tokio::time::advance` to cross the 30 s gap and the 10 s freeze→rescue switch, and assert the mode switched (freeze pushes then rescue pushes).

- [ ] **Step 2: Run to verify failure, then implement the test seam, then pass**

Run: `cargo test -p rs-delivery fast_endpoint_survives_upload_gap_without_dying`
Expected: FAIL first (seam/test not present), PASS after wiring.

- [ ] **Step 3: Commit**

```bash
git add crates/rs-delivery/src/endpoint_task_tests.rs crates/rs-delivery/src/lib.rs
git commit -m "test(delivery): regression gate — fast endpoint survives upload jitter [red->green]"
```

---

## Task 7: Disk-pressure logging — emit only on level transitions (small, secondary)

**Files:**
- Modify: `crates/rs-endpoint/src/disk_pressure.rs`

Reduces the every-minute `LocalDiskPressure` warn while the disk sits at one level (203 rows in one event). Keeps the signal (transitions) and the `disk_critical`/`disk_level` atomics unchanged. This does NOT change retention (host keeps chunks until uploaded — continuity guarantee).

- [ ] **Step 1: Write/adjust the test**

In the `#[cfg(test)]` block of `disk_pressure.rs`, add a test asserting the transition predicate (extract a tiny pure helper):

```rust
/// True when a new pressure reading should be logged (level changed).
pub(crate) fn should_log_transition(prev: DiskPressure, now: DiskPressure) -> bool {
    prev != now
}

#[test]
fn logs_only_on_transition() {
    assert!(should_log_transition(DiskPressure::Ok, DiskPressure::Warn));
    assert!(!should_log_transition(DiskPressure::Warn, DiskPressure::Warn));
    assert!(should_log_transition(DiskPressure::Warn, DiskPressure::Critical));
    assert!(should_log_transition(DiskPressure::Critical, DiskPressure::Warn));
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p rs-endpoint --features testing disk_pressure`
Expected: FAIL — `should_log_transition` not defined.

- [ ] **Step 3: Implement transition-only emit**

In `run_disk_monitor`, add `let mut last_pressure = DiskPressure::Ok;` before the loop. Replace the `rl.allow(Action::LocalDiskPressure, class)` gate with `should_log_transition(last_pressure, pressure)`, and after the (possible) emit set `last_pressure = pressure;`. Keep emitting for `Ok` transitions too is unnecessary — but DO update `last_pressure` for every reading (including the `Ok => continue` path) so an Ok→Warn later still logs. Restructure: classify, store atomics, update `last_pressure` AFTER deciding to log; for `Ok` set `last_pressure = Ok` and `continue` (no audit row for Ok, matching today's behaviour). The `RateLimiter` can stay as a secondary guard or be removed (transition gate already bounds volume).

- [ ] **Step 4: Run to verify pass + the rs-endpoint suite**

Run: `cargo test -p rs-endpoint --features testing`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/rs-endpoint/src/disk_pressure.rs
git commit -m "feat(endpoint): log local disk pressure only on level transitions"
```

---

## Task 8: Final review, full gate, push, CI, deploy verification

- [ ] **Step 1: Run the full local pre-push gate (Tier-2)**

```bash
cargo fmt --all --check \
 && cargo check --workspace \
 && cargo clippy --workspace --all-targets -- -D warnings \
 && cargo test --no-run --workspace
```
All must pass. Then run the changed-crate suites in full:

```bash
cargo test -p rs-delivery && cargo test -p rs-core audit && cargo test -p rs-endpoint --features testing
```
Do NOT mask exit codes with `tail`/`grep` (memory: `feedback_dont_mask_gate_exit_code`).

- [ ] **Step 2: Self-review the diff** against the spec; run `/review` and `superpowers:requesting-code-review`; fix every 🔴/🟡/🔵.

- [ ] **Step 3: Push once, monitor CI to all-green** (per `ci-monitoring`). Single push.

- [ ] **Step 4: Open PR `dev` → `main`** titled `Fast-stream self-healing: adaptive read-delay + never-crash keepalive (fixes restart loop)`, body listing the root-cause evidence + the soak verification still owed. Ensure `mergeable: true` AND `mergeable_state: "clean"`.

- [ ] **Step 5: Post-merge soak verification (owed before "done")** — on streampp, run a real event with the fast endpoint enabled; assert via the audit log: zero starvation-driven `endpoint_rtmp_push_died`, visible `fast_delay_grown`/`fast_keepalive_*` events during an induced disk-busy spike, and a continuous stream. "Working" = sustained soak, 0 deaths, 0 crashes (memory: `feedback_definition_of_working`).

---

## Self-Review (planner)

**1. Spec coverage:**
- Principle 1 (never crash on starvation) → Task 5 (keepalive holds the session; only real socket errors reconnect) + Task 6 (regression gate asserts no close).
- Principle 2 (self-tuning delay) → Tasks 1, 4 (grow/shrink controller wired into the lag-probe).
- Principle 3 (buffering-then-resume; freeze→rescue) → Task 5 (`keepalive_mode` freeze<10 s then rescue) + user-selected content.
- Principle 4 (always 1×) → controller only moves the read pointer / keepalive uses self-pacing `push_flv_bytes`; no speed-up anywhere. Noted in Task header.
- Component 3 (local jitter) → Task 7 (transition-only logging) + ops note (host retention intentionally uncapped — corrected from spec's "tighter retention").
- Component 4 (regression test) → Task 6 + Task 1/5 unit tests.
- Audit observability → Tasks 2, 3, wired in 4 & 5.

**2. Placeholder scan:** Tasks 1–3, 7 have complete code. Tasks 4–6 carry exact insertion points + complete new functions, with explicit IMPLEMENTER NOTES where a borrow-scope/refactor judgment is required (these are integration judgments against verbatim current code, not vague TODOs). The integration test (Task 6) gives two concrete seam options and full assertions.

**3. Type consistency:** `FastDelayController::{new, with_params, target_secs, on_starvation, on_healthy, delay_chunks}`, `KeepaliveMode::{Freeze,Rescue}`, `keepalive_mode/keepalive_bytes`, audit helpers `emit_delay_grown/emit_delay_shrank/emit_keepalive_started/emit_keepalive_ended`, and `Action::{FastDelayGrown,FastDelayShrank,FastKeepaliveStarted,FastKeepaliveEnded}` are used consistently across Tasks 1–6. `Pushable` matches the verbatim trait in `endpoint_consumer_helpers.rs`.

**Decomposition note:** one feature = one PR (per `autonomous-batch-issue-development`). All tasks land on `dev` in one PR. Task 7 (disk logging) is <30 LoC same-area polish — included, not deferred.
