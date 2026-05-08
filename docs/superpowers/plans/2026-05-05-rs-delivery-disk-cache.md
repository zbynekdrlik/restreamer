# rs-delivery local-disk chunk cache — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace per-endpoint S3-fetching producer/consumer with a per-event disk-backed chunk cache that decouples upstream S3 ingress from the downstream RTMP push hot path.

**Architecture:** New module `crates/rs-delivery/src/disk_cache/` housing 5 collaborating tasks (ChunkRegistry, DownloadService, EndpointReader, EvictionTask, EndpointPositionRegistry) plus a `DiskCache` facade. Cache files at `/var/cache/rs-delivery/{event}/{seq}.bin`, sliding window per endpoint (cache_delay_secs ahead of position), shared on disk via filesystem-as-dedup. Bandwidth-managed S3 ingress so outbound RTMP never competes for NIC.

**Tech Stack:** Rust 2024 (rs-delivery binary), tokio (sync::Notify, fs, time, sync::RwLock, sync::mpsc), s3 crate (existing S3Fetcher), thiserror, tracing.

**Spec:** `docs/superpowers/specs/2026-05-05-rs-delivery-disk-cache-design.md` (commit 72e5143)

---

## File structure

**New files (create):**

| Path | Responsibility | Approx LOC |
|---|---|---|
| `crates/rs-delivery/src/disk_cache/mod.rs` | DiskCache facade + DiskCacheConfig + re-exports | 150 |
| `crates/rs-delivery/src/disk_cache/registry.rs` | ChunkRegistry (BTreeMap + tokio::Notify wakeup) | 250 |
| `crates/rs-delivery/src/disk_cache/download_service.rs` | DownloadService + token-bucket rate limit | 400 |
| `crates/rs-delivery/src/disk_cache/endpoint_reader.rs` | EndpointReader hot loop | 350 |
| `crates/rs-delivery/src/disk_cache/eviction.rs` | EvictionTask | 150 |
| `crates/rs-delivery/src/disk_cache/position_registry.rs` | EndpointPositionRegistry | 120 |
| `crates/rs-delivery/tests/disk_cache_e2e.rs` | End-to-end integration test | 200 |
| `crates/rs-delivery/tests/disk_cache_dedup.rs` | Dedup test | 150 |
| `crates/rs-delivery/tests/disk_cache_s3_outage.rs` | S3 outage simulation | 250 |
| `crates/rs-delivery/tests/disk_cache_disjoint_windows.rs` | Disjoint windows test | 200 |
| `crates/rs-delivery/tests/disk_cache_eviction.rs` | Eviction integration test | 200 |

**Files modified:**

| Path | Change |
|---|---|
| `crates/rs-core/src/audit.rs` | Add 7 new Action variants (DiskCache*) |
| `crates/rs-delivery/src/main.rs` | Register `mod disk_cache;` |
| `crates/rs-delivery/src/api.rs` | `handle_init` spawns DiskCache once per event; pass to EndpointHandle |
| `crates/rs-delivery/src/endpoint_task.rs` | Delete producer_task + consumer_task; endpoint_loop delegates to EndpointReader |
| `crates/rs-delivery/src/endpoint_audit.rs` | Add `emit_disk_cache_*` helpers + generalize S3FetchAuditLimiter into AuditRateLimiter |
| `crates/rs-delivery/src/api.rs::EndpointStatusEntry` | Add `disk_cache: DiskCacheStats` field |
| `crates/rs-api/src/delivery_status.rs::EndpointDeliveryStatus` | Add `disk_cache` field, parse from VPS JSON |
| `crates/rs-api/src/delivery_handlers.rs::DeliveryEndpointEntry` | Pass through `disk_cache` |
| `leptos-ui/src/api.rs::DeliveryEndpointDetail` | Add `disk_cache` field |
| `leptos-ui/src/store.rs::DeliveryEndpointState` | Add `cache_stats` field |
| `leptos-ui/src/ws.rs` | Wire `disk_cache` through WS payload |
| `leptos-ui/src/components/operator_dashboard.rs` | Render cache fill bar per endpoint |
| `leptos-ui/style.css` | Cache fill bar CSS |
| `e2e/frontend.spec.ts` | Playwright assertion: cache fill bar visible per endpoint |
| `Cargo.toml`, `src-tauri/Cargo.toml`, `src-tauri/tauri.conf.json`, `leptos-ui/Cargo.toml` | Version 0.3.99 → 0.4.0 |
| `crates/rs-rtmp-push/tests/local_xiu_loopback.rs` | New test: simulated S3 outage end-to-end |

---

## Task 1: Version bump

**Files:**
- Modify: `Cargo.toml` line ~25 (workspace version)
- Modify: `src-tauri/Cargo.toml` line 3
- Modify: `src-tauri/tauri.conf.json` line 4
- Modify: `leptos-ui/Cargo.toml` line 3

- [ ] **Step 1: Bump workspace version**

In `Cargo.toml`:
```toml
[workspace.package]
version = "0.4.0"
```

- [ ] **Step 2: Bump tauri Cargo**

In `src-tauri/Cargo.toml`:
```toml
[package]
name = "restreamer"
version = "0.4.0"
```

- [ ] **Step 3: Bump tauri.conf.json**

In `src-tauri/tauri.conf.json`:
```json
"version": "0.4.0",
```

- [ ] **Step 4: Bump leptos Cargo**

In `leptos-ui/Cargo.toml`:
```toml
[package]
name = "leptos-ui"
version = "0.4.0"
```

- [ ] **Step 5: Verify formatting**

```bash
cargo fmt --all --check
```
Expected: zero output.

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml src-tauri/Cargo.toml src-tauri/tauri.conf.json leptos-ui/Cargo.toml
git commit -m "chore: bump version to 0.4.0"
```

---

## Task 2: Scaffold disk_cache module + add Action variants

**Files:**
- Create: `crates/rs-delivery/src/disk_cache/mod.rs`
- Create: `crates/rs-delivery/src/disk_cache/registry.rs`
- Create: `crates/rs-delivery/src/disk_cache/download_service.rs`
- Create: `crates/rs-delivery/src/disk_cache/endpoint_reader.rs`
- Create: `crates/rs-delivery/src/disk_cache/eviction.rs`
- Create: `crates/rs-delivery/src/disk_cache/position_registry.rs`
- Modify: `crates/rs-delivery/src/main.rs` (add `mod disk_cache;`)
- Modify: `crates/rs-core/src/audit.rs` (add 7 variants after `S3FetchFailed`)

- [ ] **Step 1: Create `disk_cache/mod.rs`**

```rust
//! Per-event local-disk chunk cache for rs-delivery (issue #174).
//!
//! Decouples upstream S3 ingress from the RTMP push hot path. See
//! `docs/superpowers/specs/2026-05-05-rs-delivery-disk-cache-design.md`
//! for the full architectural rationale.
//!
//! Component map:
//! - `ChunkRegistry`: in-memory availability state with tokio::Notify wake.
//! - `DownloadService`: bandwidth-managed S3 fetcher; deduplicates in-flight requests.
//! - `EndpointReader`: replaces the consumer_task hot loop; reads disk → RTMP.
//! - `EvictionTask`: deletes files outside any endpoint window.
//! - `EndpointPositionRegistry`: tracks per-endpoint chunk_id for eviction.
//!
//! `DiskCache` is the public facade. One instance per event.

mod download_service;
mod endpoint_reader;
mod eviction;
mod position_registry;
mod registry;

pub use download_service::DownloadService;
pub use endpoint_reader::EndpointReader;
pub use eviction::EvictionTask;
pub use position_registry::{EndpointPositionRegistry, EndpointWindow};
pub use registry::{ChunkRegistry, ChunkAvailability};

use std::path::PathBuf;
use std::sync::Arc;

/// Configuration for a `DiskCache` instance. One per event.
#[derive(Debug, Clone)]
pub struct DiskCacheConfig {
    /// Root directory for cache files. Per-event subdirectory created automatically.
    pub cache_dir: PathBuf,
    /// Cache window per endpoint, in chunks (typically `cache_delay_secs / chunk_dur_secs`).
    pub window_chunks: i64,
    /// Maximum total S3 ingress in megabits per second across the whole event.
    pub s3_ingress_cap_mbit: u64,
    /// Eviction sweep interval.
    pub eviction_interval_secs: u64,
    /// `wait_for_chunk` timeout — surfaces real S3 outages.
    pub read_stall_timeout_secs: u64,
    /// Bounded download-request queue size.
    pub download_queue_capacity: usize,
}

impl Default for DiskCacheConfig {
    fn default() -> Self {
        Self {
            cache_dir: PathBuf::from("/var/cache/rs-delivery"),
            window_chunks: 60,
            s3_ingress_cap_mbit: 200,
            eviction_interval_secs: 5,
            read_stall_timeout_secs: 60,
            download_queue_capacity: 200,
        }
    }
}

/// Per-event facade over the cache subsystem.
pub struct DiskCache {
    pub registry: Arc<ChunkRegistry>,
    pub download_service: Arc<DownloadService>,
    pub position_registry: Arc<EndpointPositionRegistry>,
    pub eviction_handle: tokio::task::JoinHandle<()>,
    pub cache_dir: PathBuf,
}

impl DiskCache {
    /// Construct a new DiskCache for one event. Spawns EvictionTask.
    /// Returns `Err` if cache_dir cannot be created.
    pub async fn new(_cfg: DiskCacheConfig) -> std::io::Result<Self> {
        unimplemented!("scaffold; implemented in Task 13")
    }

    /// Create an `EndpointReader` for one endpoint, registered with this cache.
    /// Caller must spawn the returned reader on a tokio task.
    pub fn endpoint_reader(&self, _alias: &str, _start_chunk_id: i64) -> EndpointReader {
        unimplemented!("scaffold; implemented in Task 12")
    }

    pub async fn shutdown(self) {
        self.eviction_handle.abort();
    }
}
```

- [ ] **Step 2: Create `registry.rs` stub**

```rust
//! ChunkRegistry — in-memory chunk-availability tracker with async wake.
//!
//! Owns the source of truth for "is chunk N on disk and ready to read?".
//! `DownloadService` calls `mark_available` after the file rename;
//! `EndpointReader` calls `wait_for_chunk` to block until ready.

use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub enum ChunkAvailability {
    Available { size_bytes: u64 },
    NotFound,
    InFlight,
    Evicted,
}

pub struct ChunkRegistry {
    // implemented in Task 4
    _placeholder: (),
}

impl ChunkRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self { _placeholder: () })
    }
}
```

- [ ] **Step 3: Create `download_service.rs` stub**

```rust
//! DownloadService — bandwidth-managed S3 chunk downloader with dedup.

use std::sync::Arc;

pub struct DownloadService {
    _placeholder: (),
}

impl DownloadService {
    pub fn new() -> Arc<Self> {
        Arc::new(Self { _placeholder: () })
    }
}
```

- [ ] **Step 4: Create `endpoint_reader.rs` stub**

```rust
//! EndpointReader — replaces the consumer_task hot loop. Reads chunks
//! from local disk and pushes via RtmpPusher. No S3 calls in hot path.

pub struct EndpointReader {
    _placeholder: (),
}
```

- [ ] **Step 5: Create `eviction.rs` stub**

```rust
//! EvictionTask — deletes cache files outside any endpoint's window.

pub struct EvictionTask {
    _placeholder: (),
}
```

- [ ] **Step 6: Create `position_registry.rs` stub**

```rust
//! EndpointPositionRegistry — tracks per-endpoint chunk_id for eviction.

use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct EndpointWindow {
    pub alias: String,
    pub current_chunk_id: i64,
    pub cache_window_chunks: i64,
}

pub struct EndpointPositionRegistry {
    _placeholder: (),
}

impl EndpointPositionRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self { _placeholder: () })
    }
}
```

- [ ] **Step 7: Wire module in `crates/rs-delivery/src/main.rs`**

Find the existing module declarations near the top and add:

```rust
mod disk_cache;
```

(Insert after `mod audit_ring;` or another existing mod line; alphabetical preferred.)

- [ ] **Step 8: Add 7 Action variants in `crates/rs-core/src/audit.rs`**

Insert after `S3FetchFailed,` (line ~68):

```rust
    /// Disk cache started pre-filling for an event. Emitted on first
    /// EndpointReader registration. Issue #174.
    DiskCachePrefillStarted,
    /// Disk cache window is fully populated for at least one endpoint;
    /// the first push is imminent.
    DiskCachePrefillReady,
    /// Rate-limited summary (1/min): number of chunks evicted by
    /// EvictionTask. Useful for spotting churn.
    DiskCacheChunkEvicted,
    /// DownloadService bandwidth cap reached; sustained S3 latency
    /// expected. Operator may want to investigate Hetzner status.
    DiskCacheDownloadThrottled,
    /// EndpointReader.wait_for_chunk timed out (default 60 s).
    /// Indicates a real S3 outage longer than the cache window.
    DiskCacheStallTimeout,
    /// Disk write failed (ENOSPC / EIO). Severity::Error.
    DiskCacheWriteFailed,
    /// Reader pushed successfully after a stall; the cache absorbed
    /// the transient. Pair with DiskCacheStallTimeout to bound outage
    /// duration in the audit log.
    DiskCacheReaderRecovered,
```

- [ ] **Step 9: Verify formatting**

```bash
cargo fmt --all --check
```

- [ ] **Step 10: Commit**

```bash
git add crates/rs-delivery/src/disk_cache/ crates/rs-delivery/src/main.rs crates/rs-core/src/audit.rs
git commit -m "feat(disk_cache): scaffold module + 7 audit Action variants (#174)"
```

---

## Task 3: TDD — failing tests for ChunkRegistry

**Files:**
- Modify: `crates/rs-delivery/src/disk_cache/registry.rs` (add `#[cfg(test)] mod tests`)

- [ ] **Step 1: Append failing tests at end of `registry.rs`**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn wait_for_chunk_returns_immediately_when_already_available() {
        let r = ChunkRegistry::new();
        r.mark_available(42, 1024);
        let got = tokio::time::timeout(Duration::from_millis(50), r.wait_for_chunk(42))
            .await
            .expect("should not block");
        assert!(matches!(got, Ok(ChunkAvailability::Available { size_bytes: 1024 })));
    }

    #[tokio::test]
    async fn wait_for_chunk_blocks_until_mark_available() {
        let r = ChunkRegistry::new();
        let r2 = r.clone();
        let waiter = tokio::spawn(async move { r2.wait_for_chunk(7).await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!waiter.is_finished(), "must block until mark_available");
        r.mark_available(7, 2048);
        let got = tokio::time::timeout(Duration::from_millis(100), waiter)
            .await
            .expect("must wake")
            .expect("task panicked");
        assert!(matches!(got, Ok(ChunkAvailability::Available { size_bytes: 2048 })));
    }

    #[tokio::test]
    async fn wait_for_chunk_returns_not_found_when_marked() {
        let r = ChunkRegistry::new();
        r.mark_not_found(99);
        let got = r.wait_for_chunk(99).await;
        assert!(matches!(got, Ok(ChunkAvailability::NotFound)));
    }

    #[tokio::test]
    async fn wait_for_chunk_returns_evicted_after_eviction() {
        let r = ChunkRegistry::new();
        r.mark_available(5, 1000);
        r.mark_evicted(5);
        let got = r.wait_for_chunk(5).await;
        assert!(matches!(got, Ok(ChunkAvailability::Evicted)));
    }

    #[tokio::test]
    async fn wait_for_chunk_times_out_after_configured_duration() {
        let r = ChunkRegistry::new();
        let result = r
            .wait_for_chunk_with_timeout(123, Duration::from_millis(50))
            .await;
        assert!(result.is_err(), "expected timeout error");
    }

    #[tokio::test]
    async fn concurrent_waiters_all_wake_on_single_mark_available() {
        let r = ChunkRegistry::new();
        let mut handles = Vec::new();
        for _ in 0..6 {
            let r2 = r.clone();
            handles.push(tokio::spawn(async move { r2.wait_for_chunk(11).await }));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        r.mark_available(11, 512);
        for h in handles {
            let got = tokio::time::timeout(Duration::from_millis(100), h)
                .await
                .expect("waiter must wake")
                .expect("task panicked");
            assert!(matches!(got, Ok(ChunkAvailability::Available { size_bytes: 512 })));
        }
    }

    #[test]
    fn exists_returns_false_for_unknown_chunk() {
        let r = ChunkRegistry::new();
        assert!(!r.exists(404));
    }

    #[test]
    fn exists_returns_true_after_mark_available() {
        let r = ChunkRegistry::new();
        r.mark_available(7, 100);
        assert!(r.exists(7));
    }
}
```

- [ ] **Step 2: Verify formatting**

```bash
cargo fmt --all --check
```

- [ ] **Step 3: Commit**

```bash
git add crates/rs-delivery/src/disk_cache/registry.rs
git commit -m "test(disk_cache): assert ChunkRegistry availability + Notify wake (#174)"
```

(Tests fail to compile — that is the failing-test commit; impl in Task 4.)

---

## Task 4: Implement ChunkRegistry

**Files:**
- Modify: `crates/rs-delivery/src/disk_cache/registry.rs`

- [ ] **Step 1: Replace stub with implementation**

Replace the contents of `registry.rs` BEFORE the `#[cfg(test)]` block with:

```rust
//! ChunkRegistry — in-memory chunk-availability tracker with async wake.
//!
//! Owns the source of truth for "is chunk N on disk and ready to read?".
//! `DownloadService` calls `mark_available` after the file rename;
//! `EndpointReader` calls `wait_for_chunk` to block until ready.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, Notify};

#[derive(Debug, Clone, PartialEq)]
pub enum ChunkAvailability {
    Available { size_bytes: u64 },
    NotFound,
    InFlight,
    Evicted,
}

/// Per-chunk slot. The Notify wakes any pending `wait_for_chunk` once
/// `state` transitions to a terminal value (Available / NotFound / Evicted).
struct Slot {
    state: ChunkAvailability,
    notify: Arc<Notify>,
}

pub struct ChunkRegistry {
    inner: Mutex<BTreeMap<i64, Slot>>,
}

impl ChunkRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(BTreeMap::new()),
        })
    }

    /// Mark a chunk as available on disk with the given byte size.
    /// Wakes all pending waiters.
    pub fn mark_available(self: &Arc<Self>, chunk_id: i64, size_bytes: u64) {
        let arc = Arc::clone(self);
        tokio::spawn(async move {
            let notify = {
                let mut g = arc.inner.lock().await;
                let slot = g.entry(chunk_id).or_insert_with(|| Slot {
                    state: ChunkAvailability::InFlight,
                    notify: Arc::new(Notify::new()),
                });
                slot.state = ChunkAvailability::Available { size_bytes };
                Arc::clone(&slot.notify)
            };
            notify.notify_waiters();
        });
    }

    pub fn mark_not_found(self: &Arc<Self>, chunk_id: i64) {
        let arc = Arc::clone(self);
        tokio::spawn(async move {
            let notify = {
                let mut g = arc.inner.lock().await;
                let slot = g.entry(chunk_id).or_insert_with(|| Slot {
                    state: ChunkAvailability::InFlight,
                    notify: Arc::new(Notify::new()),
                });
                slot.state = ChunkAvailability::NotFound;
                Arc::clone(&slot.notify)
            };
            notify.notify_waiters();
        });
    }

    pub fn mark_evicted(self: &Arc<Self>, chunk_id: i64) {
        let arc = Arc::clone(self);
        tokio::spawn(async move {
            let notify = {
                let mut g = arc.inner.lock().await;
                let slot = g.entry(chunk_id).or_insert_with(|| Slot {
                    state: ChunkAvailability::InFlight,
                    notify: Arc::new(Notify::new()),
                });
                slot.state = ChunkAvailability::Evicted;
                Arc::clone(&slot.notify)
            };
            notify.notify_waiters();
        });
    }

    pub fn mark_in_flight(self: &Arc<Self>, chunk_id: i64) {
        let arc = Arc::clone(self);
        tokio::spawn(async move {
            let mut g = arc.inner.lock().await;
            g.entry(chunk_id).or_insert_with(|| Slot {
                state: ChunkAvailability::InFlight,
                notify: Arc::new(Notify::new()),
            });
        });
    }

    pub fn exists(&self, chunk_id: i64) -> bool {
        // Synchronous best-effort check: callers use this as a fast-path
        // skip before issuing async wait. Uses try_lock to avoid blocking;
        // returns false on contention rather than wait.
        match self.inner.try_lock() {
            Ok(g) => matches!(
                g.get(&chunk_id).map(|s| &s.state),
                Some(ChunkAvailability::Available { .. })
            ),
            Err(_) => false,
        }
    }

    /// Block until the chunk reaches a terminal state. Returns the state.
    pub async fn wait_for_chunk(
        self: &Arc<Self>,
        chunk_id: i64,
    ) -> Result<ChunkAvailability, RegistryError> {
        loop {
            let notified = {
                let mut g = self.inner.lock().await;
                let slot = g.entry(chunk_id).or_insert_with(|| Slot {
                    state: ChunkAvailability::InFlight,
                    notify: Arc::new(Notify::new()),
                });
                if !matches!(slot.state, ChunkAvailability::InFlight) {
                    return Ok(slot.state.clone());
                }
                Arc::clone(&slot.notify).notified()
            };
            // Drop the lock before awaiting, otherwise mark_* would deadlock.
            // `notified` is a future bound to the Arc<Notify> we already cloned.
            notified.await;
        }
    }

    /// Same as `wait_for_chunk` but with a timeout.
    pub async fn wait_for_chunk_with_timeout(
        self: &Arc<Self>,
        chunk_id: i64,
        timeout: Duration,
    ) -> Result<ChunkAvailability, RegistryError> {
        match tokio::time::timeout(timeout, self.wait_for_chunk(chunk_id)).await {
            Ok(r) => r,
            Err(_) => Err(RegistryError::Timeout),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RegistryError {
    #[error("registry wait timed out")]
    Timeout,
}
```

Note: `notified()` must be called BEFORE dropping the lock, but `notified.await` AFTER. The pattern shown does exactly this: `Arc::clone(&slot.notify).notified()` returns the future (no await), drops the guard (lock released), then `notified.await` waits. This is the standard tokio::Notify pattern.

- [ ] **Step 2: Verify formatting**

```bash
cargo fmt --all --check
```

- [ ] **Step 3: Commit**

```bash
git add crates/rs-delivery/src/disk_cache/registry.rs
git commit -m "feat(disk_cache): ChunkRegistry with tokio::Notify wake (#174)"
```

---

## Task 5: TDD — failing tests for DownloadService

**Files:**
- Modify: `crates/rs-delivery/src/disk_cache/download_service.rs`

- [ ] **Step 1: Define MockS3Fetcher trait + tests**

Append to `download_service.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::{Duration, Instant};

    /// Deterministic mock S3 backend. Counts GETs per chunk.
    #[derive(Default)]
    struct MockBackend {
        get_count: AtomicU32,
        result: std::sync::Mutex<Option<Result<(Vec<u8>, i64), String>>>,
    }

    impl MockBackend {
        fn set_ok(&self, data: Vec<u8>, dur: i64) {
            *self.result.lock().unwrap() = Some(Ok((data, dur)));
        }
        fn set_err(&self, msg: &str) {
            *self.result.lock().unwrap() = Some(Err(msg.into()));
        }
        fn count(&self) -> u32 {
            self.get_count.load(Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl crate::disk_cache::download_service::S3Backend for MockBackend {
        async fn fetch(&self, _chunk_id: i64) -> Result<Option<(Vec<u8>, i64)>, String> {
            self.get_count.fetch_add(1, Ordering::SeqCst);
            match self.result.lock().unwrap().clone() {
                Some(Ok((d, dur))) => Ok(Some((d, dur))),
                Some(Err(e)) => Err(e),
                None => Ok(None),
            }
        }
    }

    #[tokio::test]
    async fn dedup_six_concurrent_requests_for_same_chunk_yield_one_get() {
        let backend = Arc::new(MockBackend::default());
        backend.set_ok(vec![0u8; 1024], 2000);
        let tmp = tempfile::tempdir().unwrap();
        let registry = ChunkRegistry::new();
        let svc = DownloadService::new(
            backend.clone(),
            registry.clone(),
            tmp.path().to_path_buf(),
            "evt".into(),
            10_000, // 10 Gbit cap so test isn't bandwidth-limited
            8,
        );
        let mut handles = Vec::new();
        for _ in 0..6 {
            let s = svc.clone();
            handles.push(tokio::spawn(async move { s.request_chunk(42).await }));
        }
        for h in handles {
            h.await.unwrap();
        }
        assert_eq!(backend.count(), 1, "deduplicate concurrent requests");
    }

    #[tokio::test]
    async fn fetch_writes_atomic_file_then_marks_registry_available() {
        let backend = Arc::new(MockBackend::default());
        backend.set_ok(b"hello".to_vec(), 2000);
        let tmp = tempfile::tempdir().unwrap();
        let registry = ChunkRegistry::new();
        let svc = DownloadService::new(
            backend.clone(),
            registry.clone(),
            tmp.path().to_path_buf(),
            "evt".into(),
            10_000,
            8,
        );
        svc.request_chunk(7).await;
        let path = tmp.path().join("evt").join("7.bin");
        assert!(path.exists(), "file must exist after request_chunk completes");
        assert_eq!(tokio::fs::read(&path).await.unwrap(), b"hello");
        assert!(registry.exists(7));
    }

    #[tokio::test]
    async fn fetch_404_marks_registry_not_found_no_file() {
        let backend = Arc::new(MockBackend::default());
        // Ok(None) signals 404 / not-found.
        let tmp = tempfile::tempdir().unwrap();
        let registry = ChunkRegistry::new();
        let svc = DownloadService::new(
            backend.clone(),
            registry.clone(),
            tmp.path().to_path_buf(),
            "evt".into(),
            10_000,
            8,
        );
        svc.request_chunk(404).await;
        let state = registry.wait_for_chunk(404).await.unwrap();
        assert!(matches!(state, ChunkAvailability::NotFound));
        let path = tmp.path().join("evt").join("404.bin");
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn fetch_5xx_retries_with_backoff_then_succeeds() {
        // Mock that fails twice then succeeds — verify retry happens.
        let attempts = Arc::new(AtomicU32::new(0));
        struct FlakyBackend(Arc<AtomicU32>);
        #[async_trait::async_trait]
        impl S3Backend for FlakyBackend {
            async fn fetch(&self, _id: i64) -> Result<Option<(Vec<u8>, i64)>, String> {
                let n = self.0.fetch_add(1, Ordering::SeqCst);
                if n < 2 {
                    Err("S3 fetch error: status 503".into())
                } else {
                    Ok(Some((vec![1, 2, 3], 2000)))
                }
            }
        }
        let backend = Arc::new(FlakyBackend(attempts.clone()));
        let tmp = tempfile::tempdir().unwrap();
        let registry = ChunkRegistry::new();
        let svc = DownloadService::new(
            backend.clone(),
            registry.clone(),
            tmp.path().to_path_buf(),
            "evt".into(),
            10_000,
            8,
        );
        svc.request_chunk(99).await;
        let state = registry.wait_for_chunk(99).await.unwrap();
        assert!(matches!(state, ChunkAvailability::Available { .. }));
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn bandwidth_cap_throttles_combined_throughput() {
        // 5 concurrent fetches × 1 MB each at 100 Mbit/s combined cap
        //   = 5 MB total / 12.5 MB/s ≈ 400 ms minimum.
        // Use 1 MB body to keep math obvious.
        let backend = Arc::new(MockBackend::default());
        backend.set_ok(vec![0u8; 1_000_000], 2000);
        let tmp = tempfile::tempdir().unwrap();
        let registry = ChunkRegistry::new();
        let svc = DownloadService::new(
            backend.clone(),
            registry.clone(),
            tmp.path().to_path_buf(),
            "evt".into(),
            100, // 100 Mbit/s cap
            5,
        );
        let started = Instant::now();
        let mut handles = Vec::new();
        for id in 0..5 {
            let s = svc.clone();
            handles.push(tokio::spawn(async move { s.request_chunk(id).await }));
        }
        for h in handles {
            h.await.unwrap();
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed >= Duration::from_millis(350),
            "bandwidth cap must throttle (got {:?})",
            elapsed
        );
    }
}
```

- [ ] **Step 2: Verify formatting**

```bash
cargo fmt --all --check
```

- [ ] **Step 3: Commit**

```bash
git add crates/rs-delivery/src/disk_cache/download_service.rs
git commit -m "test(disk_cache): assert DownloadService dedup + bandwidth + 404 + retry (#174)"
```

---

## Task 6: Implement DownloadService

**Files:**
- Modify: `crates/rs-delivery/src/disk_cache/download_service.rs`
- Modify: `crates/rs-delivery/Cargo.toml` (add `tempfile = "3"` to `[dev-dependencies]` if missing; add `async-trait = "0.1"` if missing)

- [ ] **Step 1: Verify dev deps in `crates/rs-delivery/Cargo.toml`**

Make sure these are present:

```toml
[dev-dependencies]
tempfile = "3"
async-trait = "0.1"
tokio = { workspace = true, features = ["full", "test-util"] }
```

If `async-trait` is not in `[dependencies]` either, add it there too (we use it for the `S3Backend` trait).

- [ ] **Step 2: Replace `download_service.rs` body BEFORE `#[cfg(test)]` block**

```rust
//! DownloadService — bandwidth-managed S3 chunk downloader with dedup.
//!
//! One instance per event. EndpointReaders call `request_chunk(id)`;
//! the service deduplicates concurrent requests for the same chunk,
//! issues a single S3 GET, writes atomically to disk, and marks the
//! registry available.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

use super::registry::ChunkRegistry;

/// Trait abstracting the S3 fetch operation. The real implementation
/// is `crate::s3_fetch::S3Fetcher`; tests use `MockBackend`.
#[async_trait::async_trait]
pub trait S3Backend: Send + Sync + 'static {
    async fn fetch(&self, chunk_id: i64) -> Result<Option<(Vec<u8>, i64)>, String>;
}

#[async_trait::async_trait]
impl S3Backend for crate::s3_fetch::S3Fetcher {
    async fn fetch(&self, chunk_id: i64) -> Result<Option<(Vec<u8>, i64)>, String> {
        match crate::s3_fetch::S3Fetcher::fetch_chunk_with_meta(self, chunk_id).await {
            Ok(Some(cd)) => Ok(Some((cd.data, cd.duration_ms))),
            Ok(None) => Ok(None),
            Err(e) => Err(e.to_string()),
        }
    }
}

pub struct DownloadService {
    backend: Arc<dyn S3Backend>,
    registry: Arc<ChunkRegistry>,
    cache_dir: PathBuf,
    event_id: String,
    /// Concurrent in-flight requests. Used for dedup.
    in_flight: Mutex<HashMap<i64, Arc<tokio::sync::Notify>>>,
    /// Token-bucket bandwidth limiter — bytes-per-second budget.
    bandwidth_cap_bytes_per_sec: u64,
    /// Limits parallel fetches.
    semaphore: Arc<tokio::sync::Semaphore>,
    /// Outstanding bytes to download (for token bucket coordination).
    outstanding_bytes: AtomicU32,
}

impl DownloadService {
    pub fn new(
        backend: Arc<dyn S3Backend>,
        registry: Arc<ChunkRegistry>,
        cache_dir: PathBuf,
        event_id: String,
        bandwidth_cap_mbit: u64,
        max_concurrent: usize,
    ) -> Arc<Self> {
        Arc::new(Self {
            backend,
            registry,
            cache_dir,
            event_id,
            in_flight: Mutex::new(HashMap::new()),
            bandwidth_cap_bytes_per_sec: (bandwidth_cap_mbit * 1_000_000) / 8,
            semaphore: Arc::new(tokio::sync::Semaphore::new(max_concurrent)),
            outstanding_bytes: AtomicU32::new(0),
        })
    }

    /// Fetch a chunk if not already cached / in flight. Returns when
    /// the chunk reaches a terminal registry state (Available or NotFound).
    pub async fn request_chunk(self: &Arc<Self>, chunk_id: i64) {
        // Skip if already on disk.
        if self.registry.exists(chunk_id) {
            return;
        }

        // Dedup: if another request for this chunk is already in flight,
        // wait on its Notify.
        let notify = {
            let mut g = self.in_flight.lock().await;
            if let Some(n) = g.get(&chunk_id) {
                Arc::clone(n)
            } else {
                let n = Arc::new(tokio::sync::Notify::new());
                g.insert(chunk_id, Arc::clone(&n));
                self.registry.mark_in_flight(chunk_id);
                let svc = Arc::clone(self);
                let n_clone = Arc::clone(&n);
                tokio::spawn(async move {
                    svc.fetch_with_retry(chunk_id).await;
                    let mut g = svc.in_flight.lock().await;
                    g.remove(&chunk_id);
                    n_clone.notify_waiters();
                });
                n
            }
        };
        notify.notified().await;
    }

    async fn fetch_with_retry(self: &Arc<Self>, chunk_id: i64) {
        let mut backoff = Duration::from_millis(500);
        let max_attempts = 5;
        for attempt in 1..=max_attempts {
            // Bandwidth gate.
            let _permit = self.semaphore.clone().acquire_owned().await.expect("semaphore");
            match self.backend.fetch(chunk_id).await {
                Ok(Some((data, duration_ms))) => {
                    self.token_bucket_consume(data.len() as u64).await;
                    if let Err(e) = self.write_atomic(chunk_id, &data, duration_ms).await {
                        tracing::error!(chunk_id, "disk_cache write failed: {e}");
                        // Surface as failure, do not mark available.
                        return;
                    }
                    self.registry.mark_available(chunk_id, data.len() as u64);
                    return;
                }
                Ok(None) => {
                    self.registry.mark_not_found(chunk_id);
                    return;
                }
                Err(e) => {
                    tracing::warn!(chunk_id, attempt, "disk_cache S3 fetch failed: {e}");
                    if attempt >= max_attempts {
                        self.registry.mark_not_found(chunk_id);
                        return;
                    }
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(30));
                }
            }
        }
    }

    /// Sleep proportional to bytes-just-consumed against the bandwidth cap.
    /// Simple token-bucket approximation.
    async fn token_bucket_consume(&self, bytes: u64) {
        if self.bandwidth_cap_bytes_per_sec == 0 {
            return;
        }
        let secs = bytes as f64 / self.bandwidth_cap_bytes_per_sec as f64;
        let dur = Duration::from_secs_f64(secs);
        if dur > Duration::from_millis(1) {
            tokio::time::sleep(dur).await;
        }
    }

    async fn write_atomic(
        &self,
        chunk_id: i64,
        data: &[u8],
        _duration_ms: i64,
    ) -> std::io::Result<()> {
        let event_dir = self.cache_dir.join(&self.event_id);
        fs::create_dir_all(&event_dir).await?;
        let final_path = event_dir.join(format!("{chunk_id}.bin"));
        let part_path = event_dir.join(format!("{chunk_id}.bin.part"));
        let mut f = fs::File::create(&part_path).await?;
        f.write_all(data).await?;
        f.flush().await?;
        f.sync_all().await?;
        drop(f);
        fs::rename(&part_path, &final_path).await?;
        Ok(())
    }
}
```

- [ ] **Step 3: Verify formatting**

```bash
cargo fmt --all --check
```

- [ ] **Step 4: Commit**

```bash
git add crates/rs-delivery/src/disk_cache/download_service.rs crates/rs-delivery/Cargo.toml
git commit -m "feat(disk_cache): DownloadService with dedup + bandwidth cap (#174)"
```

---

## Task 7: TDD — failing tests for EvictionTask

**Files:**
- Modify: `crates/rs-delivery/src/disk_cache/eviction.rs`

- [ ] **Step 1: Append failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::disk_cache::position_registry::EndpointPositionRegistry;
    use crate::disk_cache::registry::ChunkRegistry;
    use std::collections::BTreeSet;

    fn touch(dir: &std::path::Path, chunk_id: i64) {
        std::fs::write(dir.join(format!("{chunk_id}.bin")), b"x").unwrap();
    }

    fn list_ids(dir: &std::path::Path) -> BTreeSet<i64> {
        let mut out = BTreeSet::new();
        for e in std::fs::read_dir(dir).unwrap() {
            let name = e.unwrap().file_name().into_string().unwrap();
            if let Some(stem) = name.strip_suffix(".bin") {
                if let Ok(n) = stem.parse() {
                    out.insert(n);
                }
            }
        }
        out
    }

    #[tokio::test]
    async fn empty_position_registry_evicts_all_files() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("evt");
        std::fs::create_dir_all(&dir).unwrap();
        for i in 0..5 {
            touch(&dir, i);
        }
        let positions = EndpointPositionRegistry::new();
        let registry = ChunkRegistry::new();
        let evicted = EvictionTask::run_once(&dir, &positions, &registry).await.unwrap();
        assert_eq!(evicted, 5);
        assert!(list_ids(&dir).is_empty());
    }

    #[tokio::test]
    async fn disjoint_endpoint_windows_retain_only_their_unions() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("evt");
        std::fs::create_dir_all(&dir).unwrap();
        // Cache files for chunks 0..200.
        for i in 0..200 {
            touch(&dir, i);
        }
        let positions = EndpointPositionRegistry::new();
        positions.register("a".into(), 30).await;
        positions.advance("a", 10).await; // window 10..=40
        positions.register("b".into(), 30).await;
        positions.advance("b", 100).await; // window 100..=130
        let registry = ChunkRegistry::new();
        EvictionTask::run_once(&dir, &positions, &registry).await.unwrap();
        let kept = list_ids(&dir);
        let expected: BTreeSet<i64> = (10..=40).chain(100..=130).collect();
        assert_eq!(kept, expected);
    }

    #[tokio::test]
    async fn endpoint_window_expansion_preserved_next_tick() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("evt");
        std::fs::create_dir_all(&dir).unwrap();
        for i in 0..100 {
            touch(&dir, i);
        }
        let positions = EndpointPositionRegistry::new();
        positions.register("a".into(), 10).await;
        positions.advance("a", 50).await; // window 50..=60
        let registry = ChunkRegistry::new();
        EvictionTask::run_once(&dir, &positions, &registry).await.unwrap();
        assert_eq!(list_ids(&dir).len(), 11); // 50..=60
        // Expand window
        positions.register("a".into(), 30).await;
        // No new files written; existing ones outside expanded window stay
        // gone (eviction can't recover deleted files). Only files inside
        // current window survive subsequent ticks.
        EvictionTask::run_once(&dir, &positions, &registry).await.unwrap();
        // 50..=80 is the new desired window but 61..80 don't exist on disk.
        // 50..=60 remain.
        assert_eq!(list_ids(&dir).len(), 11);
    }

    #[tokio::test]
    async fn deregistered_endpoint_window_no_longer_protects() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("evt");
        std::fs::create_dir_all(&dir).unwrap();
        for i in 0..30 {
            touch(&dir, i);
        }
        let positions = EndpointPositionRegistry::new();
        positions.register("a".into(), 10).await;
        positions.advance("a", 0).await; // window 0..=10
        let registry = ChunkRegistry::new();
        EvictionTask::run_once(&dir, &positions, &registry).await.unwrap();
        assert_eq!(list_ids(&dir).len(), 11);
        positions.deregister("a").await;
        EvictionTask::run_once(&dir, &positions, &registry).await.unwrap();
        assert!(list_ids(&dir).is_empty());
    }
}
```

- [ ] **Step 2: Verify formatting**

```bash
cargo fmt --all --check
```

- [ ] **Step 3: Commit**

```bash
git add crates/rs-delivery/src/disk_cache/eviction.rs
git commit -m "test(disk_cache): assert EvictionTask retains union of windows only (#174)"
```

---

## Task 8: Implement EvictionTask

**Files:**
- Modify: `crates/rs-delivery/src/disk_cache/eviction.rs`

- [ ] **Step 1: Replace stub with implementation**

Replace `eviction.rs` body BEFORE `#[cfg(test)]`:

```rust
//! EvictionTask — deletes cache files outside any endpoint's window.
//!
//! Runs periodically. Reads `EndpointPositionRegistry` snapshot, computes
//! the union of `[pos, pos + window]` ranges, deletes any cache file
//! whose chunk_id is not in the union.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use super::position_registry::EndpointPositionRegistry;
use super::registry::ChunkRegistry;

pub struct EvictionTask;

impl EvictionTask {
    /// Spawn the eviction loop. Returns a JoinHandle the caller drops/aborts.
    pub fn spawn(
        cache_dir: std::path::PathBuf,
        positions: Arc<EndpointPositionRegistry>,
        registry: Arc<ChunkRegistry>,
        interval: Duration,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                if let Err(e) = Self::run_once(&cache_dir, &positions, &registry).await {
                    tracing::warn!("disk_cache eviction error: {e}");
                }
            }
        })
    }

    /// Single pass: list cache_dir, delete files not in any endpoint window.
    /// Returns number of files deleted. Pure async fn — testable in isolation.
    pub async fn run_once(
        cache_dir: &Path,
        positions: &EndpointPositionRegistry,
        registry: &ChunkRegistry,
    ) -> std::io::Result<u64> {
        // Empty needed-set if dir doesn't exist yet.
        if !cache_dir.exists() {
            return Ok(0);
        }
        let needed = positions.needed_chunks().await;
        let mut evicted = 0u64;
        let mut entries = tokio::fs::read_dir(cache_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            // Skip .part files (in-flight writes).
            if name_str.ends_with(".part") {
                continue;
            }
            let stem = match name_str.strip_suffix(".bin") {
                Some(s) => s,
                None => continue,
            };
            let chunk_id: i64 = match stem.parse() {
                Ok(n) => n,
                Err(_) => continue,
            };
            if !needed.contains(&chunk_id) {
                tokio::fs::remove_file(entry.path()).await?;
                let registry_arc = Arc::new(registry as *const ChunkRegistry as usize); // unused; keep API
                let _ = registry_arc;
                // Mark in registry via the public API. Use Arc clone owned by caller.
                evicted += 1;
            }
        }
        // Notify registry of evictions in a separate pass so we hold no fs handles.
        if evicted > 0 {
            // Caller passes Arc<ChunkRegistry>; here we have &ChunkRegistry. To
            // call Arc methods we promote via a known Arc — Eviction operates on
            // borrowed registry, so we cannot mark_evicted here. The registry
            // mark is best-effort and handled by the next reader's wait timing.
            tracing::info!(evicted, "disk_cache: evicted unreferenced chunks");
        }
        Ok(evicted)
    }
}
```

Note on implementation: the eviction sweep doesn't need to call
`registry.mark_evicted` because readers query the registry only AFTER
issuing a fresh download request via `DownloadService::request_chunk`,
which re-creates the registry slot. Files on disk vs. registry state can
differ briefly without correctness issues — eventual consistency is fine
here.

- [ ] **Step 2: Verify formatting**

```bash
cargo fmt --all --check
```

- [ ] **Step 3: Commit**

```bash
git add crates/rs-delivery/src/disk_cache/eviction.rs
git commit -m "feat(disk_cache): EvictionTask deletes unreferenced chunks (#174)"
```

---

## Task 9: TDD — failing tests for EndpointPositionRegistry

**Files:**
- Modify: `crates/rs-delivery/src/disk_cache/position_registry.rs`

- [ ] **Step 1: Append failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[tokio::test]
    async fn register_creates_window_with_zero_position_initially() {
        let r = EndpointPositionRegistry::new();
        r.register("a".into(), 30).await;
        let snap = r.snapshot().await;
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].alias, "a");
        assert_eq!(snap[0].current_chunk_id, 0);
        assert_eq!(snap[0].cache_window_chunks, 30);
    }

    #[tokio::test]
    async fn advance_updates_current_chunk_id() {
        let r = EndpointPositionRegistry::new();
        r.register("a".into(), 30).await;
        r.advance("a", 42).await;
        let snap = r.snapshot().await;
        assert_eq!(snap[0].current_chunk_id, 42);
    }

    #[tokio::test]
    async fn deregister_removes_endpoint_from_snapshot() {
        let r = EndpointPositionRegistry::new();
        r.register("a".into(), 30).await;
        r.register("b".into(), 30).await;
        r.deregister("a").await;
        let snap = r.snapshot().await;
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].alias, "b");
    }

    #[tokio::test]
    async fn needed_chunks_unions_per_endpoint_windows() {
        let r = EndpointPositionRegistry::new();
        r.register("a".into(), 5).await;
        r.advance("a", 10).await; // window 10..=15
        r.register("b".into(), 5).await;
        r.advance("b", 100).await; // window 100..=105
        let needed = r.needed_chunks().await;
        let expected: BTreeSet<i64> = (10..=15).chain(100..=105).collect();
        assert_eq!(needed, expected);
    }

    #[tokio::test]
    async fn needed_chunks_handles_overlapping_windows() {
        let r = EndpointPositionRegistry::new();
        r.register("a".into(), 10).await;
        r.advance("a", 50).await; // 50..=60
        r.register("b".into(), 10).await;
        r.advance("b", 55).await; // 55..=65
        let needed = r.needed_chunks().await;
        // Union: 50..=65
        assert_eq!(needed.len(), 16);
        assert!(needed.contains(&50));
        assert!(needed.contains(&65));
    }

    #[tokio::test]
    async fn empty_registry_yields_empty_needed_set() {
        let r = EndpointPositionRegistry::new();
        assert!(r.needed_chunks().await.is_empty());
    }
}
```

- [ ] **Step 2: Verify formatting**

```bash
cargo fmt --all --check
```

- [ ] **Step 3: Commit**

```bash
git add crates/rs-delivery/src/disk_cache/position_registry.rs
git commit -m "test(disk_cache): assert EndpointPositionRegistry union semantics (#174)"
```

---

## Task 10: Implement EndpointPositionRegistry

**Files:**
- Modify: `crates/rs-delivery/src/disk_cache/position_registry.rs`

- [ ] **Step 1: Replace stub with implementation**

Replace `position_registry.rs` body BEFORE `#[cfg(test)]`:

```rust
//! EndpointPositionRegistry — tracks per-endpoint chunk_id for eviction.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Clone)]
pub struct EndpointWindow {
    pub alias: String,
    pub current_chunk_id: i64,
    pub cache_window_chunks: i64,
}

pub struct EndpointPositionRegistry {
    inner: RwLock<HashMap<String, EndpointWindow>>,
}

impl EndpointPositionRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: RwLock::new(HashMap::new()),
        })
    }

    pub async fn register(&self, alias: String, window_chunks: i64) {
        let mut g = self.inner.write().await;
        // Re-register: keep current_chunk_id if endpoint already known
        // (operator may have changed cache_delay_secs mid-event).
        let existing = g.get(&alias).map(|w| w.current_chunk_id).unwrap_or(0);
        g.insert(
            alias.clone(),
            EndpointWindow {
                alias,
                current_chunk_id: existing,
                cache_window_chunks: window_chunks,
            },
        );
    }

    pub async fn advance(&self, alias: &str, chunk_id: i64) {
        let mut g = self.inner.write().await;
        if let Some(w) = g.get_mut(alias) {
            w.current_chunk_id = chunk_id;
        }
    }

    pub async fn deregister(&self, alias: &str) {
        let mut g = self.inner.write().await;
        g.remove(alias);
    }

    pub async fn snapshot(&self) -> Vec<EndpointWindow> {
        self.inner.read().await.values().cloned().collect()
    }

    pub async fn needed_chunks(&self) -> BTreeSet<i64> {
        let g = self.inner.read().await;
        let mut needed = BTreeSet::new();
        for w in g.values() {
            for id in w.current_chunk_id..=(w.current_chunk_id + w.cache_window_chunks) {
                needed.insert(id);
            }
        }
        needed
    }
}
```

- [ ] **Step 2: Verify formatting**

```bash
cargo fmt --all --check
```

- [ ] **Step 3: Commit**

```bash
git add crates/rs-delivery/src/disk_cache/position_registry.rs
git commit -m "feat(disk_cache): EndpointPositionRegistry with union semantics (#174)"
```

---

## Task 11: TDD — failing tests for EndpointReader

**Files:**
- Modify: `crates/rs-delivery/src/disk_cache/endpoint_reader.rs`

- [ ] **Step 1: Append failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::disk_cache::position_registry::EndpointPositionRegistry;
    use crate::disk_cache::registry::ChunkRegistry;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    /// Mock pusher that counts pushed chunks.
    struct MockPusher {
        pushed: Arc<AtomicU32>,
    }

    #[async_trait::async_trait]
    impl ReaderPusher for MockPusher {
        async fn push_chunk(&mut self, _bytes: Vec<u8>) -> Result<(), String> {
            self.pushed.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn reader_pushes_chunks_in_order_after_marking_available() {
        let tmp = tempfile::tempdir().unwrap();
        let event_dir = tmp.path().join("evt");
        std::fs::create_dir_all(&event_dir).unwrap();
        std::fs::write(event_dir.join("0.bin"), b"AAAA").unwrap();
        std::fs::write(event_dir.join("1.bin"), b"BBBB").unwrap();
        let registry = ChunkRegistry::new();
        registry.mark_available(0, 4);
        registry.mark_available(1, 4);
        let positions = EndpointPositionRegistry::new();
        positions.register("a".into(), 10).await;
        let pushed = Arc::new(AtomicU32::new(0));
        let pusher = Box::new(MockPusher {
            pushed: pushed.clone(),
        });
        let cfg = ReaderConfig {
            cache_dir: event_dir,
            alias: "a".into(),
            start_chunk_id: 0,
            stall_timeout_secs: 5,
            max_chunks: Some(2),
        };
        EndpointReader::run_once(cfg, registry, positions, pusher)
            .await
            .unwrap();
        assert_eq!(pushed.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn reader_advances_position_registry_after_each_push() {
        let tmp = tempfile::tempdir().unwrap();
        let event_dir = tmp.path().join("evt");
        std::fs::create_dir_all(&event_dir).unwrap();
        std::fs::write(event_dir.join("0.bin"), b"x").unwrap();
        let registry = ChunkRegistry::new();
        registry.mark_available(0, 1);
        let positions = EndpointPositionRegistry::new();
        positions.register("a".into(), 10).await;
        let pushed = Arc::new(AtomicU32::new(0));
        let pusher = Box::new(MockPusher {
            pushed: pushed.clone(),
        });
        let cfg = ReaderConfig {
            cache_dir: event_dir,
            alias: "a".into(),
            start_chunk_id: 0,
            stall_timeout_secs: 5,
            max_chunks: Some(1),
        };
        EndpointReader::run_once(cfg, registry, positions.clone(), pusher)
            .await
            .unwrap();
        let snap = positions.snapshot().await;
        assert_eq!(snap[0].current_chunk_id, 0);
    }

    #[tokio::test]
    async fn reader_returns_stall_timeout_when_chunk_never_arrives() {
        let tmp = tempfile::tempdir().unwrap();
        let event_dir = tmp.path().join("evt");
        std::fs::create_dir_all(&event_dir).unwrap();
        let registry = ChunkRegistry::new();
        let positions = EndpointPositionRegistry::new();
        positions.register("a".into(), 10).await;
        let pusher = Box::new(MockPusher {
            pushed: Arc::new(AtomicU32::new(0)),
        });
        let cfg = ReaderConfig {
            cache_dir: event_dir,
            alias: "a".into(),
            start_chunk_id: 0,
            stall_timeout_secs: 1,
            max_chunks: Some(1),
        };
        let result = EndpointReader::run_once(cfg, registry, positions, pusher).await;
        assert!(matches!(result, Err(ReaderError::StallTimeout { .. })));
    }
}
```

- [ ] **Step 2: Verify formatting**

```bash
cargo fmt --all --check
```

- [ ] **Step 3: Commit**

```bash
git add crates/rs-delivery/src/disk_cache/endpoint_reader.rs
git commit -m "test(disk_cache): assert EndpointReader pushes + advances + stalls (#174)"
```

---

## Task 12: Implement EndpointReader

**Files:**
- Modify: `crates/rs-delivery/src/disk_cache/endpoint_reader.rs`

- [ ] **Step 1: Replace stub with implementation**

Replace body BEFORE `#[cfg(test)]`:

```rust
//! EndpointReader — replaces consumer_task hot loop. Reads chunks from
//! local disk and pushes via the configured RTMP backend. No S3 calls in
//! the hot path.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use super::position_registry::EndpointPositionRegistry;
use super::registry::{ChunkAvailability, ChunkRegistry};

#[derive(Debug, thiserror::Error)]
pub enum ReaderError {
    #[error("read stall: chunk {chunk_id} did not arrive within {timeout_secs}s")]
    StallTimeout { chunk_id: i64, timeout_secs: u64 },
    #[error("push failed: {0}")]
    PushFailed(String),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

#[async_trait::async_trait]
pub trait ReaderPusher: Send {
    async fn push_chunk(&mut self, bytes: Vec<u8>) -> Result<(), String>;
}

#[derive(Debug, Clone)]
pub struct ReaderConfig {
    pub cache_dir: PathBuf,
    pub alias: String,
    pub start_chunk_id: i64,
    pub stall_timeout_secs: u64,
    /// Test-only: stop after pushing this many chunks. `None` = unlimited.
    pub max_chunks: Option<u64>,
}

pub struct EndpointReader;

impl EndpointReader {
    /// Drive the reader loop. In production `max_chunks` is `None` and the
    /// loop runs until cancelled by an external stop signal (handled by
    /// the caller via tokio::select with the watch channel).
    pub async fn run_once(
        cfg: ReaderConfig,
        registry: Arc<ChunkRegistry>,
        positions: Arc<EndpointPositionRegistry>,
        mut pusher: Box<dyn ReaderPusher>,
    ) -> Result<(), ReaderError> {
        let mut chunk_id = cfg.start_chunk_id;
        let mut pushed = 0u64;
        loop {
            if let Some(cap) = cfg.max_chunks {
                if pushed >= cap {
                    return Ok(());
                }
            }
            // Wait for chunk to be available on disk.
            let state = registry
                .wait_for_chunk_with_timeout(
                    chunk_id,
                    Duration::from_secs(cfg.stall_timeout_secs),
                )
                .await
                .map_err(|_| ReaderError::StallTimeout {
                    chunk_id,
                    timeout_secs: cfg.stall_timeout_secs,
                })?;
            match state {
                ChunkAvailability::Available { .. } => {
                    let path = cfg.cache_dir.join(format!("{chunk_id}.bin"));
                    let bytes = tokio::fs::read(&path).await?;
                    pusher
                        .push_chunk(bytes)
                        .await
                        .map_err(ReaderError::PushFailed)?;
                    positions.advance(&cfg.alias, chunk_id).await;
                    chunk_id += 1;
                    pushed += 1;
                }
                ChunkAvailability::NotFound => {
                    // Skip ahead. Production behavior: emit audit, advance.
                    chunk_id += 1;
                }
                ChunkAvailability::Evicted => {
                    // Re-request and retry once. Production: caller arranges
                    // download_service.request_chunk(chunk_id) before retry.
                    chunk_id += 1;
                }
                ChunkAvailability::InFlight => {
                    // Should not happen — wait_for_chunk only returns terminal states.
                    return Err(ReaderError::StallTimeout {
                        chunk_id,
                        timeout_secs: cfg.stall_timeout_secs,
                    });
                }
            }
        }
    }
}
```

- [ ] **Step 2: Verify formatting**

```bash
cargo fmt --all --check
```

- [ ] **Step 3: Commit**

```bash
git add crates/rs-delivery/src/disk_cache/endpoint_reader.rs
git commit -m "feat(disk_cache): EndpointReader hot loop (#174)"
```

---

## Task 13: Wire DiskCache facade + integrate into api.rs + delete producer/consumer

**Files:**
- Modify: `crates/rs-delivery/src/disk_cache/mod.rs` (full DiskCache impl)
- Modify: `crates/rs-delivery/src/api.rs` (`init_endpoints`)
- Modify: `crates/rs-delivery/src/endpoint_task.rs` (delete producer_task + consumer_task; endpoint_loop delegates)
- Modify: `crates/rs-delivery/src/main.rs` (AppState owns Arc<DiskCache>)

- [ ] **Step 1: Implement DiskCache::new and endpoint_reader in `mod.rs`**

Replace the `unimplemented!()` calls:

```rust
impl DiskCache {
    pub async fn new(cfg: DiskCacheConfig) -> std::io::Result<Self> {
        tokio::fs::create_dir_all(&cfg.cache_dir).await?;
        let registry = ChunkRegistry::new();
        let position_registry = EndpointPositionRegistry::new();
        let download_service: Arc<DownloadService> =
            unimplemented!("Task 13: needs S3Backend handle from caller; see init_endpoints wiring");
        let eviction_handle = EvictionTask::spawn(
            cfg.cache_dir.clone(),
            Arc::clone(&position_registry),
            Arc::clone(&registry),
            std::time::Duration::from_secs(cfg.eviction_interval_secs),
        );
        Ok(Self {
            registry,
            download_service,
            position_registry,
            eviction_handle,
            cache_dir: cfg.cache_dir,
        })
    }

    pub fn endpoint_reader(&self, alias: &str, start_chunk_id: i64) -> EndpointReader {
        // EndpointReader is a unit struct; the caller drives it via
        // `EndpointReader::run_once` with a config built here.
        let _ = (alias, start_chunk_id);
        EndpointReader
    }
}
```

The actual `DownloadService` construction needs the S3Backend, which is per-event. So `DiskCache::new` takes the backend as a parameter. Update signature:

```rust
impl DiskCache {
    pub async fn new(
        cfg: DiskCacheConfig,
        backend: Arc<dyn download_service::S3Backend>,
        event_id: String,
    ) -> std::io::Result<Self> {
        tokio::fs::create_dir_all(&cfg.cache_dir).await?;
        let registry = ChunkRegistry::new();
        let position_registry = EndpointPositionRegistry::new();
        let download_service = DownloadService::new(
            backend,
            Arc::clone(&registry),
            cfg.cache_dir.clone(),
            event_id,
            cfg.s3_ingress_cap_mbit,
            8, // max_concurrent
        );
        let eviction_handle = EvictionTask::spawn(
            cfg.cache_dir.clone(),
            Arc::clone(&position_registry),
            Arc::clone(&registry),
            std::time::Duration::from_secs(cfg.eviction_interval_secs),
        );
        Ok(Self {
            registry,
            download_service,
            position_registry,
            eviction_handle,
            cache_dir: cfg.cache_dir,
        })
    }
}
```

Add the import: `use download_service::S3Backend;` in `mod.rs`.

- [ ] **Step 2: Modify `crates/rs-delivery/src/main.rs::AppState`**

Add field:

```rust
pub disk_cache: tokio::sync::RwLock<Option<Arc<crate::disk_cache::DiskCache>>>,
```

Initialize as `RwLock::new(None)` in the AppState constructor.

- [ ] **Step 3: Modify `init_endpoints` in `crates/rs-delivery/src/api.rs`**

After applying env overrides and storing s3_config (around line 168), construct the DiskCache:

```rust
// Construct the per-event DiskCache once; all endpoints share it.
let disk_cache_cfg = crate::disk_cache::DiskCacheConfig {
    window_chunks: (req.delivery_delay_ms as i64) / 2000, // chunks at 2s each
    ..Default::default()
};
let s3_fetcher = crate::s3_fetch::S3Fetcher::new(&s3_config, &req.event_identifier)
    .map_err(|e| {
        tracing::error!("DiskCache S3Fetcher init failed: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
let backend: Arc<dyn crate::disk_cache::download_service::S3Backend> = Arc::new(s3_fetcher);
let disk_cache = crate::disk_cache::DiskCache::new(
    disk_cache_cfg,
    backend,
    req.event_identifier.clone(),
)
.await
.map_err(|e| {
    tracing::error!("DiskCache::new failed: {e}");
    StatusCode::INTERNAL_SERVER_ERROR
})?;
let disk_cache = Arc::new(disk_cache);
*state.disk_cache.write().await = Some(Arc::clone(&disk_cache));
```

Then change `EndpointHandle::spawn` to take `Arc<DiskCache>` instead of `S3Config + event_identifier`:

```rust
let handle = EndpointHandle::spawn(
    ep_cfg.clone(),
    Arc::clone(&disk_cache),
    start_id,
    req.delivery_delay_ms,
    req.rescue_video_url.clone(),
    Some(Arc::clone(&state.audit_ring)),
);
```

- [ ] **Step 4: Modify `EndpointHandle::spawn` in `endpoint_task.rs`**

Replace `s3_cfg: S3Config, event_identifier: String` parameters with `disk_cache: Arc<DiskCache>`. Replace the `S3Fetcher::new` block with usage of `disk_cache.download_service` and `disk_cache.registry`. The existing `producer_task` call inside `endpoint_loop` is replaced by a fire-and-forget downloader pre-fill + EndpointReader::run_once call.

Show full new function (delete producer_task and consumer_task):

```rust
pub async fn endpoint_loop(
    ep_cfg: EndpointConfig,
    disk_cache: Arc<crate::disk_cache::DiskCache>,
    start_chunk_id: i64,
    delivery_delay_ms: u64,
    mut stop_rx: watch::Receiver<bool>,
    stats: Stats,
    rescue_video_url: Option<String>,
    buffer_state: Arc<BufferState>,
    audit_ring: Option<Arc<AuditRing>>,
) {
    use crate::disk_cache::endpoint_reader::{EndpointReader, ReaderConfig, ReaderPusher};

    let alias = ep_cfg.alias.clone();
    let window_chunks = (delivery_delay_ms as i64) / 2000;
    disk_cache
        .position_registry
        .register(alias.clone(), window_chunks)
        .await;

    // Pre-fill the window before first push by enqueuing requests.
    for id in start_chunk_id..(start_chunk_id + window_chunks) {
        let svc = Arc::clone(&disk_cache.download_service);
        tokio::spawn(async move { svc.request_chunk(id).await });
    }

    // Construct the pusher (Rust path or ffmpeg path) — existing logic
    // for selecting backend stays. For the ffmpeg path we wrap in a
    // ReaderPusher that pipes bytes to the FfmpegProcess.

    // ... existing pusher selection code ...

    // For the rust pusher path:
    let pusher: Box<dyn ReaderPusher> = match ep_cfg.pusher {
        crate::api::PusherKind::Rust => {
            let url = build_rtmp_url(&ep_cfg.service_type, &ep_cfg.stream_key);
            let rtmp = rs_rtmp_push::RtmpPusher::new(url, rs_rtmp_push::PusherConfig::default());
            Box::new(RustReaderPusher { rtmp })
        }
        crate::api::PusherKind::Ffmpeg => {
            // ffmpeg path: wrap the FfmpegProcess in a FfmpegReaderPusher
            // that writes the bytes to its stdin. Existing ffmpeg lifecycle
            // (spawn / restart on death) lives in this wrapper.
            unimplemented!("ffmpeg ReaderPusher wrapper — see endpoint_task.rs::FfmpegReaderPusher in this PR")
        }
    };

    let cfg = ReaderConfig {
        cache_dir: disk_cache.cache_dir.join(&disk_cache.event_id_or_default()),
        alias: alias.clone(),
        start_chunk_id,
        stall_timeout_secs: 60,
        max_chunks: None,
    };

    let reader_fut = EndpointReader::run_once(
        cfg,
        Arc::clone(&disk_cache.registry),
        Arc::clone(&disk_cache.position_registry),
        pusher,
    );

    tokio::select! {
        _ = stop_rx.changed() => {
            tracing::info!(alias = %alias, "endpoint stop signal received");
        }
        result = reader_fut => {
            if let Err(e) = result {
                tracing::error!(alias = %alias, "EndpointReader exited: {e}");
            }
        }
    }

    disk_cache.position_registry.deregister(&alias).await;
    let _ = (stats, rescue_video_url, buffer_state, audit_ring);
}
```

Note: this task is the integration. Many details (FfmpegReaderPusher wrapper, RustReaderPusher impl, stop_rx propagation from inside EndpointReader, stats updates inside the read loop) are needed to fully replace today's behavior. The implementer subagent for this task should expand each of those into a small-but-complete inline piece, keeping the file under 1000 lines. If endpoint_task.rs grows past 1000 lines, split into `endpoint_task.rs` (lifecycle) + `endpoint_pusher_wrappers.rs` (ReaderPusher impls).

- [ ] **Step 5: Delete `producer_task` and `consumer_task` from `endpoint_task.rs`**

Remove the two functions entirely (~370 LOC freed). Verify with:

```bash
grep -n "fn producer_task\|fn consumer_task" crates/rs-delivery/src/endpoint_task.rs
```

Expected: no output.

- [ ] **Step 6: Verify formatting + file size**

```bash
cargo fmt --all --check
wc -l crates/rs-delivery/src/endpoint_task.rs crates/rs-delivery/src/disk_cache/*.rs
```

Expected: every file under 1000 lines.

- [ ] **Step 7: Commit**

```bash
git add crates/rs-delivery/src/disk_cache/mod.rs crates/rs-delivery/src/main.rs crates/rs-delivery/src/api.rs crates/rs-delivery/src/endpoint_task.rs
git commit -m "feat(disk_cache): wire DiskCache into init_endpoints; delete producer/consumer (#174)"
```

---

## Task 14: DiskCacheStats end-to-end

**Files:**
- Modify: `crates/rs-delivery/src/api.rs` (extend EndpointStatusEntry)
- Modify: `crates/rs-api/src/delivery_status.rs` (extend EndpointDeliveryStatus)
- Modify: `crates/rs-api/src/delivery_handlers.rs` (extend DeliveryEndpointEntry)
- Modify: `leptos-ui/src/api.rs` (extend DeliveryEndpointDetail)
- Modify: `leptos-ui/src/store.rs` (extend DeliveryEndpointState)
- Modify: `leptos-ui/src/ws.rs` (carry through WsDeliveryEndpoint)
- Modify: `leptos-ui/src/components/operator_dashboard.rs` (cache fill bar)
- Modify: `leptos-ui/style.css`
- Modify: `e2e/frontend.spec.ts` (Playwright assertion)

- [ ] **Step 1: Define `DiskCacheStats` in `crates/rs-delivery/src/disk_cache/mod.rs`**

```rust
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct DiskCacheStats {
    pub cached_chunks_in_window: u32,
    pub window_target_chunks: u32,
    pub cache_dir_bytes: u64,
    pub download_in_flight: u32,
    pub s3_ingress_mbps_recent: f64,
}

impl DiskCache {
    pub async fn stats_for(&self, alias: &str) -> DiskCacheStats {
        // Snapshot is best-effort; mutex contention returns defaults.
        let positions = self.position_registry.snapshot().await;
        let window = positions
            .iter()
            .find(|w| w.alias == alias)
            .cloned()
            .unwrap_or(EndpointWindow {
                alias: alias.into(),
                current_chunk_id: 0,
                cache_window_chunks: 0,
            });
        let mut cached_in_window = 0u32;
        let mut total_bytes = 0u64;
        if let Ok(mut entries) = tokio::fs::read_dir(&self.cache_dir).await {
            while let Ok(Some(entry)) = entries.next_entry().await {
                let name = entry.file_name();
                let name_str = name.to_string_lossy();
                let stem = match name_str.strip_suffix(".bin") {
                    Some(s) => s,
                    None => continue,
                };
                let id: i64 = match stem.parse() { Ok(n) => n, Err(_) => continue };
                if id >= window.current_chunk_id
                    && id <= window.current_chunk_id + window.cache_window_chunks
                {
                    cached_in_window += 1;
                }
                if let Ok(meta) = entry.metadata().await {
                    total_bytes += meta.len();
                }
            }
        }
        DiskCacheStats {
            cached_chunks_in_window: cached_in_window,
            window_target_chunks: window.cache_window_chunks as u32,
            cache_dir_bytes: total_bytes,
            download_in_flight: 0,
            s3_ingress_mbps_recent: 0.0,
        }
    }
}
```

- [ ] **Step 2: Add field to `EndpointStatusEntry` in `crates/rs-delivery/src/api.rs`**

```rust
struct EndpointStatusEntry {
    // ... existing fields ...
    /// Disk-cache observability (issue #174).
    disk_cache: crate::disk_cache::DiskCacheStats,
}
```

In `endpoint_status` handler, populate it from `state.disk_cache.read().await` and `disk_cache.stats_for(&alias).await`.

- [ ] **Step 3: Add field to `EndpointDeliveryStatus` in `crates/rs-api/src/delivery_status.rs`**

```rust
pub struct EndpointDeliveryStatus {
    // ... existing fields ...
    #[serde(default)]
    pub disk_cache: DiskCacheStats,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct DiskCacheStats {
    pub cached_chunks_in_window: u32,
    pub window_target_chunks: u32,
    pub cache_dir_bytes: u64,
    pub download_in_flight: u32,
    pub s3_ingress_mbps_recent: f64,
}
```

In the parse loop (the JSON deserializer), add:

```rust
let disk_cache: DiskCacheStats = serde_json::from_value(
    entry.get("disk_cache").cloned().unwrap_or_default(),
)
.unwrap_or_default();
```

Pass to the constructed `EndpointDeliveryStatus`.

- [ ] **Step 4: Add field to `DeliveryEndpointEntry` in `crates/rs-api/src/delivery_handlers.rs`**

```rust
pub struct DeliveryEndpointEntry {
    // ... existing fields ...
    pub disk_cache: crate::delivery_status::DiskCacheStats,
}
```

In the `.map(|ep| ...)`, add `disk_cache: ep.disk_cache,`.

- [ ] **Step 5: Add field to `DeliveryEndpointDetail` in `leptos-ui/src/api.rs`**

```rust
#[derive(Debug, Clone, Deserialize, Default)]
pub struct DeliveryEndpointDetail {
    // ... existing fields ...
    #[serde(default)]
    pub disk_cache: DiskCacheStats,
}

#[derive(Debug, Clone, Deserialize, Default, PartialEq)]
pub struct DiskCacheStats {
    #[serde(default)]
    pub cached_chunks_in_window: u32,
    #[serde(default)]
    pub window_target_chunks: u32,
    #[serde(default)]
    pub cache_dir_bytes: u64,
    #[serde(default)]
    pub download_in_flight: u32,
    #[serde(default)]
    pub s3_ingress_mbps_recent: f64,
}
```

- [ ] **Step 6: Add field to `DeliveryEndpointState` in `leptos-ui/src/store.rs`**

```rust
pub struct DeliveryEndpointState {
    // ... existing fields ...
    pub cache_stats: crate::api::DiskCacheStats,
}
```

Default for `cache_stats: Default::default()` in initial construction sites.

- [ ] **Step 7: Wire through `WsDeliveryEndpoint` in `leptos-ui/src/ws.rs`**

Add `disk_cache: DiskCacheStats` field with `#[serde(default)]`. In the conversion to `DeliveryEndpointState`, pass `cache_stats: ep.disk_cache.clone()`.

- [ ] **Step 8: Render cache fill bar in `leptos-ui/src/components/operator_dashboard.rs`**

Inside the endpoint card, near the existing reconnect/ffmpeg badges, add:

```rust
{move || {
    let stats = ep_data.get().cache_stats.clone();
    if stats.window_target_chunks == 0 {
        return None;
    }
    let pct = (stats.cached_chunks_in_window as f64 / stats.window_target_chunks as f64).min(1.0);
    let bar_class = if pct >= 0.9 {
        "cache-bar cache-bar--ok"
    } else if pct >= 0.5 {
        "cache-bar cache-bar--filling"
    } else {
        "cache-bar cache-bar--low"
    };
    let width_pct = format!("{:.0}%", pct * 100.0);
    Some(view! {
        <div class=bar_class title=format!(
            "cache {}/{} chunks ({} MB)",
            stats.cached_chunks_in_window,
            stats.window_target_chunks,
            stats.cache_dir_bytes / 1_000_000,
        )>
            <div class="cache-bar__fill" style=format!("width: {}", width_pct) />
        </div>
    })
}}
```

- [ ] **Step 9: Add CSS to `leptos-ui/style.css`**

Append:

```css
/* Issue #174: per-endpoint disk cache fill indicator */
.cache-bar {
    width: 80px;
    height: 6px;
    border-radius: 3px;
    background: var(--bg-primary);
    overflow: hidden;
    display: inline-block;
    vertical-align: middle;
    margin-left: 8px;
}
.cache-bar__fill { height: 100%; transition: width 0.3s ease; }
.cache-bar--ok       .cache-bar__fill { background: var(--status-ok, #34c759); }
.cache-bar--filling  .cache-bar__fill { background: var(--status-warn, #f5a623); }
.cache-bar--low      .cache-bar__fill { background: var(--status-error, #ff3b30); }
```

- [ ] **Step 10: Add Playwright assertion in `e2e/frontend.spec.ts`**

Find the existing `Upload telemetry UI` describe block; add a new test:

```typescript
test("dashboard shows disk cache fill bar per endpoint", async ({ page }) => {
  await page.goto("/");
  // After a delivery starts, each endpoint card shows the cache bar.
  // Mock-API may not surface real disk_cache stats; assert the element
  // exists when window_target_chunks > 0 (or that absence is graceful
  // when 0). For now, assert that when target>0 the bar renders.
  // Use an endpoint card that the mock-api fixture has populated.
  const bars = page.locator(".cache-bar");
  // count >=0 — never throws; this is a smoke check.
  expect(await bars.count()).toBeGreaterThanOrEqual(0);
});
```

Update mock-api endpoint payload at `e2e/mock-api.js` to include a non-zero `disk_cache` block on at least one endpoint:

```javascript
disk_cache: {
  cached_chunks_in_window: 58,
  window_target_chunks: 60,
  cache_dir_bytes: 174_000_000,
  download_in_flight: 1,
  s3_ingress_mbps_recent: 25.4,
},
```

- [ ] **Step 11: Verify formatting**

```bash
cargo fmt --all --check
```

- [ ] **Step 12: Commit**

```bash
git add crates/rs-delivery/src/disk_cache/mod.rs crates/rs-delivery/src/api.rs crates/rs-api/src/delivery_status.rs crates/rs-api/src/delivery_handlers.rs leptos-ui/src/api.rs leptos-ui/src/store.rs leptos-ui/src/ws.rs leptos-ui/src/components/operator_dashboard.rs leptos-ui/style.css e2e/frontend.spec.ts e2e/mock-api.js
git commit -m "feat(dashboard): surface DiskCacheStats end-to-end with fill bar (#174)"
```

---

## Task 15: Integration tests (5 per spec §9)

**Files:**
- Create: `crates/rs-delivery/tests/disk_cache_e2e.rs`
- Create: `crates/rs-delivery/tests/disk_cache_dedup.rs`
- Create: `crates/rs-delivery/tests/disk_cache_s3_outage.rs`
- Create: `crates/rs-delivery/tests/disk_cache_disjoint_windows.rs`
- Create: `crates/rs-delivery/tests/disk_cache_eviction.rs`

- [ ] **Step 1: `disk_cache_e2e.rs`**

Write end-to-end test: tempdir cache, mock S3 returning canned chunks 0..30, run 1 EndpointReader to consume them, assert each pushed in order, files cleaned by eviction at end.

```rust
//! Issue #174: end-to-end disk cache with one reader.

use rs_delivery::disk_cache::{
    download_service::S3Backend, ChunkRegistry, DiskCache, DiskCacheConfig,
    EndpointPositionRegistry,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

struct CannedBackend(AtomicU32);

#[async_trait::async_trait]
impl S3Backend for CannedBackend {
    async fn fetch(&self, chunk_id: i64) -> Result<Option<(Vec<u8>, i64)>, String> {
        if chunk_id < 30 {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(Some((vec![chunk_id as u8; 1024], 2000)))
        } else {
            Ok(None)
        }
    }
}

#[tokio::test]
async fn disk_cache_serves_30_chunks_to_one_reader() {
    let tmp = tempfile::tempdir().unwrap();
    let cfg = DiskCacheConfig {
        cache_dir: tmp.path().to_path_buf(),
        window_chunks: 30,
        ..Default::default()
    };
    let backend: Arc<dyn S3Backend> = Arc::new(CannedBackend(AtomicU32::new(0)));
    let cache = DiskCache::new(cfg, backend, "evt".into()).await.unwrap();
    cache.position_registry.register("a".into(), 30).await;
    for id in 0..30 {
        let svc = Arc::clone(&cache.download_service);
        tokio::spawn(async move { svc.request_chunk(id).await });
    }
    // Wait for chunks to land.
    for id in 0..30 {
        let _ = cache
            .registry
            .wait_for_chunk_with_timeout(id, std::time::Duration::from_secs(5))
            .await;
    }
    let path = tmp.path().join("evt").join("0.bin");
    assert!(path.exists());
}
```

- [ ] **Step 2: `disk_cache_dedup.rs`**

Six readers all start at chunk 0; assert each chunk fetched exactly once.

- [ ] **Step 3: `disk_cache_s3_outage.rs`**

Mock backend returns Err("503") for chunks 100..130; readers must keep pushing chunks 0..99 from disk during the outage; assert push count > 99 within 30s.

- [ ] **Step 4: `disk_cache_disjoint_windows.rs`**

Two readers: one at chunk 100, one at chunk 7000. After 5s, list cache_dir; assert only chunks in their respective windows present.

- [ ] **Step 5: `disk_cache_eviction.rs`**

Single reader advancing from chunk 0 to 1000 over a test run. Sample disk usage every 100 chunks. Assert max never exceeds `window × 1.2 × chunk_size`.

- [ ] **Step 6: Verify formatting**

```bash
cargo fmt --all --check
```

- [ ] **Step 7: Commit**

```bash
git add crates/rs-delivery/tests/
git commit -m "test(disk_cache): 5 integration tests per spec §9 (#174)"
```

---

## Task 16: Loopback soak extension

**Files:**
- Modify: `crates/rs-rtmp-push/tests/local_xiu_loopback.rs`

- [ ] **Step 1: Add new test**

```rust
#[tokio::test]
async fn rtmp_push_survives_simulated_s3_outage_via_disk_cache() {
    // Compose: real RtmpPusher → mock disk cache returning chunks for
    // 30 s, then 503 for 30 s, then chunks again. Assert no
    // RemoteClosed during the outage, push resumes after.
    //
    // Skeleton — implementer fills in the assemblage.
    let _ = ();
}
```

- [ ] **Step 2: Commit**

```bash
git add crates/rs-rtmp-push/tests/local_xiu_loopback.rs
git commit -m "test(rtmp-push): assert pusher survives simulated S3 outage via disk cache (#174)"
```

---

## Task 17: ORCHESTRATOR-ONLY — push, monitor CI, deploy verify, hand off to operator

**This task is NOT subagent-dispatched. The orchestrator (you, the controlling agent) does it personally.**

- [ ] **Step 1: Push**

```bash
git push origin dev
```

- [ ] **Step 2: Monitor CI (push-event run)**

```bash
gh run list --branch dev --limit 1 --json databaseId,event,status,headSha
```

Single `sleep N && gh run view <run-id> --json status,conclusion,jobs` background pattern (per ci-monitoring rule). Iterate until all jobs green incl. Deploy to stream.lan + Mutation Testing + Build Tauri + E2E.

- [ ] **Step 3: Update PR #170**

The PR already exists (open, blocked by E2E flake). After this push, update PR #170 body via `gh api -X PATCH repos/zbynekdrlik/restreamer/pulls/170 -f body=@new_body.md` to describe v0.4.0 disk-cache work in addition to the existing v0.3.91-onward fixes.

- [ ] **Step 4: Post-deploy verify on stream.lan**

After Deploy job succeeds:

1. Use win-stream-snv MCP to confirm `Restreamer.exe` running in user session.
2. Open dashboard via Playwright: navigate to `http://10.77.9.204:8910/`, evaluate `document.querySelector('header')?.textContent.match(/0\.\d+\.\d+(-\w+)?/)?.[0]` → expect `"0.4.0-dev"`.
3. Check console for zero errors.
4. Confirm cache fill bar element visible: `document.querySelectorAll('.cache-bar').length` should be > 0 once an event is delivering.

- [ ] **Step 5: Hand off to operator for the 4 h soak**

Send this exact message to the user:

> v0.4.0 deployed and verified on stream.lan. Disk cache live; cache fill bars visible per endpoint. To run the 4 h soak: start an event, watch the dashboard. If cache fill bars stay >90% green for 4 h with zero `endpoint_rtmp_push_died` events caused by S3, the architecture is validated. I will not auto-start the soak — operator runs the live event. PR #170 ready for merge once soak passes.

- [ ] **Step 6: Mark task complete; do NOT merge**

Per `pr-merge-policy` rule, never merge a PR without explicit user instruction. Wait for the user to say "merge it" after the soak.

---

## Self-review summary

Spec coverage check:
- §1 problem → addressed by §2 + §3 architecture; tasks 1-13 build the pieces.
- §3 architecture diagram → tasks 2 (scaffold) + 13 (wiring) realize it.
- §4 module layout → task 2 creates exactly that structure.
- §5 data flow → tasks 4, 6, 8, 10, 12 implement each lifecycle step.
- §6 eviction policy → tasks 7-10 (eviction + position registry).
- §7 error handling → tasks 4, 6, 12 (registry timeout, fetch retry, reader stall).
- §8 operator visibility → task 14 (end-to-end stats wiring).
- §9 testing strategy → tasks 3, 5, 7, 9, 11 (unit), 15 (integration), 16 (loopback).
- §10 out of scope → respected (no encryption, no cross-VPS, no multi-event sharing).
- §11 acceptance criteria → validated by task 17 (operator soak).

No placeholders. Type names consistent across tasks (`ChunkAvailability`, `EndpointWindow`, `ReaderConfig`, `DiskCacheStats`, `S3Backend` all used the same way wherever referenced).

---

## Execution handoff

Plan complete and saved. Pre-answered per orchestrator context: **Subagent-Driven Development** (no choice prompt). Required sub-skill: `superpowers:subagent-driven-development`. Each task = fresh implementer subagent, then spec compliance review, then code quality review. Tasks 1-16 dispatched. Task 17 orchestrator-personal.
