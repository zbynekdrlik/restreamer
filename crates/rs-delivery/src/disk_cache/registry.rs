//! ChunkRegistry — in-memory chunk-availability tracker with async wake.
//!
//! Owns the source of truth for "is chunk N on disk and ready to read?".
//! `DownloadService` calls `mark_available` after the file rename;
//! `EndpointReader` calls `wait_for_chunk` to block until ready.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

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
    /// Wakes all pending waiters. Safe to call from both sync and async contexts.
    pub fn mark_available(self: &Arc<Self>, chunk_id: i64, size_bytes: u64) {
        let notify = {
            let mut g = self.inner.lock().unwrap();
            let slot = g.entry(chunk_id).or_insert_with(|| Slot {
                state: ChunkAvailability::InFlight,
                notify: Arc::new(Notify::new()),
            });
            slot.state = ChunkAvailability::Available { size_bytes };
            Arc::clone(&slot.notify)
        };
        notify.notify_waiters();
    }

    pub fn mark_not_found(self: &Arc<Self>, chunk_id: i64) {
        let notify = {
            let mut g = self.inner.lock().unwrap();
            let slot = g.entry(chunk_id).or_insert_with(|| Slot {
                state: ChunkAvailability::InFlight,
                notify: Arc::new(Notify::new()),
            });
            slot.state = ChunkAvailability::NotFound;
            Arc::clone(&slot.notify)
        };
        notify.notify_waiters();
    }

    pub fn mark_evicted(self: &Arc<Self>, chunk_id: i64) {
        let notify = {
            let mut g = self.inner.lock().unwrap();
            let slot = g.entry(chunk_id).or_insert_with(|| Slot {
                state: ChunkAvailability::InFlight,
                notify: Arc::new(Notify::new()),
            });
            slot.state = ChunkAvailability::Evicted;
            Arc::clone(&slot.notify)
        };
        notify.notify_waiters();
    }

    /// Set the slot to InFlight, ALWAYS — even if a previous fetch
    /// already terminated (NotFound / Evicted). Without this active
    /// reset, a PrefetchReader retry against a chunk that previously
    /// 404'd would observe stale NotFound state via `wait_for_chunk`
    /// and never block on the new in-flight fetch (#184).
    pub fn mark_in_flight(self: &Arc<Self>, chunk_id: i64) {
        let mut g = self.inner.lock().unwrap();
        let slot = g.entry(chunk_id).or_insert_with(|| Slot {
            state: ChunkAvailability::InFlight,
            notify: Arc::new(Notify::new()),
        });
        slot.state = ChunkAvailability::InFlight;
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
    ///
    /// Race-safe pattern: the `Notified` future is created and `enable`d
    /// BEFORE the state check, so concurrent `mark_*` calls cannot fire
    /// `notify_waiters()` in the gap and lose the wake.
    pub async fn wait_for_chunk(
        self: &Arc<Self>,
        chunk_id: i64,
    ) -> Result<ChunkAvailability, RegistryError> {
        loop {
            // 1. Get-or-create the slot's Notify Arc, drop the lock immediately.
            let notify_arc = {
                let mut g = self.inner.lock().unwrap();
                let slot = g.entry(chunk_id).or_insert_with(|| Slot {
                    state: ChunkAvailability::InFlight,
                    notify: Arc::new(Notify::new()),
                });
                Arc::clone(&slot.notify)
            };

            // 2. Register the waiter BEFORE checking state. `enable` polls the
            //    future once so any subsequent `notify_waiters` reaches it.
            let notified = notify_arc.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            // 3. Check state under the lock again. If terminal, return now;
            //    any concurrent `notify_waiters` either already woke us
            //    (we just don't await) or transitioned the state we now see.
            {
                let g = self.inner.lock().unwrap();
                if let Some(slot) = g.get(&chunk_id) {
                    if !matches!(slot.state, ChunkAvailability::InFlight) {
                        return Ok(slot.state.clone());
                    }
                }
            }

            // 4. Still InFlight — wait for the wake. If `mark_*` fired between
            //    steps 2 and 3, the Notified holds the wake permit and this
            //    returns immediately, then we re-check on the next iteration.
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
        assert!(matches!(
            got,
            Ok(ChunkAvailability::Available { size_bytes: 1024 })
        ));
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
        assert!(matches!(
            got,
            Ok(ChunkAvailability::Available { size_bytes: 2048 })
        ));
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
            assert!(matches!(
                got,
                Ok(ChunkAvailability::Available { size_bytes: 512 })
            ));
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
