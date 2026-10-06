//! "The RTMP publisher has been stable since" (#234, lock-free since #368).
//!
//! The ingest thread sets it when OBS connects and clears it when OBS leaves
//! (`InpointState::mark_connected` / `mark_disconnected`, called by the
//! `MediaReceiver` on the `restreamer-ingest` runtime). The API side reads
//! it: `/status` reports `rtmp_stable_secs`, `POST /delivery/start` gates the
//! VPS on it, and the Tauri tray's `get_status` mirrors `/status`.
//!
//! It used to be a `tokio::sync::Mutex<Option<Instant>>`. The single-threaded
//! ingest runtime then awaited a lock an API task on the main runtime could
//! hold. A publisher that connected meanwhile had its frames processed only
//! once the API task let go, and a session shorter than the hold was lost
//! whole (#368 issuecomment-6013075985 item 2).
//!
//! Now it is one atomic: nothing can hold it, and every method is
//! synchronous, so no caller can await it. The time stays monotonic. It is
//! stored as nanoseconds from an `Instant` taken when the cell is created,
//! never as wall-clock time, which an NTP or PTP step would move (and with it
//! the delivery-start gate).

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, Instant};

/// The stored value that means "no publisher".
const NO_PUBLISHER: i64 = i64::MIN;

#[derive(Debug)]
struct Cell {
    /// The origin of `offset_ns`, fixed for the cell's lifetime.
    anchor: Instant,
    /// Signed nanoseconds from `anchor`, or [`NO_PUBLISHER`].
    offset_ns: AtomicI64,
}

/// A shared, lock-free `Option<Instant>`. Clones share one cell, so the copy
/// the ingest thread writes and the copies the API reads see the same value.
#[derive(Debug, Clone)]
pub struct StableSince(Arc<Cell>);

impl StableSince {
    /// A cell that holds `None` (no publisher).
    pub fn new() -> Self {
        Self(Arc::new(Cell {
            anchor: Instant::now(),
            offset_ns: AtomicI64::new(NO_PUBLISHER),
        }))
    }

    /// Record that a publisher has been stable since `since`, or `None` for
    /// no publisher. Never blocks.
    pub fn set(&self, since: Option<Instant>) {
        let raw = since.map_or(NO_PUBLISHER, |at| {
            stored_offset(signed_offset_ns(self.0.anchor, at))
        });
        self.0.offset_ns.store(raw, Ordering::Release);
    }

    /// The recorded instant; `None` when no publisher is connected. Never
    /// blocks.
    pub fn get(&self) -> Option<Instant> {
        let raw = self.0.offset_ns.load(Ordering::Acquire);
        (raw != NO_PUBLISHER).then(|| instant_at(self.0.anchor, raw))
    }

    /// Whole seconds the publisher has been stable; 0 with no publisher.
    pub fn stable_secs(&self) -> u64 {
        self.get().map_or(0, |since| since.elapsed().as_secs())
    }

    /// Whether `self` and `other` share one cell.
    pub fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Default for StableSince {
    fn default() -> Self {
        Self::new()
    }
}

/// `at` as signed nanoseconds from `anchor` (negative when `at` is earlier).
/// Lossless: a `Duration`'s nanoseconds always fit an `i128`.
fn signed_offset_ns(anchor: Instant, at: Instant) -> i128 {
    match at.checked_duration_since(anchor) {
        Some(after) => after.as_nanos() as i128,
        None => -(anchor.duration_since(at).as_nanos() as i128),
    }
}

/// The value stored for a signed offset: clamped to +/-292 years, so it
/// never reaches [`NO_PUBLISHER`].
fn stored_offset(ns: i128) -> i64 {
    ns.clamp(-i128::from(i64::MAX), i128::from(i64::MAX)) as i64
}

/// The instant `raw` nanoseconds from `anchor`. An instant the platform
/// cannot represent (only a clamped offset could ask for one) reads as
/// `anchor`.
fn instant_at(anchor: Instant, raw: i64) -> Instant {
    let shifted = match u64::try_from(raw) {
        Ok(after) => anchor.checked_add(Duration::from_nanos(after)),
        Err(_) => anchor.checked_sub(Duration::from_nanos(raw.unsigned_abs())),
    };
    shifted.unwrap_or(anchor)
}

#[cfg(test)]
#[path = "stable_since_tests.rs"]
mod tests;
