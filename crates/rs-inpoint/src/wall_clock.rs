//! Injectable wall-clock seam for the FLV chunker (#367).
//!
//! The chunker reads the wall clock for chunk file names, the producer
//! write timestamp (`ChunkInfo::wall_clock_written_at_ms`) and the
//! `drift_debug` src-vs-wall diagnostic. Production uses
//! [`SystemWallClock`]. Tests inject their own clock so that arrival-time
//! scenarios (a GOP-cache replay burst, a whole-process freeze, network
//! jitter) are deterministic instead of depending on real `SystemTime`.

use std::sync::Arc;
use std::time::SystemTime;

/// Source of Unix-epoch milliseconds.
pub trait WallClock: Send + Sync {
    /// Current Unix-epoch time in milliseconds.
    fn now_ms(&self) -> i64;
}

/// The real system clock (`SystemTime::now()`).
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemWallClock;

impl WallClock for SystemWallClock {
    fn now_ms(&self) -> i64 {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64
    }
}

/// The production clock, shared.
pub fn system_clock() -> Arc<dyn WallClock> {
    Arc::new(SystemWallClock)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn system_clock_tracks_system_time() {
        let before = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let now = system_clock().now_ms();
        let after = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        assert!(
            (before..=after).contains(&now),
            "system clock {now} outside [{before}, {after}]"
        );
    }
}
