//! Per-session frame bookkeeping behind the media receiver's diagnostics
//! (#367): the frame counters and the periodic heartbeat, kept as plain logic
//! so the counting is tested rather than only logged.

use std::time::Duration;

use tokio::time::Instant;

/// Interval of the frame-processing heartbeat log.
pub(crate) const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(60);

/// One heartbeat: frames since the previous heartbeat, and in the session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Heartbeat {
    pub(crate) frames_since_last: u64,
    pub(crate) total_frames: u64,
}

/// Frame counters of one published-stream session.
#[derive(Debug)]
pub(crate) struct FrameStats {
    total: u64,
    since_heartbeat: u64,
    last_heartbeat: Instant,
}

impl FrameStats {
    pub(crate) fn new(now: Instant) -> Self {
        Self {
            total: 0,
            since_heartbeat: 0,
            last_heartbeat: now,
        }
    }

    /// Frames received in the session so far.
    pub(crate) fn total(&self) -> u64 {
        self.total
    }

    /// Count one frame received at `now`. Returns the heartbeat to log once
    /// `HEARTBEAT_INTERVAL` has passed since the previous one.
    pub(crate) fn count(&mut self, now: Instant) -> Option<Heartbeat> {
        self.total += 1;
        self.since_heartbeat += 1;
        if now.duration_since(self.last_heartbeat) < HEARTBEAT_INTERVAL {
            return None;
        }
        let beat = Heartbeat {
            frames_since_last: self.since_heartbeat,
            total_frames: self.total,
        };
        self.since_heartbeat = 0;
        self.last_heartbeat = now;
        Some(beat)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_frames_and_beats_once_per_interval() {
        let t0 = Instant::now();
        let mut stats = FrameStats::new(t0);
        assert_eq!(stats.count(t0 + Duration::from_secs(1)), None);
        assert_eq!(
            stats.count(t0 + HEARTBEAT_INTERVAL - Duration::from_millis(1)),
            None
        );
        assert_eq!(
            stats.count(t0 + HEARTBEAT_INTERVAL),
            Some(Heartbeat {
                frames_since_last: 3,
                total_frames: 3
            }),
            "the interval is inclusive"
        );
        assert_eq!(
            stats.count(t0 + HEARTBEAT_INTERVAL + Duration::from_secs(1)),
            None,
            "the next interval starts at the beat"
        );
        assert_eq!(
            stats.count(t0 + 2 * HEARTBEAT_INTERVAL),
            Some(Heartbeat {
                frames_since_last: 2,
                total_frames: 5
            })
        );
        assert_eq!(stats.total(), 5);
    }
}
