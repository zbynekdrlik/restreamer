//! Captures ffmpeg progress events and exposes them for host-side polling.
//!
//! The VPS stores recent `FfmpegProgress` samples in a bounded in-memory ring.
//! stream.lan polls `/api/status` which includes `recent_progress` rows, then
//! persists them to `ffmpeg_progress_samples` via `rs_core::db::drift`.

use parking_lot::Mutex;
use rs_ffmpeg::FfmpegProgress;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

/// One progress row stored in the ring.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProgressRow {
    /// Monotonic cursor for host-side `?since=<cursor>` polling.
    pub id: i64,
    pub endpoint_alias: String,
    pub media_time_ms: i64,
    pub wall_clock_ms: i64,
}

/// Bounded in-memory ring of recent ffmpeg progress samples.
pub struct ProgressRing {
    cap: usize,
    rows: Mutex<VecDeque<ProgressRow>>,
    next_id: AtomicI64,
}

impl ProgressRing {
    pub fn new(cap: usize) -> Arc<Self> {
        Arc::new(Self {
            cap,
            rows: Mutex::new(VecDeque::with_capacity(cap)),
            next_id: AtomicI64::new(1),
        })
    }

    /// Push a progress sample. Returns the stored row (with assigned cursor id).
    pub fn push(&self, alias: &str, media_time_ms: i64, wall_clock_ms: i64) -> ProgressRow {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let row = ProgressRow {
            id,
            endpoint_alias: alias.to_string(),
            media_time_ms,
            wall_clock_ms,
        };
        let mut rows = self.rows.lock();
        if rows.len() >= self.cap {
            rows.pop_front();
        }
        rows.push_back(row.clone());
        row
    }

    /// Return rows with id > cursor, and the new cursor (largest id returned,
    /// or the input cursor if none).
    pub fn since(&self, cursor: i64) -> (Vec<ProgressRow>, i64) {
        let rows = self.rows.lock();
        let filtered: Vec<ProgressRow> = rows.iter().filter(|r| r.id > cursor).cloned().collect();
        let new_cursor = filtered.last().map(|r| r.id).unwrap_or(cursor);
        (filtered, new_cursor)
    }
}

/// Spawn a background task that drains the progress channel and writes each
/// sample into the `ProgressRing`.
pub fn spawn_progress_capture(
    mut rx: tokio::sync::mpsc::Receiver<FfmpegProgress>,
    alias: String,
    ring: Arc<ProgressRing>,
) {
    tokio::spawn(async move {
        while let Some(p) = rx.recv().await {
            ring.push(&alias, p.media_time_ms, p.wall_clock_ms);
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn progress_ring_since_returns_rows_after_cursor() {
        let ring = ProgressRing::new(100);
        let a = ring.push("yt1", 1000, 1_700_000_000_000);
        let b = ring.push("yt1", 2000, 1_700_000_001_000);

        let (rows, cursor) = ring.since(0);
        assert_eq!(rows.len(), 2);
        assert_eq!(cursor, b.id);

        let (rows, _) = ring.since(a.id);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, b.id);

        let (rows, _) = ring.since(b.id);
        assert!(rows.is_empty());
    }

    #[test]
    fn progress_ring_drops_oldest_when_cap_reached() {
        let ring = ProgressRing::new(2);
        ring.push("yt1", 1000, 100);
        let b = ring.push("yt1", 2000, 200);
        let c = ring.push("yt1", 3000, 300);

        let (rows, _) = ring.since(0);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].id, b.id, "oldest dropped");
        assert_eq!(rows[1].id, c.id);
    }

    #[tokio::test]
    async fn spawn_progress_capture_routes_events_to_ring() {
        let ring = ProgressRing::new(100);
        let (tx, rx) = tokio::sync::mpsc::channel(8);

        spawn_progress_capture(rx, "yt_test".to_string(), Arc::clone(&ring));

        tx.send(FfmpegProgress {
            media_time_ms: 5000,
            wall_clock_ms: 1_700_000_000_000,
        })
        .await
        .unwrap();
        // Drop sender so the task drains and exits.
        drop(tx);
        // Give the tokio task a moment to process.
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        let (rows, _) = ring.since(0);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].endpoint_alias, "yt_test");
        assert_eq!(rows[0].media_time_ms, 5000);
        assert_eq!(rows[0].wall_clock_ms, 1_700_000_000_000);
    }
}
