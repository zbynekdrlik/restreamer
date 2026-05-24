//! In-memory audit ring for rs-delivery. Last N rows kept + optional JSONL append.
//!
//! Host-side `/api/v1/delivery/poll` uses `since=<cursor>` to mirror VPS
//! audit rows into the host `audit_log` table without needing the VPS to
//! push to the host directly.

use parking_lot::Mutex;
use rs_core::audit::{Action, Severity, Source};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RingRow {
    pub id: i64,
    pub ts: String,
    pub severity: Severity,
    pub source: Source,
    pub endpoint: Option<String>,
    pub action: Action,
    pub detail: serde_json::Value,
}

/// The per-row fields for [`AuditRing::push_parts`]. Lets builder functions
/// (e.g. `rescue_audit`) return one value instead of a 5-tuple, keeping
/// emission call sites tidy.
pub struct RingRowParts {
    pub severity: Severity,
    pub source: Source,
    pub endpoint: Option<String>,
    pub action: Action,
    pub detail: serde_json::Value,
}

pub struct AuditRing {
    cap: usize,
    rows: Mutex<VecDeque<RingRow>>,
    next_id: AtomicI64,
    jsonl_path: Mutex<Option<std::path::PathBuf>>,
}

impl AuditRing {
    pub fn new(cap: usize) -> Arc<Self> {
        Arc::new(Self {
            cap,
            rows: Mutex::new(VecDeque::with_capacity(cap)),
            next_id: AtomicI64::new(1),
            jsonl_path: Mutex::new(None),
        })
    }

    pub fn set_jsonl_path<P: Into<std::path::PathBuf>>(&self, p: P) {
        *self.jsonl_path.lock() = Some(p.into());
    }

    pub fn push(
        &self,
        severity: Severity,
        source: Source,
        endpoint: Option<String>,
        action: Action,
        detail: serde_json::Value,
    ) -> RingRow {
        let ts = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        self.push_ts(ts, severity, source, endpoint, action, detail)
    }

    /// Push a pre-built [`RingRowParts`] bundle (current wall-clock ts).
    pub fn push_parts(&self, p: RingRowParts) -> RingRow {
        self.push(p.severity, p.source, p.endpoint, p.action, p.detail)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn push_ts(
        &self,
        ts: String,
        severity: Severity,
        source: Source,
        endpoint: Option<String>,
        action: Action,
        detail: serde_json::Value,
    ) -> RingRow {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let row = RingRow {
            id,
            ts,
            severity,
            source,
            endpoint,
            action,
            detail,
        };

        let mut rows = self.rows.lock();
        if rows.len() >= self.cap {
            rows.pop_front();
        }
        rows.push_back(row.clone());
        drop(rows);

        // Best-effort JSONL append.
        if let Some(p) = self.jsonl_path.lock().clone() {
            if let Ok(line) = serde_json::to_string(&row) {
                use std::io::Write;
                if let Ok(mut f) = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&p)
                {
                    let _ = writeln!(f, "{line}");
                }
            }
        }
        row
    }

    /// Return rows with id > cursor, and the new cursor (largest id returned,
    /// or input cursor if none).
    pub fn since(&self, cursor: i64) -> (Vec<RingRow>, i64) {
        let rows = self.rows.lock();
        let filtered: Vec<RingRow> = rows.iter().filter(|r| r.id > cursor).cloned().collect();
        let new_cursor = filtered.last().map(|r| r.id).unwrap_or(cursor);
        (filtered, new_cursor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rs_core::audit::{Action, Severity, Source};

    #[test]
    fn ring_since_returns_rows_after_cursor() {
        let ring = AuditRing::new(500);
        let a = ring.push_ts(
            "t1".into(),
            Severity::Info,
            Source::Vps,
            Some("yt".into()),
            Action::EndpointStarted,
            serde_json::json!({}),
        );
        let b = ring.push_ts(
            "t2".into(),
            Severity::Warn,
            Source::Ffmpeg,
            Some("yt".into()),
            Action::EndpointFfmpegDied,
            serde_json::json!({}),
        );

        let (rows, cursor) = ring.since(0);
        assert_eq!(rows.len(), 2);
        assert_eq!(cursor, b.id);

        let (rows, _) = ring.since(a.id);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, b.id);

        let (rows, _) = ring.since(b.id);
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn ring_drops_oldest_when_cap_reached() {
        let ring = AuditRing::new(3);
        let _r1 = ring.push_ts(
            "t1".into(),
            Severity::Info,
            Source::Vps,
            None,
            Action::EndpointStarted,
            serde_json::json!({}),
        );
        let r2 = ring.push_ts(
            "t2".into(),
            Severity::Info,
            Source::Vps,
            None,
            Action::EndpointStarted,
            serde_json::json!({}),
        );
        let _r3 = ring.push_ts(
            "t3".into(),
            Severity::Info,
            Source::Vps,
            None,
            Action::EndpointStarted,
            serde_json::json!({}),
        );
        let r4 = ring.push_ts(
            "t4".into(),
            Severity::Info,
            Source::Vps,
            None,
            Action::EndpointStarted,
            serde_json::json!({}),
        );

        let (rows, _) = ring.since(0);
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].id, r2.id, "oldest dropped");
        assert_eq!(rows[2].id, r4.id);
    }
}
