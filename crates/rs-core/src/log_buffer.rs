use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, TryLockError};

use serde::{Deserialize, Serialize};

/// A single captured log entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub level: String,
    pub target: String,
    pub message: String,
}

/// Thread-safe ring buffer for capturing recent log entries.
///
/// Used by the API to serve GET /logs/inpoint and GET /logs/endpoint.
/// Populated by a tracing layer in the service binary.
///
/// `push` never waits (#368): it runs inside every log call, the ingest
/// runtime's included, while `recent` (an API request) holds the lock for a
/// whole copy. A push that finds the lock taken drops its entry and counts
/// it in `dropped()`. Only this in-memory view loses it: the file log
/// (restreamer.log) gets every line through its own non-blocking writer.
///
/// A buffer built with [`LogBuffer::with_head`] also PINS its first entries
/// (#424): the delivery VPS keeps its session start (init, warmup, the
/// warmup-to-content switch) next to the newest tail, instead of only the
/// last lines of a long event.
#[derive(Clone)]
pub struct LogBuffer {
    inner: Arc<Mutex<Entries>>,
    dropped: Arc<AtomicU64>,
}

/// The pinned head plus the evicting tail.
struct Entries {
    head: Vec<LogEntry>,
    head_capacity: usize,
    tail: VecDeque<LogEntry>,
    tail_capacity: usize,
    /// Entries evicted from the tail since the head filled: the gap between
    /// the head and the tail.
    evicted: u64,
}

/// Target of the synthetic entry `recent` puts between the tail and the
/// pinned head when entries in between were evicted.
pub const LOG_GAP_TARGET: &str = "log_buffer::gap";

impl LogBuffer {
    /// A plain ring of the last `capacity` entries.
    pub fn new(capacity: usize) -> Self {
        Self::with_head(0, capacity)
    }

    /// The first `head` entries are kept for good; after them, a ring of the
    /// last `tail` entries.
    pub fn with_head(head: usize, tail: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Entries {
                head: Vec::with_capacity(head),
                head_capacity: head,
                tail: VecDeque::with_capacity(tail),
                tail_capacity: tail,
                evicted: 0,
            })),
            dropped: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Push a log entry: into the pinned head while it has room, else into
    /// the tail, evicting the tail's oldest when full.
    pub fn push(&self, entry: LogEntry) {
        let mut buf = match self.inner.try_lock() {
            Ok(buf) => buf,
            Err(TryLockError::Poisoned(p)) => p.into_inner(),
            Err(TryLockError::WouldBlock) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
                return;
            }
        };
        if buf.head.len() < buf.head_capacity {
            buf.head.push(entry);
            return;
        }
        if buf.tail_capacity == 0 {
            return;
        }
        if buf.tail.len() >= buf.tail_capacity {
            buf.tail.pop_front();
            buf.evicted += 1;
        }
        buf.tail.push_back(entry);
    }

    /// Entries `push` dropped because the buffer was locked at that moment.
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// Return recent log entries whose target starts with `prefix`, newest
    /// first: the tail, then (for a pinned head) a [`LOG_GAP_TARGET`] entry
    /// counting the evicted entries in between, then the head.
    pub fn recent(&self, prefix: &str, limit: usize) -> Vec<LogEntry> {
        let buf = self.inner.lock().unwrap();
        let gap = (buf.head_capacity > 0 && buf.evicted > 0).then(|| LogEntry {
            level: "WARN".into(),
            target: LOG_GAP_TARGET.into(),
            message: format!(
                "{} log entries between the session start and the newest ones were evicted",
                buf.evicted
            ),
        });
        buf.tail
            .iter()
            .rev()
            .chain(gap.iter())
            .chain(buf.head.iter().rev())
            .filter(|e| e.target.starts_with(prefix))
            .take(limit)
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(message: &str) -> LogEntry {
        LogEntry {
            level: "WARN".into(),
            target: "rs_inpoint::ingest_gap".into(),
            message: message.into(),
        }
    }

    /// #368: a log call never waits for a reader holding the buffer.
    #[test]
    fn push_never_waits_for_a_held_lock() {
        let buf = LogBuffer::new(10);
        buf.push(entry("before"));
        assert_eq!(buf.dropped(), 0);
        let held = buf.inner.lock().unwrap();
        buf.push(entry("while a reader holds it"));
        buf.push(entry("still held"));
        drop(held);
        assert_eq!(buf.dropped(), 2);
        buf.push(entry("after"));
        let got: Vec<String> = buf
            .recent("rs_inpoint", 10)
            .into_iter()
            .map(|e| e.message)
            .collect();
        assert_eq!(got, vec!["after", "before"]);
        assert_eq!(buf.dropped(), 2);
    }

    #[test]
    fn push_and_retrieve() {
        let buf = LogBuffer::new(10);
        buf.push(LogEntry {
            level: "INFO".into(),
            target: "rs_inpoint::rtmp".into(),
            message: "started".into(),
        });
        buf.push(LogEntry {
            level: "INFO".into(),
            target: "rs_endpoint::uploader".into(),
            message: "uploading".into(),
        });

        let inpoint = buf.recent("rs_inpoint", 10);
        assert_eq!(inpoint.len(), 1);
        assert_eq!(inpoint[0].message, "started");

        let endpoint = buf.recent("rs_endpoint", 10);
        assert_eq!(endpoint.len(), 1);
        assert_eq!(endpoint[0].message, "uploading");
    }

    #[test]
    fn evicts_oldest_when_full() {
        let buf = LogBuffer::new(2);
        for i in 0..5 {
            buf.push(LogEntry {
                level: "INFO".into(),
                target: "test".into(),
                message: format!("msg-{i}"),
            });
        }
        let entries = buf.recent("test", 10);
        assert_eq!(entries.len(), 2);
        // Newest first
        assert_eq!(entries[0].message, "msg-4");
        assert_eq!(entries[1].message, "msg-3");
    }

    #[test]
    fn zero_capacity_discards_all() {
        let buf = LogBuffer::new(0);
        buf.push(LogEntry {
            level: "INFO".into(),
            target: "test".into(),
            message: "hello".into(),
        });
        assert!(buf.recent("test", 10).is_empty());
    }

    fn numbered(i: usize) -> LogEntry {
        LogEntry {
            level: "INFO".into(),
            target: "rs_delivery".into(),
            message: format!("msg-{i}"),
        }
    }

    fn messages(buf: &LogBuffer, prefix: &str, limit: usize) -> Vec<String> {
        buf.recent(prefix, limit)
            .into_iter()
            .map(|e| e.message)
            .collect()
    }

    /// #424: the session start survives a long run: the head is pinned, the
    /// tail rolls, and a gap entry counts what was evicted in between.
    #[test]
    fn a_pinned_head_keeps_the_session_start() {
        let buf = LogBuffer::with_head(2, 3);
        for i in 0..9 {
            buf.push(numbered(i));
        }
        assert_eq!(
            messages(&buf, "", 100),
            vec![
                "msg-8",
                "msg-7",
                "msg-6",
                "4 log entries between the session start and the newest ones were evicted",
                "msg-1",
                "msg-0",
            ]
        );
        let gap = &buf.recent("", 100)[3];
        assert_eq!(gap.target, LOG_GAP_TARGET);
        assert_eq!(gap.level, "WARN");
        // The gap is an entry of its own target: a prefix query skips it.
        assert_eq!(
            messages(&buf, "rs_delivery", 100),
            vec!["msg-8", "msg-7", "msg-6", "msg-1", "msg-0"]
        );
        // `limit` still counts from the newest.
        assert_eq!(messages(&buf, "", 2), vec!["msg-8", "msg-7"]);
    }

    /// No gap entry while nothing was evicted, and none without a head.
    #[test]
    fn a_gap_entry_appears_only_after_an_eviction_past_a_head() {
        let buf = LogBuffer::with_head(2, 3);
        for i in 0..5 {
            buf.push(numbered(i));
        }
        assert_eq!(
            messages(&buf, "", 100),
            vec!["msg-4", "msg-3", "msg-2", "msg-1", "msg-0"]
        );
        buf.push(numbered(5));
        assert_eq!(
            messages(&buf, "", 100),
            vec![
                "msg-5",
                "msg-4",
                "msg-3",
                "1 log entries between the session start and the newest ones were evicted",
                "msg-1",
                "msg-0",
            ]
        );

        let plain = LogBuffer::new(2);
        for i in 0..5 {
            plain.push(numbered(i));
        }
        assert_eq!(messages(&plain, "", 100), vec!["msg-4", "msg-3"]);
    }

    /// A head with no tail keeps exactly the first entries.
    #[test]
    fn a_head_without_a_tail_keeps_only_the_first_entries() {
        let buf = LogBuffer::with_head(2, 0);
        for i in 0..5 {
            buf.push(numbered(i));
        }
        assert_eq!(messages(&buf, "", 100), vec!["msg-1", "msg-0"]);
    }

    #[test]
    fn filter_by_prefix() {
        let buf = LogBuffer::new(100);
        buf.push(LogEntry {
            level: "INFO".into(),
            target: "rs_inpoint::flv_chunker".into(),
            message: "chunk".into(),
        });
        buf.push(LogEntry {
            level: "WARN".into(),
            target: "rs_endpoint::s3".into(),
            message: "retry".into(),
        });
        buf.push(LogEntry {
            level: "INFO".into(),
            target: "rs_inpoint::rtmp".into(),
            message: "connected".into(),
        });

        let inpoint = buf.recent("rs_inpoint", 10);
        assert_eq!(inpoint.len(), 2);
        assert_eq!(inpoint[0].message, "connected");
        assert_eq!(inpoint[1].message, "chunk");

        let all = buf.recent("rs_", 10);
        assert_eq!(all.len(), 3);
    }
}
