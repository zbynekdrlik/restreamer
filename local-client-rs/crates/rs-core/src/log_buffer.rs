use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

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
#[derive(Clone)]
pub struct LogBuffer {
    inner: Arc<Mutex<VecDeque<LogEntry>>>,
    capacity: usize,
}

impl LogBuffer {
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(VecDeque::with_capacity(capacity))),
            capacity,
        }
    }

    /// Push a log entry into the buffer, evicting the oldest if full.
    pub fn push(&self, entry: LogEntry) {
        if self.capacity == 0 {
            return;
        }
        let mut buf = self.inner.lock().unwrap();
        if buf.len() >= self.capacity {
            buf.pop_front();
        }
        buf.push_back(entry);
    }

    /// Return recent log entries whose target starts with `prefix`, newest first.
    pub fn recent(&self, prefix: &str, limit: usize) -> Vec<LogEntry> {
        let buf = self.inner.lock().unwrap();
        buf.iter()
            .rev()
            .filter(|e| e.target.starts_with(prefix))
            .take(limit)
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn filter_by_prefix() {
        let buf = LogBuffer::new(100);
        buf.push(LogEntry {
            level: "INFO".into(),
            target: "rs_inpoint::chunker".into(),
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
