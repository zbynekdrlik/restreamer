//! A single-owner audit-row throttle that COUNTS what it holds back (#368).
//!
//! A storm of short stalls or ingest frame gaps must not flood `audit_log`,
//! and it must not vanish either: the owner asked that every dropout be
//! measurable. So the throttle lets one row through per `min_interval` and
//! aggregates the rest (count, max, total). The aggregate rides on the next
//! row it lets through (`Admission::Emit { suppressed }`), or the owner
//! flushes it as a row of its own once the interval has passed
//! (`take_due`), or at the end of a stream (`take_pending`).
//!
//! It is plain data driven by explicit instants: no lock, no clock read, no
//! allocation. That makes it safe on the single-threaded ingest runtime and
//! on the stall detector's thread, and deterministic in tests.
//! `audit::RateLimiter` does not fit either caller: it drops without
//! counting, takes a DashMap shard lock per call, and reads the real clock.

use std::time::{Duration, Instant};

use serde_json::{Value, json};

/// What a throttle held back since the last row it let through.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Suppressed {
    /// Incidents held back.
    pub count: u64,
    /// The largest value among them (ms of stall/gap, or frames).
    pub max: u64,
    /// The sum of their values.
    pub total: u64,
    /// When the first one happened.
    pub first_at: Instant,
    /// When the last one happened.
    pub last_at: Instant,
}

impl Suppressed {
    fn new(at: Instant, value: u64) -> Self {
        Self {
            count: 1,
            max: value,
            total: value,
            first_at: at,
            last_at: at,
        }
    }

    fn add(&mut self, at: Instant, value: u64) {
        self.count += 1;
        self.max = self.max.max(value);
        self.total = self.total.saturating_add(value);
        self.last_at = at;
    }

    /// Audit-detail JSON: `{count, max, total, unit, span_ms}`, where
    /// `span_ms` is the time from the first held-back incident to the last.
    pub fn to_json(&self, unit: &str) -> Value {
        let span = self.last_at.saturating_duration_since(self.first_at);
        json!({
            "count": self.count,
            "max": self.max,
            "total": self.total,
            "unit": unit,
            "span_ms": u64::try_from(span.as_millis()).unwrap_or(u64::MAX),
        })
    }
}

/// The throttle's answer to one incident.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admission {
    /// Write the row now, carrying what was held back before it, if anything.
    Emit { suppressed: Option<Suppressed> },
    /// Do not write a row; the incident is counted into the next aggregate.
    Suppress,
}

/// One row per `min_interval`, the rest aggregated. One instance per
/// incident source (a detector, one kind of ingest gap).
#[derive(Debug, Clone)]
pub struct AuditThrottle {
    min_interval: Duration,
    last_emit: Option<Instant>,
    pending: Option<Suppressed>,
}

impl AuditThrottle {
    /// `min_interval` zero lets every row through.
    pub fn new(min_interval: Duration) -> Self {
        Self {
            min_interval,
            last_emit: None,
            pending: None,
        }
    }

    /// Whether a row may be written at `now`.
    fn open(&self, now: Instant) -> bool {
        self.last_emit
            .is_none_or(|t| now.saturating_duration_since(t) >= self.min_interval)
    }

    /// An incident of `value` happened at `now`.
    pub fn admit(&mut self, now: Instant, value: u64) -> Admission {
        if self.open(now) {
            self.last_emit = Some(now);
            return Admission::Emit {
                suppressed: self.pending.take(),
            };
        }
        match self.pending.as_mut() {
            Some(p) => p.add(now, value),
            None => self.pending = Some(Suppressed::new(now, value)),
        }
        Admission::Suppress
    }

    /// The held-back aggregate, once a row may be written again: the owner
    /// writes it as a row of its own. This counts as that interval's row.
    pub fn take_due(&mut self, now: Instant) -> Option<Suppressed> {
        if self.pending.is_none() || !self.open(now) {
            return None;
        }
        self.last_emit = Some(now);
        self.pending.take()
    }

    /// The held-back aggregate, regardless of the interval (a stream ended,
    /// or the owner shuts down).
    pub fn take_pending(&mut self) -> Option<Suppressed> {
        self.pending.take()
    }
}

#[cfg(test)]
#[path = "audit_throttle_tests.rs"]
mod tests;
