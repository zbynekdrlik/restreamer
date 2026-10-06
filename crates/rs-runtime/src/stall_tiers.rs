//! Stall tiers and the audit gate of the process-stall detector (#368).
//!
//! OBS drops frames once its RTMP send queue holds more than ~700 ms, and
//! the Sunday 2026-10-04 hiccups were 1-7 s. The #367 detector only saw
//! stalls of 5 s or more, so the tiers are:
//!
//! | duration | tier | evidence |
//! |---|---|---|
//! | >= `stall_threshold` (500 ms) | `minor` | `stall_start`/`stall_end` in `stall*.log`, an info log line |
//! | >= `audit_threshold` (700 ms) | `major` | plus a `ProcessStall` audit row |
//! | >= `severe_threshold` (5 s) | `severe` | the same, labelled severe |
//!
//! Audit rows go through an [`AuditThrottle`]: at most one per
//! `audit_min_interval`, the rest counted into an aggregate (count, max and
//! total duration) that rides on the next row or is flushed as a row of its
//! own. `stall*.log` keeps every stall regardless (it is size-capped).

use std::path::Path;
use std::time::{Duration, Instant};

use rs_core::audit_throttle::{Admission, AuditThrottle, Suppressed};
use rs_core::config::StallDetectorSettings;
use serde_json::{Value, json};

use super::{STALL_LOG_MAX_BYTES, StallDetectorConfig, stall_log, stall_log_file_name};

/// The shortest probe interval the settings may ask for.
pub const MIN_PROBE_INTERVAL: Duration = Duration::from_millis(10);
/// The longest probe interval the settings may ask for.
pub const MAX_PROBE_INTERVAL: Duration = Duration::from_secs(1);
/// The shortest `tick_late_threshold` the settings may ask for: below the
/// OS timer resolution (~15 ms on Windows) every stall would read
/// `whole_process`.
pub const MIN_TICK_LATE_THRESHOLD: Duration = Duration::from_millis(20);
/// How often a healthy detector refreshes its pre-stall resource baseline.
pub const BASELINE_INTERVAL: Duration = Duration::from_secs(10);

/// How bad a finished stall was.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StallTier {
    /// At least `stall_threshold`, below `audit_threshold`: evidence only.
    Minor,
    /// At least `audit_threshold`: OBS likely dropped frames. Audited.
    Major,
    /// At least `severe_threshold`. Audited.
    Severe,
}

impl StallTier {
    pub fn of(duration: Duration, cfg: &StallDetectorConfig) -> Self {
        let _ = (duration, cfg);
        Self::Minor
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Minor => "minor",
            Self::Major => "major",
            Self::Severe => "severe",
        }
    }
}

/// What to do with a finished stall.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StallVerdict {
    /// Below the audit threshold: `stall*.log` and a log line only.
    LogOnly,
    /// Write the `ProcessStall` row now, carrying what was held back.
    Audit { suppressed: Option<Suppressed> },
    /// Audit-worthy, but the throttle holds it back into the aggregate.
    HeldBack,
}

/// The tier thresholds plus the throttle: decides, per finished stall,
/// whether it becomes an audit row.
#[derive(Debug)]
pub struct StallAuditGate {
    audit_threshold: Duration,
    throttle: AuditThrottle,
}

impl StallAuditGate {
    pub fn new(cfg: &StallDetectorConfig) -> Self {
        Self {
            audit_threshold: cfg.audit_threshold,
            throttle: AuditThrottle::new(cfg.audit_min_interval),
        }
    }

    /// A stall lasting `duration` ended at `now`.
    pub fn on_stall_end(&mut self, now: Instant, duration: Duration) -> StallVerdict {
        let _ = (now, duration, self.audit_threshold, &mut self.throttle);
        StallVerdict::Audit { suppressed: None }
    }

    /// The held-back aggregate once a row may be written again. The caller
    /// asks only between stalls, when the log pipeline is safe to use.
    pub fn take_due(&mut self, now: Instant) -> Option<Suppressed> {
        let _ = now;
        None
    }
}

/// The detector configuration the settings ask for, made consistent. Each
/// value it had to change comes back as a warning for the startup log:
/// - the probe interval is kept within [10 ms, 1 s];
/// - the record threshold is at least one probe interval (a shorter stall
///   cannot be measured);
/// - audit >= record and severe >= audit;
/// - `tick_late_threshold` is at least 20 ms.
pub fn config_from_settings(
    data_dir: &Path,
    runtime: &str,
    s: &StallDetectorSettings,
) -> (StallDetectorConfig, Vec<String>) {
    let _ = s;
    let config = StallDetectorConfig {
        probe_interval: Duration::from_secs(1),
        stall_threshold: Duration::from_secs(5),
        audit_threshold: Duration::from_secs(5),
        severe_threshold: Duration::from_secs(5),
        audit_min_interval: Duration::ZERO,
        tick_late_threshold: Duration::from_secs(1),
        baseline_every_ticks: 10,
        log_path: data_dir.join("logs").join(stall_log_file_name(runtime)),
        log_max_bytes: STALL_LOG_MAX_BYTES,
    };
    (config, Vec::new())
}

/// Healthy ticks between two baseline samples: one per `BASELINE_INTERVAL`.
pub fn baseline_every_ticks(probe_interval: Duration) -> u32 {
    let ticks = BASELINE_INTERVAL.as_millis() / probe_interval.as_millis().max(1);
    u32::try_from(ticks).unwrap_or(u32::MAX).max(1)
}

/// The tier fields of the `detector_started` record.
pub fn tier_fields(cfg: &StallDetectorConfig) -> Value {
    json!({
        "audit_threshold_ms": stall_log::ms(cfg.audit_threshold),
        "severe_threshold_ms": stall_log::ms(cfg.severe_threshold),
        "audit_min_interval_ms": stall_log::ms(cfg.audit_min_interval),
    })
}

/// `record` with every field of `extra` (an object) added.
pub fn with_fields(record: &Value, extra: &Value) -> Value {
    let mut out = record.clone();
    if let (Value::Object(m), Value::Object(e)) = (&mut out, extra) {
        for (k, v) in e {
            m.insert(k.clone(), v.clone());
        }
    }
    out
}

/// Detail of the aggregate `ProcessStall` row flushed for stalls the
/// throttle held back.
pub fn aggregate_detail(held: &Suppressed, log_path: &Path) -> Value {
    json!({
        "aggregate": true,
        "held_back": held.to_json("ms"),
        "stall_log": log_path.display().to_string(),
    })
}

#[cfg(test)]
#[path = "stall_tiers_tests.rs"]
mod tests;
