//! `stall.log` writer + record builders for the #367 process-stall detector.
//!
//! Records are JSON lines appended from the detector's own OS thread with
//! plain `std::fs` — no async, no `log` crate — so a stall in the tokio
//! runtime or the logging pipeline can never prevent its own evidence from
//! being written. The file is size-capped: once it reaches `max_bytes` it is
//! renamed to `stall.log.old` (replacing any previous one) before the next
//! append, so the pair is bounded at ~2x `max_bytes`.

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{Value, json};

use super::resources::ResourceSnapshot;
use super::{StallReport, StallStart};

/// Append-only, size-capped JSON-lines file.
#[derive(Debug, Clone)]
pub struct StallLog {
    path: PathBuf,
    max_bytes: u64,
}

impl StallLog {
    pub fn new(path: PathBuf, max_bytes: u64) -> Self {
        Self { path, max_bytes }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Where the previous generation goes on rotation (`stall.log.old`).
    pub fn rotated_path(&self) -> PathBuf {
        let mut name = self.path.file_name().unwrap_or_default().to_os_string();
        name.push(".old");
        self.path.with_file_name(name)
    }

    /// Append one record as a single JSON line, rotating first when the file
    /// already holds `max_bytes` or more. The record is `sync_data`'d: stall
    /// records are rare and are exactly the evidence that must survive a crash
    /// that may follow the freeze.
    pub fn append(&self, record: &Value) -> std::io::Result<()> {
        if let Some(dir) = self.path.parent() {
            fs::create_dir_all(dir)?;
        }
        if let Ok(meta) = fs::metadata(&self.path) {
            if meta.len() >= self.max_bytes {
                fs::rename(&self.path, self.rotated_path())?;
            }
        }
        let mut line = serde_json::to_string(record)?;
        line.push('\n');
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        file.write_all(line.as_bytes())?;
        file.sync_data()
    }
}

/// One (`Instant`, wall-clock) pair read together. Monotonic instants are
/// what the detector measures with; wall time is only for humans reading the
/// record, so every instant is converted relative to this one anchor.
#[derive(Debug, Clone, Copy)]
pub struct WallAnchor {
    pub now: Instant,
    pub wall: DateTime<Utc>,
}

impl WallAnchor {
    pub fn read() -> Self {
        Self {
            now: Instant::now(),
            wall: Utc::now(),
        }
    }

    /// UTC RFC 3339 (ms) wall time of `at`, which may lie before OR after
    /// `now` (a probe can run just after the detector read its clock).
    pub fn wall_of(&self, at: Instant) -> String {
        let back = chrono::Duration::from_std(self.now.saturating_duration_since(at))
            .unwrap_or(chrono::Duration::zero());
        let forward = chrono::Duration::from_std(at.saturating_duration_since(self.now))
            .unwrap_or(chrono::Duration::zero());
        (self.wall - back + forward).to_rfc3339_opts(SecondsFormat::Millis, true)
    }
}

pub fn ms(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

/// Common header on every record: which process wrote it, and when.
fn header(event: &str, at: WallAnchor) -> serde_json::Map<String, Value> {
    let mut m = serde_json::Map::new();
    m.insert("event".into(), json!(event));
    m.insert(
        "ts".into(),
        json!(at.wall.to_rfc3339_opts(SecondsFormat::Millis, true)),
    );
    m.insert("pid".into(), json!(std::process::id()));
    m.insert("version".into(), json!(env!("CARGO_PKG_VERSION")));
    m
}

/// Written once when the detector thread starts, so an ABSENT `stall_start`
/// line provably means "no stall" rather than "detector not running".
pub fn started_record(
    probe_interval: Duration,
    stall_threshold: Duration,
    tick_late_threshold: Duration,
    resources: &ResourceSnapshot,
    at: WallAnchor,
) -> Value {
    let mut m = header("detector_started", at);
    m.insert("probe_interval_ms".into(), json!(ms(probe_interval)));
    m.insert("stall_threshold_ms".into(), json!(ms(stall_threshold)));
    m.insert(
        "tick_late_threshold_ms".into(),
        json!(ms(tick_late_threshold)),
    );
    m.insert("resources".into(), resources.to_json());
    Value::Object(m)
}

/// Written the moment a stall is DETECTED — while it is (for a runtime
/// starvation) still ongoing, so `resources` is the in-stall reading. The
/// `baseline` is the last healthy sample, for before/after comparison.
pub fn stall_start_record(
    start: &StallStart,
    resources: &ResourceSnapshot,
    baseline: Option<(&ResourceSnapshot, Duration)>,
    at: WallAnchor,
) -> Value {
    let mut m = header("stall_start", at);
    m.insert("started_at".into(), json!(at.wall_of(start.started_at)));
    m.insert("detected_at".into(), json!(at.wall_of(start.detected_at)));
    m.insert("trigger".into(), json!(start.trigger.as_str()));
    m.insert("probe_age_ms".into(), json!(start.probe_age.map(ms)));
    m.insert("detector_late_ms".into(), json!(ms(start.detector_late)));
    m.insert("resources".into(), resources.to_json());
    m.insert(
        "baseline".into(),
        match baseline {
            Some((snap, age)) => json!({ "age_ms": ms(age), "resources": snap.to_json() }),
            None => Value::Null,
        },
    );
    Value::Object(m)
}

/// Written when the runtime has answered a probe again — the stall is over.
pub fn stall_end_record(
    report: &StallReport,
    resources: &ResourceSnapshot,
    at: WallAnchor,
) -> Value {
    let mut m = header("stall_end", at);
    m.extend(report_fields(report, at));
    m.insert("resources".into(), resources.to_json());
    Value::Object(m)
}

/// Detail JSON of the `ProcessStall` audit row: the report fields plus the
/// in-stall and post-stall resource readings, where the full evidence lives,
/// and any `stall.log` write error (which could not be logged mid-stall).
pub fn audit_detail(
    report: &StallReport,
    start: Option<(&StallStart, &ResourceSnapshot)>,
    end_resources: &ResourceSnapshot,
    log_path: &Path,
    write_error: Option<String>,
    at: WallAnchor,
) -> Value {
    let mut m = report_fields(report, at);
    m.insert(
        "probe_age_at_detect_ms".into(),
        json!(start.and_then(|(s, _)| s.probe_age).map(ms)),
    );
    m.insert(
        "resources_at_start".into(),
        start.map_or(Value::Null, |(_, r)| r.to_json()),
    );
    m.insert("resources_at_end".into(), end_resources.to_json());
    m.insert("stall_log".into(), json!(log_path.display().to_string()));
    m.insert("stall_log_error".into(), json!(write_error));
    Value::Object(m)
}

/// The report fields shared by the `stall_end` record and the audit detail.
pub fn report_fields(report: &StallReport, at: WallAnchor) -> serde_json::Map<String, Value> {
    let mut m = serde_json::Map::new();
    m.insert("class".into(), json!(report.class.as_str()));
    m.insert("trigger".into(), json!(report.trigger.as_str()));
    m.insert("started_at".into(), json!(at.wall_of(report.started_at)));
    m.insert("ended_at".into(), json!(at.wall_of(report.ended_at)));
    m.insert("duration_ms".into(), json!(ms(report.duration)));
    m.insert(
        "detector_max_late_ms".into(),
        json!(ms(report.detector_max_late)),
    );
    m.insert(
        "detector_total_late_ms".into(),
        json!(ms(report.detector_total_late)),
    );
    m
}
