//! Local chunk-store disk-pressure monitor. Alert-only: we never drop a
//! buffered chunk (continuity guarantee). At critical, the endpoint
//! lifecycle goes RED Attention (operator must act).

use rs_core::audit::{Action, AuditRow, Severity, Source};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiskPressure {
    Ok,
    Warn,
    Critical,
}

impl DiskPressure {
    /// Compact level for sharing through an `AtomicU8` (the disk monitor
    /// publishes this so `/api/v1/status` can expose the warn/critical state
    /// to the dashboard disk-pressure banner -- #231).
    pub fn as_u8(self) -> u8 {
        match self {
            DiskPressure::Ok => 0,
            DiskPressure::Warn => 1,
            DiskPressure::Critical => 2,
        }
    }

    /// Inverse of [`DiskPressure::as_u8`]. Unknown values decode to `Ok`.
    pub fn from_u8(v: u8) -> Self {
        match v {
            2 => DiskPressure::Critical,
            1 => DiskPressure::Warn,
            _ => DiskPressure::Ok,
        }
    }

    /// Lowercase operator-facing label used in the `/api/v1/status` payload.
    pub fn as_str(self) -> &'static str {
        match self {
            DiskPressure::Ok => "ok",
            DiskPressure::Warn => "warn",
            DiskPressure::Critical => "critical",
        }
    }
}

/// Classify by fraction of the volume USED (0.0..=1.0).
pub fn classify_disk_pressure(used_fraction: f64) -> DiskPressure {
    if used_fraction >= 0.90 {
        DiskPressure::Critical
    } else if used_fraction >= 0.80 {
        DiskPressure::Warn
    } else {
        DiskPressure::Ok
    }
}

/// True when a new pressure reading should be logged (level changed).
pub(crate) fn should_log_transition(prev: DiskPressure, now: DiskPressure) -> bool {
    prev != now
}

/// Sample the volume containing `chunk_dir` every 10s; emit LocalDiskPressure
/// on level transitions (Ok↔Warn↔Critical) only. Returns when the
/// shutdown channel fires.
///
/// `disk_critical`, when set, is updated every sample to reflect whether the
/// volume is at `DiskPressure::Critical` (true) or not (false). It feeds the
/// endpoint lifecycle so endpoints go RED Attention on a critically-full
/// chunk disk — the compensating signal for never-drop. It self-clears once
/// the disk recovers.
pub async fn run_disk_monitor(
    chunk_dir: PathBuf,
    audit_tx: Option<mpsc::Sender<AuditRow>>,
    disk_critical: Option<Arc<std::sync::atomic::AtomicBool>>,
    disk_level: Option<Arc<std::sync::atomic::AtomicU8>>,
    mut shutdown: broadcast::Receiver<()>,
) {
    let mut last_pressure = DiskPressure::Ok;
    loop {
        tokio::select! {
            _ = shutdown.recv() => return,
            _ = tokio::time::sleep(Duration::from_secs(10)) => {}
        }
        let Some((used, total)) = volume_usage(&chunk_dir) else {
            continue;
        };
        if total == 0 {
            continue;
        }
        let frac = used as f64 / total as f64;
        let pressure = classify_disk_pressure(frac);
        // Update the shared critical flag every sample (set true only on
        // Critical, false otherwise) so it self-clears when disk recovers.
        if let Some(f) = &disk_critical {
            f.store(
                pressure == DiskPressure::Critical,
                std::sync::atomic::Ordering::Relaxed,
            );
        }
        // Publish the full ok/warn/critical level (#231) every sample so the
        // dashboard banner shows the early Warn (80%) state -- not just the
        // Critical red wall -- and self-clears when the disk recovers.
        if let Some(l) = &disk_level {
            l.store(pressure.as_u8(), std::sync::atomic::Ordering::Relaxed);
        }
        // Emit an audit row only on a level transition (Ok↔Warn↔Critical).
        // last_pressure is updated BEFORE the Ok-continue so that a subsequent
        // Ok→Warn transition still logs.
        let log_it = should_log_transition(last_pressure, pressure);
        last_pressure = pressure;
        if pressure == DiskPressure::Ok {
            continue;
        }
        if log_it {
            if let Some(tx) = &audit_tx {
                let sev = match pressure {
                    DiskPressure::Warn => Severity::Warn,
                    DiskPressure::Critical => Severity::Critical,
                    DiskPressure::Ok => unreachable!(),
                };
                rs_core::audit::record(
                    tx,
                    AuditRow {
                        severity: sev,
                        source: Source::Inpoint,
                        event_id: None,
                        instance_id: None,
                        endpoint: None,
                        action: Action::LocalDiskPressure,
                        detail: serde_json::json!({
                            "used_fraction": frac,
                            "used_bytes": used,
                            "total_bytes": total,
                        }),
                        ts_override: None,
                    },
                );
            }
        }
    }
}

/// `(used_bytes, total_bytes)` for the volume holding `path`, via sysinfo.
/// Picks the disk whose mount point is the longest prefix of `path` so a
/// nested mount (e.g. `C:\ProgramData` on a separate volume) wins over the
/// root. Returns `None` if no mounted disk contains `path`.
fn volume_usage(path: &std::path::Path) -> Option<(u64, u64)> {
    use sysinfo::Disks;
    let disks = Disks::new_with_refreshed_list();
    let mut best: Option<(&sysinfo::Disk, usize)> = None;
    for d in disks.list() {
        let mp = d.mount_point();
        if path.starts_with(mp) {
            let len = mp.as_os_str().len();
            if best.map(|(_, l)| len > l).unwrap_or(true) {
                best = Some((d, len));
            }
        }
    }
    let (d, _) = best?;
    let total = d.total_space();
    let avail = d.available_space();
    Some((total.saturating_sub(avail), total))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn logs_only_on_transition() {
        assert!(should_log_transition(DiskPressure::Ok, DiskPressure::Warn));
        assert!(!should_log_transition(
            DiskPressure::Warn,
            DiskPressure::Warn
        ));
        assert!(should_log_transition(
            DiskPressure::Warn,
            DiskPressure::Critical
        ));
        assert!(should_log_transition(
            DiskPressure::Critical,
            DiskPressure::Warn
        ));
    }

    #[test]
    fn classify_boundaries() {
        assert_eq!(classify_disk_pressure(0.0), DiskPressure::Ok);
        assert_eq!(classify_disk_pressure(0.799), DiskPressure::Ok);
        assert_eq!(classify_disk_pressure(0.80), DiskPressure::Warn);
        assert_eq!(classify_disk_pressure(0.899), DiskPressure::Warn);
        assert_eq!(classify_disk_pressure(0.90), DiskPressure::Critical);
        assert_eq!(classify_disk_pressure(1.0), DiskPressure::Critical);
    }

    #[test]
    fn pressure_level_encoding_round_trips() {
        // #231: the disk monitor publishes the level via AtomicU8 and the
        // status handler decodes it for the dashboard banner. Encoding must be
        // stable in both directions and map to the operator-facing labels.
        for p in [DiskPressure::Ok, DiskPressure::Warn, DiskPressure::Critical] {
            assert_eq!(DiskPressure::from_u8(p.as_u8()), p);
        }
        assert_eq!(DiskPressure::Ok.as_u8(), 0);
        assert_eq!(DiskPressure::Warn.as_u8(), 1);
        assert_eq!(DiskPressure::Critical.as_u8(), 2);
        assert_eq!(DiskPressure::Ok.as_str(), "ok");
        assert_eq!(DiskPressure::Warn.as_str(), "warn");
        assert_eq!(DiskPressure::Critical.as_str(), "critical");
        // Unknown byte decodes to the safe default.
        assert_eq!(DiskPressure::from_u8(99), DiskPressure::Ok);
    }

    #[test]
    fn volume_usage_for_a_real_path_is_consistent() {
        // The temp dir always lives on a mounted volume, so usage must
        // resolve, total must be non-zero, and used must not exceed total.
        let dir = std::env::temp_dir();
        if let Some((used, total)) = volume_usage(&dir) {
            assert!(total > 0, "a mounted volume must report non-zero total");
            assert!(
                used <= total,
                "used ({used}) must not exceed total ({total})"
            );
        }
        // If no disk matched (sandboxed CI with no enumerable mounts), the
        // monitor simply skips that tick — `None` is an acceptable outcome,
        // so we do not fail the test on `None`.
    }
}
