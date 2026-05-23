//! Local chunk-store disk-pressure monitor. Alert-only: we never drop a
//! buffered chunk (continuity guarantee). At critical, the endpoint
//! lifecycle goes RED Attention (operator must act).

use rs_core::audit::{Action, AuditRow, RateLimiter, Severity, Source};
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

/// Sample the volume containing `chunk_dir` every 10s; emit LocalDiskPressure
/// (rate-limited 1/min per severity) on Warn/Critical. Returns when the
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
    mut shutdown: broadcast::Receiver<()>,
) {
    let rl = Arc::new(RateLimiter::new());
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
        let (sev, class) = match pressure {
            DiskPressure::Ok => continue,
            DiskPressure::Warn => (Severity::Warn, "warn"),
            DiskPressure::Critical => (Severity::Critical, "critical"),
        };
        if let Some(tx) = &audit_tx {
            if rl.allow(Action::LocalDiskPressure, class) {
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
    fn classify_boundaries() {
        assert_eq!(classify_disk_pressure(0.0), DiskPressure::Ok);
        assert_eq!(classify_disk_pressure(0.799), DiskPressure::Ok);
        assert_eq!(classify_disk_pressure(0.80), DiskPressure::Warn);
        assert_eq!(classify_disk_pressure(0.899), DiskPressure::Warn);
        assert_eq!(classify_disk_pressure(0.90), DiskPressure::Critical);
        assert_eq!(classify_disk_pressure(1.0), DiskPressure::Critical);
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
