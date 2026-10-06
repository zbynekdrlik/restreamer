//! Local chunk-store disk-pressure monitor. Alert-only: we never drop a
//! buffered chunk (continuity guarantee). At critical, the endpoint
//! lifecycle goes RED Attention (operator must act).

use rs_core::audit::{Action, AuditRow, Severity, Source};
use std::path::{Path, PathBuf};
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

/// How often the volume holding the chunk dir is sampled.
const SAMPLE_INTERVAL: Duration = Duration::from_secs(10);
/// A volume enumeration still running after this is not waited for in this
/// sample (#368).
pub const VOLUME_PROBE_TIMEOUT: Duration = Duration::from_secs(5);

/// `(used_bytes, total_bytes)` of the volume holding a path, `None` when no
/// mounted volume holds it. Production: [`volume_usage`].
pub(crate) type VolumeProbe = Arc<dyn Fn(&Path) -> Option<(u64, u64)> + Send + Sync>;

/// Runs the volume probe for the disk monitor, every `interval`.
///
/// The probe (`sysinfo` disk enumeration) is a BLOCKING call, so it runs on
/// tokio's blocking pool, never on an async worker: inline, a slow
/// enumeration stalled every task sharing that worker, the RTMP ingest
/// included (#368). A sample waits for it at most `timeout`. An enumeration
/// still running then stays in flight and the NEXT sample waits for that same
/// one: a stuck enumeration holds one blocking thread, never one per tick.
pub(crate) struct VolumeSampler {
    probe: VolumeProbe,
    interval: Duration,
    timeout: Duration,
    in_flight: Option<tokio::task::JoinHandle<Option<(u64, u64)>>>,
}

impl VolumeSampler {
    pub(crate) fn new(probe: VolumeProbe, interval: Duration) -> Self {
        Self {
            probe,
            interval,
            timeout: VOLUME_PROBE_TIMEOUT,
            in_flight: None,
        }
    }

    /// The same sampler with another enumeration timeout.
    #[cfg(test)]
    pub(crate) fn with_timeout(self, timeout: Duration) -> Self {
        Self { timeout, ..self }
    }

    /// One sample of the volume holding `path`; `None` when no volume holds
    /// it, or the enumeration failed or is still running.
    pub(crate) async fn sample(&mut self, path: &Path) -> Option<(u64, u64)> {
        let mut enumeration = match self.in_flight.take() {
            Some(running) => running,
            None => {
                let probe = Arc::clone(&self.probe);
                let path = path.to_path_buf();
                tokio::task::spawn_blocking(move || probe(&path))
            }
        };
        match tokio::time::timeout(self.timeout, &mut enumeration).await {
            Ok(Ok(usage)) => usage,
            Ok(Err(e)) => {
                tracing::warn!("disk monitor: the volume enumeration failed: {e}");
                None
            }
            Err(_) => {
                tracing::warn!(
                    "disk monitor: the volume enumeration is still running after {:?}; \
                     this sample is skipped and the next one waits for it",
                    self.timeout
                );
                self.in_flight = Some(enumeration);
                None
            }
        }
    }
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
    shutdown: broadcast::Receiver<()>,
) {
    run_disk_monitor_with(
        VolumeSampler::new(Arc::new(volume_usage), SAMPLE_INTERVAL),
        chunk_dir,
        audit_tx,
        disk_critical,
        disk_level,
        shutdown,
    )
    .await;
}

/// [`run_disk_monitor`] with the volume probe and the sample interval given.
pub(crate) async fn run_disk_monitor_with(
    mut sampler: VolumeSampler,
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
            _ = tokio::time::sleep(sampler.interval) => {}
        }
        let Some((used, total)) = sampler.sample(&chunk_dir).await else {
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

    /// #368 design test (iv). `sysinfo`'s volume enumeration is a blocking
    /// call. Run inline on an async worker every 10 s, a slow enumeration
    /// stalls every other task on that worker, the RTMP ingest included.
    /// Here the runtime has ONE worker: a concurrent timer task must keep
    /// its schedule while a 1.5 s enumeration is running.
    #[tokio::test(flavor = "current_thread")]
    async fn slow_volume_enumeration_never_delays_other_tasks() {
        const SLOW: Duration = Duration::from_millis(1_500);
        const TICK: Duration = Duration::from_millis(20);
        let entered = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let probe: VolumeProbe = {
            let entered = Arc::clone(&entered);
            Arc::new(move |_: &Path| {
                entered.store(true, std::sync::atomic::Ordering::SeqCst);
                std::thread::sleep(SLOW);
                Some((1, 10))
            })
        };
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
        let monitor = tokio::spawn(run_disk_monitor_with(
            VolumeSampler::new(probe, Duration::from_millis(10)),
            PathBuf::from("/chunks"),
            None,
            None,
            None,
            shutdown_rx,
        ));

        let mut worst_late = Duration::ZERO;
        let watch_until = std::time::Instant::now() + SLOW + Duration::from_millis(500);
        while std::time::Instant::now() < watch_until {
            let asked = std::time::Instant::now();
            tokio::time::sleep(TICK).await;
            worst_late = worst_late.max(asked.elapsed().saturating_sub(TICK));
        }
        assert!(
            entered.load(std::sync::atomic::Ordering::SeqCst),
            "the monitor never sampled the volume"
        );
        assert!(
            worst_late < Duration::from_millis(500),
            "a timer task on the same runtime ran {worst_late:?} late while the volume \
             enumeration ran: the blocking call must not run on an async worker (#368)"
        );
        shutdown_tx.send(()).expect("the monitor is listening");
        tokio::time::timeout(Duration::from_secs(10), monitor)
            .await
            .expect("the monitor stops on shutdown")
            .expect("the monitor must not panic");
    }

    fn probe_returning(usage: Option<(u64, u64)>) -> VolumeProbe {
        Arc::new(move |_: &Path| usage)
    }

    #[tokio::test]
    async fn a_sample_is_the_probe_result() {
        let mut sampler = VolumeSampler::new(probe_returning(Some((3, 10))), SAMPLE_INTERVAL);
        assert_eq!(sampler.sample(Path::new("/chunks")).await, Some((3, 10)));
        let mut sampler = VolumeSampler::new(probe_returning(None), SAMPLE_INTERVAL);
        assert_eq!(sampler.sample(Path::new("/chunks")).await, None);
    }

    /// #368: a stuck enumeration costs one blocking thread, never one per
    /// tick. A sample that times out leaves it in flight, the next sample
    /// waits for that same enumeration, and its result is used once it ends.
    #[tokio::test]
    async fn a_slow_enumeration_is_waited_for_again_never_duplicated() {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        let calls = Arc::new(AtomicUsize::new(0));
        let released = Arc::new(AtomicBool::new(false));
        let probe: VolumeProbe = {
            let (calls, released) = (Arc::clone(&calls), Arc::clone(&released));
            Arc::new(move |path: &Path| {
                calls.fetch_add(1, Ordering::SeqCst);
                let deadline = std::time::Instant::now() + Duration::from_secs(10);
                while !released.load(Ordering::SeqCst) && std::time::Instant::now() < deadline {
                    std::thread::sleep(Duration::from_millis(2));
                }
                assert_eq!(path, Path::new("/chunks"));
                Some((7, 9))
            })
        };
        let mut sampler =
            VolumeSampler::new(probe, SAMPLE_INTERVAL).with_timeout(Duration::from_millis(50));
        let path = Path::new("/chunks");

        assert_eq!(sampler.sample(path).await, None, "timed out");
        assert_eq!(sampler.sample(path).await, None, "still running");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "the second sample waited for the running enumeration, it did not start another"
        );
        released.store(true, Ordering::SeqCst);
        // From here the enumeration ends at once; give it all the time a
        // loaded runner needs.
        sampler.timeout = Duration::from_secs(10);
        assert_eq!(
            sampler.sample(path).await,
            Some((7, 9)),
            "the slow enumeration's own result"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(sampler.sample(path).await, Some((7, 9)));
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "a finished one is not reused"
        );
    }

    #[tokio::test]
    async fn a_failed_enumeration_is_an_empty_sample() {
        let mut sampler = VolumeSampler::new(
            Arc::new(|_: &Path| -> Option<(u64, u64)> { panic!("sysinfo failed") }),
            SAMPLE_INTERVAL,
        );
        assert_eq!(sampler.sample(Path::new("/chunks")).await, None);
        assert_eq!(sampler.sample(Path::new("/chunks")).await, None);
    }

    /// The production entry point runs until shutdown, then returns.
    #[tokio::test]
    async fn run_disk_monitor_runs_until_shutdown() {
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
        let monitor = tokio::spawn(run_disk_monitor(
            std::env::temp_dir(),
            None,
            None,
            None,
            shutdown_rx,
        ));
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!monitor.is_finished(), "the monitor keeps sampling");
        shutdown_tx.send(()).expect("the monitor is listening");
        tokio::time::timeout(Duration::from_secs(10), monitor)
            .await
            .expect("the monitor stops on shutdown")
            .expect("the monitor must not panic");
    }
}
