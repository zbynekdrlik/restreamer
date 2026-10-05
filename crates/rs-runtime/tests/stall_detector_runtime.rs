//! #367 part 2 integration: the process-stall detector against a REAL tokio
//! runtime. A current_thread runtime whose only thread is blocked must be
//! reported as `runtime_starved` — in `stall.log` (written during the stall)
//! and as a `ProcessStall` audit row (emitted after recovery).
//!
//! Thresholds are scaled down so the test runs in ~2 s. The detector thread is
//! never blocked, so its ticks stay on time; `tick_late_threshold` is generous
//! (2 s on a 50 ms interval) so CI scheduler jitter can never flip the class.

use std::path::Path;
use std::time::{Duration, Instant};

use rs_core::audit::{Action, AuditRow, Severity, Source};
use rs_runtime::stall_detector::{StallDetectorConfig, spawn_stall_detector};
use serde_json::Value;
use tokio::sync::mpsc;

fn test_config(dir: &Path) -> StallDetectorConfig {
    StallDetectorConfig {
        probe_interval: Duration::from_millis(50),
        stall_threshold: Duration::from_millis(300),
        tick_late_threshold: Duration::from_secs(2),
        baseline_every_ticks: 2,
        log_path: dir.join("logs").join("stall.log"),
        log_max_bytes: 1_000_000,
    }
}

fn records(path: &Path) -> Vec<Value> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .map(|l| serde_json::from_str(l).expect("stall.log line is JSON"))
        .collect()
}

fn events(path: &Path) -> Vec<String> {
    records(path)
        .iter()
        .map(|r| r["event"].as_str().unwrap_or_default().to_string())
        .collect()
}

#[test]
fn blocked_current_thread_runtime_is_reported_as_runtime_starved() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let cfg = test_config(dir.path());
    let log_path = cfg.log_path.clone();
    let (audit_tx, mut audit_rx) = mpsc::channel::<AuditRow>(16);

    let mut guard = rt
        .block_on(async {
            spawn_stall_detector(tokio::runtime::Handle::current(), cfg, Some(audit_tx))
        })
        .expect("detector thread starts");

    // The stimulus: block the runtime's ONLY thread, so no probe can run.
    let blocked_for = Duration::from_millis(1_500);
    rt.block_on(async { std::thread::sleep(blocked_for) });

    // stall.log already holds the stall_start, written on the detector thread
    // WHILE the runtime was blocked.
    let during = events(&log_path);
    assert_eq!(during.first().map(String::as_str), Some("detector_started"));
    assert!(
        during.iter().any(|e| e == "stall_start"),
        "stall_start must be written during the stall, got {during:?}"
    );

    // Drive the runtime again: the probe runs, the detector closes the stall
    // and emits the audit row from its thread (via Handle::enter).
    let row = rt
        .block_on(async { tokio::time::timeout(Duration::from_secs(10), audit_rx.recv()).await })
        .expect("ProcessStall row within 10 s of recovery")
        .expect("audit channel open");
    guard.stop();

    assert_eq!(row.action, Action::ProcessStall);
    assert_eq!(row.severity, Severity::Warn);
    assert_eq!(row.source, Source::System);
    assert_eq!(row.detail["class"], "runtime_starved");
    assert_eq!(row.detail["trigger"], "probe_overdue");
    let duration_ms = row.detail["duration_ms"].as_u64().expect("duration_ms");
    assert!(
        duration_ms >= 1_400,
        "stall spans at least the {} ms block, got {duration_ms} ms",
        blocked_for.as_millis()
    );
    assert!(row.detail["resources_at_start"]["working_set_bytes"].as_u64() > Some(0));
    assert!(row.detail["resources_at_end"]["working_set_bytes"].as_u64() > Some(0));
    assert_eq!(
        row.detail["stall_log"],
        log_path.display().to_string(),
        "the row points at the evidence file"
    );
    assert_eq!(row.detail["stall_log_error"], Value::Null);

    let recs = records(&log_path);
    let start = recs
        .iter()
        .find(|r| r["event"] == "stall_start")
        .expect("stall_start");
    assert_eq!(start["trigger"], "probe_overdue");
    assert!(
        start["baseline"].is_object(),
        "start carries the pre-stall baseline"
    );
    let end = recs
        .iter()
        .find(|r| r["event"] == "stall_end")
        .expect("stall_end");
    assert_eq!(end["class"], "runtime_starved");
    assert_eq!(end["duration_ms"], duration_ms);
    assert_eq!(
        recs.iter().filter(|r| r["event"] == "stall_start").count(),
        1,
        "one block = one stall"
    );
}

#[test]
fn responsive_runtime_reports_no_stall() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let cfg = test_config(dir.path());
    let log_path = cfg.log_path.clone();
    let (audit_tx, mut audit_rx) = mpsc::channel::<AuditRow>(16);

    let mut guard = spawn_stall_detector(rt.handle().clone(), cfg, Some(audit_tx)).unwrap();
    // ~20 probe round trips (3x the stall threshold) on a runtime whose
    // workers are free the whole time.
    rt.block_on(async { tokio::time::sleep(Duration::from_millis(1_000)).await });
    guard.stop();

    assert!(audit_rx.try_recv().is_err(), "no ProcessStall row");
    assert_eq!(events(&log_path), vec!["detector_started".to_string()]);
}

#[test]
fn detector_exits_quietly_when_the_runtime_shuts_down() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let cfg = test_config(dir.path());
    let log_path = cfg.log_path.clone();

    let guard = spawn_stall_detector(rt.handle().clone(), cfg, None).unwrap();
    assert!(guard.is_running());
    rt.block_on(async { tokio::time::sleep(Duration::from_millis(200)).await });
    drop(rt); // shuts the runtime down while the guard (stop channel) is still alive

    // The next probe is cancelled by the closed runtime; the detector must
    // notice and exit on its own instead of reporting a never-ending stall.
    let deadline = Instant::now() + Duration::from_secs(10);
    while guard.is_running() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        !guard.is_running(),
        "detector thread exits after runtime shutdown"
    );
    assert_eq!(
        events(&log_path),
        vec!["detector_started".to_string()],
        "a runtime shutdown is never recorded as a stall"
    );
}

#[test]
fn dropping_the_guard_stops_the_detector_thread() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    // The detector owns the ONLY sender: the channel closes exactly when the
    // detector thread has exited and dropped its state.
    let (audit_tx, mut audit_rx) = mpsc::channel::<AuditRow>(1);
    let guard =
        spawn_stall_detector(rt.handle().clone(), test_config(dir.path()), Some(audit_tx)).unwrap();
    assert!(guard.is_running());
    drop(guard); // what ServiceCore does at shutdown — must not block

    let closed =
        rt.block_on(async { tokio::time::timeout(Duration::from_secs(10), audit_rx.recv()).await });
    assert!(
        matches!(closed, Ok(None)),
        "detector thread exited after its guard was dropped"
    );
}
