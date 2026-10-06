//! #367 part 2 integration: the process-stall detector against a REAL tokio
//! runtime. A current_thread runtime whose only thread is blocked must be
//! reported as `runtime_starved` — in `stall.log` (written during the stall)
//! and as a `ProcessStall` audit row (emitted after recovery).
//!
//! Thresholds are scaled down so the tests run in ~3 s. The detector thread is
//! never blocked, so its ticks stay on time; `tick_late_threshold` is generous
//! (2 s on a 50 ms interval) so CI scheduler jitter can never flip the class.
//!
//! The threshold depends on what a test asserts (#367).
//!
//! A test that PROVES a stall blocks the runtime far longer than
//! `STALL_THRESHOLD`, so it uses that short threshold.
//!
//! A test that proves there is NO stall cannot use 300 ms. A loaded box can
//! starve a responsive process that long, and the detector is RIGHT to report
//! it. On dev2 under build load, with the 300 ms threshold:
//! - `responsive_runtime_reports_no_stall` failed 3 of 187 runs;
//! - `detector_exits_quietly_when_the_runtime_shuts_down` failed once.
//!
//! Those two tests use `QUIET_THRESHOLD` and still catch a real bug:
//! - The responsive test watches for longer than the threshold, so a probe
//!   that is never answered still trips it.
//! - A detector that misses the shutdown never exits, so the shutdown test's
//!   10 s exit wait fails.

use std::path::Path;
use std::time::{Duration, Instant};

use rs_core::audit::{Action, AuditRow, Severity, Source};
use rs_runtime::stall_detector::{StallDetectorConfig, StallDetectorGuard, spawn_stall_detector};
use serde_json::Value;
use tokio::sync::mpsc;

const BLOCKED_FOR: Duration = Duration::from_millis(1_500);
const PROBE_INTERVAL: Duration = Duration::from_millis(50);
/// Stall threshold for the tests that provoke a stall (`BLOCKED_FOR` is 5x it).
const STALL_THRESHOLD: Duration = Duration::from_millis(300);
/// Stall threshold for the tests that assert NO stall: well above the
/// 300 ms+ starvation seen on a loaded box, yet shorter than `RESPONSIVE_FOR`.
const QUIET_THRESHOLD: Duration = Duration::from_secs(2);
/// How long `responsive_runtime_reports_no_stall` watches the runtime.
const RESPONSIVE_FOR: Duration = Duration::from_millis(3_000);
// The responsive window must outlast the quiet threshold by a margin of
// detector ticks, or a never-answered probe would go unreported.
const _: () = assert!(
    RESPONSIVE_FOR.as_millis() >= QUIET_THRESHOLD.as_millis() + 10 * PROBE_INTERVAL.as_millis()
);

fn test_config(dir: &Path) -> StallDetectorConfig {
    StallDetectorConfig {
        probe_interval: PROBE_INTERVAL,
        stall_threshold: STALL_THRESHOLD,
        tick_late_threshold: Duration::from_secs(2),
        baseline_every_ticks: 2,
        log_path: dir.join("logs").join("stall.log"),
        log_max_bytes: 1_000_000,
    }
}

/// `test_config` for a test that asserts the ABSENCE of a stall.
fn quiet_config(dir: &Path) -> StallDetectorConfig {
    StallDetectorConfig {
        stall_threshold: QUIET_THRESHOLD,
        ..test_config(dir)
    }
}

/// Complete lines only: the detector may be appending while a test reads.
fn records(path: &Path) -> Vec<Value> {
    let text = std::fs::read_to_string(path).unwrap_or_default();
    let complete = match text.rfind('\n') {
        Some(end) => &text[..end],
        None => "",
    };
    complete
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

/// Bounded wait for a `stall.log` event written by the detector thread.
fn wait_for_event(path: &Path, event: &str) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !events(path).iter().any(|e| e == event) {
        assert!(
            Instant::now() < deadline,
            "no `{event}` in stall.log within 10 s, got {:?}",
            events(path)
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// Bounded wait until the detector's first probe task exists on `rt`.
///
/// The detector spawns that probe a moment AFTER `detector_started` is on
/// disk. On a loaded box that moment took 250 ms, so a block started on the
/// record alone could begin BEFORE the probe existed, and the stall measured
/// 1250 ms for a 1500 ms block (#367, dev2 under load). A current_thread
/// runtime is not driven here, so the probe stays alive and unanswered.
fn wait_for_first_probe(rt: &tokio::runtime::Runtime) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while rt.metrics().num_alive_tasks() == 0 {
        assert!(
            Instant::now() < deadline,
            "the detector spawned no probe within 10 s"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// Spawn the detector on a fresh current_thread runtime, wait until its first
/// probe is queued, then block the runtime's ONLY thread for `BLOCKED_FOR`
/// (the stimulus). A current_thread runtime is not driven between `block_on`
/// calls, so that probe stays unanswered from before the block until the
/// caller drives the runtime again. Returns the block's length, measured on
/// the same `Instant` clock the detector stamps the stall with.
fn spawn_and_block(
    dir: &Path,
    audit_tx: mpsc::Sender<AuditRow>,
) -> (tokio::runtime::Runtime, StallDetectorGuard, Duration) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let cfg = test_config(dir);
    let log_path = cfg.log_path.clone();
    let guard = rt
        .block_on(async {
            spawn_stall_detector(tokio::runtime::Handle::current(), cfg, Some(audit_tx))
        })
        .expect("detector thread starts");
    wait_for_event(&log_path, "detector_started");
    wait_for_first_probe(&rt);
    let block_start = Instant::now();
    rt.block_on(async { std::thread::sleep(BLOCKED_FOR) });
    (rt, guard, block_start.elapsed())
}

fn assert_runtime_starved_row(row: &AuditRow, log_path: &Path, blocked: Duration) {
    assert_eq!(row.action, Action::ProcessStall);
    assert_eq!(row.severity, Severity::Warn);
    assert_eq!(row.source, Source::System);
    assert_eq!(row.detail["class"], "runtime_starved");
    assert_eq!(row.detail["trigger"], "probe_overdue");
    let duration_ms = row.detail["duration_ms"].as_u64().expect("duration_ms");
    // The probe was queued before `block_start` and can only run after the
    // block ends, so the stall covers the whole measured block.
    assert!(
        duration_ms >= blocked.as_millis() as u64,
        "stall spans the {} ms block, got {duration_ms} ms",
        blocked.as_millis()
    );
    assert!(row.detail["baseline"]["resources"]["working_set_bytes"].as_u64() > Some(0));
    assert!(row.detail["resources_at_detect"]["working_set_bytes"].as_u64() > Some(0));
    assert!(row.detail["resources_at_end"]["working_set_bytes"].as_u64() > Some(0));
    assert_eq!(
        row.detail["stall_log"],
        log_path.display().to_string(),
        "the row points at the evidence file"
    );
    assert_eq!(row.detail["stall_log_error"], Value::Null);
}

#[test]
fn blocked_current_thread_runtime_is_reported_as_runtime_starved() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = test_config(dir.path()).log_path;
    let (audit_tx, mut audit_rx) = mpsc::channel::<AuditRow>(16);
    let (rt, mut guard, blocked) = spawn_and_block(dir.path(), audit_tx);

    // stall_start was written on the detector thread WHILE the runtime was blocked.
    let during = events(&log_path);
    assert_eq!(
        during,
        vec!["detector_started".to_string(), "stall_start".to_string()],
        "stall_start must be written during the stall"
    );

    // Drive the runtime again: the probe runs, the detector closes the stall
    // and emits the audit row from its thread (via Handle::enter).
    let row = rt
        .block_on(async { tokio::time::timeout(Duration::from_secs(10), audit_rx.recv()).await })
        .expect("ProcessStall row within 10 s of recovery")
        .expect("audit channel open");
    guard.stop();
    assert_runtime_starved_row(&row, &log_path, blocked);

    let recs = records(&log_path);
    let start = recs
        .iter()
        .find(|r| r["event"] == "stall_start")
        .expect("stall_start");
    assert_eq!(start["trigger"], "probe_overdue");
    assert!(start["baseline"]["resources"].is_object());
    let end = recs
        .iter()
        .find(|r| r["event"] == "stall_end")
        .expect("stall_end");
    assert_eq!(end["class"], "runtime_starved");
    assert_eq!(end["duration_ms"], row.detail["duration_ms"]);
    assert_eq!(
        events(&log_path),
        vec![
            "detector_started",
            "stall_start",
            "stall_end",
            "detector_stopped"
        ],
        "one block = one stall, then a clean stop"
    );
    assert_eq!(recs.last().unwrap()["reason"], "stop_requested");
}

/// `audit::record` tokio-spawns a retry for a Warn row on a FULL channel. From
/// the detector's OS thread that spawn only works under `Handle::enter`;
/// without it the detector thread would panic and stop recording for good.
#[test]
fn process_stall_row_survives_a_full_audit_channel() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = test_config(dir.path()).log_path;
    let (audit_tx, mut audit_rx) = mpsc::channel::<AuditRow>(1);
    let filler = AuditRow {
        severity: Severity::Info,
        source: Source::System,
        event_id: None,
        instance_id: None,
        endpoint: None,
        action: Action::RestreamerStarted,
        detail: serde_json::json!({}),
        ts_override: None,
    };
    audit_tx.try_send(filler).expect("prefill the only slot");
    let (rt, mut guard, blocked) = spawn_and_block(dir.path(), audit_tx);

    // Drive the runtime (without draining the channel) until the detector has
    // closed the stall, plus a grace period for its emit right after.
    rt.block_on(async {
        let deadline = Instant::now() + Duration::from_secs(10);
        while !events(&log_path).iter().any(|e| e == "stall_end") {
            assert!(Instant::now() < deadline, "no stall_end within 10 s");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    });
    let (first, second) = rt.block_on(async {
        let first = audit_rx.recv().await;
        let second = tokio::time::timeout(Duration::from_secs(10), audit_rx.recv()).await;
        (first, second)
    });
    assert_eq!(first.expect("filler").action, Action::RestreamerStarted);
    let row = second
        .expect("ProcessStall row delivered by the retry task")
        .expect("audit channel open");
    assert_runtime_starved_row(&row, &log_path, blocked);
    assert!(guard.is_running(), "the detector survived the full channel");
    guard.stop();
    assert_eq!(
        records(&log_path).last().unwrap()["reason"],
        "stop_requested"
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
    let cfg = quiet_config(dir.path());
    let log_path = cfg.log_path.clone();
    let (audit_tx, mut audit_rx) = mpsc::channel::<AuditRow>(16);

    let mut guard = spawn_stall_detector(rt.handle().clone(), cfg, Some(audit_tx)).unwrap();
    // ~60 probe round trips on a runtime whose workers are free the whole time.
    // The window outlasts the threshold (const-asserted at the top), so a probe
    // that is never answered would still be reported here.
    rt.block_on(async { tokio::time::sleep(RESPONSIVE_FOR).await });
    guard.stop();

    assert!(audit_rx.try_recv().is_err(), "no ProcessStall row");
    assert_eq!(
        events(&log_path),
        vec!["detector_started", "detector_stopped"]
    );
}

#[test]
fn detector_exits_quietly_when_the_runtime_shuts_down() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    // A detector that misses the shutdown never exits: the 10 s wait below
    // fails, and its unanswered probe is reported after QUIET_THRESHOLD.
    let cfg = quiet_config(dir.path());
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
        vec!["detector_started", "detector_stopped"],
        "a runtime shutdown is never recorded as a stall"
    );
    assert_eq!(
        records(&log_path).last().unwrap()["reason"],
        "runtime_shut_down"
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
