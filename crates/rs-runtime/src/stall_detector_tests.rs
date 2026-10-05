//! Unit tests for the #367 process-stall detector. The tracker is driven with
//! explicit instants (the injected clock): every scenario is constructed
//! instant by instant, nothing sleeps.

use super::*;
use std::time::{Duration, Instant};

const S: Duration = Duration::from_secs(1);

fn ms_(n: u64) -> Duration {
    Duration::from_millis(n)
}

fn cfg() -> StallDetectorConfig {
    StallDetectorConfig::production(Path::new("/nonexistent-test-dir"))
}

/// Drives a `StallTracker` the way the detector thread does, on a synthetic
/// clock. `tick(sleep)` = the detector waits `sleep` (1 s when on time);
/// `answer_probe(after)` = the runtime runs the in-flight probe `after` its
/// send time (until then the probe stays unanswered).
struct Sim {
    tracker: StallTracker,
    now: Instant,
    in_flight: Option<(u64, Instant)>,
    latest_ack: Option<(u64, Instant)>,
}

impl Sim {
    fn new() -> Self {
        let mut tracker = StallTracker::new(&cfg());
        let now = Instant::now();
        let seq = tracker.start(now);
        Self {
            tracker,
            now,
            in_flight: Some((seq, now)),
            latest_ack: None,
        }
    }

    /// The runtime runs the in-flight probe `after` its send time.
    fn answer_probe(&mut self, after: Duration) {
        let (seq, sent) = self.in_flight.expect("a probe is in flight");
        self.latest_ack = Some((seq, sent + after));
    }

    fn tick(&mut self, sleep: Duration) -> TickOutcome {
        let tick_start = self.now;
        self.now += sleep;
        let out = self.tracker.observe(TickObservation {
            tick_start,
            now: self.now,
            latest_ack: self.latest_ack,
        });
        if let Some(seq) = out.send_probe {
            self.in_flight = Some((seq, self.now));
        }
        out
    }

    /// A healthy tick: the in-flight probe answers within 1 ms.
    fn healthy_tick(&mut self) -> TickOutcome {
        self.answer_probe(ms_(1));
        self.tick(S)
    }
}

#[test]
fn healthy_runtime_never_reports_and_keeps_one_probe_in_flight() {
    let mut sim = Sim::new();
    for i in 0..120 {
        let out = sim.healthy_tick();
        assert_eq!(out.started, None, "tick {i}");
        assert_eq!(out.ended, None, "tick {i}");
        assert!(
            out.send_probe.is_some(),
            "an answered probe is replaced (tick {i})"
        );
    }
    assert!(!sim.tracker.in_stall());
}

#[test]
fn runtime_starved_stall_opens_at_threshold_and_closes_on_answer() {
    let mut sim = Sim::new();
    sim.healthy_tick();
    let (_, probe_sent) = sim.in_flight.unwrap();

    // Runtime stops polling; the detector keeps ticking on time.
    for i in 1..5 {
        let out = sim.tick(S);
        assert_eq!(
            out.started, None,
            "{i}s unanswered is below the 5 s threshold"
        );
        assert_eq!(
            out.send_probe, None,
            "no second probe while one is unanswered"
        );
    }
    let out = sim.tick(S);
    let start = out.started.expect("5 s unanswered = stall");
    assert_eq!(start.trigger, StallTrigger::ProbeOverdue);
    assert_eq!(
        start.started_at, probe_sent,
        "stall starts when the unanswered probe was sent"
    );
    assert_eq!(start.probe_age, Some(5 * S));
    assert_eq!(out.ended, None);
    assert!(sim.tracker.in_stall());

    // Still starved for a while, then the runtime runs the probe at +30 s.
    for _ in 0..24 {
        let out = sim.tick(S);
        assert_eq!((out.started, out.ended, out.send_probe), (None, None, None));
    }
    sim.answer_probe(30 * S);
    let out = sim.tick(S);
    let report = out.ended.expect("answered probe ends the stall");
    assert_eq!(report.class, StallClass::RuntimeStarved);
    assert_eq!(report.trigger, StallTrigger::ProbeOverdue);
    assert_eq!(report.duration, 30 * S);
    assert_eq!(report.ended_at, probe_sent + 30 * S);
    assert_eq!(report.detector_max_late, Duration::ZERO);
    assert!(out.send_probe.is_some(), "probing resumes after recovery");
    assert!(!sim.tracker.in_stall());
}

#[test]
fn probe_answer_just_below_threshold_is_not_a_stall() {
    let mut sim = Sim::new();
    sim.healthy_tick();
    for _ in 0..4 {
        assert_eq!(sim.tick(S).started, None);
    }
    sim.answer_probe(ms_(4_999));
    let out = sim.tick(S);
    assert_eq!((out.started, out.ended), (None, None));
    assert!(out.send_probe.is_some());
}

/// The 2026-10-01 shape: the whole process (detector included) frozen 35 s
/// while a probe was in flight. The detector wakes late and the runtime has
/// already answered — it only sees the slow answer.
#[test]
fn whole_process_freeze_with_probe_in_flight_reports_on_wake() {
    let mut sim = Sim::new();
    sim.healthy_tick();
    let (_, probe_sent) = sim.in_flight.unwrap();

    sim.answer_probe(ms_(35_700));
    let out = sim.tick(ms_(36_000));
    let start = out.started.expect("slow answer = stall");
    assert_eq!(start.trigger, StallTrigger::ProbeSlow);
    assert_eq!(start.started_at, probe_sent);
    assert_eq!(start.detector_late, ms_(35_000));
    let report = out
        .ended
        .expect("already answered, so it also ends on this tick");
    assert_eq!(report.class, StallClass::WholeProcess);
    assert_eq!(report.duration, ms_(35_700));
    assert_eq!(report.detector_max_late, ms_(35_000));
    assert_eq!(report.detector_total_late, ms_(35_000));
}

/// Same freeze, but the detector wakes before the runtime has run the probe.
#[test]
fn whole_process_freeze_probe_still_unanswered_at_wake() {
    let mut sim = Sim::new();
    sim.healthy_tick();
    let (_, probe_sent) = sim.in_flight.unwrap();

    let out = sim.tick(ms_(36_000));
    let start = out.started.expect("probe 36 s unanswered");
    assert_eq!(start.trigger, StallTrigger::ProbeOverdue);
    assert_eq!(out.ended, None);

    sim.answer_probe(ms_(36_010));
    let report = sim.tick(S).ended.expect("answer ends it");
    assert_eq!(
        report.class,
        StallClass::WholeProcess,
        "detector overshot 35 s"
    );
    assert_eq!(report.started_at, probe_sent);
    assert_eq!(report.duration, ms_(36_010));
}

/// A freeze that begins after the last probe was answered: no probe is in
/// flight during it, so only the detector's own late wake-up reveals it, and a
/// FRESH probe must prove the runtime is back.
#[test]
fn whole_process_freeze_between_probes_detected_by_detector_late() {
    let mut sim = Sim::new();
    sim.healthy_tick();
    sim.answer_probe(ms_(1)); // answered right away, before the freeze
    let tick_start = sim.now;

    let out = sim.tick(ms_(36_000));
    let start = out.started.expect("detector silent 36 s = stall");
    assert_eq!(start.trigger, StallTrigger::DetectorLate);
    assert_eq!(
        start.started_at, tick_start,
        "last instant the detector was known running"
    );
    assert_eq!(start.probe_age, None);
    assert_eq!(start.detector_late, ms_(35_000));
    assert_eq!(out.ended, None, "the pre-freeze answer must not close it");
    let fresh = out.send_probe.expect("fresh probe after the freeze");

    sim.answer_probe(ms_(2));
    let out = sim.tick(S);
    let report = out.ended.expect("fresh probe answered");
    assert_eq!(report.class, StallClass::WholeProcess);
    assert_eq!(report.trigger, StallTrigger::DetectorLate);
    assert_eq!(report.duration, ms_(36_002));
    assert!(out.send_probe.is_some_and(|s| s > fresh));
}

#[test]
fn detector_silent_below_threshold_alone_is_not_a_stall() {
    let mut sim = Sim::new();
    sim.answer_probe(ms_(1));
    let out = sim.tick(ms_(4_999));
    assert_eq!((out.started, out.ended), (None, None));
}

/// The no-sign-of-life rule counts the detector's WHOLE silence, not just its
/// overshoot beyond the 1 s wait: 5 s silent is a stall, whatever the phase.
#[test]
fn detector_silent_for_exactly_the_threshold_is_a_stall() {
    let mut sim = Sim::new();
    sim.answer_probe(ms_(1));
    let out = sim.tick(ms_(5_000));
    let start = out.started.expect("5 s of detector silence");
    assert_eq!(start.trigger, StallTrigger::DetectorLate);
    assert_eq!(start.detector_late, ms_(4_000));
    sim.answer_probe(ms_(1));
    let report = sim.tick(S).ended.expect("fresh probe answered");
    assert_eq!(report.class, StallClass::WholeProcess);
    assert_eq!(report.duration, ms_(5_001));
}

/// `start()` must register the first probe as in flight: a runtime that never
/// runs it is a stall that began at the detector's start.
#[test]
fn the_start_probe_is_in_flight_and_can_reveal_a_stall() {
    let mut tracker = StallTracker::new(&cfg());
    let t0 = Instant::now();
    assert_eq!(tracker.start(t0), 1);
    let mut now = t0;
    for i in 1..=5 {
        let tick_start = now;
        now += S;
        let out = tracker.observe(TickObservation {
            tick_start,
            now,
            latest_ack: None,
        });
        assert_eq!(
            out.send_probe, None,
            "start probe still in flight (tick {i})"
        );
        if i < 5 {
            assert_eq!(out.started, None);
        } else {
            let start = out.started.expect("start probe unanswered for 5 s");
            assert_eq!(start.trigger, StallTrigger::ProbeOverdue);
            assert_eq!(start.started_at, t0);
        }
    }
    let out = tracker.observe(TickObservation {
        tick_start: now,
        now: now + S,
        latest_ack: Some((1, t0 + ms_(5_500))),
    });
    assert_eq!(out.ended.expect("answered").duration, ms_(5_500));
    assert_eq!(out.send_probe, Some(2));
}

#[test]
fn runtime_starved_with_scheduler_jitter_stays_runtime_starved() {
    let mut sim = Sim::new();
    sim.healthy_tick();
    for _ in 0..10 {
        sim.tick(ms_(1_400)); // 400 ms late each tick: busy box, not frozen
    }
    sim.answer_probe(ms_(14_000));
    let report = sim.tick(S).ended.expect("ends");
    assert_eq!(report.class, StallClass::RuntimeStarved);
    assert_eq!(report.detector_max_late, ms_(400));
    assert_eq!(
        report.detector_total_late,
        ms_(400 * 7),
        "only ticks inside the stall count"
    );
}

#[test]
fn one_tick_late_by_exactly_the_threshold_classifies_whole_process() {
    let mut sim = Sim::new();
    sim.healthy_tick();
    for _ in 0..5 {
        sim.tick(S);
    }
    sim.tick(ms_(2_000)); // exactly TICK_LATE_THRESHOLD late, inside the stall
    sim.answer_probe(ms_(8_000));
    let report = sim.tick(S).ended.expect("ends");
    assert_eq!(report.class, StallClass::WholeProcess);
    assert_eq!(report.detector_max_late, TICK_LATE_THRESHOLD);
}

#[test]
fn stall_after_a_stall_is_reported_again() {
    let mut sim = Sim::new();
    for round in 0..2 {
        sim.healthy_tick();
        for _ in 0..5 {
            sim.tick(S);
        }
        sim.answer_probe(ms_(5_500));
        let report = sim.tick(S).ended.expect("each stall is reported");
        assert_eq!(report.duration, ms_(5_500), "round {round}");
    }
}

#[test]
fn production_config_matches_the_design() {
    let c = StallDetectorConfig::production(Path::new("C:/ProgramData/Restreamer"));
    assert_eq!(c.probe_interval, Duration::from_secs(1));
    assert_eq!(c.stall_threshold, Duration::from_secs(5));
    assert_eq!(
        c.log_path,
        Path::new("C:/ProgramData/Restreamer")
            .join("logs")
            .join("stall.log")
    );
    assert!(c.log_max_bytes > 0);
}

#[test]
fn process_stall_audit_row_is_warn_system() {
    let row = process_stall_audit_row(serde_json::json!({"class": "whole_process"}));
    assert_eq!(row.action, Action::ProcessStall);
    assert_eq!(row.severity, Severity::Warn);
    assert_eq!(row.source, Source::System);
    assert_eq!(row.detail["class"], "whole_process");
    assert!(row.endpoint.is_none() && row.event_id.is_none());
}

// ---- stall.log writer + record shapes -------------------------------------

fn read_lines(path: &Path) -> Vec<serde_json::Value> {
    std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).expect("every line is JSON"))
        .collect()
}

#[test]
fn stall_log_creates_its_directory_and_appends_json_lines() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("logs").join("stall.log");
    let log = StallLog::new(path.clone(), 1_000_000);
    log.append(&serde_json::json!({"event": "a"})).unwrap();
    log.append(&serde_json::json!({"event": "b"})).unwrap();
    let lines = read_lines(&path);
    assert_eq!(lines.len(), 2);
    assert_eq!(lines[0]["event"], "a");
    assert_eq!(lines[1]["event"], "b");
}

#[test]
fn stall_log_rotates_to_old_once_the_file_reaches_the_cap() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("stall.log");
    // `{"event":"first"}` + newline is exactly 18 bytes: the file sits AT the
    // cap after one append, which must already trigger the rotation.
    let log = StallLog::new(path.clone(), 18);
    let first = log.append(&serde_json::json!({"event": "first"})).unwrap();
    assert_eq!(first, None);
    assert_eq!(std::fs::metadata(&path).unwrap().len(), 18);
    let rotated = log.append(&serde_json::json!({"event": "b"})).unwrap();
    assert_eq!(rotated, None, "a successful rotation is not a warning");
    log.append(&serde_json::json!({"event": "c"})).unwrap();

    assert_eq!(log.rotated_path(), dir.path().join("stall.log.old"));
    let old = read_lines(&log.rotated_path());
    assert_eq!(old.len(), 1, "the at-cap generation moved aside");
    assert_eq!(old[0]["event"], "first");
    let current = read_lines(&path);
    assert_eq!(current.len(), 2, "below the cap, appends continue in place");
    assert_eq!(current[0]["event"], "b");
    assert_eq!(current[1]["event"], "c");
}

/// Another process holding `stall.log.old` must not cost the evidence record.
#[test]
fn stall_log_failed_rotation_still_appends_and_warns() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("stall.log");
    let log = StallLog::new(path.clone(), 18);
    // A non-empty DIRECTORY at the rotation target makes the rename fail on
    // every platform.
    std::fs::create_dir(log.rotated_path()).unwrap();
    std::fs::write(log.rotated_path().join("keep"), b"x").unwrap();

    assert_eq!(
        log.append(&serde_json::json!({"event": "first"})).unwrap(),
        None
    );
    let warning = log
        .append(&serde_json::json!({"event": "second"}))
        .unwrap()
        .expect("the failed rotation is reported");
    assert!(warning.contains("rotation to"), "{warning}");
    let lines = read_lines(&path);
    assert_eq!(lines.len(), 2, "the record went to the un-rotated file");
    assert_eq!(lines[1]["event"], "second");
}

#[test]
fn stall_log_append_error_is_returned_not_panicked() {
    let dir = tempfile::tempdir().unwrap();
    // The parent "directory" is a regular file, so create_dir_all must fail.
    let blocker = dir.path().join("blocker");
    std::fs::write(&blocker, b"x").unwrap();
    let log = StallLog::new(blocker.join("stall.log"), 1_000);
    assert!(log.append(&serde_json::json!({})).is_err());
}

fn wall(rfc3339: &str) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::parse_from_rfc3339(rfc3339)
        .unwrap()
        .with_timezone(&chrono::Utc)
}

#[test]
fn wall_anchor_converts_instants_before_and_after_now() {
    let now = Instant::now();
    let at = WallAnchor {
        now,
        wall: wall("2026-10-01T15:09:00.000Z"),
    };
    assert_eq!(at.wall_of(now), "2026-10-01T15:09:00.000Z");
    assert_eq!(at.wall_of(now - ms_(1_250)), "2026-10-01T15:08:58.750Z");
    // A probe may run just after the detector read its clock.
    assert_eq!(at.wall_of(now + ms_(250)), "2026-10-01T15:09:00.250Z");
}

#[test]
fn class_and_trigger_names_are_the_documented_strings() {
    assert_eq!(StallClass::RuntimeStarved.as_str(), "runtime_starved");
    assert_eq!(StallClass::WholeProcess.as_str(), "whole_process");
    assert_eq!(StallTrigger::ProbeOverdue.as_str(), "probe_overdue");
    assert_eq!(StallTrigger::ProbeSlow.as_str(), "probe_slow");
    assert_eq!(StallTrigger::DetectorLate.as_str(), "detector_late");
}

#[test]
fn baseline_is_due_every_nth_healthy_tick() {
    let mut every3 = BaselineSchedule::new(3);
    let due: Vec<bool> = (0..7).map(|_| every3.tick()).collect();
    assert_eq!(due, [false, false, true, false, false, true, false]);
    let mut every0 = BaselineSchedule::new(0);
    assert!(every0.tick() && every0.tick(), "0 is clamped to every tick");
}

#[test]
fn baseline_is_never_refreshed_inside_a_stall() {
    let mut every2 = BaselineSchedule::new(2);
    assert!(!every2.due(false), "1st healthy tick");
    // A stall opens: the baseline must stay the PRE-stall reading, and stall
    // ticks do not count towards the next refresh either.
    for _ in 0..5 {
        assert!(!every2.due(true));
    }
    assert!(every2.due(false), "2nd healthy tick");
    assert!(!every2.due(false));
    assert!(every2.due(false));
}

#[test]
fn loop_exit_reasons_and_panic_messages_are_readable() {
    assert_eq!(LoopExit::StopRequested.as_str(), "stop_requested");
    assert_eq!(LoopExit::RuntimeShutDown.as_str(), "runtime_shut_down");
    let p = std::panic::catch_unwind(|| panic!("boom {}", 7)).unwrap_err();
    assert_eq!(panic_message(p.as_ref()), "boom 7");
    let p = std::panic::catch_unwind(|| panic!("static")).unwrap_err();
    assert_eq!(panic_message(p.as_ref()), "static");
    let p = std::panic::catch_unwind(|| std::panic::panic_any(42u8)).unwrap_err();
    assert_eq!(panic_message(p.as_ref()), "non-string panic payload");
}

#[test]
fn raw_windows_counters_map_to_bytes() {
    let mut s = ResourceSnapshot::default();
    s.set_process_counters(resources::ProcessCounters {
        working_set: 1_000,
        private_usage: 2_000,
        page_faults: 30,
        paged_pool_quota: 400,
        nonpaged_pool_quota: 50,
    });
    s.set_performance(resources::PerformancePages {
        commit_total: 10,
        commit_limit: 20,
        physical_total: 30,
        physical_available: 3,
        kernel_paged: 7,
        kernel_nonpaged: 5,
        page_size: 4_096,
        handle_count: 440_000,
        process_count: 250,
        thread_count: 4_000,
    });
    assert_eq!(s.working_set_bytes, Some(1_000));
    assert_eq!(s.private_bytes, Some(2_000));
    assert_eq!(s.page_fault_count, Some(30));
    assert_eq!(s.process_paged_pool_bytes, Some(400));
    assert_eq!(s.process_nonpaged_pool_bytes, Some(50));
    // PERFORMANCE_INFORMATION memory figures are PAGES.
    assert_eq!(s.commit_total_bytes, Some(40_960));
    assert_eq!(s.commit_limit_bytes, Some(81_920));
    assert_eq!(s.physical_total_bytes, Some(122_880));
    assert_eq!(s.physical_available_bytes, Some(12_288));
    assert_eq!(s.kernel_paged_pool_bytes, Some(28_672));
    assert_eq!(s.kernel_nonpaged_pool_bytes, Some(20_480));
    assert_eq!(s.system_handle_count, Some(440_000));
    assert_eq!(s.system_process_count, Some(250));
    assert_eq!(s.system_thread_count, Some(4_000));
    assert!(s.errors.is_empty());
}

#[test]
fn start_for_service_outside_a_runtime_starts_nothing() {
    let (tx, _rx) = mpsc::channel::<AuditRow>(1);
    let dir = tempfile::tempdir().unwrap();
    assert!(start_for_service(dir.path(), tx).is_none());
    assert!(!dir.path().join("logs").exists());
}

#[test]
fn start_for_service_runs_the_production_detector_under_data_dir() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let (tx, _rx) = mpsc::channel::<AuditRow>(1);
    let mut guard = rt
        .block_on(async { start_for_service(dir.path(), tx) })
        .expect("started inside a runtime");
    assert!(guard.is_running());

    // The detector writes its `detector_started` record from its own thread;
    // wait (bounded) for that event rather than for a fixed time.
    let path = dir.path().join("logs").join("stall.log");
    let deadline = Instant::now() + Duration::from_secs(10);
    while !path.exists() && Instant::now() < deadline {
        std::thread::sleep(ms_(10));
    }
    guard.stop();
    assert!(!guard.is_running(), "stop() joins the thread");

    let records = read_lines(&path);
    let last = records.last().expect("records");
    assert_eq!(last["event"], "detector_stopped");
    assert_eq!(last["reason"], "stop_requested");
    let first = &records[0];
    assert_eq!(first["event"], "detector_started");
    assert_eq!(first["probe_interval_ms"], 1_000);
    assert_eq!(first["stall_threshold_ms"], 5_000);
    assert_eq!(first["tick_late_threshold_ms"], 1_000);
    assert_eq!(first["version"], env!("CARGO_PKG_VERSION"));
    assert!(first["resources"]["working_set_bytes"].as_u64() > Some(0));
}

fn sample_snapshot() -> ResourceSnapshot {
    ResourceSnapshot {
        working_set_bytes: Some(512),
        handle_count: Some(440_000),
        kernel_nonpaged_pool_bytes: Some(504 << 20),
        errors: vec!["GetPerformanceInfo: denied".into()],
        ..Default::default()
    }
}

#[test]
fn records_carry_wall_times_classification_and_resources() {
    let now = Instant::now();
    let at = WallAnchor {
        now,
        wall: wall("2026-10-01T15:09:00.180Z"),
    };
    let start = StallStart {
        started_at: now - ms_(35_700),
        detected_at: now - ms_(10),
        trigger: StallTrigger::ProbeSlow,
        probe_age: Some(ms_(35_700)),
        detector_late: ms_(35_000),
    };
    let snap = sample_snapshot();
    let baseline = ResourceSnapshot {
        handle_count: Some(1_000),
        ..Default::default()
    };
    let detected = DetectedStall {
        start: start.clone(),
        at_detect: snap.clone(),
        baseline: Some((baseline, ms_(9_000))),
    };

    let rec = stall_log::stall_start_record(&detected, at);
    assert_eq!(rec["event"], "stall_start");
    assert_eq!(rec["started_at"], "2026-10-01T15:08:24.480Z");
    assert_eq!(rec["detected_at"], "2026-10-01T15:09:00.170Z");
    assert_eq!(rec["trigger"], "probe_slow");
    assert_eq!(rec["probe_age_ms"], 35_700);
    assert_eq!(rec["detector_late_ms"], 35_000);
    assert_eq!(rec["resources"]["handle_count"], 440_000);
    assert_eq!(rec["resources"]["kernel_nonpaged_pool_bytes"], 504u64 << 20);
    assert_eq!(rec["resources"]["private_bytes"], serde_json::Value::Null);
    assert_eq!(rec["resources"]["errors"][0], "GetPerformanceInfo: denied");
    assert_eq!(rec["baseline"]["age_ms"], 9_000);
    assert_eq!(rec["baseline"]["resources"]["handle_count"], 1_000);
    assert_eq!(rec["pid"], std::process::id());

    let no_baseline = DetectedStall {
        baseline: None,
        ..detected.clone()
    };
    assert_eq!(
        stall_log::stall_start_record(&no_baseline, at)["baseline"],
        serde_json::Value::Null
    );
    let stopped = stall_log::stopped_record("runtime_shut_down", at);
    assert_eq!(stopped["event"], "detector_stopped");
    assert_eq!(stopped["reason"], "runtime_shut_down");
    assert_eq!(stopped["ts"], "2026-10-01T15:09:00.180Z");

    let report = StallReport {
        started_at: start.started_at,
        ended_at: now - ms_(5),
        duration: ms_(35_695),
        class: StallClass::WholeProcess,
        trigger: StallTrigger::ProbeSlow,
        detector_max_late: ms_(35_000),
        detector_total_late: ms_(35_000),
    };
    let end = stall_log::stall_end_record(&report, &snap, at);
    assert_eq!(end["event"], "stall_end");
    assert_eq!(end["class"], "whole_process");
    assert_eq!(end["duration_ms"], 35_695);
    assert_eq!(end["ended_at"], "2026-10-01T15:09:00.175Z");

    let detail = stall_log::audit_detail(
        &report,
        Some(&detected),
        &snap,
        Path::new("/x/logs/stall.log"),
        Some("disk full".into()),
        at,
    );
    assert_eq!(detail["class"], "whole_process");
    assert_eq!(detail["detector_max_late_ms"], 35_000);
    assert_eq!(detail["probe_age_at_detect_ms"], 35_700);
    assert_eq!(detail["baseline"]["resources"]["handle_count"], 1_000);
    assert_eq!(detail["baseline"]["age_ms"], 9_000);
    assert_eq!(detail["resources_at_detect"]["handle_count"], 440_000);
    assert_eq!(detail["resources_at_end"]["working_set_bytes"], 512);
    assert_eq!(detail["stall_log"], "/x/logs/stall.log");
    assert_eq!(detail["stall_log_error"], "disk full");

    let unknown_start = stall_log::audit_detail(
        &report,
        None,
        &snap,
        Path::new("/x/logs/stall.log"),
        None,
        at,
    );
    assert_eq!(
        unknown_start["resources_at_detect"],
        serde_json::Value::Null
    );
    assert_eq!(unknown_start["baseline"], serde_json::Value::Null);
    assert_eq!(unknown_start["stall_log_error"], serde_json::Value::Null);
}

#[test]
fn resource_sample_reads_real_process_and_system_memory() {
    let s = resources::sample();
    assert!(s.errors.is_empty(), "sampling errors: {:?}", s.errors);
    assert!(s.working_set_bytes.is_some_and(|b| b > 0));
    let total = s.physical_total_bytes.expect("physical total");
    let avail = s.physical_available_bytes.expect("physical available");
    assert!(total > 0 && avail <= total);
    #[cfg(windows)]
    {
        assert!(s.handle_count.is_some_and(|h| h > 0));
        assert!(s.system_handle_count.is_some_and(|h| h > 0));
        assert!(s.kernel_nonpaged_pool_bytes.is_some_and(|b| b > 0));
        assert!(s.private_bytes.is_some_and(|b| b > 0));
    }
}
