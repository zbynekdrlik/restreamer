//! Tier thresholds, the audit gate and the settings sanitiser (#368), on
//! explicit instants.

use super::*;

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

fn prod() -> StallDetectorConfig {
    StallDetectorConfig::production(Path::new("C:/ProgramData/Restreamer"))
}

#[test]
fn production_tiers_match_the_design() {
    let c = prod();
    assert_eq!(c.probe_interval, ms(100));
    assert_eq!(c.stall_threshold, ms(500), "stall.log from 500 ms");
    assert_eq!(c.audit_threshold, ms(700), "ProcessStall row from 700 ms");
    assert_eq!(c.severe_threshold, ms(5_000), "5 s stays the severe class");
    assert_eq!(c.audit_min_interval, ms(10_000));
    assert_eq!(c.tick_late_threshold, ms(250));
    assert_eq!(c.baseline_every_ticks, 100, "a baseline every 10 s");
}

#[test]
fn tiers_have_inclusive_lower_bounds() {
    let c = prod();
    assert_eq!(StallTier::of(ms(500), &c), StallTier::Minor);
    assert_eq!(StallTier::of(ms(699), &c), StallTier::Minor);
    assert_eq!(StallTier::of(ms(700), &c), StallTier::Major);
    assert_eq!(StallTier::of(ms(4_999), &c), StallTier::Major);
    assert_eq!(StallTier::of(ms(5_000), &c), StallTier::Severe);
    assert_eq!(StallTier::of(ms(35_700), &c), StallTier::Severe);
    assert_eq!(
        [StallTier::Minor, StallTier::Major, StallTier::Severe].map(StallTier::as_str),
        ["minor", "major", "severe"]
    );
}

#[test]
fn a_stall_below_the_audit_threshold_is_never_audited() {
    let mut gate = StallAuditGate::new(&prod());
    let t0 = Instant::now();
    assert_eq!(gate.on_stall_end(t0, ms(600)), StallVerdict::LogOnly);
    assert_eq!(gate.on_stall_end(t0, ms(699)), StallVerdict::LogOnly);
    assert_eq!(
        gate.on_stall_end(t0, ms(700)),
        StallVerdict::Audit { suppressed: None },
        "a minor stall never uses up the interval"
    );
}

#[test]
fn a_stall_storm_writes_one_row_and_counts_the_rest() {
    let mut gate = StallAuditGate::new(&prod());
    let t0 = Instant::now();
    assert_eq!(
        gate.on_stall_end(t0, ms(800)),
        StallVerdict::Audit { suppressed: None }
    );
    for (i, d) in [900, 1_500, 6_000].into_iter().enumerate() {
        let at = t0 + ms(1_000 * (i as u64 + 1));
        assert_eq!(gate.on_stall_end(at, ms(d)), StallVerdict::HeldBack);
    }
    assert_eq!(
        gate.on_stall_end(t0 + ms(3_500), ms(650)),
        StallVerdict::LogOnly
    );
    assert_eq!(gate.take_due(t0 + ms(9_999)), None);

    let held = gate.take_due(t0 + ms(10_000)).expect("aggregate due");
    assert_eq!((held.count, held.max, held.total), (3, 6_000, 8_400));
    assert_eq!(
        aggregate_detail(&held, Path::new("/x/logs/stall.log")),
        json!({
            "aggregate": true,
            "held_back": {"count": 3, "max": 6_000, "total": 8_400, "unit": "ms", "span_ms": 2_000},
            "stall_log": "/x/logs/stall.log",
        })
    );
}

#[test]
fn the_next_row_after_a_storm_carries_the_aggregate() {
    let mut gate = StallAuditGate::new(&prod());
    let t0 = Instant::now();
    gate.on_stall_end(t0, ms(800));
    gate.on_stall_end(t0 + ms(100), ms(750));
    let StallVerdict::Audit {
        suppressed: Some(held),
    } = gate.on_stall_end(t0 + ms(10_000), ms(900))
    else {
        panic!("the row after the interval carries what was held back");
    };
    assert_eq!((held.count, held.max), (1, 750));
}

#[test]
fn settings_become_the_detector_config() {
    let settings = StallDetectorSettings {
        probe_interval_ms: 50,
        record_threshold_ms: 400,
        audit_threshold_ms: 900,
        severe_threshold_ms: 3_000,
        tick_late_threshold_ms: 300,
        audit_min_interval_ms: 0,
    };
    let (c, warnings) = config_from_settings(Path::new("/data"), "ingest", &settings);
    assert!(warnings.is_empty(), "{warnings:?}");
    assert_eq!(
        c,
        StallDetectorConfig {
            probe_interval: ms(50),
            stall_threshold: ms(400),
            audit_threshold: ms(900),
            severe_threshold: ms(3_000),
            audit_min_interval: Duration::ZERO,
            tick_late_threshold: ms(300),
            baseline_every_ticks: 200,
            log_path: Path::new("/data").join("logs").join("stall-ingest.log"),
            log_max_bytes: STALL_LOG_MAX_BYTES,
        }
    );
}

#[test]
fn inconsistent_settings_are_adjusted_with_a_warning() {
    let settings = StallDetectorSettings {
        probe_interval_ms: 0,
        record_threshold_ms: 5,
        audit_threshold_ms: 1,
        severe_threshold_ms: 2,
        tick_late_threshold_ms: 0,
        audit_min_interval_ms: 10_000,
    };
    let (c, warnings) = config_from_settings(Path::new("/data"), "main", &settings);
    assert_eq!(c.probe_interval, MIN_PROBE_INTERVAL);
    assert_eq!(c.stall_threshold, MIN_PROBE_INTERVAL, "record >= probe");
    assert_eq!(c.audit_threshold, c.stall_threshold, "audit >= record");
    assert_eq!(c.severe_threshold, c.audit_threshold, "severe >= audit");
    assert_eq!(c.tick_late_threshold, MIN_TICK_LATE_THRESHOLD);
    assert_eq!(
        warnings,
        vec![
            "stall_detector.probe_interval_ms = 0 ms adjusted to 10 ms",
            "stall_detector.record_threshold_ms = 5 ms adjusted to 10 ms",
            "stall_detector.audit_threshold_ms = 1 ms adjusted to 10 ms",
            "stall_detector.severe_threshold_ms = 2 ms adjusted to 10 ms",
            "stall_detector.tick_late_threshold_ms = 0 ms adjusted to 20 ms",
        ]
    );

    let (slow, warnings) = config_from_settings(
        Path::new("/data"),
        "main",
        &StallDetectorSettings {
            probe_interval_ms: 1_001,
            ..StallDetectorSettings::default()
        },
    );
    assert_eq!(slow.probe_interval, MAX_PROBE_INTERVAL);
    assert_eq!(slow.stall_threshold, MAX_PROBE_INTERVAL);
    assert_eq!(warnings.len(), 3, "probe, record, audit: {warnings:?}");
    let (edge, warnings) = config_from_settings(
        Path::new("/data"),
        "main",
        &StallDetectorSettings {
            probe_interval_ms: 1_000,
            record_threshold_ms: 1_000,
            audit_threshold_ms: 1_000,
            severe_threshold_ms: 1_000,
            tick_late_threshold_ms: 20,
            ..StallDetectorSettings::default()
        },
    );
    assert!(
        warnings.is_empty(),
        "the bounds are inclusive: {warnings:?}"
    );
    assert_eq!(edge.probe_interval, ms(1_000));
}

#[test]
fn baseline_is_sampled_every_ten_seconds_of_ticks() {
    assert_eq!(baseline_every_ticks(ms(100)), 100);
    assert_eq!(baseline_every_ticks(ms(1_000)), 10);
    assert_eq!(baseline_every_ticks(ms(30)), 333);
    assert_eq!(
        baseline_every_ticks(Duration::from_secs(20)),
        1,
        "at least 1"
    );
    assert_eq!(
        baseline_every_ticks(Duration::ZERO),
        10_000,
        "no division by 0"
    );
}

#[test]
fn tier_fields_and_with_fields_extend_a_record() {
    let rec = with_fields(&json!({"event": "detector_started"}), &tier_fields(&prod()));
    assert_eq!(
        rec,
        json!({
            "event": "detector_started",
            "record_threshold_ms": 500,
            "audit_threshold_ms": 700,
            "severe_threshold_ms": 5_000,
            "audit_min_interval_ms": 10_000,
        })
    );
    assert_eq!(with_fields(&json!("x"), &json!({"a": 1})), json!("x"));
}

#[test]
fn take_pending_hands_over_the_aggregate_at_once() {
    let mut gate = StallAuditGate::new(&prod());
    let t0 = Instant::now();
    gate.on_stall_end(t0, ms(800));
    assert_eq!(
        gate.on_stall_end(t0 + ms(1), ms(900)),
        StallVerdict::HeldBack
    );
    let held = gate.take_pending().expect("held back");
    assert_eq!((held.count, held.max), (1, 900));
    assert_eq!(gate.take_pending(), None);
}
