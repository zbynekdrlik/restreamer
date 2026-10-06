//! The ingest gap metric (#368) on explicit instants and synthetic source
//! timestamps: nothing sleeps.

use super::*;

/// Source ts (ms) of frame `k` at `fps`, rounded like an RTMP publisher.
fn ts(k: u64, fps: f64) -> u32 {
    (k as f64 * 1_000.0 / fps).round() as u32
}

/// A tracker that has seen frames `0..=last` at `fps`.
fn warmed(fps: f64, last: u64) -> IngestGapTracker {
    let mut t = IngestGapTracker::default();
    for k in 0..=last {
        assert_eq!(t.on_video(ts(k, fps)), VideoStep::Normal, "frame {k}");
    }
    t
}

#[test]
fn a_jump_of_n_frames_counts_exactly_n_minus_1_dropped() {
    for fps in [25.0, 29.97, 30.0, 50.0, 59.94, 60.0] {
        for n in [2u64, 3, 5, 10, 30, 100, 150] {
            let mut t = warmed(fps, 120);
            let step = t.on_video(ts(120 + n, fps));
            let VideoStep::Jump(j) = step else {
                panic!("{fps} fps, a {n}-frame step is a jump, got {step:?}");
            };
            assert_eq!(j.dropped, n - 1, "{fps} fps, {n}-frame step: {j:?}");
            assert_eq!((j.from_ts, j.to_ts), (ts(120, fps), ts(120 + n, fps)));
            assert_eq!(j.delta_ms, ts(120 + n, fps) - ts(120, fps));

            // The stream goes on from the new position; normal again.
            assert_eq!(t.on_video(ts(121 + n, fps)), VideoStep::Normal);
        }
    }
}

#[test]
fn the_exact_count_holds_soon_after_the_warmup_too() {
    // 10 deltas accepted after the 8-delta warm-up: the window estimate.
    for n in [2u64, 4, 12] {
        let mut t = warmed(29.97, 18);
        let VideoStep::Jump(j) = t.on_video(ts(18 + n, 29.97)) else {
            panic!("jump");
        };
        assert_eq!(j.dropped, n - 1, "{n}");
    }
}

#[test]
fn normal_jitter_is_never_a_jump() {
    let mut t = warmed(29.97, 200);
    // A late frame then an early one (the deltas still telescope).
    assert_eq!(t.on_video(ts(201, 29.97) + 15), VideoStep::Normal);
    assert_eq!(t.on_video(ts(202, 29.97)), VideoStep::Normal);
    // A duplicate timestamp.
    assert_eq!(t.on_video(ts(202, 29.97)), VideoStep::Normal);
    for k in 203..400 {
        assert_eq!(t.on_video(ts(k, 29.97)), VideoStep::Normal, "frame {k}");
    }
}

#[test]
fn just_above_one_and_a_half_intervals_is_one_dropped_frame() {
    let mut t = warmed(30.0, 100);
    let interval = t.interval.estimate().unwrap();
    let last = ts(100, 30.0);
    let at_limit = last + (1.5 * interval).floor() as u32;
    assert_eq!(t.on_video(at_limit), VideoStep::Normal, "<= 1.5 intervals");
    let mut t = warmed(30.0, 100);
    let VideoStep::Jump(j) = t.on_video(last + (1.5 * interval).floor() as u32 + 1) else {
        panic!("> 1.5 intervals is a jump");
    };
    assert_eq!(j.dropped, 1);
}

#[test]
fn nothing_is_classified_during_the_warmup() {
    let mut t = IngestGapTracker::default();
    assert_eq!(t.on_video(0), VideoStep::Normal);
    for k in 1..WARMUP_DELTAS as u64 {
        assert_eq!(t.on_video((k * 33) as u32), VideoStep::Normal);
    }
    assert_eq!(
        t.interval_us(),
        0,
        "{} deltas: not measured yet",
        WARMUP_DELTAS - 1
    );
    // The 8th delta is a big step: still warm-up, accepted, not a jump.
    assert_eq!(t.on_video(2_000), VideoStep::Normal);
    // Now measured; the median keeps the one outlier out of the estimate.
    assert_eq!(t.interval_us(), 33_000);
}

#[test]
fn the_window_estimate_keeps_deltas_within_half_and_one_and_a_half_medians() {
    let mut i = FrameInterval::default();
    for d in [34, 34, 34, 34, 34, 34, 34, 17] {
        i.accept(d, false);
    }
    assert_eq!(
        i.estimate(),
        Some((34.0 * 7.0 + 17.0) / 8.0),
        "0.5 x median is in"
    );
    let mut i = FrameInterval::default();
    for d in [34, 34, 34, 34, 34, 34, 34, 16] {
        i.accept(d, false);
    }
    assert_eq!(i.estimate(), Some(34.0), "below 0.5 x median is out");
    let mut i = FrameInterval::default();
    for d in [34, 34, 34, 34, 34, 34, 34, 51] {
        i.accept(d, false);
    }
    assert_eq!(
        i.estimate(),
        Some((34.0 * 7.0 + 51.0) / 8.0),
        "1.5 x median is in"
    );
    let mut i = FrameInterval::default();
    for d in [34, 34, 34, 34, 34, 34, 34, 52] {
        i.accept(d, false);
    }
    assert_eq!(i.estimate(), Some(34.0), "above 1.5 x median is out");
}

#[test]
fn the_window_holds_the_last_16_deltas() {
    let mut i = FrameInterval::default();
    for _ in 0..WINDOW {
        i.accept(40, false);
    }
    for _ in 0..WINDOW {
        i.accept(20, false);
    }
    assert_eq!(i.estimate(), Some(20.0), "the 40s rolled out");
}

#[test]
fn the_cumulative_mean_takes_over_and_excludes_the_warmup() {
    let mut i = FrameInterval::default();
    for _ in 0..WARMUP_DELTAS {
        i.accept(100, false);
    }
    for _ in 0..CUMULATIVE_MIN - 1 {
        i.accept(33, true);
        i.accept(34, true);
    }
    // 62 counted deltas, alternating 33 / 34: the cumulative mean is 33.5,
    // and the warm-up 100s are not in it.
    assert_eq!(i.estimate(), Some(33.5));
    let mut j = FrameInterval::default();
    for _ in 0..WARMUP_DELTAS {
        j.accept(33, false);
    }
    for _ in 0..CUMULATIVE_MIN - 1 {
        j.accept(40, true);
    }
    assert_eq!(
        j.estimate(),
        Some(40.0),
        "31 counted: still the window mean"
    );
    j.accept(40, true);
    assert_eq!(j.estimate(), Some(40.0), "32 counted: the cumulative mean");
    j.accept(10, true);
    assert_eq!(j.estimate(), Some((40.0 * 32.0 + 10.0) / 33.0));
}

#[test]
fn backward_and_far_forward_steps_are_discontinuities() {
    let mut t = warmed(30.0, 100);
    let last = ts(100, 30.0);
    assert_eq!(
        t.on_video(last - 500),
        VideoStep::Discontinuity {
            from_ts: last,
            to_ts: last - 500
        }
    );
    assert_eq!(t.interval_us(), 0, "a new timeline is measured anew");

    let mut t = warmed(30.0, 100);
    assert_eq!(
        t.on_video(last + MAX_COUNTED_JUMP_MS + 1),
        VideoStep::Discontinuity {
            from_ts: last,
            to_ts: last + MAX_COUNTED_JUMP_MS + 1
        }
    );
    let mut t = warmed(30.0, 100);
    let VideoStep::Jump(j) = t.on_video(last + MAX_COUNTED_JUMP_MS) else {
        panic!("30 s exactly is still counted");
    };
    assert_eq!(j.dropped, 899);
}

#[test]
fn reset_forgets_the_stream() {
    let mut t = warmed(30.0, 100);
    let t0 = Instant::now();
    t.on_media(t0);
    t.reset();
    assert_eq!(t.interval_us(), 0);
    assert_eq!(
        t.on_media(t0 + Duration::from_secs(5)),
        None,
        "no previous frame"
    );
    assert_eq!(t.on_video(0), VideoStep::Normal, "no previous ts");
}

#[test]
fn an_arrival_gap_of_300_ms_or_more_counts() {
    let mut t = IngestGapTracker::default();
    let t0 = Instant::now();
    assert_eq!(t.on_media(t0), None, "the first frame has no gap");
    assert_eq!(t.on_media(t0 + Duration::from_millis(33)), None);
    assert_eq!(t.on_media(t0 + Duration::from_millis(332)), None, "299 ms");
    assert_eq!(
        t.on_media(t0 + Duration::from_millis(632)),
        Some(Duration::from_millis(300)),
        "300 ms is a gap"
    );
    assert_eq!(
        t.on_media(t0 + Duration::from_millis(2_632)),
        Some(Duration::from_secs(2))
    );
}

#[test]
fn dropped_frames_rounds_to_whole_frames() {
    assert_eq!(dropped_frames(67, 33.367), 1);
    assert_eq!(dropped_frames(100, 33.367), 2);
    assert_eq!(dropped_frames(334, 33.367), 9);
    assert_eq!(
        dropped_frames(50, 33.0),
        1,
        "1.52 intervals = 2 frames = 1 dropped"
    );
}

#[test]
fn the_monitor_counts_and_audits_both_kinds_then_throttles() {
    let counters = IngestGapCounters::default();
    let mut m = IngestGapMonitor::default();
    let t0 = Instant::now();
    let frame = |k: u64| t0 + Duration::from_millis(k * 33);
    for k in 0..=100 {
        let (gaps, rows) = m.on_frame(
            frame(k),
            || 1_000 + k as i64,
            Some(ts(k, 30.0)),
            "live/obs",
            &counters,
        );
        assert_eq!(
            gaps,
            FrameGaps {
                arrival: None,
                video: VideoStep::Normal
            },
            "frame {k}"
        );
        assert!(rows.is_empty());
    }
    let interval_us = counters.snapshot().frame_interval_us;
    assert!((33_300..=33_350).contains(&interval_us), "{interval_us}");

    // OBS stalled: 400 ms of silence, then a 10-frame ts jump.
    let resumed = frame(100) + Duration::from_millis(400);
    let (gaps, rows) = m.on_frame(
        resumed,
        || 50_000,
        Some(ts(110, 30.0)),
        "live/obs",
        &counters,
    );
    assert_eq!(gaps.arrival, Some(Duration::from_millis(400)));
    assert!(matches!(gaps.video, VideoStep::Jump(j) if j.dropped == 9));
    assert_eq!(rows.len(), 2, "{rows:?}");
    assert_eq!(
        rows[0],
        json!({
            "kind": "arrival_gap", "gap_ms": 400, "threshold_ms": 300,
            "resumed_at_ms": 50_000, "stream_identifier": "live/obs",
            "held_back_before": null,
        })
    );
    let jump = &rows[1];
    assert_eq!(jump["kind"], "source_ts_jump");
    assert_eq!(
        (jump["from_ts"].clone(), jump["to_ts"].clone()),
        (json!(3_333), json!(3_667))
    );
    assert_eq!(jump["delta_ms"], 334);
    assert_eq!(jump["dropped_frames"], 9);
    assert_eq!(jump["at_ms"], 50_000);
    assert_eq!(jump["stream_identifier"], "live/obs");
    assert_eq!(jump["held_back_before"], Value::Null);
    let interval_ms = jump["frame_interval_ms"].as_f64().unwrap();
    assert!((33.3..33.35).contains(&interval_ms), "{interval_ms}");
    let snap = counters.snapshot();
    assert_eq!((snap.arrival_gaps, snap.last_arrival_gap_ms), (1, 400));
    assert_eq!((snap.source_ts_jumps, snap.dropped_frames), (1, 9));
    assert_eq!(
        (snap.last_arrival_gap_at_ms, snap.last_jump_at_ms),
        (50_000, 50_000)
    );

    // A second incident 1 s later is counted, but its rows are held back.
    let again = resumed + Duration::from_millis(1_000);
    let (_, rows) = m.on_frame(again, || 51_000, Some(ts(113, 30.0)), "live/obs", &counters);
    assert!(rows.is_empty(), "throttled: {rows:?}");
    let snap = counters.snapshot();
    assert_eq!(
        (snap.arrival_gaps, snap.source_ts_jumps, snap.dropped_frames),
        (2, 2, 11)
    );

    // Frames flow normally; the first frame 10 s after the first rows
    // flushes both aggregates, once.
    let due = resumed + AUDIT_MIN_INTERVAL;
    let mut flushed = Vec::new();
    for i in 1..=400u64 {
        let at = again + Duration::from_millis(33 * i);
        let (gaps, rows) = m.on_frame(
            at,
            || 51_000 + i as i64,
            Some(ts(113 + i, 30.0)),
            "live/obs",
            &counters,
        );
        assert_eq!(
            gaps,
            FrameGaps {
                arrival: None,
                video: VideoStep::Normal
            },
            "frame {i}"
        );
        if at < due {
            assert!(rows.is_empty(), "not due yet at frame {i}: {rows:?}");
        }
        flushed.extend(rows);
    }
    let kinds: Vec<_> = flushed
        .iter()
        .map(|r| {
            (
                r["kind"].clone(),
                r["aggregate"].clone(),
                r["held_back"]["count"].clone(),
            )
        })
        .collect();
    assert_eq!(
        kinds,
        vec![
            (json!("arrival_gap"), json!(true), json!(1)),
            (json!("source_ts_jump"), json!(true), json!(1)),
        ]
    );
    assert_eq!(flushed[0]["held_back"]["max"], 1_000, "ms");
    assert_eq!(flushed[1]["held_back"]["total"], 2, "frames");
    assert!(m.flush(None, "live/obs").is_empty(), "flushed once");
}

#[test]
fn the_end_of_a_stream_flushes_what_was_held_back() {
    let counters = IngestGapCounters::default();
    let mut m = IngestGapMonitor::default();
    let t0 = Instant::now();
    m.on_frame(t0, || 0, None, "s", &counters);
    m.on_frame(t0 + Duration::from_millis(500), || 0, None, "s", &counters);
    let (_, rows) = m.on_frame(
        t0 + Duration::from_millis(1_000),
        || 0,
        None,
        "s",
        &counters,
    );
    assert!(rows.is_empty());
    let rows = m.flush(None, "s");
    assert_eq!(
        rows,
        vec![json!({
            "kind": "arrival_gap", "aggregate": true, "stream_identifier": "s",
            "held_back": {"count": 1, "max": 500, "total": 500, "unit": "ms", "span_ms": 0},
        })]
    );
    assert_eq!(counters.snapshot().arrival_gaps, 2);
}

#[test]
fn reset_stream_keeps_the_throttles() {
    let counters = IngestGapCounters::default();
    let mut m = IngestGapMonitor::default();
    let t0 = Instant::now();
    m.on_frame(t0, || 0, None, "s", &counters);
    let (_, rows) = m.on_frame(t0 + Duration::from_millis(400), || 0, None, "s", &counters);
    assert_eq!(rows.len(), 1);
    m.reset_session();
    m.on_frame(t0 + Duration::from_millis(500), || 0, None, "s", &counters);
    let (gaps, rows) = m.on_frame(t0 + Duration::from_millis(900), || 0, None, "s", &counters);
    assert_eq!(gaps.arrival, Some(Duration::from_millis(400)));
    assert!(
        rows.is_empty(),
        "a resubscribe does not reopen the audit interval"
    );
}

#[test]
fn the_row_is_warn_inpoint_ingest_frame_gap() {
    let row = ingest_frame_gap_row(json!({"kind": "arrival_gap"}));
    assert_eq!(row.action, Action::IngestFrameGap);
    assert_eq!(row.severity, Severity::Warn);
    assert_eq!(row.source, Source::Inpoint);
    assert_eq!(row.detail["kind"], "arrival_gap");
}

#[test]
fn every_incident_gets_a_log_line() {
    let none = FrameGaps {
        arrival: None,
        video: VideoStep::Normal,
    };
    assert!(incident_messages(&none, "s").is_empty());
    let jump = SourceJump {
        from_ts: 100,
        to_ts: 500,
        delta_ms: 400,
        interval_ms: 33.333,
        dropped: 11,
    };
    let both = FrameGaps {
        arrival: Some(Duration::from_millis(533)),
        video: VideoStep::Jump(jump),
    };
    let lines = incident_messages(&both, "live/obs");
    assert_eq!(lines.len(), 2);
    assert!(
        lines[0].contains("no media frame for 533 ms on live/obs"),
        "{}",
        lines[0]
    );
    assert!(
        lines[1].contains("video ts 100 -> 500 (400 ms at 33.333 ms/frame)"),
        "{}",
        lines[1]
    );
    assert!(lines[1].contains("dropped 11 frame(s)"), "{}", lines[1]);
    let disc = FrameGaps {
        arrival: None,
        video: VideoStep::Discontinuity {
            from_ts: 9,
            to_ts: 1,
        },
    };
    let lines = incident_messages(&disc, "s");
    assert_eq!(lines.len(), 1);
    assert!(lines[0].contains("ts 9 -> 1"), "{}", lines[0]);
}

#[test]
fn record_rows_writes_one_ingest_frame_gap_row_per_detail() {
    let (tx, mut rx) = tokio::sync::mpsc::channel(4);
    record_rows(Some(&tx), vec![json!({"kind": "a"}), json!({"kind": "b"})]);
    record_rows(None, vec![json!({"kind": "c"})]);
    let first = rx.try_recv().unwrap();
    assert_eq!(
        (first.action, first.detail["kind"].clone()),
        (Action::IngestFrameGap, json!("a"))
    );
    assert_eq!(rx.try_recv().unwrap().detail["kind"], "b");
    assert!(rx.try_recv().is_err());
}

#[test]
fn exactly_one_and_a_half_intervals_is_not_a_jump() {
    // 8 deltas of 34 ms: the window estimate is exactly 34 ms, so a 51 ms
    // step is exactly 1.5 intervals: still normal (the bound is exclusive).
    let mut t = IngestGapTracker::default();
    for k in 0..=8u32 {
        assert_eq!(t.on_video(k * 34), VideoStep::Normal);
    }
    assert_eq!(t.interval_us(), 34_000);
    assert_eq!(t.on_video(8 * 34 + 51), VideoStep::Normal);
    let mut t = IngestGapTracker::default();
    for k in 0..=8u32 {
        t.on_video(k * 34);
    }
    assert!(matches!(t.on_video(8 * 34 + 52), VideoStep::Jump(j) if j.dropped == 1));
}

#[test]
fn the_next_row_after_the_interval_carries_what_was_held_back() {
    let counters = IngestGapCounters::default();
    let mut m = IngestGapMonitor::default();
    let t0 = Instant::now();
    let ms = Duration::from_millis;
    m.on_frame(t0, || 0, None, "s", &counters);
    let (_, rows) = m.on_frame(t0 + ms(400), || 1, None, "s", &counters);
    assert_eq!(rows[0]["held_back_before"], Value::Null);
    let (_, rows) = m.on_frame(t0 + ms(1_000), || 2, None, "s", &counters);
    assert!(rows.is_empty(), "held back");
    // The next gap ends exactly one interval after the first row.
    let (_, rows) = m.on_frame(
        t0 + ms(400) + AUDIT_MIN_INTERVAL,
        || 3,
        None,
        "s",
        &counters,
    );
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0]["kind"], "arrival_gap");
    assert_eq!(
        rows[0]["held_back_before"],
        json!({"count": 1, "max": 600, "total": 600, "unit": "ms", "span_ms": 0})
    );

    // Same for source-ts jumps (unit: frames).
    let mut m = IngestGapMonitor::default();
    let at = |k: u64| t0 + ms(33 * k);
    for k in 0..=100 {
        m.on_frame(at(k), || 0, Some(ts(k, 30.0)), "s", &counters);
    }
    let (_, rows) = m.on_frame(at(101), || 0, Some(ts(103, 30.0)), "s", &counters);
    assert_eq!(rows[0]["held_back_before"], Value::Null);
    let (_, rows) = m.on_frame(at(102), || 0, Some(ts(106, 30.0)), "s", &counters);
    assert!(rows.is_empty(), "held back");
    let later = at(101) + AUDIT_MIN_INTERVAL;
    let (_, rows) = m.on_frame(later, || 0, Some(ts(110, 30.0)), "s", &counters);
    let jump = rows
        .iter()
        .find(|r| r["kind"] == "source_ts_jump")
        .expect("the jump row");
    assert_eq!(jump["held_back_before"]["count"], 1);
    assert_eq!(jump["held_back_before"]["total"], 2, "frames 104 and 105");
    assert_eq!(jump["held_back_before"]["unit"], "frames");
}

#[test]
fn a_resubscription_keeps_the_arrival_clock_but_not_the_video_timeline() {
    let mut t = warmed(30.0, 100);
    let t0 = Instant::now();
    t.on_media(t0);
    t.reset_video();
    assert_eq!(t.interval_us(), 0, "the video timeline starts over");
    assert_eq!(
        t.on_media(t0 + Duration::from_secs(35)),
        Some(Duration::from_secs(35)),
        "the stall before the re-subscription is a gap"
    );
    assert_eq!(t.on_video(0), VideoStep::Normal, "no previous ts");
}
