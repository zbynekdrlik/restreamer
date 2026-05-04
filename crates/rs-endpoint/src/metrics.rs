//! In-memory upload metrics for the /uploads/stats API.
//!
//! Tracks successes + failures + durations in a bounded ring buffer per
//! worker event. Computes chunks/s (1-minute window) and p50/p95 latency.

use std::sync::Mutex;
use std::time::{Duration, Instant};

const RING_CAPACITY: usize = 2048;

#[derive(Clone, Copy, Debug)]
pub struct UploadEvent {
    pub at: Instant,
    pub duration_ms: u32,
    pub success: bool,
}

pub struct UploadMetrics {
    inner: Mutex<Inner>,
}

struct Inner {
    ring: Vec<UploadEvent>,
    head: usize,
    filled: bool,
    in_flight: usize,
    adaptive_target: usize,
    /// Count of chunks that have hit `mark_upload_permanently_failed` in
    /// the last permanent-failure window (set externally by the API layer
    /// from `db::list_recent_uploads` so the dashboard strip can show a
    /// loud-red state distinct from transient retry bursts).
    permanent_recent: u32,
}

impl Default for UploadMetrics {
    fn default() -> Self {
        Self {
            inner: Mutex::new(Inner {
                ring: Vec::with_capacity(RING_CAPACITY),
                head: 0,
                filled: false,
                in_flight: 0,
                adaptive_target: 4,
                permanent_recent: 0,
            }),
        }
    }
}

impl UploadMetrics {
    pub fn record(&self, event: UploadEvent) {
        let mut g = self.inner.lock().unwrap();
        if g.ring.len() < RING_CAPACITY {
            g.ring.push(event);
        } else {
            let h = g.head;
            g.ring[h] = event;
            g.head = (g.head + 1) % RING_CAPACITY;
            g.filled = true;
        }
    }

    pub fn set_in_flight(&self, n: usize) {
        self.inner.lock().unwrap().in_flight = n;
    }

    pub fn set_adaptive_target(&self, n: usize) {
        self.inner.lock().unwrap().adaptive_target = n;
    }

    /// Update the count of permanent upload failures observed in the last
    /// 5-minute window. The classifier uses this to escalate the strip
    /// state from yellow ("transient burst, recovered") to red ("data
    /// loss in progress").
    pub fn set_permanent_recent(&self, n: u32) {
        self.inner.lock().unwrap().permanent_recent = n;
    }

    pub fn snapshot(&self, window: Duration) -> Snapshot {
        let g = self.inner.lock().unwrap();
        let cutoff = Instant::now().checked_sub(window);
        let events: Vec<UploadEvent> = g
            .ring
            .iter()
            .copied()
            .filter(|e| cutoff.map(|c| e.at >= c).unwrap_or(true))
            .collect();

        let total = events.len();
        let successes = events.iter().filter(|e| e.success).count();
        let failures = total - successes;
        let mut durations: Vec<u32> = events
            .iter()
            .filter(|e| e.success)
            .map(|e| e.duration_ms)
            .collect();
        durations.sort_unstable();

        let median_ms = percentile(&durations, 50);
        let p95_ms = percentile(&durations, 95);
        let chunks_per_sec = if window.as_secs() == 0 {
            0.0
        } else {
            successes as f64 / window.as_secs_f64()
        };
        let error_rate = if total == 0 {
            0.0
        } else {
            failures as f64 / total as f64
        };

        let state = classify_upload_state(
            successes as u32,
            failures as u32,
            g.permanent_recent,
            g.in_flight,
        );
        let render = render_strip_state(&state);

        Snapshot {
            chunks_per_sec,
            median_ms,
            p95_ms,
            error_rate,
            in_flight: g.in_flight,
            adaptive_target: g.adaptive_target,
            permanent_recent: g.permanent_recent,
            state,
            render,
        }
    }
}

fn percentile(sorted: &[u32], p: u32) -> u32 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as u64 * p as u64) / 100).min(sorted.len() as u64 - 1) as usize;
    sorted[idx]
}

#[derive(Debug, Clone, serde::Serialize, PartialEq)]
pub struct Snapshot {
    pub chunks_per_sec: f64,
    pub median_ms: u32,
    pub p95_ms: u32,
    pub error_rate: f64,
    pub in_flight: usize,
    pub adaptive_target: usize,
    pub permanent_recent: u32,
    pub state: StripState,
    /// Server-rendered (class, label, tooltip) for the dashboard upload
    /// strip. The leptos-ui consumes these directly instead of mapping
    /// `state` itself so the visual contract is centrally tested in
    /// `render_strip_state`.
    pub render: StateRender,
}

/// Five-state semantic classification for the dashboard upload strip.
///
/// Replaces the ambiguous "errors X%" badge that conflated cache-init
/// retry bursts (transient, all recover) with real S3 outages
/// (permanent, data loss). See issue #168.
///
/// Invariants:
/// - `permanent >= 1` always escalates to red (PermanentFailures or
///   Cascading), never yellow — even one lost chunk is data loss.
/// - `Cascading` requires BOTH `permanent >= 5` AND `failures >= 15` so
///   the loudest red state only fires on a genuine outage, not a
///   single bad bucket period.
#[derive(Debug, Clone, Copy, serde::Serialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StripState {
    /// No events or zero failures: green.
    Healthy,
    /// Some retries, none permanent: yellow "transient retries
    /// (recovered)". This is the cache-init phase look — operator
    /// should be reassured, not alarmed.
    TransientBurst { retried: u32 },
    /// In-flight retries elevated, no permanent yet: yellow "elevated
    /// retries in flight". Distinguished from TransientBurst by
    /// active backpressure.
    DegradedTransient { retrying_in_flight: u32 },
    /// At least one permanent failure: red "data loss".
    PermanentFailures { count: u32 },
    /// Cascading outage: permanent >= 5 AND failures >= 15: loudest red.
    Cascading { permanent: u32, failures: u32 },
}

/// Server-rendered strip visuals. The leptos-ui component renders
/// `class` as a CSS modifier (e.g. `upload-strip__state--ok`) and
/// `label` as the visible text. `tooltip` is shown on hover and
/// explains what the state means so the operator does not have to
/// guess. See `render_strip_state` for the mapping.
#[derive(Debug, Clone, serde::Serialize, PartialEq, Eq)]
pub struct StateRender {
    pub class: &'static str,
    pub label: String,
    pub tooltip: String,
}

/// Map a `StripState` to its dashboard rendering. Pure function — no
/// IO, no allocation beyond the returned strings — so the test suite
/// can pin the visual contract per state. Adding a new state here
/// requires updating the leptos-ui CSS and a corresponding test.
pub fn render_strip_state(state: &StripState) -> StateRender {
    match state {
        StripState::Healthy => StateRender {
            class: "ok",
            label: "Uploads OK".to_string(),
            tooltip: "S3 uploads steady, no failures in the last minute.".to_string(),
        },
        StripState::TransientBurst { retried } => StateRender {
            class: "burst",
            label: format!("{retried} transient retries (recovered)"),
            tooltip: format!(
                "{retried} chunk(s) needed retry but all eventually uploaded — \
                 typical during cache initialisation. No data lost."
            ),
        },
        StripState::DegradedTransient { retrying_in_flight } => StateRender {
            class: "degraded",
            label: format!("Elevated retries: {retrying_in_flight} in flight"),
            tooltip: format!(
                "{retrying_in_flight} retry attempts active. No permanent \
                 failures yet — watch for escalation."
            ),
        },
        StripState::PermanentFailures { count } => StateRender {
            class: "permanent",
            label: format!("{count} chunk(s) lost"),
            tooltip: format!(
                "{count} chunk(s) hit retry budget and were marked permanently \
                 failed in the last 5 min. Investigate S3 / network."
            ),
        },
        StripState::Cascading {
            permanent,
            failures,
        } => StateRender {
            class: "cascading",
            label: format!("Cascading: {permanent} lost, {failures} failures"),
            tooltip: format!(
                "{permanent} permanent + {failures} failures in window — \
                 active S3 outage or sustained cascade. Page on-call."
            ),
        },
    }
}

/// Pure classifier — see `StripState` invariants. Inputs are window
/// counts (`successes` + `failures` over the snapshot window),
/// `permanent` count from DB over the permanent-failure window, and
/// the live `in_flight` worker count.
pub fn classify_upload_state(
    successes: u32,
    failures: u32,
    permanent: u32,
    in_flight: usize,
) -> StripState {
    if permanent >= 5 && failures >= 15 {
        return StripState::Cascading {
            permanent,
            failures,
        };
    }
    if permanent >= 1 {
        return StripState::PermanentFailures { count: permanent };
    }
    if failures == 0 {
        // Empty window (successes==0) or pure-success window.
        let _ = successes;
        return StripState::Healthy;
    }
    // failures > 0, permanent == 0 → transient.
    // Distinguish two transient flavors: in-flight retrying ≥ 4 means
    // the queue is actively under strain (DegradedTransient), otherwise
    // it is a recovered burst (TransientBurst).
    if in_flight >= 4 {
        StripState::DegradedTransient {
            retrying_in_flight: in_flight as u32,
        }
    } else {
        StripState::TransientBurst { retried: failures }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_snapshot_is_zero() {
        let m = UploadMetrics::default();
        let s = m.snapshot(Duration::from_secs(60));
        assert_eq!(s.chunks_per_sec, 0.0);
        assert_eq!(s.median_ms, 0);
        assert_eq!(s.p95_ms, 0);
        assert_eq!(s.error_rate, 0.0);
    }

    #[test]
    fn percentile_of_empty_is_zero() {
        assert_eq!(percentile(&[], 50), 0);
    }

    #[test]
    fn percentile_is_monotonic() {
        let v: Vec<u32> = (0..100).collect();
        let median = percentile(&v, 50);
        let p95 = percentile(&v, 95);
        assert!(p95 > median);
    }

    #[test]
    fn snapshot_counts_successes_for_rate_and_error_rate_for_failures() {
        let m = UploadMetrics::default();
        let now = Instant::now();
        for _ in 0..4 {
            m.record(UploadEvent {
                at: now,
                duration_ms: 100,
                success: true,
            });
        }
        m.record(UploadEvent {
            at: now,
            duration_ms: 5000,
            success: false,
        });

        let s = m.snapshot(Duration::from_secs(60));
        assert_eq!(s.error_rate, 0.2, "1 of 5 failed");
        assert!(s.chunks_per_sec > 0.0, "at least one success is counted");
        assert_eq!(s.median_ms, 100, "median over successes only");
    }

    #[test]
    fn set_in_flight_and_target_are_reflected() {
        let m = UploadMetrics::default();
        m.set_in_flight(7);
        m.set_adaptive_target(16);
        let s = m.snapshot(Duration::from_secs(60));
        assert_eq!(s.in_flight, 7);
        assert_eq!(s.adaptive_target, 16);
    }

    // Issue #168: dashboard "errors X%" indistinguishable between transient
    // retry burst (cache init, all recover) and persistent S3 outage.
    // classify_upload_state collapses (succ, fail, permanent, in_flight) into
    // a 5-state semantic the operator can read at a glance.

    #[test]
    fn classify_healthy_when_no_events() {
        let s = classify_upload_state(0, 0, 0, 0);
        assert_eq!(s, StripState::Healthy);
    }

    #[test]
    fn classify_healthy_when_all_succeed_no_failures() {
        let s = classify_upload_state(30, 0, 0, 2);
        assert_eq!(s, StripState::Healthy);
    }

    #[test]
    fn classify_transient_burst_when_some_fail_none_permanent() {
        // 7 of 30 retried during cache init, all eventually recovered, no
        // permanent. Yellow "transient retries (recovered)", NOT red.
        let s = classify_upload_state(23, 7, 0, 2);
        assert_eq!(s, StripState::TransientBurst { retried: 7 });
    }

    #[test]
    fn classify_degraded_transient_when_in_flight_retrying() {
        // 5 in-flight while error_rate creeps up but no permanent yet.
        // Yellow "elevated retries in flight".
        let s = classify_upload_state(10, 5, 0, 5);
        assert_eq!(
            s,
            StripState::DegradedTransient {
                retrying_in_flight: 5
            }
        );
    }

    #[test]
    fn classify_permanent_failures_promotes_to_red() {
        // Even 1 permanent failure = data loss = red, not yellow.
        let s = classify_upload_state(28, 2, 1, 2);
        assert_eq!(s, StripState::PermanentFailures { count: 1 });
    }

    #[test]
    fn classify_cascading_when_many_permanent() {
        // Sustained outage: lots of failures AND multiple permanent.
        // Loudest red state, separate from PermanentFailures so UI can alarm.
        let s = classify_upload_state(15, 15, 5, 8);
        assert_eq!(
            s,
            StripState::Cascading {
                permanent: 5,
                failures: 15,
            }
        );
    }

    #[test]
    fn classify_permanent_threshold_is_one_not_two() {
        // Killer: any single permanent must escalate from TransientBurst.
        // Mutant that uses `permanent > 1` would survive without this.
        let burst = classify_upload_state(23, 7, 0, 0);
        let one_perm = classify_upload_state(23, 7, 1, 0);
        assert!(matches!(burst, StripState::TransientBurst { .. }));
        assert!(matches!(one_perm, StripState::PermanentFailures { .. }));
    }

    #[test]
    fn classify_cascading_threshold_is_strict() {
        // Boundary: exactly 5 permanent + 15 failures = Cascading;
        // 4 permanent + 15 failures = PermanentFailures (not yet cascading).
        // Encodes the spec line in the public docstring of the function.
        let cascading = classify_upload_state(15, 15, 5, 0);
        let just_permanent = classify_upload_state(15, 15, 4, 0);
        assert!(matches!(cascading, StripState::Cascading { .. }));
        assert!(matches!(
            just_permanent,
            StripState::PermanentFailures { .. }
        ));
    }

    #[test]
    fn snapshot_carries_state_field() {
        let m = UploadMetrics::default();
        let now = Instant::now();
        for _ in 0..4 {
            m.record(UploadEvent {
                at: now,
                duration_ms: 100,
                success: true,
            });
        }
        let s = m.snapshot(Duration::from_secs(60));
        assert_eq!(s.state, StripState::Healthy);
    }

    // --- render_strip_state ---
    //
    // Pure visual-mapping fn so the leptos-ui component can render the
    // strip via server-supplied (class, label, tooltip) instead of
    // re-implementing the same match arms client-side. Native tests
    // here cover the mapping; UI just consumes `Snapshot::render`.

    #[test]
    fn render_healthy_is_ok_class() {
        let r = render_strip_state(&StripState::Healthy);
        assert_eq!(r.class, "ok");
        assert_eq!(r.label, "Uploads OK");
        assert!(r.tooltip.contains("no failures"));
    }

    #[test]
    fn render_transient_burst_is_burst_class_yellow() {
        let r = render_strip_state(&StripState::TransientBurst { retried: 7 });
        assert_eq!(r.class, "burst");
        assert_eq!(r.label, "7 transient retries (recovered)");
        assert!(r.tooltip.contains("recovered"), "{}", r.tooltip);
    }

    #[test]
    fn render_degraded_transient_is_degraded_class() {
        let r = render_strip_state(&StripState::DegradedTransient {
            retrying_in_flight: 5,
        });
        assert_eq!(r.class, "degraded");
        assert!(r.label.contains("5"), "{}", r.label);
        assert!(r.label.contains("in flight"), "{}", r.label);
    }

    #[test]
    fn render_permanent_failures_is_permanent_class_red() {
        let r = render_strip_state(&StripState::PermanentFailures { count: 2 });
        assert_eq!(r.class, "permanent");
        assert!(r.label.contains("2"), "{}", r.label);
        assert!(r.label.contains("lost"), "{}", r.label);
    }

    #[test]
    fn render_cascading_is_cascading_class_alarm() {
        let r = render_strip_state(&StripState::Cascading {
            permanent: 5,
            failures: 15,
        });
        assert_eq!(r.class, "cascading");
        assert!(r.label.contains("5"), "{}", r.label);
        assert!(r.label.contains("15"), "{}", r.label);
        assert!(
            r.tooltip.contains("outage") || r.tooltip.contains("cascade"),
            "{}",
            r.tooltip
        );
    }

    #[test]
    fn render_classes_are_distinct_for_each_state() {
        // Mutant killer: ensures no two states produce the same class
        // (a "return Healthy always" mutant collapses the strip and
        // would survive without this assertion).
        let classes = [
            render_strip_state(&StripState::Healthy).class,
            render_strip_state(&StripState::TransientBurst { retried: 1 }).class,
            render_strip_state(&StripState::DegradedTransient {
                retrying_in_flight: 1,
            })
            .class,
            render_strip_state(&StripState::PermanentFailures { count: 1 }).class,
            render_strip_state(&StripState::Cascading {
                permanent: 5,
                failures: 15,
            })
            .class,
        ];
        let mut seen = std::collections::HashSet::new();
        for c in classes {
            assert!(seen.insert(c), "duplicate class {c}");
        }
    }
}
