//! Publishing the chunker's A/V guards to the operator surface.
//!
//! Two guards watch the chunker's output at every chunk boundary:
//! - the baseline-relative ingest skew monitor (#354, `ingest_skew`), which
//!   sees a desync that APPEARS mid-stream (source-side drift);
//! - the absolute A/V invariant guard (#367, `rs_rtmp_push::AvInvariantGuard`),
//!   which sees ANY offset the chunker itself puts between the tracks,
//!   including one present from the first chunk.
//!
//! Both feed the SAME #354 surface: the ingest banner (`InpointState`
//! skew value + latch, which also gates `Start Delivering`), and audit rows
//! that the outage notifier turns into Discord alerts. The banner is raised
//! while EITHER guard is latched. Everything here runs OUTSIDE the
//! chunker's inner lock.

use rs_core::audit::{Action, AuditRow, Severity, Source};
use rs_core::models::InpointState;
use rs_rtmp_push::{AV_INVARIANT_TOLERANCE_MS, AvInvariantEvent, AvInvariantGuard};

use crate::ingest_skew::{IngestSkewMonitor, SkewEvent, SkewTransition};

/// What one chunk boundary left to publish.
#[derive(Debug, Default)]
pub(crate) struct BoundaryReport {
    /// Skew-monitor evaluation of the closing chunk (#354).
    pub(crate) skew: Option<SkewTransition>,
    /// Invariant-guard edge at this boundary (#367).
    pub(crate) invariant: Option<AvInvariantEvent>,
    /// Banner latch after this boundary: either guard latched.
    pub(crate) banner_active: bool,
    /// Banner value: the invariant delta while it is violated, else the
    /// monitor's baseline-relative skew.
    pub(crate) banner_skew_ms: i64,
}

impl BoundaryReport {
    /// Snapshot the banner state from both guards (call under the lock,
    /// right after evaluating them).
    pub(crate) fn capture(
        monitor: &IngestSkewMonitor,
        guard: &AvInvariantGuard,
        skew: Option<SkewTransition>,
        invariant: Option<AvInvariantEvent>,
    ) -> Self {
        let latched_delta = guard.latched_delta_ms();
        Self {
            skew,
            invariant,
            banner_active: monitor.is_active() || guard.is_violated(),
            banner_skew_ms: latched_delta.unwrap_or_else(|| monitor.skew_ms()),
        }
    }

    /// Whether anything at this boundary needs publishing.
    pub(crate) fn is_empty(&self) -> bool {
        self.skew.is_none() && self.invariant.is_none()
    }
}

/// Publish a boundary: banner value + latch, plus ONE audit row per guard
/// edge. Logs the invariant edge even when no ingest state is wired.
pub(crate) fn publish_boundary(state: Option<&InpointState>, threshold_ms: i64, b: BoundaryReport) {
    if b.is_empty() {
        return;
    }
    if let Some(ev) = b.invariant {
        log_invariant_edge(&ev);
    }
    let Some(state) = state else {
        return;
    };
    // Value BEFORE the latch: a reader that sees the latch also sees a value
    // at least as fresh (see `InpointState::set_ingest_skew_ms`).
    state.set_ingest_skew_ms(b.banner_skew_ms);
    state.set_ingest_skew_active(b.banner_active);
    if let Some(t) = b.skew {
        audit_skew_edge(state, threshold_ms, t);
    }
    if let Some(ev) = b.invariant {
        audit_invariant_edge(state, &ev);
    }
}

/// Publish a session re-anchor: both guards were reset, so the banner
/// clears; a latched invariant violation closes with one Restored row.
pub(crate) fn publish_reanchor(state: Option<&InpointState>, invariant: Option<AvInvariantEvent>) {
    if let Some(ev) = invariant {
        log_invariant_edge(&ev);
    }
    let Some(state) = state else {
        return;
    };
    state.set_ingest_skew_active(false);
    state.set_ingest_skew_ms(0);
    if let Some(ev) = invariant {
        audit_invariant_edge(state, &ev);
    }
}

fn log_invariant_edge(ev: &AvInvariantEvent) {
    match ev {
        AvInvariantEvent::Violated(v) => tracing::warn!(
            stage = "ingest",
            a_rel_ms = v.a_rel_ms,
            v_rel_ms = v.v_rel_ms,
            delta_ms = v.delta_ms,
            tolerance_ms = AV_INVARIANT_TOLERANCE_MS,
            "flv_chunker: A/V INVARIANT VIOLATED -- the chunk A/V relation differs from the \
             publisher's source relation (#367)"
        ),
        AvInvariantEvent::Restored { delta_ms } => tracing::info!(
            stage = "ingest",
            delta_ms,
            "flv_chunker: A/V invariant restored (#367)"
        ),
    }
}

/// One audit row per skew-monitor edge. Uses the paired
/// `IngestSkewDetected`/`IngestSkewRecovered` actions (not one action plus
/// `detail.state`) so `notify::OutageNotifier`'s Onset/Recovery
/// `classify()` alerts on them like `HostInternetUnreachable` (#354).
fn audit_skew_edge(state: &InpointState, threshold_ms: i64, t: SkewTransition) {
    let (severity, action, state_str) = match t.event {
        Some(SkewEvent::Detected) => (Severity::Warn, Action::IngestSkewDetected, "detected"),
        Some(SkewEvent::Cleared) => (Severity::Info, Action::IngestSkewRecovered, "recovered"),
        None => return,
    };
    audit(
        state,
        severity,
        action,
        serde_json::json!({
            "skew_ms": t.skew_ms,
            "threshold_ms": threshold_ms,
            "state": state_str,
        }),
    );
}

fn audit_invariant_edge(state: &InpointState, ev: &AvInvariantEvent) {
    let (severity, action, detail) = match ev {
        AvInvariantEvent::Violated(v) => (
            Severity::Warn,
            Action::AvInvariantViolated,
            serde_json::json!({
                "stage": "ingest",
                "a_rel_ms": v.a_rel_ms,
                "v_rel_ms": v.v_rel_ms,
                "delta_ms": v.delta_ms,
                "tolerance_ms": AV_INVARIANT_TOLERANCE_MS,
            }),
        ),
        AvInvariantEvent::Restored { delta_ms } => (
            Severity::Info,
            Action::AvInvariantRestored,
            serde_json::json!({ "stage": "ingest", "delta_ms": delta_ms }),
        ),
    };
    audit(state, severity, action, detail);
}

fn audit(state: &InpointState, severity: Severity, action: Action, detail: serde_json::Value) {
    if let Some(tx) = state.audit_tx() {
        rs_core::audit::record(
            tx,
            AuditRow {
                severity,
                source: Source::Inpoint,
                event_id: None,
                instance_id: None,
                endpoint: None,
                action,
                detail,
                ts_override: None,
            },
        );
    }
}
