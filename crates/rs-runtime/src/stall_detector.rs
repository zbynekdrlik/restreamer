//! #367 part 2 — process-stall detector.
//!
//! On 2026-10-01 the whole Restreamer process froze for 35.7 s (17:08:24.5 to
//! 17:09:00.2 CEST). Every task went silent: the heartbeat, the S3 uploader,
//! the RTMP accept loop and the logging itself. OBS's RTMP send timed out, and
//! the re-subscribe that followed produced the 25-minute YouTube A/V desync.
//! `restreamer.log` showed only a HOLE. Nothing could say whether the tokio
//! runtime starved or the OS stopped running the process, nor what memory,
//! handle or kernel-pool state the box was in.
//!
//! This detector is a dedicated `std::thread`, NOT a tokio task: a task cannot
//! observe the runtime it is starving with. Every `probe_interval` (1 s) it
//! 1. measures its own wait. A late wake-up means the OS did not run this
//!    process's thread (a whole-process / OS stall);
//! 2. checks the round trip of a tiny probe task spawned onto the runtime via
//!    `Handle::spawn`. An unanswered probe means the runtime is not polling.
//!
//! A probe unanswered for `stall_threshold` (5 s), or a detector wake-up that
//! late, is a stall. It is classified `runtime_starved` (the detector kept
//! ticking on time) or `whole_process` (its own wait overshot by at least
//! `tick_late_threshold`).
//!
//! Evidence goes to `<data_dir>/logs/stall.log` ON THE THREAD (plain `std::fs`,
//! no async, no `log` crate): a resource snapshot when the stall is detected
//! and when it ends, plus the last healthy baseline. Only after the runtime
//! answers again does it emit `log::warn!` and a `ProcessStall` audit row.
//!
//! The decision logic is the pure [`StallTracker`]: explicit instants in,
//! events out — the injected clock the unit tests drive. The thread loop only
//! feeds it real `Instant`s and performs the I/O.

use std::path::{Path, PathBuf};
use std::sync::mpsc as std_mpsc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rs_core::audit::{Action, AuditRow, Severity, Source};
use serde_json::Value;
use tokio::runtime::Handle;
use tokio::sync::mpsc;

#[path = "stall_resources.rs"]
pub mod resources;
#[path = "stall_log.rs"]
mod stall_log;

use resources::ResourceSnapshot;
pub use stall_log::{StallLog, WallAnchor};

/// How often the detector wakes, and sends a runtime probe when none is in flight.
pub const PROBE_INTERVAL: Duration = Duration::from_secs(1);
/// An unanswered probe (or a detector wake-up this late) is a stall.
pub const STALL_THRESHOLD: Duration = Duration::from_secs(5);
/// A detector wake-up at least this much later than its `PROBE_INTERVAL` wait
/// during a stall means the OS was not running the process: `whole_process`.
pub const TICK_LATE_THRESHOLD: Duration = Duration::from_secs(1);
/// Healthy ticks between two baseline resource samples.
pub const BASELINE_EVERY_TICKS: u32 = 10;
/// `stall.log` is rotated to `stall.log.old` once it reaches this size.
pub const STALL_LOG_MAX_BYTES: u64 = 1_000_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StallDetectorConfig {
    pub probe_interval: Duration,
    pub stall_threshold: Duration,
    pub tick_late_threshold: Duration,
    pub baseline_every_ticks: u32,
    pub log_path: PathBuf,
    pub log_max_bytes: u64,
}

impl StallDetectorConfig {
    /// The production thresholds, logging to `<data_dir>/logs/stall.log`
    /// (`C:\ProgramData\Restreamer\logs\stall.log` on stream.lan).
    pub fn production(data_dir: &Path) -> Self {
        Self {
            probe_interval: PROBE_INTERVAL,
            stall_threshold: STALL_THRESHOLD,
            tick_late_threshold: TICK_LATE_THRESHOLD,
            baseline_every_ticks: BASELINE_EVERY_TICKS,
            log_path: data_dir.join("logs").join("stall.log"),
            log_max_bytes: STALL_LOG_MAX_BYTES,
        }
    }
}

/// What kind of stall it was.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StallClass {
    /// The detector thread kept ticking on time; only the tokio runtime stopped
    /// polling (a blocking call on a worker, a lock held across a wait, …).
    RuntimeStarved,
    /// The detector thread itself was not run: the OS stopped scheduling the
    /// process (memory pressure / paging, suspension, kernel resource exhaustion).
    WholeProcess,
}

impl StallClass {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RuntimeStarved => "runtime_starved",
            Self::WholeProcess => "whole_process",
        }
    }
}

/// Which observation detected the stall.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StallTrigger {
    /// The in-flight probe has gone unanswered for `stall_threshold`.
    ProbeOverdue,
    /// A probe WAS answered, but its round trip took `stall_threshold` or more:
    /// the detector was frozen too, so it only saw the late answer.
    ProbeSlow,
    /// The detector's own wait overshot by `stall_threshold` with no probe in
    /// flight. The probe usually answers in microseconds, so a freeze that
    /// starts between probes is visible ONLY this way.
    DetectorLate,
}

impl StallTrigger {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ProbeOverdue => "probe_overdue",
            Self::ProbeSlow => "probe_slow",
            Self::DetectorLate => "detector_late",
        }
    }
}

/// One detector tick's measurements: the tracker's only input.
#[derive(Debug, Clone, Copy)]
pub struct TickObservation {
    /// When the detector began its `probe_interval` wait.
    pub tick_start: Instant,
    /// When it woke up.
    pub now: Instant,
    /// The newest probe the runtime answered: (sequence, when the probe ran).
    pub latest_ack: Option<(u64, Instant)>,
}

/// A stall was detected (it may still be ongoing).
#[derive(Debug, Clone, PartialEq)]
pub struct StallStart {
    /// Best estimate of when the process stopped responding.
    pub started_at: Instant,
    pub detected_at: Instant,
    pub trigger: StallTrigger,
    /// Age of the probe that revealed it, if a probe did.
    pub probe_age: Option<Duration>,
    /// How late the detector's own wake-up was on the detecting tick.
    pub detector_late: Duration,
}

/// A finished stall: the runtime answered a probe again.
#[derive(Debug, Clone, PartialEq)]
pub struct StallReport {
    pub started_at: Instant,
    /// When the first post-stall probe ran on the runtime.
    pub ended_at: Instant,
    pub duration: Duration,
    pub class: StallClass,
    pub trigger: StallTrigger,
    /// Worst single detector wake-up overshoot inside the stall.
    pub detector_max_late: Duration,
    /// Sum of detector overshoots inside the stall.
    pub detector_total_late: Duration,
}

/// What the loop must do after a tick.
#[derive(Debug, Default, PartialEq)]
pub struct TickOutcome {
    pub started: Option<StallStart>,
    pub ended: Option<StallReport>,
    /// Spawn a probe with this sequence number now.
    pub send_probe: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
struct Probe {
    seq: u64,
    sent_at: Instant,
}

#[derive(Debug, Clone, Copy)]
struct OpenStall {
    started_at: Instant,
    trigger: StallTrigger,
    max_late: Duration,
    total_late: Duration,
}

/// Pure stall state machine. Exactly one probe is in flight at a time, so a
/// starved runtime never accumulates a pile of probe tasks.
#[derive(Debug)]
pub struct StallTracker {
    probe_interval: Duration,
    stall_threshold: Duration,
    tick_late_threshold: Duration,
    next_seq: u64,
    outstanding: Option<Probe>,
    open: Option<OpenStall>,
}

impl StallTracker {
    pub fn new(config: &StallDetectorConfig) -> Self {
        Self {
            probe_interval: config.probe_interval,
            stall_threshold: config.stall_threshold,
            tick_late_threshold: config.tick_late_threshold,
            next_seq: 0,
            outstanding: None,
            open: None,
        }
    }

    /// Issue the first probe; returns its sequence number.
    pub fn start(&mut self, now: Instant) -> u64 {
        self.issue_probe(now)
    }

    /// Is a stall currently open?
    pub fn in_stall(&self) -> bool {
        self.open.is_some()
    }

    fn issue_probe(&mut self, now: Instant) -> u64 {
        self.next_seq += 1;
        self.outstanding = Some(Probe {
            seq: self.next_seq,
            sent_at: now,
        });
        self.next_seq
    }

    pub fn observe(&mut self, obs: TickObservation) -> TickOutcome {
        let mut out = TickOutcome::default();
        let late = obs
            .now
            .saturating_duration_since(obs.tick_start)
            .saturating_sub(self.probe_interval);

        // 1. Did the runtime answer the in-flight probe?
        let completed = match (self.outstanding, obs.latest_ack) {
            (Some(p), Some((seq, at))) if seq >= p.seq => Some((p, at)),
            _ => None,
        };
        if completed.is_some() {
            self.outstanding = None;
        }

        // 2. Open a stall, or account this tick's overshoot to the open one.
        if let Some(open) = self.open.as_mut() {
            open.max_late = open.max_late.max(late);
            open.total_late += late;
        } else if let Some((started_at, trigger, probe_age)) = self.detect(&obs, completed, late) {
            self.open = Some(OpenStall {
                started_at,
                trigger,
                max_late: late,
                total_late: late,
            });
            out.started = Some(StallStart {
                started_at,
                detected_at: obs.now,
                trigger,
                probe_age,
                detector_late: late,
            });
        }

        // 3. Close it once a probe sent at/after the stall began is answered.
        // (A `DetectorLate` stall starts AFTER the last probe was answered, so
        // it needs a fresh probe to prove the runtime is responsive again.)
        if let (Some(open), Some((probe, acked_at))) = (self.open, completed) {
            if probe.sent_at >= open.started_at {
                let class = if open.max_late >= self.tick_late_threshold {
                    StallClass::WholeProcess
                } else {
                    StallClass::RuntimeStarved
                };
                out.ended = Some(StallReport {
                    started_at: open.started_at,
                    ended_at: acked_at,
                    duration: acked_at.saturating_duration_since(open.started_at),
                    class,
                    trigger: open.trigger,
                    detector_max_late: open.max_late,
                    detector_total_late: open.total_late,
                });
                self.open = None;
            }
        }

        // 4. Keep exactly one probe in flight.
        if self.outstanding.is_none() {
            out.send_probe = Some(self.issue_probe(obs.now));
        }
        out
    }

    /// Returns `(started_at, trigger, probe_age)` when this tick reveals a stall.
    fn detect(
        &self,
        obs: &TickObservation,
        completed: Option<(Probe, Instant)>,
        late: Duration,
    ) -> Option<(Instant, StallTrigger, Option<Duration>)> {
        let by_probe = match (completed, self.outstanding) {
            (Some((p, acked_at)), _) => {
                let rtt = acked_at.saturating_duration_since(p.sent_at);
                (rtt >= self.stall_threshold).then_some((
                    p.sent_at,
                    StallTrigger::ProbeSlow,
                    Some(rtt),
                ))
            }
            (None, Some(p)) => {
                let age = obs.now.saturating_duration_since(p.sent_at);
                (age >= self.stall_threshold).then_some((
                    p.sent_at,
                    StallTrigger::ProbeOverdue,
                    Some(age),
                ))
            }
            (None, None) => None,
        };
        by_probe.or_else(|| {
            (late >= self.stall_threshold).then_some((
                obs.tick_start + self.probe_interval,
                StallTrigger::DetectorLate,
                None,
            ))
        })
    }
}

/// The `ProcessStall` audit row (Warn, System) carrying `detail`.
pub fn process_stall_audit_row(detail: Value) -> AuditRow {
    AuditRow {
        severity: Severity::Warn,
        source: Source::System,
        event_id: None,
        instance_id: None,
        endpoint: None,
        action: Action::ProcessStall,
        detail,
        ts_override: None,
    }
}

/// Stops the detector thread when dropped (it exits on its next wake-up; the
/// drop never blocks, so it is safe at the end of an async fn).
#[derive(Debug)]
pub struct StallDetectorGuard {
    stop_tx: Option<std_mpsc::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl StallDetectorGuard {
    pub fn is_running(&self) -> bool {
        self.thread.as_ref().is_some_and(|t| !t.is_finished())
    }

    /// Stop the thread and wait for it to exit. Blocking: call it from a
    /// plain thread (tests), never from inside the runtime.
    pub fn stop(&mut self) {
        self.stop_tx.take();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for StallDetectorGuard {
    fn drop(&mut self) {
        // Disconnects the stop channel; the thread sees it on its next wake-up.
        self.stop_tx.take();
    }
}

/// Start the detector thread against `handle`'s runtime.
pub fn spawn_stall_detector(
    handle: Handle,
    config: StallDetectorConfig,
    audit_tx: Option<mpsc::Sender<AuditRow>>,
) -> std::io::Result<StallDetectorGuard> {
    let (stop_tx, stop_rx) = std_mpsc::channel::<()>();
    let detector = Detector::new(handle, config, audit_tx);
    let thread = std::thread::Builder::new()
        .name("stall-detector".into())
        .spawn(move || detector.run(stop_rx))?;
    Ok(StallDetectorGuard {
        stop_tx: Some(stop_tx),
        thread: Some(thread),
    })
}

/// `ServiceCore` entry point: production thresholds, the current runtime,
/// `<data_dir>/logs/stall.log`. A failure to start is logged loudly and never
/// stops the service.
pub fn start_for_service(
    data_dir: &Path,
    audit_tx: mpsc::Sender<AuditRow>,
) -> Option<StallDetectorGuard> {
    let handle = match Handle::try_current() {
        Ok(h) => h,
        Err(e) => {
            log::warn!("process-stall detector NOT started: no tokio runtime: {e:?}");
            return None;
        }
    };
    let config = StallDetectorConfig::production(data_dir);
    let summary = format!(
        "probe_interval_ms={} stall_threshold_ms={} tick_late_threshold_ms={} stall_log={}",
        stall_log::ms(config.probe_interval),
        stall_log::ms(config.stall_threshold),
        stall_log::ms(config.tick_late_threshold),
        config.log_path.display()
    );
    match spawn_stall_detector(handle, config, Some(audit_tx)) {
        Ok(guard) => {
            log::info!("process-stall detector started: {summary}");
            Some(guard)
        }
        Err(e) => {
            log::warn!(
                "process-stall detector NOT started ({summary}): thread spawn failed: {e:?}"
            );
            None
        }
    }
}

/// When to refresh the pre-stall baseline: every `every`-th healthy tick.
#[derive(Debug)]
struct BaselineSchedule {
    every: u32,
    since: u32,
}

impl BaselineSchedule {
    fn new(every: u32) -> Self {
        Self {
            every: every.max(1),
            since: 0,
        }
    }

    /// Count one healthy tick; true when a baseline sample is due now.
    fn tick(&mut self) -> bool {
        self.since += 1;
        if self.since >= self.every {
            self.since = 0;
            true
        } else {
            false
        }
    }
}

/// The newest probe the runtime answered.
#[derive(Debug, Default)]
struct ProbeAck(Mutex<Option<(u64, Instant)>>);

impl ProbeAck {
    fn record(&self, seq: u64, at: Instant) {
        *self.0.lock().unwrap_or_else(|e| e.into_inner()) = Some((seq, at));
    }

    fn latest(&self) -> Option<(u64, Instant)> {
        *self.0.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// The thread-side state: the pure tracker plus everything that does I/O.
struct Detector {
    handle: Handle,
    config: StallDetectorConfig,
    audit_tx: Option<mpsc::Sender<AuditRow>>,
    log: StallLog,
    ack: Arc<ProbeAck>,
    tracker: StallTracker,
    probe: Option<(u64, tokio::task::JoinHandle<()>)>,
    baseline: Option<(Instant, ResourceSnapshot)>,
    baseline_schedule: BaselineSchedule,
    open: Option<(StallStart, ResourceSnapshot)>,
    write_error: Option<String>,
}

impl Detector {
    fn new(
        handle: Handle,
        config: StallDetectorConfig,
        audit_tx: Option<mpsc::Sender<AuditRow>>,
    ) -> Self {
        Self {
            log: StallLog::new(config.log_path.clone(), config.log_max_bytes),
            tracker: StallTracker::new(&config),
            baseline_schedule: BaselineSchedule::new(config.baseline_every_ticks),
            handle,
            config,
            audit_tx,
            ack: Arc::new(ProbeAck::default()),
            probe: None,
            baseline: None,
            open: None,
            write_error: None,
        }
    }

    fn run(mut self, stop_rx: std_mpsc::Receiver<()>) {
        let snap = resources::sample();
        self.write(&stall_log::started_record(
            self.config.probe_interval,
            self.config.stall_threshold,
            self.config.tick_late_threshold,
            &snap,
            WallAnchor::read(),
        ));
        if let Some(err) = &self.write_error {
            // Startup, not a stall: the log pipeline is safe to use.
            log::warn!(
                "process-stall detector cannot write its evidence file ({err}); stalls will reach the audit log only"
            );
        }
        let now = Instant::now();
        self.baseline = Some((now, snap));
        let seq = self.tracker.start(now);
        self.send_probe(seq);

        loop {
            let tick_start = Instant::now();
            match stop_rx.recv_timeout(self.config.probe_interval) {
                Err(std_mpsc::RecvTimeoutError::Timeout) => {}
                Ok(()) | Err(std_mpsc::RecvTimeoutError::Disconnected) => return,
            }
            let now = Instant::now();
            if self.runtime_gone() {
                return;
            }
            let outcome = self.tracker.observe(TickObservation {
                tick_start,
                now,
                latest_ack: self.ack.latest(),
            });
            self.apply(outcome, now);
        }
    }

    /// A probe task that FINISHED without answering was cancelled: the runtime
    /// shut down (tokio cancels tasks owned by a closed runtime). That is the
    /// end of this detector, never a stall.
    fn runtime_gone(&self) -> bool {
        match &self.probe {
            // `is_finished` is read BEFORE the ack: a probe that ran stored its
            // ack before completing, so a finished-and-answered probe is never
            // mistaken for a cancelled one.
            Some((seq, task)) => {
                task.is_finished() && self.ack.latest().is_none_or(|(acked, _)| acked < *seq)
            }
            None => false,
        }
    }

    fn send_probe(&mut self, seq: u64) {
        let ack = Arc::clone(&self.ack);
        let task = self.handle.spawn(async move {
            ack.record(seq, Instant::now());
        });
        self.probe = Some((seq, task));
    }

    fn apply(&mut self, outcome: TickOutcome, now: Instant) {
        // Spawn the next probe FIRST: the tracker stamped it as sent at `now`,
        // so the evidence I/O below (a snapshot plus a synced append, which may
        // be slow on a struggling box) must not inflate its measured round trip.
        if let Some(seq) = outcome.send_probe {
            self.send_probe(seq);
        }

        if let Some(start) = outcome.started {
            let snap = resources::sample();
            let baseline = self
                .baseline
                .as_ref()
                .map(|(at, s)| (s, now.saturating_duration_since(*at)));
            let record = stall_log::stall_start_record(&start, &snap, baseline, WallAnchor::read());
            self.write(&record);
            self.open = Some((start, snap));
        }

        if let Some(report) = outcome.ended {
            let snap = resources::sample();
            let at = WallAnchor::read();
            self.write(&stall_log::stall_end_record(&report, &snap, at));
            let open = self.open.take();
            let detail = stall_log::audit_detail(
                &report,
                open.as_ref().map(|(start, start_snap)| (start, start_snap)),
                &snap,
                self.log.path(),
                self.write_error.take(),
                at,
            );
            self.emit_after_recovery(&report, detail);
        } else if !self.tracker.in_stall() && self.baseline_schedule.tick() {
            self.baseline = Some((now, resources::sample()));
        }
    }

    fn write(&mut self, record: &Value) {
        if let Err(e) = self.log.append(record) {
            // No `log` here: this may run mid-stall. The error rides on the
            // next ProcessStall audit row and its post-recovery warning.
            self.write_error = Some(format!("{}: {e:?}", self.log.path().display()));
        }
    }

    /// The runtime answered a probe again, so the stall is over and the `log`
    /// pipeline and the audit channel are safe to use from here.
    fn emit_after_recovery(&self, report: &StallReport, detail: Value) {
        log::warn!(
            "process stall: class={} duration_ms={} trigger={} detector_max_late_ms={} detector_total_late_ms={} evidence={} evidence_write_error={}",
            report.class.as_str(),
            stall_log::ms(report.duration),
            report.trigger.as_str(),
            stall_log::ms(report.detector_max_late),
            stall_log::ms(report.detector_total_late),
            self.log.path().display(),
            detail["stall_log_error"].as_str().unwrap_or("none")
        );
        if let Some(tx) = &self.audit_tx {
            // `audit::record` may `tokio::spawn` a retry for a Warn row on a full
            // channel; entering the runtime gives that spawn its context.
            let _rt = self.handle.enter();
            rs_core::audit::record(tx, process_stall_audit_row(detail));
        }
    }
}

#[cfg(test)]
#[path = "stall_detector_tests.rs"]
mod tests;
