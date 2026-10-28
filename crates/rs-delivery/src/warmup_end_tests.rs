//! #424 go-live repeat: the warmup clip's publishes end CLEANLY before the
//! content publish starts.
//!
//! Before the fix `run_warmup_loop` aborted the warmup task: the primary
//! session vanished without FCUnpublish/deleteStream, and the dropped backup
//! mirror only `abort()`ed its task without waiting, so the content publish on
//! the backup key could open while the warmup backup session was still alive.
//! Measured: a repeat of part of a chunk + single-AAC-frame audio steps ~40 s
//! after the warmup-to-content switch (camera-box av-gate, 25.10 + 27.10).

use super::*;
use crate::backup_mirror::{BackupMirror, TeePusher};
use crate::pushable::Pushable;
use rs_rtmp_push::PushError;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

/// One shared, ordered record of what the primary and the backup did.
type Log = Arc<std::sync::Mutex<Vec<&'static str>>>;

/// A recording pusher. Each push takes `push_ms` of (virtual) time, like the
/// real pusher's real-time pacing; `finish_hangs` makes `finish` pend forever.
struct Rec {
    name: &'static str,
    log: Log,
    push_ms: u64,
    finish_hangs: bool,
    _gone: Gone,
}

/// Set (and logged) when the pusher is dropped: its session is gone.
struct Gone {
    name: &'static str,
    log: Log,
    flag: Arc<AtomicBool>,
}

impl Drop for Gone {
    fn drop(&mut self) {
        self.flag.store(true, Ordering::SeqCst);
        self.log.lock().unwrap().push(match self.name {
            "primary" => "primary gone",
            _ => "backup gone",
        });
    }
}

impl Rec {
    fn new(name: &'static str, log: &Log, push_ms: u64) -> (Self, Arc<AtomicBool>) {
        let flag = Arc::new(AtomicBool::new(false));
        let rec = Self {
            name,
            log: Arc::clone(log),
            push_ms,
            finish_hangs: false,
            _gone: Gone {
                name,
                log: Arc::clone(log),
                flag: Arc::clone(&flag),
            },
        };
        (rec, flag)
    }

    fn note(&self, what: &'static str) {
        self.log.lock().unwrap().push(what);
    }
}

impl Pushable for Rec {
    async fn push_flv_bytes(&mut self, _data: &[u8]) -> Result<(), PushError> {
        self.note(match self.name {
            "primary" => "primary push",
            _ => "backup push",
        });
        tokio::time::sleep(Duration::from_millis(self.push_ms)).await;
        Ok(())
    }

    async fn close(&mut self) {
        self.note(match self.name {
            "primary" => "primary close",
            _ => "backup close",
        });
    }

    async fn finish(&mut self) {
        self.note(match self.name {
            "primary" => "primary finish",
            _ => "backup finish",
        });
        if self.finish_hangs {
            std::future::pending::<()>().await;
        }
    }

    fn reconnect_count(&self) -> u32 {
        0
    }

    fn av_skew_ms(&self) -> i64 {
        0
    }

    fn take_av_invariant_events(&mut self) -> Vec<rs_rtmp_push::AvInvariantEvent> {
        Vec::new()
    }
}

/// Aborts the test process if the test is still running after 30 s of REAL
/// time: a mutant that makes a task spin stops the paused clock, and every
/// virtual-time bound would then wait forever (mutation-killable-code.md).
struct Watchdog(#[allow(dead_code)] std::sync::mpsc::Sender<()>);

fn watchdog(test: &'static str) -> Watchdog {
    let (tx, rx) = std::sync::mpsc::channel::<()>();
    std::thread::spawn(move || {
        if rx.recv_timeout(Duration::from_secs(30))
            == Err(std::sync::mpsc::RecvTimeoutError::Timeout)
        {
            eprintln!("{test}: still running after 30 s of real time -- aborting");
            std::process::abort();
        }
    });
    Watchdog(tx)
}

/// Every chunk is on S3 already, but each probe takes 1 s, so the warmup
/// clip plays a few segments before the buffer target is met.
struct PacedFetcher;

impl ChunkFetcher for PacedFetcher {
    async fn fetch_chunk_with_meta(
        &self,
        _chunk_id: i64,
    ) -> Result<Option<(Vec<u8>, i64)>, String> {
        unreachable!("the warmup loop only calls chunk_duration_ms")
    }

    async fn chunk_duration_ms(&self, _chunk_id: i64) -> Result<Option<i64>, String> {
        tokio::time::sleep(Duration::from_secs(1)).await;
        Ok(Some(1000))
    }
}

struct Setup {
    log: Log,
    backup_gone: Arc<AtomicBool>,
    tee: TeePusher<Rec>,
}

/// A YouTube-shaped pusher: a primary plus a backup mirror, pushing a
/// warmup segment in 400 ms (primary) and 300 ms (backup).
fn youtube_tee(primary_finish_hangs: bool) -> Setup {
    let log: Log = Arc::default();
    let (mut primary, _primary_gone) = Rec::new("primary", &log, 400);
    primary.finish_hangs = primary_finish_hangs;
    let (backup, backup_gone) = Rec::new("backup", &log, 300);
    let tee = TeePusher::new(
        primary,
        Some(BackupMirror::spawn(backup, "e2e rtmp".to_string(), None)),
    );
    Setup {
        log,
        backup_gone,
        tee,
    }
}

async fn run_warmup(tee: TeePusher<Rec>) -> (bool, Stats) {
    let alias = unique_alias("warmup-end");
    let ep_cfg = test_endpoint_config(&alias, false);
    let stats: Stats = Arc::new(Mutex::new(EndpointStats::default()));
    let (_stop_tx, mut stop_rx) = watch::channel(false);
    let stopped = run_warmup_loop_with_pusher(
        &PacedFetcher,
        &alias,
        &ep_cfg,
        0,
        3000,
        None,
        &stats,
        &mut stop_rx,
        None,
        Some(tee),
    )
    .await;
    (stopped, stats)
}

/// The warmup end unpublishes the primary (FCUnpublish + deleteStream +
/// drain = `finish`) before `run_warmup_loop` returns, i.e. before the
/// consumer builds the content pusher on the same key.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn the_warmup_end_unpublishes_the_primary_before_the_content_publish() {
    let _wd = watchdog("the_warmup_end_unpublishes_the_primary_before_the_content_publish");
    let setup = youtube_tee(false);
    let log = Arc::clone(&setup.log);

    let (stopped, stats) = tokio::time::timeout(Duration::from_secs(60), run_warmup(setup.tee))
        .await
        .expect("the warmup ends within the bound");

    assert!(!stopped, "the buffer target was met, not a stop");
    assert_eq!(stats.lock().await.delivery_mode, "normal");
    let events = log.lock().unwrap().clone();
    assert!(
        events.iter().filter(|e| **e == "primary push").count() >= 2,
        "the warmup clip played before the switch: {events:?}"
    );
    assert!(
        events.contains(&"primary finish"),
        "the warmup primary must be unpublished (finish), not dropped: {events:?}"
    );
    assert!(
        !events.contains(&"primary close"),
        "a plain close drops the send queue without an unpublish: {events:?}"
    );
}

/// The warmup backup session is gone before `run_warmup_loop` returns, so the
/// content backup publish never overlaps it on the same key.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn the_warmup_backup_session_is_gone_before_the_content_publish() {
    let _wd = watchdog("the_warmup_backup_session_is_gone_before_the_content_publish");
    let setup = youtube_tee(false);
    let log = Arc::clone(&setup.log);
    let backup_gone = Arc::clone(&setup.backup_gone);

    let (stopped, _stats) = tokio::time::timeout(Duration::from_secs(60), run_warmup(setup.tee))
        .await
        .expect("the warmup ends within the bound");

    // Read right after the return, with no await in between: the consumer
    // builds the content pusher at exactly this point.
    let gone_at_return = backup_gone.load(Ordering::SeqCst);
    assert!(!stopped);
    let events = log.lock().unwrap().clone();
    assert!(
        events.contains(&"backup push"),
        "the warmup clip reached the backup ingest: {events:?}"
    );
    assert!(
        gone_at_return,
        "the warmup backup session must be closed before the content publish: {events:?}"
    );
    let finish = events.iter().position(|e| *e == "primary finish");
    let gone = events.iter().position(|e| *e == "backup gone");
    assert!(
        matches!((finish, gone), (Some(f), Some(g)) if f < g),
        "the primary is unpublished first, then the backup is stopped: {events:?}"
    );
}

/// A warmup end that hangs (the primary's finish never returns) is aborted
/// after `WARMUP_STOP_MAX_WAIT`, so the content publish still starts.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_hung_warmup_end_is_aborted_after_the_bound() {
    use crate::warmup_push::{WARMUP_STOP_MAX_WAIT, WarmupEnd, WarmupPush};
    let _wd = watchdog("a_hung_warmup_end_is_aborted_after_the_bound");
    assert_eq!(
        WARMUP_STOP_MAX_WAIT,
        Duration::from_secs(20),
        "the primary's 15 s drain bound plus 5 s for the backup"
    );
    let setup = youtube_tee(true);
    let log = Arc::clone(&setup.log);
    let stats: Stats = Arc::new(Mutex::new(EndpointStats::default()));
    let warmup = WarmupPush::spawn(
        setup.tee,
        "e2e rtmp".to_string(),
        crate::rescue_segments::RescueClipSource::Countdown,
        stats,
    );
    tokio::time::sleep(Duration::from_secs(2)).await;

    let started = tokio::time::Instant::now();
    let end = tokio::time::timeout(Duration::from_secs(60), warmup.stop("e2e rtmp"))
        .await
        .expect("the stop is bounded");
    let waited = started.elapsed();

    assert_eq!(end, WarmupEnd::Aborted);
    assert!(
        waited >= WARMUP_STOP_MAX_WAIT && waited < WARMUP_STOP_MAX_WAIT + Duration::from_secs(1),
        "aborted at the bound, not before and not much after: {waited:?}"
    );
    let events = log.lock().unwrap().clone();
    assert!(events.contains(&"primary finish"), "{events:?}");
    assert!(
        events.contains(&"primary gone"),
        "the aborted task dropped its sessions: {events:?}"
    );
}

/// A clean end reports `Finished` without waiting for the bound.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_clean_warmup_end_reports_finished_at_once() {
    use crate::warmup_push::{WarmupEnd, WarmupPush};
    let _wd = watchdog("a_clean_warmup_end_reports_finished_at_once");
    let setup = youtube_tee(false);
    let stats: Stats = Arc::new(Mutex::new(EndpointStats::default()));
    let warmup = WarmupPush::spawn(
        setup.tee,
        "e2e rtmp".to_string(),
        crate::rescue_segments::RescueClipSource::Countdown,
        stats,
    );
    tokio::time::sleep(Duration::from_secs(2)).await;

    let started = tokio::time::Instant::now();
    let end = tokio::time::timeout(Duration::from_secs(60), warmup.stop("e2e rtmp"))
        .await
        .expect("the stop is bounded");

    assert_eq!(end, WarmupEnd::Finished);
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "{:?}",
        started.elapsed()
    );
    assert!(setup.backup_gone.load(Ordering::SeqCst));
}
