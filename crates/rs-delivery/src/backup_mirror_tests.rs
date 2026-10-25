//! #368: the YouTube backup ingest mirror. A slow, hung or dead backup must
//! never delay or fail the primary; the backup gets the same bytes in order;
//! its deaths are its own audit action; `close()` (the primary's reconnect
//! hook) keeps the backup up, `shutdown()` stops both.

use super::*;
use rs_core::audit::Action;
use std::collections::VecDeque;
use std::io;

#[derive(Debug, Clone, PartialEq, Eq)]
enum Event {
    Push(Vec<u8>),
    Close,
    Slow(std::time::Duration),
    Finish,
}

type Log = Arc<std::sync::Mutex<Vec<Event>>>;

/// A recording pusher. Pushes play back `results` (Ok once it is empty,
/// unless `fail_rest`); `hang` makes every push pend forever. `dropped` is
/// set when the pusher is dropped (its session would close).
#[derive(Default)]
struct Mock {
    log: Log,
    results: VecDeque<Result<(), PushError>>,
    fail_rest: bool,
    hang: bool,
    dropped: DropFlag,
    reconnects: u32,
    skew: i64,
    events: Vec<rs_rtmp_push::AvInvariantEvent>,
    ack: Option<std::time::Instant>,
}

/// Set when the mock (and so its "session") is dropped.
#[derive(Default)]
struct DropFlag(Arc<AtomicBool>);

impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

impl Pushable for Mock {
    async fn push_flv_bytes(&mut self, data: &[u8]) -> Result<(), PushError> {
        self.log.lock().unwrap().push(Event::Push(data.to_vec()));
        if self.hang {
            std::future::pending::<()>().await;
        }
        match self.results.pop_front() {
            Some(r) => r,
            None if self.fail_rest => Err(remote_closed()),
            None => Ok(()),
        }
    }

    async fn close(&mut self) {
        self.log.lock().unwrap().push(Event::Close);
    }

    fn reconnect_count(&self) -> u32 {
        self.reconnects
    }

    fn av_skew_ms(&self) -> i64 {
        self.skew
    }

    async fn finish(&mut self) {
        self.log.lock().unwrap().push(Event::Finish);
    }

    fn slow_pacing(&mut self, by: std::time::Duration) {
        self.log.lock().unwrap().push(Event::Slow(by));
    }

    fn take_av_invariant_events(&mut self) -> Vec<rs_rtmp_push::AvInvariantEvent> {
        std::mem::take(&mut self.events)
    }

    fn last_upstream_ack_at(&self) -> Option<std::time::Instant> {
        self.ack
    }
}

fn remote_closed() -> PushError {
    PushError::RemoteClosed(io::Error::from(io::ErrorKind::BrokenPipe))
}

fn pushes(log: &Log) -> Vec<Vec<u8>> {
    log.lock()
        .unwrap()
        .iter()
        .filter_map(|e| match e {
            Event::Push(d) => Some(d.clone()),
            Event::Close | Event::Slow(_) | Event::Finish => None,
        })
        .collect()
}

fn chunk(i: u8) -> Vec<u8> {
    vec![b'F', b'L', b'V', i, i, i]
}

/// Let the mirror task run: yield (no clock advance) up to `n` times until
/// `done` holds.
async fn settle(n: usize, done: impl Fn() -> bool) {
    for _ in 0..n {
        if done() {
            return;
        }
        tokio::task::yield_now().await;
    }
}

fn tee_with(primary: Mock, backup: Mock, ring: Option<Arc<AuditRing>>) -> TeePusher<Mock> {
    TeePusher::new(
        primary,
        Some(BackupMirror::spawn(backup, "YT NLCH 4K".to_string(), ring)),
    )
}

#[tokio::test(start_paused = true)]
async fn the_backup_gets_the_same_bytes_in_order() {
    let primary = Mock::default();
    let primary_log = Arc::clone(&primary.log);
    let backup = Mock::default();
    let backup_log = Arc::clone(&backup.log);
    let mut tee = tee_with(primary, backup, None);
    let state = tee.backup.as_ref().expect("backup").state();

    for i in 1..=3 {
        tee.push_flv_bytes(&chunk(i)).await.expect("primary push");
    }
    settle(100, || pushes(&backup_log).len() == 3).await;

    let sent = vec![chunk(1), chunk(2), chunk(3)];
    assert_eq!(pushes(&primary_log), sent, "the primary pushes every chunk");
    assert_eq!(
        pushes(&backup_log),
        sent,
        "the backup gets the same bytes in order"
    );
    let s = state.snapshot();
    assert!(s.connected, "{s:?}");
    assert_eq!(s.chunks_pushed, 3);
    assert_eq!(s.dropped_chunks, 0);
    assert_eq!(s.last_error, None);
}

#[tokio::test(start_paused = true)]
async fn a_hung_backup_never_slows_or_fails_the_primary() {
    let ring = AuditRing::new(32);
    let primary = Mock::default();
    let primary_log = Arc::clone(&primary.log);
    let backup = Mock {
        hang: true,
        ..Mock::default()
    };
    let mut tee = tee_with(primary, backup, Some(Arc::clone(&ring)));
    let state = tee.backup.as_ref().expect("backup").state();

    // Ten chunks with no virtual time passing: a tee that waited on the
    // backup would hit the 100 ms bound (the clock is paused, so any wait
    // advances it straight to the backup's 30 s write timeout).
    tokio::time::timeout(Duration::from_millis(100), async {
        for i in 0..10 {
            tee.push_flv_bytes(&chunk(i)).await.expect("primary push");
        }
    })
    .await
    .expect("the primary must not wait for a hung backup");
    assert_eq!(pushes(&primary_log).len(), 10);
    assert_eq!(
        state.snapshot().dropped_chunks,
        (10 - BACKUP_QUEUE_CHUNKS) as u64,
        "a full backup queue drops whole NEW chunks and counts them"
    );

    // The hung backup push times out on its own and is reported as a
    // BACKUP death, never as the endpoint's push death.
    tokio::time::sleep(Duration::from_secs(31)).await;
    let (rows, _) = ring.since(0);
    let backup_rows: Vec<_> = rows
        .iter()
        .filter(|r| r.action == Action::EndpointBackupPushDied)
        .collect();
    assert_eq!(backup_rows.len(), 1, "{rows:?}");
    assert_eq!(backup_rows[0].detail["error"], "operation timed out");
    assert!(
        rows.iter()
            .all(|r| r.action != Action::EndpointRtmpPushDied)
    );
    let s = state.snapshot();
    assert!(!s.connected);
    assert_eq!(s.last_error.as_deref(), Some("operation timed out"));
}

#[tokio::test(start_paused = true)]
async fn a_failing_backup_writes_only_backup_death_rows_and_the_primary_keeps_pushing() {
    let ring = AuditRing::new(32);
    let primary = Mock::default();
    let primary_log = Arc::clone(&primary.log);
    let backup = Mock {
        fail_rest: true,
        reconnects: 4,
        ..Mock::default()
    };
    let backup_log = Arc::clone(&backup.log);
    let mut tee = tee_with(primary, backup, Some(Arc::clone(&ring)));
    let state = tee.backup.as_ref().expect("backup").state();

    tee.push_flv_bytes(&chunk(1)).await.expect("primary push");
    tee.push_flv_bytes(&chunk(2)).await.expect("primary push");
    tokio::time::sleep(Duration::from_secs(10)).await;

    assert_eq!(pushes(&primary_log), vec![chunk(1), chunk(2)]);
    assert!(
        backup_log.lock().unwrap().contains(&Event::Close),
        "the backup closes its own session after a death"
    );
    let (rows, _) = ring.since(0);
    assert!(
        rows.iter()
            .all(|r| r.action != Action::EndpointRtmpPushDied)
    );
    let died: Vec<_> = rows
        .iter()
        .filter(|r| r.action == Action::EndpointBackupPushDied)
        .collect();
    assert_eq!(died.len(), 2, "one row per backup death: {rows:?}");
    assert_eq!(died[0].endpoint.as_deref(), Some("YT NLCH 4K"));
    assert_eq!(died[0].detail["ingest"], "youtube_backup");
    assert_eq!(
        died[0].detail["backoff_ms"], 3_000,
        "the backup's own RemoteClosed floor (never healthy)"
    );
    assert_eq!(died[0].detail["reconnect_count"], 4);
    let s = state.snapshot();
    assert!(!s.connected);
    assert_eq!(s.reconnect_count, 4);
    assert!(
        s.last_error
            .as_deref()
            .is_some_and(|e| e.contains("upstream closed connection mid-stream")),
        "{s:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn close_keeps_the_backup_and_shutdown_stops_both() {
    let primary = Mock::default();
    let primary_log = Arc::clone(&primary.log);
    let backup = Mock::default();
    let backup_dropped = Arc::clone(&backup.dropped.0);
    let mut tee = tee_with(primary, backup, None);
    let state = tee.backup.as_ref().expect("backup").state();

    tee.push_flv_bytes(&chunk(1)).await.expect("push");
    settle(100, || state.snapshot().chunks_pushed == 1).await;
    assert!(state.snapshot().connected);

    // close() = the primary's reconnect after a push error.
    tee.close().await;
    assert_eq!(
        primary_log.lock().unwrap().last(),
        Some(&Event::Close),
        "close() closes the primary"
    );
    tee.push_flv_bytes(&chunk(2)).await.expect("push");
    settle(100, || state.snapshot().chunks_pushed == 2).await;
    assert_eq!(
        state.snapshot().chunks_pushed,
        2,
        "the backup keeps streaming while the primary reconnects"
    );
    assert!(!backup_dropped.load(Ordering::SeqCst));

    tee.shutdown().await;
    assert!(
        backup_dropped.load(Ordering::SeqCst),
        "shutdown() returns only once the backup session is gone"
    );
    assert!(!state.snapshot().connected);
    assert_eq!(
        primary_log.lock().unwrap().last(),
        Some(&Event::Close),
        "shutdown() closes the primary"
    );
}

#[tokio::test(start_paused = true)]
async fn dropping_the_tee_drops_a_hung_backup_at_once() {
    let backup = Mock {
        hang: true,
        ..Mock::default()
    };
    let backup_log = Arc::clone(&backup.log);
    let backup_dropped = Arc::clone(&backup.dropped.0);
    let mut tee = tee_with(Mock::default(), backup, None);
    tee.push_flv_bytes(&chunk(1)).await.expect("push");
    settle(100, || pushes(&backup_log).len() == 1).await;
    assert_eq!(
        pushes(&backup_log).len(),
        1,
        "the backup is stuck in its push"
    );

    drop(tee);
    settle(100, || backup_dropped.load(Ordering::SeqCst)).await;
    assert!(
        backup_dropped.load(Ordering::SeqCst),
        "dropping the tee must abort the hung backup without waiting for its timeout"
    );
}

#[tokio::test(start_paused = true)]
async fn a_repush_goes_to_the_primary_only() {
    let primary = Mock::default();
    let primary_log = Arc::clone(&primary.log);
    let backup = Mock::default();
    let backup_log = Arc::clone(&backup.log);
    let mut tee = tee_with(primary, backup, None);
    let state = tee.backup.as_ref().expect("backup").state();

    tee.repush_flv_bytes(&chunk(9)).await.expect("repush");
    settle(50, || false).await;

    assert_eq!(pushes(&primary_log), vec![chunk(9)]);
    assert!(
        backup_log.lock().unwrap().is_empty(),
        "the backup already has a re-sent chunk"
    );
    assert_eq!(state.snapshot().dropped_chunks, 0);
}

/// #424: the end of an endpoint finishes the primary (unpublish + wait for
/// its send queue) and stops the backup.
#[tokio::test(start_paused = true)]
async fn finish_finishes_the_primary_and_stops_the_backup() {
    let primary = Mock::default();
    let primary_log = Arc::clone(&primary.log);
    let backup = Mock::default();
    let backup_dropped = Arc::clone(&backup.dropped.0);
    let mut tee = tee_with(primary, backup, None);

    // Through the trait, as the consumer's end paths reach it.
    Pushable::finish(&mut tee).await;
    settle(100, || backup_dropped.load(Ordering::SeqCst)).await;

    assert_eq!(*primary_log.lock().unwrap(), vec![Event::Finish]);
    assert!(
        backup_dropped.load(Ordering::SeqCst),
        "the backup is stopped"
    );
}

/// #424: the refill slows the primary; the backup only ever gets the chunks
/// the primary offers it, so it cannot run ahead.
#[tokio::test(start_paused = true)]
async fn a_refill_slows_the_primary_only() {
    let primary = Mock::default();
    let primary_log = Arc::clone(&primary.log);
    let backup = Mock::default();
    let backup_log = Arc::clone(&backup.log);
    let mut tee = tee_with(primary, backup, None);

    tee.slow_pacing(std::time::Duration::from_millis(40));
    settle(50, || false).await;

    assert_eq!(
        *primary_log.lock().unwrap(),
        vec![Event::Slow(std::time::Duration::from_millis(40))]
    );
    assert!(backup_log.lock().unwrap().is_empty());
}

#[tokio::test]
async fn the_tee_reports_the_primary_s_telemetry() {
    use rs_rtmp_push::{AvInvariantEvent, AvInvariantViolation};
    let ack = std::time::Instant::now();
    let violation = AvInvariantEvent::Violated(AvInvariantViolation {
        a_rel_ms: 1,
        v_rel_ms: 2,
        delta_ms: -1,
    });
    let primary = Mock {
        reconnects: 7,
        skew: -42,
        events: vec![violation],
        ack: Some(ack),
        ..Mock::default()
    };
    let backup = Mock {
        reconnects: 99,
        skew: 99,
        ..Mock::default()
    };
    let mut tee = tee_with(primary, backup, None);
    assert_eq!(tee.reconnect_count(), 7);
    assert_eq!(tee.av_skew_ms(), -42);
    assert_eq!(tee.last_upstream_ack_at(), Some(ack));
    assert_eq!(tee.take_av_invariant_events(), vec![violation]);
    assert!(tee.take_av_invariant_events().is_empty());
}

// --- MirrorBackoff: the backup's own reconnect policy ----------------------

#[test]
fn the_backup_reconnects_at_once_after_a_healthy_minute_like_the_primary() {
    let t0 = Instant::now();
    let mut b = MirrorBackoff::default();
    b.on_ok(t0);
    b.on_ok(t0 + Duration::from_secs(50)); // the session stays healthy since t0
    assert_eq!(
        b.on_error(&remote_closed(), t0 + Duration::from_secs(60)),
        Some(0)
    );
    let bad_name = PushError::PublishRejected {
        code: "NetStream.Publish.BadName".into(),
        description: String::new(),
    };
    let later = t0 + Duration::from_secs(61);
    assert_eq!(b.on_error(&bad_name, later), Some(3_000), "short retry");
    assert_eq!(b.on_error(&bad_name, later), Some(30_000));
}

#[test]
fn a_backup_close_inside_its_first_minute_keeps_the_floor() {
    let t0 = Instant::now();
    let mut b = MirrorBackoff::default();
    b.on_ok(t0);
    assert_eq!(
        b.on_error(&remote_closed(), t0 + Duration::from_millis(59_999)),
        Some(3_000)
    );
    // Never healthy at all.
    let mut fresh = MirrorBackoff::default();
    assert_eq!(fresh.on_error(&remote_closed(), t0), Some(3_000));
    assert_eq!(fresh.on_error(&PushError::LocalCancel, t0), None);
}

#[test]
fn a_successful_backup_push_resets_the_error_streak() {
    let t0 = Instant::now();
    let mut b = MirrorBackoff::default();
    assert_eq!(b.on_error(&remote_closed(), t0), Some(3_000));
    assert_eq!(b.on_error(&remote_closed(), t0), Some(3_000));
    b.on_ok(t0);
    assert_eq!(
        b.on_error(&remote_closed(), t0 + Duration::from_secs(60)),
        Some(0),
        "after a success the next close is the FIRST again"
    );
}

#[test]
fn the_backup_death_row_is_its_own_action() {
    let row = backup_died_row(
        "YT NLW 4k",
        "upstream closed connection mid-stream: x",
        3_000,
        2,
    );
    assert_eq!(row.action, Action::EndpointBackupPushDied);
    assert_eq!(row.severity, rs_core::audit::Severity::Warn);
    assert_eq!(row.source, rs_core::audit::Source::Vps);
    assert_eq!(row.endpoint.as_deref(), Some("YT NLW 4k"));
    assert_eq!(row.detail["ingest"], "youtube_backup");
    assert_eq!(
        row.detail["error"],
        "upstream closed connection mid-stream: x"
    );
    assert_eq!(row.detail["backoff_ms"], 3_000);
    assert_eq!(row.detail["reconnect_count"], 2);
}

// --- the one factory ----------------------------------------------------------

fn target(service_type: ServiceType, backup_ingest: bool) -> PushTarget {
    PushTarget {
        alias: "ep".to_string(),
        service_type,
        stream_key: "key-1".to_string(),
        backup_ingest,
        audit_ring: None,
    }
}

#[test]
fn only_a_youtube_target_with_the_backup_on_has_a_backup_url() {
    assert_eq!(
        backup_url_for(&target(ServiceType::YtRtmp, true)).as_deref(),
        Some("rtmp://b.rtmp.youtube.com/live2?backup=1/key-1")
    );
    assert_eq!(backup_url_for(&target(ServiceType::YtRtmp, false)), None);
    for svc in [
        ServiceType::Facebook,
        ServiceType::Vimeo,
        ServiceType::Instagram,
        ServiceType::TestFile,
    ] {
        assert_eq!(backup_url_for(&target(svc, true)), None, "{svc:?}");
    }
}

#[tokio::test]
async fn the_factory_mirrors_youtube_and_publishes_the_backup_state() {
    let stats: Stats = Arc::new(tokio::sync::Mutex::new(
        crate::endpoint_stats::EndpointStats::default(),
    ));
    let yt = build_endpoint_pusher(&target(ServiceType::YtRtmp, true), &stats).await;
    assert!(yt.mirrors());
    assert!(
        stats.lock().await.backup_ingest.is_some(),
        "the endpoint stats carry the mirror's state"
    );

    let fb = build_endpoint_pusher(&target(ServiceType::Facebook, true), &stats).await;
    assert!(!fb.mirrors());
    assert!(
        stats.lock().await.backup_ingest.is_none(),
        "a pusher without a backup clears the stats entry"
    );

    let off = build_endpoint_pusher(&target(ServiceType::YtRtmp, false), &stats).await;
    assert!(!off.mirrors(), "the kill switch turns the mirror off");
}

/// Every live and rescue pusher must come from `build_endpoint_pusher`, or
/// that path silently streams without the backup (keepalive and rescue
/// content included). No other production file may construct a
/// `RtmpPusher` itself.
#[test]
fn every_rust_pusher_is_built_by_the_one_factory() {
    let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut offenders = Vec::new();
    for entry in std::fs::read_dir(&src).expect("read src") {
        let path = entry.expect("dir entry").path();
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_default();
        if !name.ends_with(".rs") || name.ends_with("_tests.rs") || name == "backup_mirror.rs" {
            continue;
        }
        let text = std::fs::read_to_string(&path).expect("read source");
        let production = text.split("#[cfg(test)]").next().unwrap_or_default();
        if production.contains("RtmpPusher::new(") {
            offenders.push(name.to_string());
        }
    }
    assert!(
        offenders.is_empty(),
        "build the pusher with backup_mirror::build_endpoint_pusher: {offenders:?}"
    );
}

// --- the kill switch (#368: delivery.youtube_backup_ingest) ------------------

fn vps_endpoint(json: serde_json::Value) -> crate::api::EndpointConfig {
    serde_json::from_value(json).expect("a VPS endpoint entry")
}

#[test]
fn an_endpoint_entry_without_the_switch_keeps_the_backup_on() {
    let ep = vps_endpoint(serde_json::json!({
        "alias": "YT NLCH 4K", "service_type": "YT_RTMP", "stream_key": "k"
    }));
    assert!(ep.youtube_backup_ingest, "an older host sends no switch");
    let off = vps_endpoint(serde_json::json!({
        "alias": "YT NLCH 4K", "service_type": "YT_RTMP", "stream_key": "k",
        "youtube_backup_ingest": false
    }));
    assert!(!off.youtube_backup_ingest);
}

#[test]
fn the_endpoint_push_target_follows_the_switch() {
    for on in [true, false] {
        let ep = vps_endpoint(serde_json::json!({
            "alias": "YT NLW 4k", "service_type": "YT_RTMP", "stream_key": "key-9",
            "youtube_backup_ingest": on
        }));
        let t = PushTarget::for_endpoint(&ep, ServiceType::YtRtmp, None);
        assert_eq!(t.backup_ingest, on);
        assert_eq!(t.alias, "YT NLW 4k");
        assert_eq!(t.stream_key, "key-9");
        assert_eq!(t.service_type, ServiceType::YtRtmp);
        assert_eq!(
            backup_url_for(&t).is_some(),
            on,
            "the switch decides whether a YouTube endpoint is mirrored"
        );
    }
}

/// #368 review: a chunk as the producer queues it for the consumer.
fn prefetched(i: u8) -> PrefetchedChunk {
    PrefetchedChunk {
        chunk_id: i64::from(i),
        data: chunk(i),
        duration_ms: 2_000,
    }
}

#[tokio::test(start_paused = true)]
async fn the_backup_gets_the_next_chunk_while_the_primary_is_stuck_on_the_current_one() {
    // A primary stuck on chunk 1 (reconnecting, re-sending it): before the
    // look-ahead the backup got chunk 2 only after the primary was done with
    // chunk 1, so it went quiet exactly while YouTube rolled over to it.
    let primary = Mock {
        hang: true,
        ..Mock::default()
    };
    let backup = Mock::default();
    let backup_log = Arc::clone(&backup.log);
    let mut tee = tee_with(primary, backup, None);
    let (tx, mut rx) = mpsc::channel(PREFETCH_TEST_DEPTH);
    for i in 1..=2 {
        tx.send(prefetched(i)).await.expect("queue");
    }
    let mut lookahead = None;
    let current = rx.recv().await.expect("chunk 1");

    feed_backup_ahead(&mut tee, &current.data, false, &mut rx, &mut lookahead);
    let stuck =
        tokio::time::timeout(Duration::from_secs(5), tee.push_flv_bytes(&current.data)).await;
    assert!(stuck.is_err(), "the primary is stuck on chunk 1");
    settle(100, || pushes(&backup_log).len() == 2).await;

    assert_eq!(
        pushes(&backup_log),
        vec![chunk(1), chunk(2)],
        "the backup already has chunk 2 while the primary is stuck on chunk 1"
    );
    assert_eq!(
        lookahead.as_ref().map(|c| c.chunk_id),
        Some(2),
        "the consumer keeps chunk 2 for its own next turn"
    );
}

#[tokio::test(start_paused = true)]
async fn a_look_ahead_chunk_reaches_the_backup_exactly_once_and_in_order() {
    let primary = Mock::default();
    let primary_log = Arc::clone(&primary.log);
    let backup = Mock::default();
    let backup_log = Arc::clone(&backup.log);
    let mut tee = tee_with(primary, backup, None);
    let state = tee.backup.as_ref().expect("backup").state();
    let (tx, mut rx) = mpsc::channel(PREFETCH_TEST_DEPTH);
    for i in 1..=3 {
        tx.send(prefetched(i)).await.expect("queue");
    }
    drop(tx);

    // The consumer's turn order: the look-ahead first, then the queue.
    let mut lookahead: Option<PrefetchedChunk> = None;
    loop {
        let from_lookahead = lookahead.is_some();
        let current = match lookahead.take() {
            Some(c) => c,
            None => match rx.try_recv() {
                Ok(c) => c,
                Err(_) => break,
            },
        };
        feed_backup_ahead(
            &mut tee,
            &current.data,
            from_lookahead,
            &mut rx,
            &mut lookahead,
        );
        tee.push_flv_bytes(&current.data)
            .await
            .expect("primary push");
    }
    // A chunk pushed with nothing offered ahead (a keepalive frame) still
    // reaches the backup through the tee.
    tee.push_flv_bytes(&chunk(9)).await.expect("primary push");
    settle(100, || pushes(&backup_log).len() == 4).await;

    let sent = vec![chunk(1), chunk(2), chunk(3), chunk(9)];
    assert_eq!(pushes(&primary_log), sent);
    assert_eq!(
        pushes(&backup_log),
        sent,
        "every chunk reaches the backup once, in order"
    );
    assert_eq!(state.snapshot().dropped_chunks, 0);
}

#[tokio::test]
async fn without_a_backup_the_consumer_queue_is_left_alone() {
    let mut tee = TeePusher::new(Mock::default(), None);
    let (tx, mut rx) = mpsc::channel(PREFETCH_TEST_DEPTH);
    tx.send(prefetched(2)).await.expect("queue");
    let mut lookahead = None;

    feed_backup_ahead(&mut tee, &chunk(1), false, &mut rx, &mut lookahead);

    assert!(lookahead.is_none(), "no look-ahead without a backup");
    assert_eq!(
        rx.try_recv().expect("chunk 2 still queued").chunk_id,
        2,
        "a non-mirroring endpoint keeps its exact receive order"
    );
}

const PREFETCH_TEST_DEPTH: usize = 10;
