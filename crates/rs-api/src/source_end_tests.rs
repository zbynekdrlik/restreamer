//! #408: a deliberate OBS stop is announced to the VPS as the end of the
//! source, once every chunk is on S3; nothing else ever is.

use super::*;
use crate::obs::ObsState;
use wiremock::matchers::{body_json, header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// The ingest is gone, OBS is connected and stopped on purpose at `at`,
/// every chunk of event 7 is on S3 up to chunk 41.
fn stopped_at(at: Instant) -> Inputs {
    Inputs {
        event_id: 7,
        obs_connected: true,
        obs_stopped_at: Some(at),
        pending_uploads: 0,
        last_sent_chunk: Some(41),
    }
}

/// OBS stopped at the epoch of these tests.
fn stopped() -> Inputs {
    stopped_at(epoch())
}

fn epoch() -> Instant {
    static EPOCH: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
    *EPOCH.get_or_init(Instant::now)
}

/// A tracker that has seen the ingest gone since `t0`.
fn settled(t0: Instant) -> Tracker {
    let mut t = Tracker::default();
    assert_eq!(t.due(t0, &stopped()), None, "never before SETTLE");
    t
}

#[test]
fn the_end_is_due_once_the_ingest_has_been_gone_for_the_settle_time() {
    let t0 = epoch();
    let mut t = settled(t0);
    assert_eq!(
        t.due(t0 + SETTLE - Duration::from_millis(1), &stopped()),
        None
    );
    assert_eq!(t.due(t0 + SETTLE, &stopped()), Some(41));
}

#[test]
fn nothing_is_due_unless_obs_is_connected_and_stopped_on_purpose() {
    let t0 = epoch();
    let later = t0 + SETTLE * 2;
    for inputs in [
        Inputs {
            obs_connected: false,
            ..stopped()
        },
        Inputs {
            obs_stopped_at: None,
            ..stopped()
        },
        Inputs {
            pending_uploads: 1,
            ..stopped()
        },
        Inputs {
            last_sent_chunk: None,
            ..stopped()
        },
    ] {
        let mut t = Tracker::default();
        assert_eq!(t.due(t0, &inputs), None);
        assert_eq!(t.due(later, &inputs), None, "{inputs:?}");
    }
}

/// The operator stops the stream and quits OBS within seconds: the
/// websocket is gone by the announcing tick, but the stop was seen.
#[test]
fn obs_quitting_right_after_a_deliberate_stop_still_ends_the_source() {
    let t0 = epoch();
    let mut t = settled(t0);
    let obs_gone = Inputs {
        obs_connected: false,
        obs_stopped_at: None,
        ..stopped()
    };
    assert_eq!(t.due(t0 + SETTLE, &obs_gone), Some(41));
}

/// An OBS stop is the cause only of a disconnect at about the same time:
/// one long before (CI's ffmpeg publisher dropping hours after OBS was
/// stopped) or long after is an outage.
#[test]
fn the_obs_stop_must_match_the_disconnect() {
    let t0 = epoch() + Duration::from_secs(3600);
    for (obs_stop, due) in [
        (t0 - MATCH_WINDOW, true),
        (t0 + MATCH_WINDOW, true),
        (t0 - MATCH_WINDOW - Duration::from_millis(1), false),
        (t0 + MATCH_WINDOW + Duration::from_millis(1), false),
    ] {
        let mut t = Tracker::default();
        let inputs = stopped_at(obs_stop);
        assert_eq!(t.due(t0, &inputs), None);
        assert_eq!(
            t.due(t0 + SETTLE, &inputs).is_some(),
            due,
            "OBS stop {:?} from the disconnect",
            obs_stop.duration_since(t0).max(t0.duration_since(obs_stop))
        );
    }
}

#[test]
fn a_connected_ingest_is_never_an_end_and_starts_over() {
    let t0 = epoch();
    let mut t = settled(t0);
    t.publisher_connected();
    // The gone-since clock restarted at the next disconnect, and what OBS
    // reported at the old one no longer counts.
    let t1 = t0 + SETTLE * 3;
    let obs_gone = Inputs {
        obs_connected: false,
        obs_stopped_at: None,
        ..stopped()
    };
    assert_eq!(t.due(t1, &obs_gone), None);
    assert_eq!(t.due(t1 + SETTLE, &obs_gone), None);
    let mut t = settled(t0);
    t.publisher_connected();
    assert_eq!(t.due(t1, &stopped()), None);
    assert_eq!(t.due(t1 + SETTLE, &stopped()), Some(41));
}

#[test]
fn an_accepted_end_is_announced_again_only_when_a_late_chunk_raised_it() {
    let t0 = epoch();
    let mut t = settled(t0);
    let later = t0 + SETTLE;
    assert_eq!(t.due(later, &stopped()), Some(41));
    assert!(t.accepted(41), "the first announcement of this stop");
    assert_eq!(t.due(later, &stopped()), None);
    let raised = Inputs {
        last_sent_chunk: Some(42),
        ..stopped()
    };
    assert_eq!(t.due(later, &raised), Some(42));
    assert!(!t.accepted(42), "a re-announcement of the same stop");
    assert_eq!(t.due(later, &raised), None);
}

#[test]
fn a_restart_after_an_announced_stop_announces_the_next_stop_again() {
    let t0 = epoch();
    let mut t = settled(t0);
    assert_eq!(t.due(t0 + SETTLE, &stopped()), Some(41));
    assert!(t.accepted(41));
    t.publisher_connected();
    let t1 = t0 + SETTLE * 4;
    assert_eq!(t.due(t1, &stopped()), None);
    assert_eq!(t.due(t1 + SETTLE, &stopped()), Some(41));
    assert!(t.accepted(41), "a new stop records its own row");
}

#[test]
fn another_event_starts_over() {
    let t0 = epoch();
    let mut t = settled(t0);
    assert_eq!(t.due(t0 + SETTLE, &stopped()), Some(41));
    assert!(t.accepted(41));
    let other = Inputs {
        event_id: 8,
        ..stopped()
    };
    assert_eq!(t.due(t0 + SETTLE, &other), None, "a new event settles anew");
    assert_eq!(t.due(t0 + SETTLE * 2, &other), Some(41));
}

#[tokio::test]
async fn announce_posts_the_end_with_the_vps_token() {
    let vps = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/source"))
        .and(header("authorization", "Bearer vps-token"))
        .and(body_json(serde_json::json!({ "ended_at_chunk_id": 41 })))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&vps)
        .await;
    announce(&vps.uri(), "vps-token", 41).await.unwrap();
}

#[tokio::test]
async fn an_old_vps_without_the_route_is_an_error() {
    let vps = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&vps)
        .await;
    let err = announce(&vps.uri(), "vps-token", 41).await.unwrap_err();
    assert!(err.contains("404"), "{err}");
}

struct Rig {
    watch: SourceEndWatch,
    audit_rx: tokio::sync::mpsc::Receiver<AuditRow>,
    vps: MockServer,
    event_id: i64,
}

/// A delivering event with chunks 1..=3 on S3, its VPS on a mock server,
/// the ingest gone, and OBS in `obs`.
async fn rig(obs: Option<ObsState>) -> Rig {
    let pool = db::create_memory_pool().await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    let event_id = db::upsert_streaming_event(&pool, "PP-live").await.unwrap();
    db::set_delivering_activated(&pool, event_id, true)
        .await
        .unwrap();
    for n in 0..3 {
        let id = db::insert_chunk(&pool, event_id, &format!("/tmp/{n}.bin"), 10, "m", 2000)
            .await
            .unwrap();
        db::set_chunk_sent(&pool, id).await.unwrap();
    }
    let vps = MockServer::start().await;
    db::create_delivery_instance(
        &pool,
        1,
        "rs-delivery-evt1",
        "127.0.0.1",
        "cpx32",
        Some(event_id),
        "vps-token",
    )
    .await
    .unwrap();
    let (audit_tx, audit_rx) = tokio::sync::mpsc::channel(16);
    let watch = SourceEndWatch {
        pool,
        inpoint: InpointState::new(),
        obs: Arc::new(RwLock::new(obs.map(|s| Arc::new(ObsClient::with_state(s))))),
        audit_tx,
        vps_port: vps.address().port(),
    };
    Rig {
        watch,
        audit_rx,
        vps,
        event_id,
    }
}

/// OBS stopped on purpose just now (the rig's ingest is already gone).
fn deliberately_stopped() -> ObsState {
    ObsState {
        connected: true,
        stopped_deliberately: true,
        stopped_at: Some(Instant::now()),
        ..ObsState::default()
    }
}

#[tokio::test]
async fn a_deliberate_stop_is_announced_once_and_recorded_once() {
    let mut r = rig(Some(deliberately_stopped())).await;
    Mock::given(method("POST"))
        .and(path("/api/source"))
        .and(header("authorization", "Bearer vps-token"))
        .and(body_json(serde_json::json!({ "ended_at_chunk_id": 3 })))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&r.vps)
        .await;
    let mut tracker = Tracker::default();
    let t0 = epoch();
    assert_eq!(r.watch.tick(&mut tracker, t0).await, None, "settling");
    assert_eq!(r.watch.tick(&mut tracker, t0 + SETTLE).await, Some(3));
    assert_eq!(r.watch.tick(&mut tracker, t0 + SETTLE * 2).await, None);

    let row = r.audit_rx.try_recv().expect("one audit row");
    assert_eq!(row.action, Action::SourceStoppedDeliberately);
    assert_eq!(row.event_id, Some(r.event_id));
    assert_eq!(row.detail, serde_json::json!({ "ended_at_chunk_id": 3 }));
    assert!(r.audit_rx.try_recv().is_err(), "recorded once");
}

#[tokio::test]
async fn no_announcement_without_a_deliberate_stop_or_a_delivering_event() {
    for obs in [
        None,
        Some(ObsState {
            connected: true,
            ..ObsState::default()
        }),
    ] {
        let mut r = rig(obs).await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&r.vps)
            .await;
        let mut tracker = Tracker::default();
        let t0 = epoch();
        r.watch.tick(&mut tracker, t0).await;
        assert_eq!(r.watch.tick(&mut tracker, t0 + SETTLE).await, None);
        assert!(r.audit_rx.try_recv().is_err());
    }

    let mut r = rig(Some(deliberately_stopped())).await;
    db::set_delivering_activated(&r.watch.pool, r.event_id, false)
        .await
        .unwrap();
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&r.vps)
        .await;
    let mut tracker = Tracker::default();
    let t0 = epoch();
    r.watch.tick(&mut tracker, t0).await;
    assert_eq!(r.watch.tick(&mut tracker, t0 + SETTLE).await, None);
    assert!(r.audit_rx.try_recv().is_err());
}

#[tokio::test]
async fn a_refused_announcement_is_retried_and_not_recorded() {
    let mut r = rig(Some(deliberately_stopped())).await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(503))
        .up_to_n_times(1)
        .mount(&r.vps)
        .await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&r.vps)
        .await;
    let mut tracker = Tracker::default();
    let t0 = epoch();
    r.watch.tick(&mut tracker, t0).await;
    assert_eq!(r.watch.tick(&mut tracker, t0 + SETTLE).await, None, "503");
    assert!(
        r.audit_rx.try_recv().is_err(),
        "nothing recorded on a refusal"
    );
    assert_eq!(r.watch.tick(&mut tracker, t0 + SETTLE * 2).await, Some(3));
    assert_eq!(
        r.audit_rx.try_recv().unwrap().action,
        Action::SourceStoppedDeliberately
    );
}

/// While a publisher is connected nothing is announced (and the DB is not
/// read); a stop after it settles anew.
#[tokio::test]
async fn a_live_publisher_is_never_an_end() {
    let mut r = rig(Some(deliberately_stopped())).await;
    Mock::given(method("POST"))
        .and(path("/api/source"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&r.vps)
        .await;
    let mut tracker = Tracker::default();
    let t0 = Instant::now();
    r.watch.inpoint.set_connected(true);
    assert_eq!(r.watch.tick(&mut tracker, t0).await, None);
    assert_eq!(r.watch.tick(&mut tracker, t0 + SETTLE * 2).await, None);
    assert!(r.audit_rx.try_recv().is_err());

    r.watch.inpoint.set_connected(false);
    let t1 = Instant::now();
    assert_eq!(r.watch.tick(&mut tracker, t1).await, None, "settling");
    assert_eq!(r.watch.tick(&mut tracker, t1 + SETTLE).await, Some(3));
}
