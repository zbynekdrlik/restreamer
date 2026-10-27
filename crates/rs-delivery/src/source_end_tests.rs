//! #408: the end of the source on the VPS — announced, outrun, cleared by
//! the next chunk, and the idle wait an endpoint spends in it.

use super::*;
use crate::endpoint_stats::EndpointStats;
use rs_ffmpeg::ServiceType;
use std::time::Duration;
use tokio::sync::Mutex;

#[test]
fn an_announced_end_holds_until_a_later_chunk_is_fetched() {
    let end = SourceEnd::new();
    assert_eq!(end.ended_at(), None);
    end.note_fetched(7);
    assert!(end.announce(7), "the last fetched chunk may be the end");
    assert_eq!(end.ended_at(), Some(7));
    end.note_fetched(7);
    assert_eq!(end.ended_at(), Some(7), "the end itself is no resume");
    end.note_fetched(8);
    assert_eq!(end.ended_at(), None, "a chunk past the end: live again");
}

#[test]
fn an_announcement_the_source_already_outran_is_refused() {
    let end = SourceEnd::new();
    end.note_fetched(12);
    assert!(!end.announce(11));
    assert_eq!(end.ended_at(), None);
    assert!(end.announce(12));
}

#[test]
fn a_later_announcement_moves_the_end() {
    let end = SourceEnd::new();
    assert!(end.announce(4));
    assert!(end.announce(5), "a late chunk raised the end");
    assert_eq!(end.ended_at(), Some(5));
}

#[test]
fn clones_share_one_end() {
    let a = SourceEnd::new();
    let b = a.clone();
    assert!(a.announce(3));
    assert_eq!(b.ended_at(), Some(3));
    b.note_fetched(4);
    assert_eq!(a.ended_at(), None);
}

#[test]
fn reached_and_past_end_are_exact_at_the_boundary() {
    assert_eq!(reached(None, 100), None);
    assert_eq!(reached(Some(10), 9), None);
    assert_eq!(reached(Some(10), 10), Some(10));
    assert_eq!(reached(Some(10), 11), Some(10));
    assert!(!past_end(None, 100));
    assert!(!past_end(Some(10), 10));
    assert!(past_end(Some(10), 11));
}

#[test]
fn rows_name_the_endpoint_and_the_chunk() {
    let r = ended_row("YT-BB", 41);
    assert_eq!(r.action, Action::EndpointSourceEnded);
    assert_eq!(r.severity, Severity::Info);
    assert_eq!(r.source, Source::Vps);
    assert_eq!(r.endpoint.as_deref(), Some("YT-BB"));
    assert_eq!(r.detail, serde_json::json!({ "ended_at_chunk_id": 41 }));
    let r = resumed_row("YT-BB", 42);
    assert_eq!(r.action, Action::EndpointSourceResumed);
    assert_eq!(r.severity, Severity::Info);
    assert_eq!(r.source, Source::Vps);
    assert_eq!(r.endpoint.as_deref(), Some("YT-BB"));
    assert_eq!(r.detail, serde_json::json!({ "chunk_id": 42 }));
}

fn target(ring: &Arc<AuditRing>) -> PushTarget {
    PushTarget {
        alias: "ep".to_string(),
        service_type: ServiceType::TestFile,
        stream_key: "k".to_string(),
        backup_ingest: false,
        audit_ring: Some(Arc::clone(ring)),
    }
}

fn actions(ring: &Arc<AuditRing>) -> Vec<Action> {
    ring.since(0).0.into_iter().map(|r| r.action).collect()
}

#[tokio::test]
async fn an_ended_endpoint_closes_its_publish_and_resumes_on_the_next_chunk() {
    let ring = AuditRing::new(50);
    let stats: Stats = Arc::new(Mutex::new(EndpointStats::default()));
    let target = target(&ring);
    let mut pusher = Some(build_endpoint_pusher(&target, &stats).await);
    let (tx, mut rx) = mpsc::channel(4);
    let (_stop_tx, mut stop_rx) = watch::channel(false);

    let audit = Some(Arc::clone(&ring));
    let idle = tokio::spawn({
        let stats = stats.clone();
        async move {
            let got = idle_until_resume(
                &target,
                41,
                &mut rx,
                &mut stop_rx,
                &stats,
                &audit,
                &mut pusher,
                Vec::new(),
            )
            .await;
            (got, pusher)
        }
    });

    // While idle: shown as ended, the publish is gone, one ended row.
    tokio::time::timeout(Duration::from_secs(5), async {
        while stats.lock().await.delivery_mode != "ended" {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("delivery_mode must read 'ended' while idle");
    assert_eq!(actions(&ring), vec![Action::EndpointSourceEnded]);

    tx.send(PrefetchedChunk {
        chunk_id: 42,
        data: vec![1, 2, 3],
        duration_ms: 2000,
    })
    .await
    .unwrap();
    let (got, pusher) = tokio::time::timeout(Duration::from_secs(5), idle)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.map(|c| c.chunk_id), Some(42));
    assert!(pusher.is_some(), "a fresh pusher for the resumed source");
    assert_eq!(stats.lock().await.delivery_mode, "normal");
    assert_eq!(
        actions(&ring),
        vec![Action::EndpointSourceEnded, Action::EndpointSourceResumed]
    );
}

#[tokio::test]
async fn a_stop_while_ended_ends_the_wait() {
    let ring = AuditRing::new(50);
    let stats: Stats = Arc::new(Mutex::new(EndpointStats::default()));
    let target = target(&ring);
    let mut pusher = Some(build_endpoint_pusher(&target, &stats).await);
    let (_tx, mut rx) = mpsc::channel(4);
    let (stop_tx, mut stop_rx) = watch::channel(false);
    let audit = Some(Arc::clone(&ring));
    let idle = tokio::spawn(async move {
        idle_until_resume(
            &target,
            41,
            &mut rx,
            &mut stop_rx,
            &stats,
            &audit,
            &mut pusher,
            Vec::new(),
        )
        .await
        .map(|c| c.chunk_id)
    });
    // A false "stop" (a changed value that is not a stop) keeps waiting.
    stop_tx.send(false).unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!idle.is_finished());
    stop_tx.send(true).unwrap();
    let got = tokio::time::timeout(Duration::from_secs(5), idle)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got, None);
    assert_eq!(actions(&ring), vec![Action::EndpointSourceEnded]);
}

#[tokio::test]
async fn a_closed_channel_while_ended_ends_the_wait() {
    let ring = AuditRing::new(50);
    let stats: Stats = Arc::new(Mutex::new(EndpointStats::default()));
    let target = target(&ring);
    let mut pusher = None;
    let (tx, mut rx) = mpsc::channel::<PrefetchedChunk>(4);
    let (_stop_tx, mut stop_rx) = watch::channel(false);
    drop(tx);
    let got = tokio::time::timeout(
        Duration::from_secs(5),
        idle_until_resume(
            &target,
            41,
            &mut rx,
            &mut stop_rx,
            &stats,
            &Some(Arc::clone(&ring)),
            &mut pusher,
            Vec::new(),
        ),
    )
    .await
    .unwrap();
    assert!(got.is_none());
    assert!(
        pusher.is_none(),
        "no pusher is built for a source that never came back"
    );
}
