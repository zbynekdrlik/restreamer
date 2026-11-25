//! #437: a chunk-id gap starts a new pusher mapping (primary AND backup),
//! and a full backup queue catches up instead of staying full.
//!
//! PP live 25.11.2026 (evt16): every ~6 chunks the fast endpoint's backup
//! skipped one chunk id; the next push paced the skipped ~2 s as wall time
//! (a 2 s pause on the wire, a freeze on YouTube), which let the primary
//! offer one more chunk into the full queue, which dropped the NEW chunk:
//! the loop sustained itself.

use super::*;

fn log_of(log: &Log) -> Vec<Event> {
    log.lock().unwrap().clone()
}

/// The events of `log` with every push reduced to its chunk byte (see
/// `chunk`), so a sequence reads `[P(1), M, P(3)]`.
#[derive(Debug, PartialEq, Eq)]
enum Seen {
    P(u8),
    M,
}

fn seen(log: &Log) -> Vec<Seen> {
    log_of(log)
        .into_iter()
        .filter_map(|e| match e {
            Event::Push(d) => Some(Seen::P(d[3])),
            Event::NewMapping => Some(Seen::M),
            Event::Close | Event::Finish => None,
        })
        .collect()
}

async fn push_labelled(tee: &mut TeePusher<Mock>, id: u8) {
    Pushable::label_chunk(tee, i64::from(id));
    tee.push_flv_bytes(&chunk(id)).await.expect("primary push");
}

#[tokio::test(start_paused = true)]
async fn a_chunk_id_gap_starts_a_new_mapping_on_the_primary_only_at_the_gap() {
    let primary = Mock::default();
    let primary_log = Arc::clone(&primary.log);
    let mut tee = TeePusher::new(primary, None);

    for id in [1, 2, 4, 5] {
        push_labelled(&mut tee, id).await;
    }
    assert_eq!(
        seen(&primary_log),
        vec![Seen::P(1), Seen::P(2), Seen::M, Seen::P(4), Seen::P(5)],
        "a new mapping before chunk 4 (2 -> 4), never before a contiguous chunk"
    );
}

#[tokio::test(start_paused = true)]
async fn a_clip_segment_between_live_chunks_is_no_gap() {
    // A keepalive freeze / clip segment carries no chunk id. The live chunk
    // after it continues the previous LIVE chunk, so it is no gap.
    let primary = Mock::default();
    let primary_log = Arc::clone(&primary.log);
    let mut tee = TeePusher::new(primary, None);

    push_labelled(&mut tee, 1).await;
    tee.push_flv_bytes(&chunk(9)).await.expect("clip push");
    push_labelled(&mut tee, 2).await;
    push_labelled(&mut tee, 7).await;
    assert_eq!(
        seen(&primary_log),
        vec![Seen::P(1), Seen::P(9), Seen::P(2), Seen::M, Seen::P(7)]
    );
}

#[tokio::test(start_paused = true)]
async fn a_backward_chunk_id_is_a_gap_too() {
    // A respawn may resume a chunk the pusher already sent: not id + 1.
    let primary = Mock::default();
    let primary_log = Arc::clone(&primary.log);
    let mut tee = TeePusher::new(primary, None);

    push_labelled(&mut tee, 5).await;
    push_labelled(&mut tee, 5).await;
    assert_eq!(seen(&primary_log), vec![Seen::P(5), Seen::M, Seen::P(5)]);
}

#[tokio::test(start_paused = true)]
async fn the_backup_starts_a_new_mapping_at_its_own_gap() {
    let backup = Mock::default();
    let backup_log = Arc::clone(&backup.log);
    let primary = Mock::default();
    let primary_log = Arc::clone(&primary.log);
    let mut tee = tee_with(primary, backup, None);

    for id in [1, 2, 4] {
        push_labelled(&mut tee, id).await;
    }
    settle(100, || pushes(&backup_log).len() == 3).await;
    assert_eq!(
        seen(&backup_log),
        vec![Seen::P(1), Seen::P(2), Seen::M, Seen::P(4)],
        "the backup pusher gets its own new mapping before its gap"
    );
    assert_eq!(
        seen(&primary_log),
        vec![Seen::P(1), Seen::P(2), Seen::M, Seen::P(4)]
    );
}

#[tokio::test(start_paused = true)]
async fn a_full_backup_queue_drops_the_stale_chunks_and_catches_up() {
    // The backup is held on chunk 1 while the primary goes on. Before #437
    // the full queue kept the stale chunks 2-5 and dropped the NEWEST one
    // (6), so the backup stayed full and behind for good.
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let backup = Mock {
        gate: Some(Arc::clone(&gate)),
        ..Mock::default()
    };
    let backup_log = Arc::clone(&backup.log);
    let mut tee = tee_with(Mock::default(), backup, None);
    let state = tee.backup.as_ref().expect("backup").state();

    push_labelled(&mut tee, 1).await;
    settle(100, || pushes(&backup_log).len() == 1).await;
    assert_eq!(pushes(&backup_log).len(), 1, "the backup holds chunk 1");
    for id in 2..=(2 + BACKUP_QUEUE_CHUNKS as u8) {
        push_labelled(&mut tee, id).await;
    }
    let newest = 2 + BACKUP_QUEUE_CHUNKS as u8;
    assert_eq!(
        state.snapshot().dropped_chunks,
        BACKUP_QUEUE_CHUNKS as u64,
        "the stale queued chunks are dropped and counted"
    );

    gate.add_permits(10);
    settle(100, || pushes(&backup_log).len() == 2).await;
    assert_eq!(
        seen(&backup_log),
        vec![Seen::P(1), Seen::M, Seen::P(newest)],
        "the backup jumps to the newest chunk once, as a clean cut"
    );
    let s = state.snapshot();
    assert_eq!(s.last_chunk_id, Some(i64::from(newest)));
    assert_eq!(s.chunks_pushed, 2);

    // Caught up: the next chunk queues and is pushed without a drop.
    push_labelled(&mut tee, newest + 1).await;
    settle(100, || pushes(&backup_log).len() == 3).await;
    assert_eq!(pushes(&backup_log).len(), 3);
    assert_eq!(state.snapshot().dropped_chunks, BACKUP_QUEUE_CHUNKS as u64);
}

#[tokio::test(start_paused = true)]
async fn a_queue_with_room_drops_nothing() {
    // One below the bound: everything queued reaches the backup in order.
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let backup = Mock {
        gate: Some(Arc::clone(&gate)),
        ..Mock::default()
    };
    let backup_log = Arc::clone(&backup.log);
    let mut tee = tee_with(Mock::default(), backup, None);
    let state = tee.backup.as_ref().expect("backup").state();

    push_labelled(&mut tee, 1).await;
    settle(100, || pushes(&backup_log).len() == 1).await;
    for id in 2..=(1 + BACKUP_QUEUE_CHUNKS as u8) {
        push_labelled(&mut tee, id).await;
    }
    assert_eq!(state.snapshot().dropped_chunks, 0);
    gate.add_permits(10);
    let total = 1 + BACKUP_QUEUE_CHUNKS;
    settle(100, || pushes(&backup_log).len() == total).await;
    assert_eq!(
        pushes(&backup_log),
        (1..=total as u8).map(chunk).collect::<Vec<_>>()
    );
    assert!(
        !seen(&backup_log).contains(&Seen::M),
        "no gap, no new mapping"
    );
}

fn mirror_chunk(id: i64) -> MirrorChunk {
    MirrorChunk {
        chunk_id: Some(id),
        data: Arc::from(chunk(id as u8).as_slice()),
    }
}

#[tokio::test]
async fn the_queue_drops_its_stale_chunks_only_when_full_and_everything_once_closed() {
    let q = MirrorQueue::new();
    for id in 0..BACKUP_QUEUE_CHUNKS as i64 {
        assert_eq!(q.offer(mirror_chunk(id)), 0, "room for chunk {id}");
    }
    assert_eq!(
        q.offer(mirror_chunk(9)),
        BACKUP_QUEUE_CHUNKS,
        "a full queue drops every stale chunk"
    );
    assert_eq!(q.recv().await.and_then(|c| c.chunk_id), Some(9));
    q.close();
    assert_eq!(
        q.offer(mirror_chunk(10)),
        1,
        "a closed queue drops the new chunk"
    );
    assert!(
        q.recv().await.is_none(),
        "a closed queue ends the mirror task"
    );
}
