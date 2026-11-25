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
