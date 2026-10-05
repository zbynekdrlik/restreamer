---
paths:
  - "crates/rs-inpoint/src/flv_chunker.rs"
  - "crates/rs-inpoint/src/flv_chunker_ingest.rs"
  - "crates/rs-inpoint/src/flv_chunker_tests.rs"
  - "crates/rs-inpoint/src/flv_chunker_time_tests.rs"
  - "crates/rs-inpoint/src/media_receiver.rs"
  - "crates/rs-inpoint/src/media_receiver_tests.rs"
  - "crates/rs-inpoint/src/media_receiver_takeover_tests.rs"
  - "crates/rs-inpoint/src/media_receiver_remembered_tests.rs"
  - "crates/rs-inpoint/src/frame_stats.rs"
  - "crates/rs-inpoint/src/ingest_report.rs"
  - "crates/rs-inpoint/src/src_track.rs"
  - "crates/rs-rtmp-push/src/pusher.rs"
  - "crates/rs-rtmp-push/src/pusher_tests.rs"
  - "crates/rs-rtmp-push/tests/av_*.rs"
  - "crates/rs-rtmp-push/src/state.rs"
  - "crates/rs-rtmp-push/src/av_invariant.rs"
  - "crates/rs-rtmp-push/src/skew.rs"
---

# The A/V time model — the invariant every stage must keep (#367)

**INVARIANT: the A/V relationship comes ONLY from the publisher's source
timestamps. Each stage applies ONE shared transform (same origin, same base,
same rate) to BOTH tracks. No stage may stamp a track by arrival time.**

OBS uses one shared `start_dts_offset` for audio and video, so the source ts
ARE the truth. All four recurrences (#255, #257, #354, #367) patched symptoms of
one flaw: arrival time used as content time for one track. On 2026-10-01 a
late re-subscribe got xiu's 1-GOP cache replayed as a burst. Video was stamped
by arrival (the burst collapsed to about 0 ms), audio by source ts, and the
result was a constant +1430 ms audio-late offset on YouTube. Every guard was
baseline-relative, so none of them saw it.

## The stages

- **Chunker (`flv_chunker.rs`, per-tag path in `flv_chunker_ingest.rs`,
  `src_track.rs`):** `out = src_ts - session_origin` for both tracks.
  `session_origin` is the source ts of the session's first video keyframe; on
  a GOP replay that is the cached keyframe. Audio before the origin is
  dropped. `chunk_first_ts`/`chunk_last_ts`/`duration_ms` come from VIDEO
  tags only (#146); `chunk_first_ts` is the chunk's EARLIEST video ts (a
  glitched keyframe can open a chunk). Each track's last two source ts
  classify a new one (`SrcStep`):
  - a backward step <= 1000 ms is jitter: clamped to the track's last ts;
  - a forward step > 30 s is `FarForward`: written and recorded as is, but
    the jumped tag does not extend the chunk duration (a real sustained jump
    does from the next tag on). If its successor walks back onto the old
    timeline (`AfterGlitch`), it was a lone glitch, dropped from the history;
  - a FAR backward step is only a candidate: a new publisher and a lone LOW
    glitch look the same at that tag. The tag is HELD (`HeldTag`, at most
    one). If the next tag of EITHER track is also far behind, it is a new
    timeline: flush, clear the origin AND both histories (otherwise the other
    track trips a second re-anchor), and the held tag heads the new session,
    so a new publisher's first keyframe survives. If the next tag of the
    held tag's OWN track is back on the old timeline, the held tag was a
    glitch: written clamped at the track's last ts, no re-anchor. A session
    restart drops a held tag.

  A re-anchor costs a flush plus a drop until the next keyframe, so one odd
  timestamp must never cause one. Never write a per-track self-heal again.
  A clamped tag (jitter, a glitch) is off the transform: never feed it to
  the invariant guard.
- **Receiver (`media_receiver.rs`):** one `select!` loop (`biased`, hub events
  first; the inner frame/reply waits are biased too, so a frame ready after a
  freeze beats an expired stall timer).
  - A Publish of the SAME stream at ANY phase supersedes the subscription,
    calls `start_new_session` and subscribes immediately. A DIFFERENT stream
    supersedes only a session that is not Streaming; otherwise it waits in
    `pending_publish` and is taken over (`settle()`, after every wakeup) the
    moment the live one stops streaming: stalled (RetryWait), ended or given
    up (Idle). Never orphan a live publisher, never wait out a stalled one's
    whole retry ladder.
  - Taking over a deferred Publish and looking for a Publish a lag hid are
    PROBES (`Probe { identifier, trigger }`): accept starts the session
    (audited with the trigger); a failure (rejected or timed out) only logs
    and returns to Idle: never a "connected" inpoint, never a retry ladder.
  - Every stream the receiver LEAVES without seeing it end is remembered
    (`remembered`, deduplicated, via `remember()`): a NON-streaming SESSION
    (stalled, retrying, first Subscribe in flight) a takeover or an
    `on_publish` of ANOTHER stream supersedes, an in-flight probe such a
    Publish abandons, a deferred Publish a newer one overwrites, and the
    previous last stream with a pending lag when another stream's session
    starts (`begin_session`). Seeing a stream end (publisher closed, given
    up, UnPublish) never remembers it by itself; a pending lag still can.
    Each time the receiver is Idle with no session, `settle()` probes ONE
    remembered stream (most recent first), and only then a pending lag.
    Sending any probe of a stream or starting its session forgets it. A
    stalled publisher stays registered at the hub and can resume without a
    new Publish; two keys do reach stream.lan (OBS `live/obs-e2e-test`, the
    CI ffmpeg `live/ci-e2e-test`). Each remembered entry costs at most one
    probe, and a lag at most one probe per stream it can belong to: never a
    loop. Do not patch one more path with its own flag: feed `remember()`.
    An UnPublish (streamhub 0.2.4 sends none) ends only the session of the
    stream it names and drops a matching deferral. Accepted limit: nothing
    remembered is probed while a session runs its retry ladder.
  - A successful re-subscribe after frames flowed (`dirty`) also re-anchors,
    because xiu has no session id.
  - streamhub NEVER broadcasts UnPublish; an end is a closed frame channel.
  - `Lagged` is survived and kept (`lag_unprobed`, it belongs to
    `last_identifier`): once the receiver is Idle with no session (and
    nothing remembered is left) it probes `last_identifier`. A lag while
    streaming can hide the live stream's own reconnect Publish.
    `begin_session` (the ONE place the last stream changes) settles it:
    another previous stream is remembered; for the same one the Publish or
    probe read after the lag is newer than anything it lost. Sending
    ANY probe of `last_identifier` clears it (`send_subscribe`); `settle`
    consumes it when it sends the lag probe; a lag during a probe sets it
    again. An accepted Subscribe sets it to `lagged`: a lag while that
    Subscribe was in flight (`Phase::Subscribing { lagged }`) stays set.
  - A `Closed` hub channel and a StreamsHub exit both return `Err`, so the
    orchestrator restarts.
  - Unsubscribe every dropped subscription (xiu keeps dead senders and logs
    per frame).
- **Pusher (`pusher.rs` / `state.rs`):** ONE `origin_ts` + ONE `base_ms` for
  both tracks (`PusherState::wire_ts`, used in `map_media_tag`). The origin is
  per MAPPING, never per chunk; a per-chunk rebase is the #103 click.
  - A new mapping (connect / re-anchor) sets base = max(last outputs) + 1.
    Outputs advance PER TAG (#124), so a mid-chunk re-anchor never goes
    backward.
  - It re-pins the origin with `robust_pin`: the min remaining media ts
    within `MAX_TAG_TS_JUMP_MS` of the LOCAL median (the next 9 media tags).
    A plain minimum lets one corrupt ts shift the whole chunk.
  - One shared origin makes a single bad ts dangerous, so `track_input_ts`
    clamps outliers to the track's last wire ts + 1 instead of pinning or
    re-anchoring:
    - a jumped tag the rest of the chunk does not follow;
    - a head tag of a new mapping that its neighbourhood disagrees with.

    Known limits (documented at the code, accepted): a real head cluster of
    < ~5 tags before a 30 s+ hole in a reconnect's first chunk is clamped
    too; in the chunker, TWO consecutive forward-glitched tags end as one
    re-anchor.

## The guards — two kinds, both kept

- **Absolute (`av_invariant.rs`, no baseline):** `(a_out - v_out) == (a_in -
  v_in)` within 50 ms for the latest tag of each track. It fires from the
  first chunk. On a legitimate transform change call `begin_new_transform()`
  (pusher: connect + re-anchor), and on a new session call `reset()` (chunker
  `clear_session_epoch`). Never pair samples from two transforms. The pusher
  feeds the u32 ts it actually sends.
  Edges become `AvInvariantViolated` / `AvInvariantRestored` audit rows:
  ingest through `ingest_report.rs`, push through
  `RtmpPusher::take_av_invariant_events` -> `Pushable` ->
  `endpoint_audit::emit_av_invariant_event`. Both are notifier onset /
  recovery (Discord), and the ingest one also raises the #354 banner.
- **Relative (`SkewTracker`, `ingest_skew.rs`, #257/#354/#359):** baseline
  first chunk. They catch a source-side DRIFT. A constant offset from chunk 0
  is the publisher's own and folds away BY DESIGN; a pipeline-made one is the
  absolute guard's job.

## Rate (the #135 question) — measured, not assumed

#135/#140 moved video to wall clock because the 2026-04 OBS rig ran at 0.994x
wall. On 2026-10-05 the current rig measured |src/wall - 1| <= 0.0005 %:
`av_skew_ms` of never-reconnected YT pushers stayed within ±48 ms after
2.5-3.1 h (#367, issuecomment-5993074073). So there is no rate correction.
If a future rig drifts, apply ONE common rate factor to BOTH tracks; never
go back to wall-clock for one track. Since #367 `drift_debug`
(`RUST_LOG=drift_debug=debug`) has `tag_span_ms` in the source domain, so it
measures the rate again. The default log level does not emit it.

## Tests

`flv_chunker_time_tests.rs` uses a `ManualClock` (`with_wall_clock`) to replay
arrival patterns deterministically (burst at one instant, dead air, new
publisher). `media_receiver_tests.rs` has a programmable hub (publisher slot,
accept/reject, subscribe + UnSubscribe log) driving `run()` under
`start_paused`; the takeover/probe cases live in its child
`media_receiver_takeover_tests.rs` (plus a hand-answered hub) and its child
`media_receiver_remembered_tests.rs`.

**Every paused-clock receiver test starts with `let _wd = watchdog(..)`**
(why: `.claude/rules/mutation-killable-code.md`). With the hand-answered hub
(`manual_receiver` / `accept`) the TEST sends the reply, so the receiver has
not processed it yet when the test's next line runs, and its `biased` select
handles a queued hub event FIRST: sleep a few ms after `accept(..)` before
publishing another stream, or the "deferred" Publish supersedes a stream that
is still Subscribing. The programmable hub has no such race (it replies before
it logs, and tokio runs the woken receiver first).
`rs-rtmp-push/tests/av_relation_loopback.rs` checks the WIRE relation on the
real xiu server across a re-anchor and a reconnect. The CI gate is `GATE
late-join republish keeps chunk A/V aligned (#367)` in `E2E Streaming Test`
(ffmpeg publisher only, never OBS).
