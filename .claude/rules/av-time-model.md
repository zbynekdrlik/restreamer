---
paths:
  - "crates/rs-inpoint/src/flv_chunker.rs"
  - "crates/rs-inpoint/src/flv_chunker_tests.rs"
  - "crates/rs-inpoint/src/flv_chunker_time_tests.rs"
  - "crates/rs-inpoint/src/media_receiver.rs"
  - "crates/rs-inpoint/src/media_receiver_tests.rs"
  - "crates/rs-inpoint/src/ingest_report.rs"
  - "crates/rs-rtmp-push/src/pusher.rs"
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

- **Chunker (`flv_chunker.rs`):** `out = src_ts - session_origin` for both
  tracks. `session_origin` is the source ts of the session's first video
  keyframe; on a GOP replay that is the cached keyframe. Audio before the
  origin is dropped. `chunk_first_ts`/`chunk_last_ts`/`duration_ms` come from
  VIDEO tags only (#146). A backward source jump on EITHER track re-anchors
  BOTH tracks (flush, then clear the origin AND both `last_*_src` trackers;
  otherwise the other track trips a second re-anchor). Never write a per-track
  self-heal again.
- **Receiver (`media_receiver.rs`):** one `select!` loop (`biased`, hub events
  first). A Publish at ANY phase supersedes the subscription, calls
  `start_new_session` and subscribes immediately. A successful re-subscribe
  after frames flowed (`dirty`) also re-anchors, because xiu has no session id.
  streamhub NEVER broadcasts UnPublish; an end is a closed frame channel.
  `Lagged` is survived; `Closed` returns `Err`. Unsubscribe every dropped
  subscription (xiu keeps dead senders and logs per frame).
- **Pusher (`pusher.rs` / `state.rs`):** ONE `origin_ts` + ONE `base_ms` for
  both tracks (`PusherState::wire_ts`). A new mapping (connect / re-anchor)
  sets base = max(last outputs) + 1 and re-pins the origin to the MIN of the
  chunk's remaining media tags (`media_pin_suffix`, sequence headers
  excluded). The origin is per MAPPING, never per chunk; a per-chunk rebase
  is the #103 click.

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
accept/reject, subscribe log) driving `run()` under `start_paused`.
`rs-rtmp-push/tests/av_relation_loopback.rs` checks the WIRE relation on the
real xiu server across a re-anchor and a reconnect. The CI gate is `GATE
late-join republish keeps chunk A/V aligned (#367)` in `E2E Streaming Test`
(ffmpeg publisher only, never OBS).
