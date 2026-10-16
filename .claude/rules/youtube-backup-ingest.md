---
paths:
  - "crates/rs-delivery/src/backup_mirror.rs"
  - "crates/rs-delivery/src/backup_mirror_tests.rs"
  - "crates/rs-delivery/src/pushable.rs"
  - "crates/rs-delivery/src/endpoint_consumer_helpers.rs"
  - "crates/rs-delivery/src/endpoint_rtmp_url.rs"
  - "crates/rs-delivery/src/rust_rescue_push.rs"
  - "crates/rs-delivery/src/rescue.rs"
  - "crates/rs-rtmp-push/src/error.rs"
  - "crates/rs-rtmp-push/src/session_read_loop.rs"
---

# YouTube push-death survival: backup ingest, re-send, fast reconnect (#368)

Owner decision 2026-10-16: an occasional ~3 s viewer-visible gap when one
YouTube ingest session drops is NOT an acceptable baseline. Before #368 a
push death (`Broken pipe` after 44 min, 2026-10-15) closed the session, slept
the 3 s `RemoteClosed` floor and moved on to the NEXT chunk: ~3.2 s gap plus
a ~2 s content hole. Three layers fix it; keep all three.

## 1. Backup ingest mirror (`backup_mirror.rs`)

- Every YT_RTMP endpoint also streams to `rtmp://b.rtmp.youtube.com/live2?backup=1/<key>`
  (`build_backup_rtmp_url`, same key). YouTube rolls viewers over to the
  backup when the primary session stops.
- **`build_endpoint_pusher` is the ONE place a live or rescue pusher is built**
  (consumer, outage/defensive rescue reconnect, every rescue/warmup clip
  push). A new pusher site that calls `RtmpPusher::new` directly streams
  without the backup: `every_rust_pusher_is_built_by_the_one_factory` scans
  the production sources and fails.
- `TeePusher` offers each payload to the mirror with a non-blocking
  `try_send` (bounded queue of `BACKUP_QUEUE_CHUNKS` whole chunks; full =
  drop the NEW chunk, counted). A hung, slow or dead backup can never delay or
  fail the primary. The backup has its own pusher, 30 s write timeout and the
  primary's `reconnect_delay_ms` policy over its own streak.
- **`close()` closes ONLY the primary**: it is the primary's reconnect hook
  after a push error, and the backup must stay up exactly then.
  `shutdown()` closes both and awaits the backup task; the rescue paths use
  it before building the next pusher on the same key (no overlap). Dropping
  the tee aborts the backup task.
- `repush_flv_bytes` (the re-send below) goes to the primary only: the backup
  already has that chunk.
- A backup death writes `EndpointBackupPushDied` (`detail.ingest =
  "youtube_backup"`), never `EndpointRtmpPushDied`: the outage notifier
  ignores it and the CI death gates query `endpoint_rtmp_push_died` only.
  The backup's own A/V invariant edges are drained, not audited (the
  push-stage invariant episode is keyed per endpoint and belongs to the
  primary).
- State on VPS `/api/status` as `endpoints[].backup_ingest {connected,
  reconnect_count, chunks_pushed, dropped_chunks, last_error}` (omitted
  without a backup). The host maps VPS fields one by one, so it is NOT shown
  on the dashboard yet.
- Kill switch: host `delivery.youtube_backup_ingest` (default true) is sent
  per endpoint as `youtube_backup_ingest` (`build_vps_endpoint_entry`, init
  AND add); a VPS payload without it keeps the backup ON.
- Limit: one VPS feeds both ingests (no protection against a VPS loss), and
  the backup is fed at the primary's push cadence, so a primary stuck in a
  30 s write timeout starves the backup too.
- CI: the sustained YT gate asserts `e2e rtmp` backup connected, >= 80 % of
  the primary's chunks pushed in the window, and no `*Mismatch`
  configuration issue (pinned by a test-integrity step).

## 2. Re-send the failed chunk once (`endpoint_consumer_helpers::push_chunk`)

A push that failed in a way that may have lost its chunk (`may_lose_chunk`:
transport + connect failures, and the write-timeout arm) is pushed AGAIN on
the fresh session, at most once. Never after `AvSkewExceeded` (raised after
the whole chunk went out), `MalformedInput` or a stop. The flag rides the
telemetry of the next connect (`RtmpPushTelemetry::after_failure`). The
pusher's new mapping (base = last output + 1) keeps the wire monotonic.

## 3. Fast first reconnect (`rs_rtmp_push::reconnect_delay_ms`)

0 ms for the FIRST `RemoteClosed` (`consecutive_errors == 1`) after >= 60 s of
healthy push (time since the connect's first pushed chunk); a publish/connect
rejection right after such a fast retry gets one 3 s retry, not the 30 s
floor. Everything else keeps the old ladder; the #236 dead-target 30 s
override still applies.

## RTMP ping + ack (`session_read_loop.rs`)

xiu 0.6.5 `parse_event` knows UserControl events 0/3/4 only, so the read loop
reads a PingRequest (event 6) from the raw chunk first and answers with a
PingResponse carrying the same timestamp. Incoming Acknowledgements land in
`UpstreamSignals`; `handle_rust_push` copies the last one into the telemetry,
so the death row's `time_since_last_upstream_ack_ms` is measured. The read
loop inherits the negotiation's unpacketizer (server chunk size).
Wire test: `tests/upstream_control_loopback.rs` with the hand-rolled
`common/control_server.rs`.
