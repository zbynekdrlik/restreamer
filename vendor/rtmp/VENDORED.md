# Vendored xiu `rtmp` 0.6.5

This is a copy of crates.io `rtmp` 0.6.5 (xiu, https://github.com/harlanc/xiu).
It is licensed MIT, as its `Cargo.toml` states. The workspace root and
`src-tauri/Cargo.toml` both pull it in through `[patch.crates-io]`. It is
excluded from the workspace, so `cargo clippy --workspace` does not lint it.

## Why (#368)

xiu gives every RTMP subscriber an unbounded frame queue. A local player that
reads slower than real time made that queue grow to 8 GB on stream.lan; the
player in question was the operator's VLC on `rtmp://127.0.0.1:1234/live`.
Freeing the queue when the player closed stalled the ingest for up to 7 s,
and OBS dropped 117 frames on 2026-10-18. The queue sits in `Common`'s private
`data_receiver`, so there is no way to bound it from outside the crate.

## The only changes from 0.6.5

- `session/common.rs`: `Common` gets `pub play_queue_limit: Option<usize>`,
  which defaults to `None` (upstream behaviour). In `send_channel_data`, a
  play session with more frames than that still queued ends with
  `SessionErrorValue::PlayQueueOverflow`. It then takes xiu's own error path:
  unsubscribe, then the session ends.
- `session/common.rs`: when `play_queue_limit` is set, every play write is
  bounded by `PLAY_WRITE_STALL` (10 s), and a timeout ends the session with
  `SessionErrorValue::PlayerStalled`. A player that stops reading entirely
  blocks the write forever, so the loop never gets back to the queue check
  while the hub keeps queueing.
- `session/common.rs`: `subscribe_from_stream_hub` records how many frames
  were already queued when the subscription was answered (`play_replayed`).
  The hub replays the GOP so far BEFORE it answers, and those frames count on
  top of the limit. Without this, a long GOP cut every new player on its
  first frame.
- `session/errors.rs`: the `PlayQueueOverflow { queued, limit }` and
  `PlayerStalled { secs }` variants.
- The crate's `rtmp_specification_1.0.pdf` is left out.
- `Cargo.toml`: `[lints.rust] non_local_definitions = "allow"`. failure's derive
  trips that lint, and cargo shows a path dependency's warnings, unlike a
  registry one's.

Restreamer's `RtmpServer` sets the limit on every session it accepts
(`crates/rs-inpoint/src/rtmp_server.rs`). Search for `restreamer #368` to
find every changed line. To upgrade xiu, re-apply these lines on the new
version, or drop the vendor copy if upstream bounds the queue itself.
