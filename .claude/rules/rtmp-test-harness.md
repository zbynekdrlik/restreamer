---
paths:
  - "crates/rs-rtmp-push/tests/**"
---

# rs-rtmp-push wire-level test harnesses (in-process RTMP servers)

The integration tests in `crates/rs-rtmp-push/tests/` drive the real
`RtmpPusher` / `Session` client against an in-process RTMP **server**. Two
kinds exist, and picking the wrong one wastes a lot of time.

## Prefer the REAL xiu server when you can (`common/mod.rs`)

`spawn_xiu_server` / `spawn_recording_xiu_server[_at|_tls]` run xiu's real
`rtmp::rtmp::RtmpServer` + `streamhub::StreamsHub` (with
`hub.set_rtmp_push_enabled(true)`, required or the server never advances past
publish). Use these for any test of the SUCCESS path (handshake, connect,
publish accepted, media byte-fidelity, reconnect). They are byte-perfect because
they ARE the production server.

## When you must HAND-ROLL a server (rejection / error paths)

xiu's real server **cannot** produce a publish REJECTION: `ServerSession` only
ever writes `NetStream.Publish.Start`, and its `auth` hook rejects by returning
`Err` (connection close → the client sees an I/O error, not a
`NetStream.Publish.*` onStatus). So testing `PushError::PublishRejected`
(`session.rs::wait_for_publish_start`) needs a hand-rolled server —
`common/rejecting_server.rs::run_rejecting_server` is the working example (#149).

**The one gotcha that cost PR #103 its test (and #149 exists to fix): mirror
xiu's real `ServerSession` accept sequence, do not improvise.** Two things the
first (removed) hand-roll got wrong, both of which desync the client's
`ChunkUnpacketizer` ("pack error" / "none return") so it never parses your
onStatus and the test hangs to timeout:

1. **Use the complex `HandshakeServer`, NOT `SimpleHandshakeServer`, and feed
   its `get_remaining_bytes()` into the unpacketizer after `Finish`.** The
   pusher sends `SetChunkSize + connect` immediately after C2, so on loopback
   those bytes routinely coalesce into the same read that carried C2.
   `SimpleHandshakeServer` has no way to hand you those leftover bytes;
   `HandshakeServer` does (and auto-falls back to simple for the pusher's
   `SimpleHandshakeClient`). Dropping them loses the client's `connect`.
2. **Drain the unpacketizer BEFORE the next blocking `read()`.** The coalesced
   `connect` may already sit in the handshake leftover; if you block on `read()`
   first you deadlock (the client is waiting for your `_result`).

Beyond that, mirror `server_session.rs`: on `connect` →
`write_window_acknowledgement_size` + `write_set_peer_bandwidth` +
`NetConnection::write_connect_response`; on `createStream` →
`write_create_stream_response(&tid, &STREAM_ID)`; echo the client's transaction
id (`amf_u8` matches it). Reuse xiu's constants from `rtmp::session::define`.

**`read_chunks()` result semantics (don't guess):** it returns `Ok(Chunks)` on
success, `Err(EmptyChunks)` for need-more-bytes (break → read more), and a
sticky `Err(CannotParse)` on genuine desync. Match `CannotParse` explicitly and
FAIL LOUD (return an error) — swallowing it lets a harness desync masquerade as
the production death-loop your test's timeout message warns about.

## Test-file conventions

- `common/mod.rs` and its submodules carry `#![allow(dead_code)]` — each test
  binary (`local_xiu_loopback`, `local_tls_loopback`, `fb_mock_server`) uses only
  a subset, so per-binary unused warnings are expected. A new submodule needs its
  OWN `#![allow(dead_code)]`, and do NOT `pub use`-re-export a helper only one
  binary calls (it trips `-D unused-imports` in the others) — call it by module
  path.
- New spec in `local_xiu_loopback.rs`: bind the `TcpListener` BEFORE
  `tokio::spawn(server)` — the kernel backlog queues the client SYN, so no
  startup sleep is needed (unlike the helpers that bind inside the task).
- 1000-line CI cap applies to `tests/` files too: `common/mod.rs` is large —
  put a new harness in its own `common/<name>.rs` (`pub mod <name>;`).
