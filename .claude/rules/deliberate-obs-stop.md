---
paths:
  - "crates/rs-api/src/source_end*.rs"
  - "crates/rs-api/src/obs.rs"
  - "crates/rs-delivery/src/source_end*.rs"
  - "crates/rs-delivery/src/api_source*.rs"
  - "crates/rs-delivery/src/fast_keepalive_escalation.rs"
  - "crates/rs-delivery/src/endpoint_producer.rs"
---

# A deliberate OBS stop ends the destinations, not rescue (#408)

Owner decision 2026-10-18: when the operator stops the stream in OBS (OBS stays
connected), restreamer plays the buffer out and then simply stops sending. No
outage clip and no outage alert. A crash, a lost obs-websocket or an OBS
reconnect keeps the outage path (keepalive, rescue, alerts).

## What "deliberate" is

- **The RTMP end reason cannot tell.** The ingest logs `publisher_closed` for a
  clean stop and for a drop or xiu's 2 s read timeout. xiu's `session.run()`
  even returned `Err` for a real deliberate stop (stream-pp, 2026-10-18).
- **The signal is obs-websocket `outputState`.**
  - `outputActive` alone is false for STOPPED and for RECONNECTING alike: only
    STARTED/RESUMED/RECONNECTED are active (obs-websocket
    `GetOutputStateActive`).
  - `obs::stopped_deliberately_after` sets the verdict only on STOPPED right
    after STOPPING. libobs signals "stopping" only for a user/API stop
    (`obs_output_stop`, `obs_output_force_stop`).
  - STOPPED straight from STARTED, RECONNECTED or RECONNECTING is
    `obs_output_signal_stop` with an error: an encoder failure, a link OBS will
    not reconnect, or reconnects given up. That is an outage (review of #408).
  - `ObsState::mark_disconnected` (websocket lost, `ExitStarted`) clears it.
- **The OBS stop must coincide with the ingest disconnect** (`MATCH_WINDOW`,
  10 s; `ObsState::stopped_at`). The verdict alone would stay true for hours
  after an operator stop, and the next publisher that drops (CI's ffmpeg in
  the E2E Streaming job, with OBS idle) would then be taken for a deliberate
  stop instead of an outage.

## The flow

1. **Host** (`rs-api/src/source_end.rs`, `SourceEndWatch`, 2 s tick, spawned by
   `serve`). `Tracker::due` announces when all of these hold: the event is
   delivering; the ingest has been gone for `SETTLE` (2 s); OBS is connected and
   stopped on purpose within `MATCH_WINDOW` of that disconnect; and the event has
   no pending upload.
   - The deliberate stop is LATCHED at the first tick of the disconnect: an
     operator who stops the stream and quits OBS within seconds still gets an end.
   - While a publisher is connected, the tick reads no DB at all; it only calls
     `Tracker::publisher_connected`.
   - The end is `get_last_sent_sequence_for_event`, the last chunk on S3.
   - A late chunk re-announces it. A reconnecting publisher resets the tracker.
   - A refused POST is retried next tick and writes no audit row.
   - One `source_stopped_deliberately` row is written per stop.
2. **VPS** `POST /api/source {ended_at_chunk_id}` (`api_source.rs`, behind the
   bearer gate) sets the event's `SourceEnd`. It lives on the `DiskCache`, so
   every endpoint shares it through `BufferState::with_source_end`.
   - It returns 409 `stale` when a producer already fetched a later chunk,
     because the source is live again.
   - It returns 409 `no_event` before `/api/init`.
3. **Consumer.** `keepalive_until_chunk` returns `SourceEnded` before any freeze
   or rescue once the endpoint delivered the end. It checks at entry and at the
   top of each loop, before the escalation gate.
   `source_end::idle_until_resume` then:
   - sets `delivery_mode = "ended"` and writes `endpoint_source_ended`;
   - holds the **freeze tail** (#424, `source_end_tail.rs`): 20 s of the last
     keyframe repeated (at most the live bitrate) plus AAC-LC silence, pushed in
     ~1 s segments on the live publish. YouTube drops the last ~9-14 s before an
     unpublish (also OBS-direct: camera-box A/B, VOD `wISCgipxLoo`, 14.3 s), so
     the loss falls on the padding. A chunk mid-tail cuts it and resumes. No
     tail (logged warn) without a keyframe chunk, after a codec change, or for
     audio other than AAC-LC mono/stereo;
   - `finish()`es the pusher (primary + YouTube backup);
   - waits on `rx`.
   - `endpoint_task` keeps `last_keyframe_chunk` next to `last_chunk_bytes`.

   The next chunk rebuilds the pusher (`build_endpoint_pusher`), writes
   `endpoint_source_resumed`, and runs `reset_after_rescue` (a new session).
4. **Producer.** A miss past the end sets no `chunk_gap` stall and no fast
   read-delay growth.
   - The skip-ahead probe still runs. If the end's next chunk was abandoned
     (`upload_failed_permanently`), the probe finds the chunks after it.
     Without the probe, every endpoint would wait forever (review of #408).
   - `note_fetched` on every sent chunk clears the end when a chunk past it
     arrives.
   - A buffered endpoint resumes at the live edge with no cushion. Nothing
     rebuilds it within the event: the push never runs slower than real time
     (#424). Rescue used to hold it about 120 s first.

## Mapping and alerts

- `delivery_mode "ended"` is `EndpointLifecycle::Ended`. It comes before the
  `stall_reason` check, because a refused push leaves a stale reason.
- No outage banner. The dashboard shows a grey node, the badge
  "UKONČENÉ (OBS zastavené)", and no stall text. `/status` `endpoint.state` is
  `ended` when every endpoint ended.
- `endpoint_source_ended` is an `Endpoint` scope end in `notify.rs`: the live
  publish is over.

## Limits

- The ffmpeg pusher path keeps the old behaviour (#212 removes it).
- An announcement later than the escalation (about 10 s after the last chunk on
  a fast endpoint) is not undone. Normally it lands 2-4 s after the stop.
- Box without the OBS client (`obs.enabled=false`): the outage path, as before.

## Tests

- `rs-api` `source_end_tests.rs`: the Tracker; the watch against a wiremock VPS
  on `vps_port` with `ObsClient::with_state`.
- `obs.rs`: verdict table and event sequences.
- `rs-delivery`:
  - `source_end_tests.rs`;
  - `api_source_tests.rs`;
  - `source_end_loop_tests.rs`: keepalive with a recording pusher, plus the real
    `endpoint_loop` with a growing fetcher; buffered and fast both end and resume
    with 0 `RescueActivated`.
- CI gate "a deliberate OBS stop ends every endpoint without rescue (#408)" (OBS
  job):
  - a checked Republish with 210 s of dead air, which must outlast the deepest
    cache;
  - every endpoint `ended`, then `resumed`;
  - 0 `rescue_activated`.
- Playwright `source-ended.spec.ts` (mock scenario `source-ended`).
