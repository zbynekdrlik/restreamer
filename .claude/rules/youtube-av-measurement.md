---
paths:
  - ".github/workflows/ci.yml"
  - "e2e/**"
  - "scripts/ci/**"
  - "crates/rs-inpoint/src/media_receiver.rs"
  - "crates/rs-inpoint/src/flv_chunker*.rs"
  - "crates/rs-api/src/av_gate*.rs"
  - "crates/rs-youtube/src/manage*.rs"
---

# YouTube-measured A/V + frame-continuity session (the #357 gate, done by hand in #367)

## The session API automates the YouTube leg (#357 part 1)

Steps 1-3, 7 and 8 of the hand recipe below are now `/api/v1/av-gate/session` inside
Restreamer (`crates/rs-api/src/av_gate*.rs`). Both release gates (restreamer CI and
camera-box) call it; the caller still drives OBS, the recording, the trigger and the
measurement.

- `POST /api/v1/av-gate/session` `{"requester": "...", "title"?: "..."}` -> `201
  {session_id, broadcast_id}`. `409 {error:"busy", holder:{session_id, requester}}` while
  another session holds the rig; `429 quota` over the rolling-24 h budget
  (`av_gate.daily_quota_budget`, default 4000, ~400 per session) and `429 project_quota`
  when the project bucket shared with the health polling has < 400 left; `409
  {error:"cleanup_pending", sessions}` while an earlier teardown is being retried; `502
  {session_id, state:"failed", reason}` when a start step failed (already torn down);
  `503 not_provisioned` without a usable token or oauth file; `503 starting_up` until
  the boot reconcile ran. The start runs in its own task, so a client that drops the
  POST does not abort it: the session runs on (and is reaped); the 409 names its id.
- `GET /api/v1/av-gate/session/{id}` -> `{session_id, state, requester, title,
  broadcast_id, vod_id, reason, quota_units, cleanup_pending, timestamps{created, ready,
  stop_requested, processing, finished}}`. Poll it: `starting` -> `ready` (delivery delivering + stream
  `active` + broadcast `live`) -> after stop `processing` -> `done` (`vod_id` = the
  broadcast id) or `failed` (`reason`).
- `POST /api/v1/av-gate/session/{id}/stop` -> `202` (handed to the session); `200` when it
  is already past the stop (a retry is harmless); `409` for an unfinished row nobody
  runs (the boot reconcile owns it). The stop drains the event cache (`cache_delay_secs`
  + 15 s) BEFORE completing the broadcast, so `done` arrives ~2.5 min + YouTube's
  processing after the stop.
- `GET /api/v1/av-gate/status` -> `{reconciled, holder, cleanup_pending: [ids]}`: why a
  POST would be refused. `POST /api/v1/av-gate/session/{id}/clear-cleanup` drops a
  pending cleanup WITHOUT retrying it (audited, `reaped.cause = operator_clear`): the
  way out when a cleanup can never succeed (a broken oauth file, a VPS deleted by hand).
  Whoever clears it owns what may still be live or billing.
- **Auth:** LAN origin only (a tunneled request is refused even with an Access JWT)
  AND `Authorization: Bearer <token>` where the token is the content of
  `av_gate.api_token_file` (default `C:\ProgramData\Restreamer\av-gate\api-token`,
  >= 32 chars, not in `config.json`). No file = the API answers 503. The old
  `api.diag_token` the design mentioned no longer exists (#336/#273).
- **Cleanup on every exit path:** a failed start step, a readiness failure, a stop
  before ready, the idle reaper (`av_gate.idle_timeout_secs`, default 45 min from
  creation), a dead driver task, and the boot reconcile (`starting`/`ready` rows are
  torn down and failed, a `processing` row resumes its VOD wait). Teardown = complete
  the broadcast if a live transition was ever attempted (`went_live` is persisted
  BEFORE the call), `stop-stream` the event, then poll Hetzner
  (`app=restreamer,client_uuid=<box>,event_id=<id>`) until 0. Each half that succeeds is
  recorded (`broadcast_done`, `event_stopped`, `event_done`) and never repeated. A teardown with a
  problem sets `cleanup_pending`; the maintenance task (`run_av_gate_maintenance`:
  a supervisor around the loop, spawned by the runtime after the delivery reconcile)
  retries only the missing step with backoff (5 min doubling, 2 h cap). A stop that
  never succeeded is retried (the active event is still ours); **once the stop
  succeeded, an event that is active again belongs to another run**: the retry neither
  stops it nor waits for its servers (anything of ours left has no live delivery row,
  so the #352 orphan reaper deletes it). `live` is only sent after `went_live` was
  saved. A force-clear during a retry round gets `409 retry_running`. Audit rows:
  `av_gate_session_{started,ready,stop_requested,processing,done,failed,reaped}`
  (`reaped.cause`: `idle_timeout`, `boot_reconcile`, `driver_died`, `cleanup_retry`,
  `operator_clear`).
- **Safety:** the rig refuses while ANY event is active, the CI event included (another
  run owns it: restreamer's own CI E2E uses `E2E-Test` too), and without a Hetzner
  token, both before any YouTube write. It uses the dashboard's own
  `start_stream`/`stop_stream` (single-event guard, #354 skew gate), binds only a
  reusable stream, and never touches OBS. The event id is persisted before the start,
  and the event is re-checked inactive right before it is started. Admission claims the
  slot first, then checks pending cleanups and quota.
- **Quota:** the manage client draws from the same project bucket as the health
  polling (`delivery_status::youtube_quota_tracker`), on top of the av-gate budget.
  Completing a broadcast and reading its life cycle are charged without being refused
  (`QuotaTracker::charge`, the bucket goes into debt): a broadcast must never stay live
  because the health polling used the quota.
- **VOD done** = `processingDetails.processingStatus == succeeded` OR
  `status.uploadStatus == processed` (both read: which one a live archive reports is
  undocumented). UNVERIFIED against the real channel until part 2's first run.
- **Config** (`av_gate`, immutable through `PATCH /config`): `oauth_file`,
  `api_token_file`, `event_name` ("E2E-Test"), `stream_title` ("e2e rtmp"),
  `idle_timeout_secs`, `processing_timeout_secs` (2 h), `daily_quota_budget`.
- **Testing:** the session tests use a scripted rig (`FakeRig`) and a stateful wiremock
  YouTube (`fake_youtube` in `av_gate_driver_tests.rs`; a transition updates the
  life cycle the next read returns), split into driver / create / stop / cleanup test
  files. A test that needs a small project bucket sets `TestSeam.quota_bucket` to its
  own static tracker: draining the process-wide one breaks every parallel test.
  Read the DB BEFORE aborting a spawned maintenance task: an abort mid-query can leave
  the single in-memory SQLite connection unusable. The production rig is tested on its own against the
  real DB + stream handlers + a wiremock Hetzner. `ManageClient` takes its endpoints as
  constructor arguments, so no process-global URL override and no test lock.

The only proof that A/V sync and frame continuity hold is a measurement ON YOUTUBE: the VOD against stream
OBS's own program recording. Restreamer telemetry (`av_skew_ms`, chunk relations) cannot see
YouTube's re-encode. On 2026-10-05/06 this caught arrival-time stamping that caused ~25 dup/skip
per 17 min and a 0.85-1.4 s offset. The recipe below is what #357 automates.

## Roles
- **camera-box session** owns stream OBS: StartRecord/StopRecord, any OBS restart (clean close
  = WM_CLOSE, then the canonical launcher), and the analysis (painter-tick QR per frame +
  QPSK `--av-sync`, recording vs VOD). Restreamer may only Start/Stop OBS **streaming**
  (owner directive 2026-08-30). Scene "PRO" is never selected by anyone.
- **restreamer** owns the broadcast, the E2E-Test event (id 9278, endpoint `e2e rtmp` = 26),
  the trigger, and the cleanup.

## YouTube access (CI channel "Zbynek Drlik", owner-approved 2026-10-05 on #357)
- Restreamer's own `youtube_oauth` grants are **`youtube.readonly` only**, so `liveBroadcasts.insert`
  returns 403 with them.
- The grant-1 client is NOT a device-flow client (`invalid_client`). The device-flow client is
  `config.youtube.device_flow` in `C:\ProgramData\Restreamer\config.json`.
- A manage-scope (`https://www.googleapis.com/auth/youtube`) refresh token, obtained by owner
  device-flow consent, lives ONLY on stream.lan at `C:\ProgramData\Restreamer\av-gate\oauth.json`
  (ACL SYSTEM + Administrators). Never print it, never copy it off the box.
- Helper: `. C:\ProgramData\Restreamer\av-gate\yt.ps1`, then `Invoke-Yt <METHOD> <url> [body]`
  (it refreshes the access token on every call).
- The stream titled "e2e rtmp" (look its id up via `liveStreams?mine=true`) is `isReusable=true`. Bind a
  **fresh** broadcast to it each run, and leave the CI's own testing broadcasts untouched.

## Session script (UTC times matter — send every timestamp to camera-box)
1. `liveBroadcasts.insert`: privacy unlisted, `enableAutoStart/Stop=false`,
   `monitorStream.enableMonitorStream=false`, then `liveBroadcasts/bind`.
2. `POST /api/v1/events/9278/start-stream` (the VPS boots in ~2 min). Tell camera-box "start recording"
   and wait for its confirmation, then `POST /api/v1/obs/start-stream`.
3. When `liveStreams` reports `streamStatus=active`: `liveBroadcasts/transition?broadcastStatus=live`.
4. ~6 min steady (the A window).
5. **Thursday trigger:** suspend `Restreamer.exe` for 35 s (`NtSuspendProcess`/`NtResumeProcess`
   P/Invoke, resume in `finally`). Suspending the PUBLISHER never works: xiu cuts a silent
   publisher after ~2 s, so the 30 s stall path is never reached.
6. Fresh publish ~45 s later: either `/obs/stop-stream`, 15 s pause, `/obs/start-stream`, or a camera-box
   clean OBS restart followed by our `/obs/start-stream` (Thursday's exact shape).
7. ~6 min steady (the B window), then `/obs/stop-stream`. **Wait ~135 s** (the 120 s event cache drains
   to YouTube) before `transition?broadcastStatus=complete`.
8. Cleanup: `POST /api/v1/delivery/stop {"event_id":9278}`, `POST /events/9278/deactivate`, then confirm 0
   Hetzner servers via the label selector (see `hetzner-delivery-vps.md`).

## Pass criteria (camera-box issue 1404 method)
- A/V (video minus audio, VOD minus recording) must be **consistent within the session**: every window within
  150 ms of that session's pre-stall value. YouTube's fixed term varies per encode session
  (+42 ms vs -20 ms vs +5 ms), so never compare against a constant.
- 0 downstream dup/skip in the steady windows. Rig-side repeats, which also appear in the
  recording, cancel out.
- No late join: the VOD resumes within ~0.1 s of the publish.

## Log signatures (`C:\ProgramData\Restreamer\restreamer.log`)
- Fixed (0.29.28+): `Stream ended ... reason="superseded_by_publish"` -> `start_new_session` ->
  `Subscribed` within ~1 ms of `NetStream.Publish.Start`, plus `process stall: class=whole_process`.
- Broken (<= 0.29.27): `self-healing audio session origin` + `ingest A/V skew DETECTED ~43000 ms`,
  then on republish a stale `Stream published` + `Hub rejected subscription` x3 + a late
  `Subscribed`.
