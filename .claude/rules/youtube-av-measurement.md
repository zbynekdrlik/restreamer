---
paths:
  - ".github/workflows/ci.yml"
  - "e2e/**"
  - "scripts/ci/**"
  - "crates/rs-inpoint/src/media_receiver.rs"
  - "crates/rs-inpoint/src/flv_chunker*.rs"
---

# YouTube-measured A/V + frame-continuity session (the #357 gate, done by hand in #367)

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
