# Always-On Rust-Only Rescue Stream

**Status:** Design approved
**Date:** 2026-05-31
**Author:** brainstorming session
**Closes:** rescue gap discovered during 2026-05-30 stream.lan crash test

## Problem

During operator's 2026-05-30 test on stream.lan: the box crashed, cache drained on Hetzner VPS, ALL endpoints went dark, no preview anywhere. CI was green on the merged PR.

Three independent gaps caused this:

1. **No default rescue video.** All 5 templates in production DB have `rescue_video_url = NULL`. Rescue code only activates when URL is set (`endpoint_task.rs:665` `if let Some(ref rescue_url)`). NULL → no rescue ever.
2. **Producer-gone branch breaks loop with no rescue.** `endpoint_task.rs:658-660` — when the chunk supplier task disappears, consumer logs `"Consumer: producer gone, stopping"` and `break`s the loop. Endpoint dies with no fallback. **Note:** in the actual stream.lan crash scenario, producer does NOT disappear — it stays alive polling S3 and returning "no new chunks", which is the `cache drain + stalled` path (gap #1). Producer-gone fires on different scenarios (stop signal, producer panic). Fixing this branch is defensive hardening, not the primary fix for the production incident.
3. **Rescue uses external ffmpeg.** Push pipeline migrated to rust (`rs-rtmp-push`) in earlier work, but rescue still spawns `tokio::process::Command::new("ffmpeg")` (`rescue.rs:225`). Two pipelines = two failure modes.

CI green because all rescue tests set `rescue_video_url` before exercising the rescue branch. The "no URL configured" production scenario was never tested.

## Goals

- Rescue ALWAYS works, regardless of operator configuration.
- Zero ffmpeg at runtime on VPS. ffmpeg used only at one-time custom-video upload normalization on stream.lan.
- Producer-gone scenario enters rescue same as cache-drain scenario.
- CI tests both default-only path AND custom-video path.

## Non-goals

- Fast endpoints stay unprotected during outage (low-latency tradeoff; operator chose this).
- Stream.lan side recovery from its own crash is a separate problem (covered partially by outage continuity work #232).
- Operator notification on rescue activation is a follow-up (#237 if filed).

## Architecture

```
┌──────────────────────────────────┐         ┌────────────────────────────┐
│ stream.lan (Restreamer.exe)      │         │ Hetzner VPS (rs-delivery)  │
│                                  │  S3     │                            │
│ OBS RTMP → chunker → S3 upload   │ ◀────▶  │ chunk supplier task        │
│                                  │ chunks  │   ├─ produces to buffer    │
│ Custom rescue upload path:       │         │ consumer task              │
│   POST /rescue_video             │         │   ├─ pulls buffer          │
│     ffmpeg ONCE → normalize FLV  │         │   ├─ if cache drain OR    │
│     → S3 rescue-videos/<id>.flv  │         │   │  producer gone OR      │
│     → set template.rescue_video  │         │   │  warmup-no-chunks      │
│                                  │         │   │     → rust_rescue_push │
└──────────────────────────────────┘         │   │       (looped FLV)     │
                                             │   └─ when buffer refills  │
                                             │      → resume normal       │
                                             │                            │
                                             │ Embedded default FLV:      │
                                             │   include_bytes!(          │
                                             │     "default_rescue.flv")  │
                                             │   ~5s loop, ~500KB         │
                                             └────────────────────────────┘
```

ffmpeg footprint:
- stream.lan: ONLY at custom-rescue-upload normalization (existing chunker also uses ffmpeg, unchanged)
- VPS: ZERO ffmpeg (uninstalled from cloud-init)

## Components

| # | Component | Location | Responsibility |
|---|---|---|---|
| 1 | `default_rescue.flv` | `crates/rs-delivery/assets/default_rescue.flv` | ~5s FLV blob: 1080p30 H.264 still frame + silent AAC. Embedded via `include_bytes!`. ~500KB. Built once via `gen_rescue_flv` bin (output committed). |
| 2 | `rust_rescue_push` | `crates/rs-delivery/src/rescue.rs` (rewrite) | Replaces `run_rescue_loop` ffmpeg spawn. Takes FLV bytes + endpoint url + stream key. Loops bytes through `rs_rtmp_push::push_flv_bytes` until stop signal or buffer refilled. |
| 3 | `resolve_rescue_bytes(template)` | `rescue.rs` | Returns `Cow<'static, [u8]>`: if `template.rescue_video_url` set → fetch FLV from S3 once at endpoint start (cache in `Arc<Vec<u8>>`); else `Cow::Borrowed(DEFAULT_RESCUE_FLV)`. Legacy non-FLV URL → log + audit + fallback to default. |
| 4 | Producer-gone defensive fix | `endpoint_task.rs:658-660` + `endpoint_task.rs:959` | Two coupled changes. (a) Inside endpoint_task select-loop, when producer task finishes WHILE consumer still draining buffer, respawn producer from `last_delivered_chunk_id + 1` (today: endpoint tears down). (b) Consumer `recv() == None` branch: enter rescue mode instead of `break`; exit rescue when respawned producer delivers next chunk. Without (a), (b) is futile because endpoint_task observes producer-finish via select and tears down anyway. |
| 5 | Warmup branch | `rescue.rs:325-365` | Drop `if let Some(rescue_url)` guard. Always spawn `rust_rescue_push` from resolved bytes during warmup. |
| 6 | Custom upload transcode | `crates/rs-api/src/rescue_video_handlers.rs` | After upload, spawn `ffmpeg -i <input> -c:v libx264 -profile:v main -preset medium -r 30 -g 60 -b:v 1500k -c:a aac -ar 48000 -ac 2 -f flv -y <out>` ONE TIME on stream.lan. Stream output bytes to S3 as `.flv`. Reject upload if transcode/`ffprobe` validation fails. Delete input temp after success. |
| 7 | Template auto-default UI | `leptos-ui/src/components/templates.rs` | When `rescue_video_url` NULL, show "Using built-in default" hint instead of "No rescue configured" warning. |

## FLV blob content

- Resolution: 1920×1080 @ 30fps
- Codec: H.264 main profile, 2s keyframe interval, ~1500kbps
- Background: dark gray (#1a1a1a)
- Overlay text: "Stream temporarily interrupted — please wait"
- Logo: optional `crates/rs-delivery/assets/logo.png` if present; text-only otherwise
- Audio: silent AAC, 48kHz stereo, 64kbps
- Duration: 5s
- Total size: ~500KB

## Generation tool

`crates/rs-delivery/src/bin/gen_rescue_flv.rs`:

- Inputs: optional logo path, overlay text, duration, output path
- Uses ffmpeg ONCE at dev/CI time (NOT runtime, NOT embedded in shipped binary)
- Output committed to repo at `crates/rs-delivery/assets/default_rescue.flv`
- CI gate: `cargo run --bin gen_rescue_flv -- --check` recomputes hash and asserts matches committed file. Catches accidental edits / silent regeneration.

## Trigger state machine

```
                  ┌──────────────────┐
                  │ consumer running │
                  └────────┬─────────┘
                           │
        ┌──────────────────┼──────────────────┐
        │                  │                  │
   recv chunk         buffer empty       producer task
   from buffer        + stalled          channel = None
        │                  │                  │
        ▼                  ▼                  ▼
   push to RTMP    enter rescue        enter rescue
                   (was: only if URL)  (was: break loop)
                          │                  │
                          ▼                  ▼
                  ┌────────────────────────────────┐
                  │      RESCUE MODE               │
                  │  rust_rescue_push(             │
                  │    resolve_rescue_bytes(tpl),  │
                  │    ep_url, stream_key,         │
                  │    stop_when=buffer_refilled   │
                  │      OR stop_signal            │
                  │  )                             │
                  │  emit RescueActivated audit    │
                  └─────────────┬──────────────────┘
                                │
                                ▼
                  buffer_state.has_target_secs(K)
                  OR producer becomes active again
                                │
                                ▼
                       emit RescueRecovered audit
                       reset normalizer
                       resume consumer loop
```

## Test plan

### RED-first regression tests (commit BEFORE fix)

| # | Test | Asserts | File |
|---|---|---|---|
| R1 | `rescue_activates_when_url_null_and_cache_drains` | Endpoint with `rescue_video_url=None`, simulate cache drain + producer stalled → assert `delivery_mode == "rescue"` AND `RescueActivated` audit row | `crates/rs-delivery/src/rescue_tests.rs` |
| R2 | `rescue_activates_when_producer_gone` | Drop producer channel sender → assert consumer enters rescue (not break) | `crates/rs-delivery/src/rescue_tests.rs` |
| R3 | `warmup_always_pushes_default_rescue_when_no_url` | Boot endpoint with empty buffer + URL=None → assert rust pusher receives FLV bytes from `DEFAULT_RESCUE_FLV` | `crates/rs-delivery/src/rescue_tests.rs` |
| R4 | `default_rescue_flv_blob_integrity` | `DEFAULT_RESCUE_FLV.len() > 100_000 && starts_with(b"FLV")` + parses via `FlvStreamNormalizer` | `crates/rs-delivery/src/rescue_tests.rs` |
| R5 | `gen_rescue_check_matches_committed` | `cargo run --bin gen_rescue_flv -- --check` exits 0 only if committed blob matches regenerated | CI workflow step |

### E2E gate (closes the real-world gap)

| # | Spec | Scenario | File |
|---|---|---|---|
| E1 | `e2e-stream-lan-crash-rescue` | New CI job: start OBS→stream.lan→VPS→YT. Mid-stream kill `Restreamer.exe` on stream.lan via MCP. Poll VPS API: assert `delivery_mode=="rescue"` within 60s AND `last_pushed_chunk_id` keeps advancing (FLV loop pushing). Restart Restreamer.exe. Assert `RescueRecovered` audit within 180s AND normal delivery resumes. | `.github/workflows/ci.yml` new job |
| E2 | `e2e-event-without-rescue-url` | Modify existing `e2e-obs-youtube-test`: ensure event used has `rescue_video_url=NULL`. Asserts scenario still works with NO custom video configured. | Modify existing job |

### Dashboard test

| # | Test | File |
|---|---|---|
| D1 | Playwright: template editor with `rescue_video_url=NULL` shows "Using built-in default" hint, NOT a warning | `e2e/templates-default-rescue.spec.ts` (new) |

## Custom upload transcode

**Current flow** (`crates/rs-api/src/rescue_video_handlers.rs`):
- Operator uploads MP4/MOV → bytes stream to S3 as `rescue-videos/<uuid>.<ext>`
- URL saved to template/event
- At outage: VPS pulls URL, spawns ffmpeg to transcode-on-the-fly + push

**New flow:**
- Operator uploads any format → bytes stream to local temp file
- Spawn `ffmpeg -i <temp> -c:v libx264 -profile:v main -preset medium -r 30 -g 60 -b:v 1500k -c:a aac -ar 48000 -ac 2 -f flv -y <out>`
- Validate with `ffprobe -v error -i <out>` (must succeed)
- Size limit: 50MB max FLV (loops anyway, longer = waste)
- Stream `<out>` bytes to S3 as `rescue-videos/<uuid>.flv`
- Save S3 URL to template
- Reject upload if transcode/validation fails (HTTP 400 with stderr tail)
- Delete input temp after success

**Runtime fetch on VPS:**
- `resolve_rescue_bytes(template)`:
  - if URL set + ends with `.flv` → S3 GET into `Arc<Vec<u8>>` once at endpoint start, cache for endpoint lifetime
  - else if URL set + NOT `.flv` → emit `RescueLegacyFormatRejected` audit, return `DEFAULT_RESCUE_FLV`
  - else (NULL) → return `DEFAULT_RESCUE_FLV`
- S3 fetch failure → log warning + audit `RescueCustomFetchFailed` + return `DEFAULT_RESCUE_FLV`

## Migration & rollout

**DB:** no schema change. `rescue_video_url` column stays; NULL = use embedded default.

**Legacy custom URLs (existing MP4 in S3):**
- VPS rejects non-FLV URLs at endpoint start, falls back to default, emits `RescueLegacyFormatRejected` audit.
- Operator re-uploads via dashboard when convenient.
- No automatic re-transcode of existing S3 objects (out of scope, low value).

**Commit order (single PR):**

1. `feat(rescue): xtask gen_rescue_flv + commit default_rescue.flv asset`
2. `test(rescue): RED — R1..R5 regression tests fail without fix`
3. `feat(rescue): rust_rescue_push via rs-rtmp-push, replace ffmpeg spawn`
4. `feat(rescue): resolve_rescue_bytes always-Some, embedded default fallback`
5. `feat(rescue): producer-gone defensive — respawn producer + consumer enters rescue during gap (defensive, NOT primary fix for stream.lan crash)`
6. `feat(rescue): warmup always plays rescue blob`
7. `feat(api): transcode-on-upload custom rescue → FLV in S3`
8. `feat(api): reject legacy non-FLV rescue URL at endpoint start + fallback`
9. `test(ci): e2e-stream-lan-crash job + drop URL precondition from e2e-obs-youtube`
10. `feat(ui): template editor "Using built-in default" hint`
11. `chore(vps): uninstall ffmpeg from rs-delivery cloud-init`

## Risk

- Binary size +500KB (acceptable; rs-delivery binary already ~50MB)
- `rs_rtmp_push::push_flv_bytes` looping FLV must reset PTS/timestamps per loop iteration → existing `FlvStreamNormalizer` handles this; R4 test verifies
- Removing ffmpeg from VPS cloud-init breaks if any other code path still calls `ffmpeg` → grep VPS code for `ffmpeg` before step 11 (cloud-init removal)
- New CI job `e2e-stream-lan-crash` adds ~10 min to CI cycle; required tradeoff for closing the real-world gap

## Out of scope (file as separate issues only if user agrees)

- Fast endpoints rescue
- Operator notification when rescue activates (#237 candidate)
- Stream.lan local pre-cache to survive its own restart (continuity work #232 covers part)
- Streampp config still on nbg1 (unrelated discovery; needs separate config migration)
