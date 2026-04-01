# E2E Network Disconnect & Cache Reliability Tests

**Date:** 2026-04-01
**Trigger:** Manual cable disconnect tests revealed cache bar, auto-restart, and buffer fill bugs that no E2E test caught

## Problem

All existing cache/prediction E2E tests are CSS validation against a mock API. They don't test the real backend. Manual testing found:
1. Cache bar frozen for 20s during buffer fill, then jumps
2. Cache shows 70s when timer shows 2:30 (inconsistent)
3. VPS destructively auto-restarted when stream.lan lost internet (now fixed, but no regression test)
4. Cache delay stops at ~180s instead of filling to configured 300s
5. No test verifies cache drain during real network disconnect
6. No test verifies recovery after cache hits 0

## Design: 5 New CI Steps

All steps run in the existing `e2e-obs-youtube-test` CI job on stream.lan with real OBS → Restreamer → Hetzner VPS.

### Step 1: Buffer fill progress is monotonic

**Insert after:** "Wait for delivery server ready"
**Insert before:** "Verify delivery progression"

Poll `/api/v1/delivery/status` every 5s for 40s during buffer fill. Record `chunk_delay_secs`. Assert each reading >= previous reading (monotonically increasing). Fails if cache jumps backward or stays frozen.

### Step 2: Session timer vs cache delay consistency

**Insert with:** Step 1 (same step, additional assertion)

During buffer fill, also read `session_start` from PipelineState via WebSocket status. Compute elapsed time from session start. Compare with `current_delay_secs` from the S3 fallback. The cache delay during fill should be within 30s of elapsed time (chunks upload in real-time, so S3 buffer ≈ elapsed time, capped at 2x target).

### Step 3: Custom cache delay (300s) fills correctly

**Insert after:** "Verify cache delay meets target"

Before the test starts, the event's `cache_delay_secs` should be set. The existing "Verify cache delay meets target" step checks against target. Add an assertion that the ACTUAL filled delay is at least 80% of the configured target. For 300s target, cache must reach 240s before endpoints stream.

Actually — the existing test already does this at line 2219: `$minAcceptable = [math]::Floor($targetDelay * 0.8)`. But the event is created with default 120s. To test 300s, we need to either:
- Set `cache_delay_secs: 300` on the event during the test
- Or add a separate step that patches the event config

Since the user's production event uses 300s, we should test with 300s. Modify the event creation/activation step to set `cache_delay_secs: 300`.

### Step 4: Simulated network disconnect → cache drains → recovery

**Insert after:** OBS disconnect/reconnect resilience test
**Insert before:** "Stop OBS stream" cleanup

Use Windows Firewall to block outbound to S3 (`eu-central-1.linodeobjects.com`) AND VPS IP. This simulates cable disconnect:
1. Record current delivery state
2. Block S3 + VPS via firewall
3. Wait — verify prediction mode activates (`predicted: true` from pipeline state via API)
4. Wait until cache drains below 50% of target
5. Remove firewall rules
6. Wait 90s for recovery
7. Verify `predicted: false`, delay recovering
8. Verify VPS instance ID unchanged (no auto-restart)
9. Verify delivery still active

### Step 5: VPS instance stability assertion

**Already partially done** in the OBS disconnect test. Strengthen: capture instance ID at the START of the entire test, verify it hasn't changed at the END (across all disruption steps).

## Files Changed

| File | Change |
|------|--------|
| `.github/workflows/ci.yml` | Add 4 new steps to e2e-obs-youtube-test job |

## Hard Fail Policy

Every assertion uses `throw` on failure. No informational-only steps. No `continue-on-error`. If any cache/delay/recovery check fails, CI is red.
