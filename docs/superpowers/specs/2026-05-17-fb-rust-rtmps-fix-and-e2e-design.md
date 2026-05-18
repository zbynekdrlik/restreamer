# FB rust RTMPS push fix + CI E2E gate — Design

**Date:** 2026-05-17
**Status:** Approved
**Refs:** Issue #215, PR #211, #212, #213, #214

---

## Goal

Make `rs-rtmp-push` deliver to Facebook Live ingest (`rtmps://live-api-s.facebook.com:443/rtmp/<key>`) without rejection. Gate the fix with a CI E2E job that pushes to a real FB stream key and asserts publish success.

After this PR, FB endpoints can run on `pusher='rust'` (matching the YT path) and any future regression in the rust handshake is caught in CI before reaching production.

---

## Problem statement

Issue #215 captured: rust pusher's RTMP `NetConnection.connect` is accepted by FB ingest, but `NetStream.publish` is immediately rejected with:

```
NetStream.Publish rejected: NetStream.Publish.Failed - Publish Rejected: Invalid URL
bytes_sent_since_connect: 0
chunks_pushed: 0
```

Migration v28 (PR #211) silently flipped all `service_type='FB'` rows from `pusher='ffmpeg'` to `pusher='rust'` as part of the YT-BB fix. FB broke for every endpoint. Workaround on 2026-05-17 reverted FB rows to ffmpeg on streamsnv + streampp via direct SQL.

Today, the same operator reported live-event failure on streampp: all YT endpoints reported `bad` health mid-event. That is a separate regression — different subsystem (sustained delivery / health probe), different code path. Filed as a separate issue at end of this spec. **Not in scope here.**

---

## Root-cause candidates (concrete)

Code recon in `crates/rs-rtmp-push/src/session.rs`:

1. **`tc_url` includes default port (`:443`)**: `tc_url = format!("{scheme_str}://{raw_domain}/{app}")` where `raw_domain = "live-api-s.facebook.com:443"`. Result: `rtmps://live-api-s.facebook.com:443/rtmp`. FFmpeg + libobs emit the host without port suffix when port equals scheme default. FB validates `tcUrl` strictly. **Strongest candidate.**
2. **Missing `swfUrl` + `pageUrl` AMF fields**: ConnectProperties currently sets `flashVer`, `fpad`, `capabilities`, `audioCodecs`, `videoCodecs`, `videoFunction`, `objectEncoding`, `tcUrl`, `app`, `pubType`. libobs additionally sets `swfUrl` and `pageUrl` on every connect — some FB ingest paths check them.
3. **Unknown — captured by diagnostic dump**: full outgoing AMF map logged at `tracing::debug!` for any remaining gap, recoverable from CI E2E logs.

---

## Architecture

Three coupled changes:

### A. Fix `tc_url` construction

In `crates/rs-rtmp-push/src/session.rs` `negotiate()`:

- New helper `fn build_tc_url(scheme: Scheme, host: &str, port: u16, app: &str) -> String` that omits `:port` when `port` equals the scheme default (`443` for rtmps, `1935` for rtmp), retains `:port` otherwise.
- Replace the inline `format!("{scheme_str}://{raw_domain}/{app}")` with the helper. Pass `host` + `port` (already in scope at this point — currently joined into `raw_domain`).

Effect for FB: `tc_url = "rtmps://live-api-s.facebook.com/rtmp"`.
Effect for YT: `tc_url = "rtmp://a.rtmp.youtube.com/live2"` (unchanged — default port 1935).

### B. Add `swfUrl` + `pageUrl` to ConnectProperties

Two new lines in the connect builder block (after `props.tc_url = ...`):

```rust
props.swf_url = Some(format!("{scheme_str}://{host}/{app}"));
props.page_url = Some(format!("{scheme_str}://{host}/{app}"));
```

Same host/app, no port suffix. libobs uses identical values for FB. No behavior change for YT (YT ignores both fields).

### C. Diagnostic AMF dump

Add `tracing::debug!` immediately before `nc.write_connect(...)`:

```rust
tracing::debug!(
    target: "rs_rtmp_push::connect",
    ?props,
    host = %host,
    port = port,
    app = %app,
    "sending NetConnection.connect"
);
```

`ConnectProperties` already implements `Debug`. The log line surfaces every field rust sends — ground truth for diffing against ffmpeg / libobs if FB still rejects.

### D. Migration v29

In `crates/rs-core/src/db/migrations.rs`:

```rust
async fn migrate_v29(tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>) -> sqlx::Result<()> {
    let result = sqlx::query(
        "UPDATE endpoint_configs SET pusher='rust' \
         WHERE pusher='ffmpeg' AND service_type='FB'"
    )
    .execute(&mut **tx)
    .await?;
    let rows = result.rows_affected();
    if rows > 0 {
        tracing::info!(rows_affected = rows, "v29: flipped FB endpoints ffmpeg→rust");
    }
    Ok(())
}
```

`MAX_SCHEMA_VERSION` bumps 28 → 29. Idempotent (matches rows still on ffmpeg only). Runs automatically on next deploy of both production machines.

### E. Backwards compatibility

`PusherKind::Ffmpeg` enum variant + ffmpeg subprocess code path stays in place. No removal in this PR. Hard removal in follow-up #212, gated on:

- Issue #213 (≥4h sustained-stability soak) green
- 14 days of zero `rtmp_push_died` events on FB + YT in production audit

If A+B fix is incomplete and FB rejects from another cause, operator can revert FB to ffmpeg via SQL hot-patch (5-second fix). Code path remains compiled.

---

## CI E2E gate

### New job `e2e-fb-push`

Added to `.github/workflows/ci.yml` as peer of `e2e-streaming` and `e2e-obs-youtube`:

```yaml
e2e-fb-push:
  name: E2E FB RTMPS Push (rust pusher)
  runs-on: ubuntu-latest
  needs: rust-ci
  if: ${{ needs.rust-ci.result != 'failure' }}
  timeout-minutes: 8
  env:
    FB_TEST_STREAM_KEY: ${{ secrets.FB_TEST_STREAM_KEY }}
  steps:
    - uses: actions/checkout@v4
    - name: Install ffmpeg + sqlite
      run: sudo apt-get update && sudo apt-get install -y ffmpeg sqlite3
    - name: Download rs-delivery + restreamer binaries from rust-ci
      uses: actions/download-artifact@v4
      with:
        name: restreamer-linux
        path: ./bin
    - name: Start restreamer on 127.0.0.1
      run: |
        chmod +x ./bin/restreamer
        ./bin/restreamer --headless &
        for i in {1..30}; do
          curl -sf http://127.0.0.1:8910/api/v1/status && break
          sleep 1
        done
    - name: Configure FB endpoint with rust pusher
      run: |
        curl -X POST http://127.0.0.1:8910/api/v1/endpoints \
          -H 'Content-Type: application/json' \
          -d "{\"label\":\"fb-ci-test\",\"service_type\":\"FB\",\"stream_key\":\"$FB_TEST_STREAM_KEY\",\"pusher\":\"rust\",\"enabled\":true}"
    - name: Push 60s test stream
      timeout-minutes: 3
      run: |
        ffmpeg -re -f lavfi -i 'testsrc2=size=1280x720:rate=30' \
               -f lavfi -i 'sine=frequency=440' \
               -c:v libx264 -preset veryfast -tune zerolatency -b:v 2500k -g 60 \
               -c:a aac -b:a 128k -ar 44100 -ac 2 \
               -t 60 -f flv rtmp://127.0.0.1:1935/live/CI
    - name: Assert FB push succeeded
      run: |
        # Wait 5s for final stats to settle
        sleep 5
        STATUS=$(curl -s http://127.0.0.1:8910/api/v1/endpoints/status | jq '.endpoints[] | select(.label=="fb-ci-test")')
        echo "$STATUS"
        ALIVE=$(echo "$STATUS" | jq -r '.alive')
        CHUNKS=$(echo "$STATUS" | jq -r '.chunks_pushed')
        BYTES=$(echo "$STATUS" | jq -r '.bytes_sent_since_connect')
        DIED=$(curl -s 'http://127.0.0.1:8910/api/v1/audit?action=rtmp_push_died&label=fb-ci-test' | jq '.events | length')
        [ "$ALIVE" = "true" ] || { echo "FAIL: endpoint not alive"; exit 1; }
        [ "$CHUNKS" -gt 30 ] || { echo "FAIL: chunks_pushed=$CHUNKS (expected >30)"; exit 1; }
        [ "$BYTES" -gt 1000000 ] || { echo "FAIL: bytes_sent=$BYTES (expected >1MB)"; exit 1; }
        [ "$DIED" = "0" ] || { echo "FAIL: $DIED rtmp_push_died events"; exit 1; }
        echo "PASS: chunks=$CHUNKS bytes=$BYTES died=$DIED"
    - name: Teardown
      if: always()
      run: |
        curl -X DELETE http://127.0.0.1:8910/api/v1/endpoints/fb-ci-test || true
```

### Wire into `e2e-gate`

Add `e2e-fb-push` to the `needs:` array of `e2e-gate`. Use `!= 'failure'` matching the existing YT pattern (per `feedback_ci_live_events.md` + project CLAUDE.md).

### Three assertion layers

1. **NetStream.Publish.Start ACK**: `alive=true` implies `rtmp_push_started` audit event was emitted on receipt of `_result publish` from FB.
2. **Sustained bytes flowing**: `chunks_pushed > 30` (with target 30fps + 2s chunks = 30 chunks ≈ 60s push) and `bytes_sent_since_connect > 1_000_000` (1 MB minimum).
3. **Zero death events**: `audit` query for `rtmp_push_died` with `endpoint=fb-ci-test` returns empty.

If any of the three fail, CI fails with the exact failing assertion logged.

### Secret setup

Operator does ONE-TIME:

1. Create dedicated FB Page (or use existing test Page).
2. Open Live Producer → Streaming Software → Persistent Stream Key → Copy.
3. `gh secret set FB_TEST_STREAM_KEY --body "<key>"`

Per `feedback_fb_keys_persistent.md`: keys are Always Active, no rotation needed.

### Local mock RTMP test (unit-speed gate)

New `crates/rs-rtmp-push/tests/fb_mock_server.rs`:

- Mock RTMP server that on `NetConnection.connect` reads the AMF object.
- Asserts `tcUrl` does NOT contain `:443` or `:1935` (default ports).
- Asserts `swfUrl` and `pageUrl` are present.
- Sends `_result` if all checks pass, sends `_error "Publish Rejected: Invalid URL"` otherwise.
- rs-rtmp-push pushes against the mock; test expects success.

Catches the FB regression in `rust-ci` job in <5 seconds, no external dependency. Complements (does not replace) real-FB E2E.

---

## Data flow

```
[CI fixture: ffmpeg testsrc] ──RTMP──▶ [restreamer 127.0.0.1:1935]
                                              │
                                              ▼
                                       [FLV chunker]
                                              │
                                              ▼
                                       [Disk cache]
                                              │
                                              ▼
                                       [rs-rtmp-push] ──RTMPS──▶ [FB ingest]
                                              │                       │
                                              │                       │ _result publish
                                              ▼                       │
                                         [audit log] ◀────────────────┘
                                              │
                                              ▼
                                       [assertion script]
```

No new components. Reuses existing pipeline end-to-end.

---

## Error handling

| Failure | Detection | Action |
|---|---|---|
| FB rejects publish (Invalid URL) | `rtmp_push_died` audit event with reason | Diagnostic AMF dump in log (Section C) — diff against libobs reference, iterate fix in same PR |
| FB accepts then drops mid-stream | `chunks_pushed` low + `time_since_last_upstream_ack_ms > 5000` | Different bug — outside this PR scope; file separately |
| FB rate-limits CI traffic | Sporadic CI failures | Reduce push duration to 30s; if persistent, file FB-relations issue |
| `FB_TEST_STREAM_KEY` secret missing | curl 4xx during config step | CI fails fast with explicit "operator must seed FB_TEST_STREAM_KEY" |
| Migration v29 fails on a machine | Service won't start, logs migration error | sqlx migration is transactional; rollback automatic; service stays on schema v28 |

---

## Testing

| Layer | File | What it asserts |
|---|---|---|
| Unit | `session.rs` mod tests | `build_tc_url` omits default port for rtmp(1935) + rtmps(443); retains custom port |
| Unit | `session.rs` mod tests | ConnectProperties has `swf_url` and `page_url` populated post-build |
| Integration | `tests/fb_mock_server.rs` (NEW) | Mock RTMP server validates `tcUrl` lacks default port + `swfUrl`/`pageUrl` set; rs-rtmp-push pushes successfully against mock |
| Loopback | existing xiu mock loopback test | Unchanged — confirms YT path still negotiates |
| Migration | `migrations.rs` mod tests | v29 flips ffmpeg→rust ONLY for service_type=FB; rows with other service_types untouched; idempotent across re-runs |
| E2E real | `.github/workflows/ci.yml` e2e-fb-push (NEW) | 60s push to real FB ingest, three-assertion gate |
| E2E real | existing `e2e-obs-youtube` | Unchanged — YT regression check |
| Mutation | `cargo mutants --in-diff` | Survives 0 mutants on `build_tc_url` and new `connect_props` test helpers |

TDD commit order (per `regression-test-first.md`):

1. RED: `build_tc_url` unit test (default-port omission)
2. GREEN: implement `build_tc_url`
3. RED: `swf_url`/`page_url` unit test
4. GREEN: populate fields
5. RED: mock FB server integration test
6. GREEN: covered by steps 2 + 4
7. RED: migration v29 test (FB-scoped, idempotent)
8. GREEN: implement migration v29
9. RED: e2e-fb-push CI job (initially fails — secret not seeded)
10. GREEN: operator seeds secret + workflow runs green

---

## Acceptance gates

Before merge:

- ✅ `cargo fmt --all --check` clean
- ✅ `cargo clippy --workspace -- -D warnings` clean
- ✅ All unit tests pass (existing + 4 new)
- ✅ Mock FB server integration test passes
- ✅ Migration v29 test passes
- ✅ `e2e-fb-push` CI job green (real FB pushed for 60s, three assertions pass)
- ✅ Existing `e2e-obs-youtube` green (YT unaffected)
- ✅ Existing `e2e-streaming` + `frontend-e2e` green
- ✅ `cargo mutants --in-diff` survives 0 mutants on new code

Post-merge / post-deploy:

- ✅ Migration v29 runs on streamsnv + streampp deploy; FB rows show `pusher='rust'`
- ✅ Operator triggers test stream to FB-NewLevel — Live Producer shows preview within 10s
- ✅ 24h production audit: zero `rtmp_push_died` events on FB endpoints

---

## Out of scope

| Item | Where it goes |
|---|---|
| Remove `PusherKind::Ffmpeg` enum + ffmpeg subprocess code | Issue #212 (blocked on #213 4h soak + 14d clean post-merge) |
| Today's streampp YT live-event regression (all YT endpoints reported bad mid-event) | NEW issue filed at spec commit time with streampp audit/log dump |
| RTMPS migration for YT (currently `rtmp://`) | Not requested |
| Multi-channel YT OAuth (PR #198 territory) | Separate spec |

---

## Risks + mitigation

| Risk | Mitigation |
|---|---|
| `tc_url` fix doesn't satisfy FB (cause is something else) | Diagnostic AMF dump (Section C) captures full props; iterate in same PR using real-FB E2E log output |
| FB rejects from cause not in candidates | Same as above |
| Migration v29 breaks Vimeo/Instagram/YT | Migration scoped `WHERE service_type='FB'`; other rows untouched. Migration test asserts this. |
| Today's streampp YT regression interacts with rust pusher | OUT OF SCOPE. ffmpeg fallback stays compiled (no #212 removal in this PR). Operator can revert FB or YT via SQL hot-patch if rust fully breaks. |
| FB stream key leaks via CI logs | `FB_TEST_STREAM_KEY` secret masked by GitHub Actions. Endpoint API masks `stream_key` in JSON responses (already implemented). Audit logs already use `stream_key.masked()`. |
| Real FB rate-limits CI runs | Push duration tuned to 60s; FB has no documented rate limit for persistent stream keys. If observed, reduce to 30s. |
| CI runner can't reach FB (network egress) | GitHub-hosted Linux runners have full internet egress; FB ingest is public. No mitigation needed. |

---

## Open questions

None. All design decisions resolved during brainstorming.

---

## References

- Issue #215 — FB rust pusher rejected by Facebook with 'Invalid URL'
- Issue #212 — Remove PusherKind::Ffmpeg entirely (gated)
- Issue #213 — 4h sustained-stability soak proof
- PR #211 — Migration v28 (root cause that introduced FB regression)
- `crates/rs-rtmp-push/src/session.rs` — current rust pusher CONNECT construction
- `crates/rs-delivery/src/endpoint_task.rs:266-284` — build_rtmp_url for FB
- libobs `obs-outputs/rtmp-stream.c` — reference AMF connect properties FB validates against
