---
paths:
  - "e2e/**/*.ts"
  - "e2e/mock-api.js"
  - "e2e/lib/ws.ts"
  - "e2e/check-ws-broadcast.js"
  - "leptos-ui/src/ws.rs"
---

# Frontend E2E: pushing WebSocket events from a spec (#377)

The mock (`e2e/mock-api.js`) sends `_test/ws-broadcast` / `_test/emit-metrics-sample` ONLY
to sockets open at that instant. The Leptos app connects its WS after first render, so a
visible DOM does NOT mean the page is subscribed. A push that wins that race is lost
silently (main run 37520022262: about 1 in 160 runs, expected "RTMP Only", got
"Disconnected").

## The only allowed shape

```ts
import { broadcast, waitForWsClient } from "./lib/ws";
await page.goto("/");
await waitForWsClient(page, request);   // once per test, after the LAST goto/reload
await broadcast(request, { type: "InpointStatus", data: { ... } });
// MetricsSample burst: await broadcastMetricsSamples(request, "yt1", 5);
```

## What "broadcast-ready" means

`GET /_test/ws-clients` returns `{count, open, snapshot_sent, page_load, obs_status}`; `count` is `isWsClientReady()` in `mock-api.js`.
A client is counted in `count` only when all four of these hold:

1. **It is OPEN.**
2. **It belongs to the LATEST page load.** The mock bumps `pageLoadSeq` on every document
   navigation (`Accept: text/html`, non-API path). A socket from the previous test's page,
   or from before a `page.reload()`, therefore never counts.
3. **Its connect-time snapshot already went out.** The mock sends `DeliveryStatus` (plus
   `PipelineState` for the active scenarios) on a **200 ms `setTimeout`** after
   `connection`. A push before that is OVERWRITTEN by the late snapshot.
4. **The page's `load_initial_state` has finished.** `leptos-ui/src/ws.rs` runs five
   SEQUENTIAL HTTP fetches right after `WebSocket::open`:
   - `/status` sets `inpoint_connected`;
   - `/delivery/status/cached` sets `delivery`;
   - then `/events` and `/endpoints`;
   - LAST, `/obs/status`.

   An unfinished chain can overwrite a push. The mock records when `/obs/status` is
   requested for the current page load. A WS reconnect re-runs the chain, so a reconnect
   socket needs an `/obs/status` that arrived after it connected. **If you reorder or
   extend `load_initial_state`, keep a fetch the mock can key on as its LAST step, and
   update the middleware at the top of `mock-api.js`.** Two ways to break readiness:
   - a spec `page.route("**/obs/status")` that fulfills the request means it never
     reaches the mock, so the page never becomes ready;
   - a mock JSON route for `/obs/status` would turn its response into a store write that
     lands AFTER readiness.

`broadcast` asserts the mock's `ready` count. That is the same `isWsClientReady()`
predicate the wait polls, so a guard miss still fails loudly at runtime instead of
letting a UI assertion time out on a misleading value. Never use `waitForTimeout(…)` "to
let the WS connect"; that is the band-aid this replaced. One page per test: only sockets
of the LATEST document navigation count.

## Polled fields: readiness does not cover them

The ControlBar polls `/status` every 2 s and writes `rtmp_bind_error`, `disk_pressure`,
`vps_orphan_count`, `long_stream_warning` and `ingest_skew_*`. A push of one of those
fields can be cleared by a poll answered just after it. `rtmp-bind-error-banner.spec.ts`
shows the fix that keeps the test's "only the WS arm can raise it" meaning: track
`/status` requests from before `goto`, `page.route` to hold every later poll, wait until
no earlier poll is still open, then push.

## The guard

`node e2e/check-ws-broadcast.js` runs in the `frontend-e2e` job before Playwright, and a
test-integrity step checks that it stays there: unconditional, no `continue-on-error`.

It scans every `e2e/**/*.ts` except `lib/ws.ts`, and fails on:
- a raw push-route string;
- a push helper called before `waitForWsClient(` since the last `page.goto`/`reload`;
- an un-awaited push;
- an aliased import of a `lib/ws` helper.

Comments and string contents never count. The wait state resets at every `test(`/hook
start AND at every statement at or outside the test's own indent. So a wait placed in a
`beforeEach` is not credited: put it in the test. When you change a rule, add a case to
the `--self-test` table.

Known static limits (the runtime `ready` assertion catches them):
- the lexer does not parse regex literals, so `/a\//` or a backtick inside a regex can
  hide the rest of a line or file;
- navigation not written as `page.goto/reload/goBack/goForward` (`page2.goto`, a
  full-page `click`, `location.reload()` in `evaluate`) does not reset the wait.
