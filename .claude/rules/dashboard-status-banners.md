---
paths:
  - "leptos-ui/src/components/*_banner.rs"
  - "leptos-ui/src/store.rs"
  - "leptos-ui/src/api/status.rs"
  - "leptos-ui/src/ws.rs"
  - "crates/rs-api/src/handlers.rs"
  - "crates/rs-core/src/models.rs"
  - "src-tauri/src/commands.rs"
  - "e2e/*-banner.spec.ts"
  - "e2e/mock-api.js"
  - "e2e/tauri-mock.js"
---

# Adding a dashboard status banner (the full mirror-set)

Surfacing a backend condition as a red/amber dashboard banner touches **nine**
places. Miss one and it fails silently — the banner never shows, or shows only
in browser mode, or the E2E passes for the wrong reason. Follow the `#354`
(ingest-skew) / `#106` (RTMP bind-error) precedent exactly:

1. **State cell** — `crates/rs-core/src/models.rs` `InpointState`: add an
   `Arc<AtomicBool>` (flag) or `Arc<Mutex<Option<String>>>` (message) cell,
   shared across clones, with `set_/clear_/read` accessors. Recover a poisoned
   `std::sync::Mutex` with `lock().unwrap_or_else(|p| p.into_inner())` — never a
   silent no-op (a stuck/hidden banner is worse). NEVER hold the guard across an
   `.await`.
2. **/status** — `crates/rs-api/src/handlers.rs`: add the field under
   `inpoint.details` (sibling of `ingest_skew_active`).
3. **leptos StatusResponse** — `leptos-ui/src/api/status.rs`: add the field with
   `#[serde(default)]` AND parse it in the BROWSER branch of `get_status()`
   (nested `status["inpoint"]["details"][...]`). The Tauri branch deserializes
   the IPC struct by field name.
4. **DashboardStore** — `leptos-ui/src/store.rs`: `RwSignal` + init.
5. **Poll wiring** — `leptos-ui/src/components/operator_dashboard.rs` ControlBar
   2s `_status_poll`: `store.<field>.set(s.<field>)` (this is the authoritative
   clear/appear path — the skew banner is poll-only).
6. **WS arm (instant update)** — add the `WsEvent` variant in
   `crates/rs-core/src/models.rs`, mirror it in the LOCAL `enum WsEvent` in
   `leptos-ui/src/ws.rs`, and a `dispatch_event` arm setting the store signal.
7. **Banner component** — `leptos-ui/src/components/<x>_banner.rs`: `<Show when=…>`
   with `data-testid` + reuse the existing `banner banner--critical` / `--warn`
   CSS class (no new CSS needed). Register in `components/mod.rs` and render it
   in `operator_dashboard.rs`.
8. **Tauri IPC mirror** — BOTH `src-tauri/src/state.rs` (accessor delegating to
   `inpoint_state`) AND `src-tauri/src/commands.rs` `get_status` (add the field
   to the IPC `StatusResponse` struct + the hand-built `data`). Easy to forget;
   without it the tray webview banner silently never shows.
9. **E2E** — `e2e/mock-api.js` (a scenario in `buildStatusResponse`), AND
   `e2e/tauri-mock.js`'s `get_status` hand-composed object (it does NOT relay
   verbatim — a field present in mock-api.js but missing here silently defaults,
   and the whole frontend suite runs through the Tauri IPC path), the spec file,
   and add its basename to `playwright-frontend.config.ts` `testMatch`.

## Also write a durable audit row (a reviewer WILL flag its absence)

A banner clears when the condition ends, so it leaves no post-mortem trail. Add
`Action::<X>Failed` / `<X>Recovered` variants to `crates/rs-core/src/audit.rs`
and emit them EDGE-TRIGGERED (first onset + recovery only, never per poll tick)
via `rs_core::audit::record(inpoint_state.audit_tx()?, AuditRow{…})` — the #354
skew precedent did this. `notify::classify` has a `_ => None` catch-all, so new
Action variants add NO Discord alert unless you explicitly wire one.

## dev2 frontend-E2E on a shared box (the mock traps)

- `mock-api.js` must be started with `setsid nohup node mock-api.js …` to
  survive the ssh session teardown; a plain `nohup … &` still dies under load.
- A sibling autopilot lane's `pkill -f "node mock-api.js"` kills YOUR mock too,
  and `ss -ltn | grep :8910` (MOCK_UP) can be a sibling's STALE mock lacking
  your new field. Before trusting an E2E failure, `curl /api/v1/status` and
  confirm the field you added is actually present in the SERVED response.
