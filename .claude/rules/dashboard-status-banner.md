---
paths:
  - "crates/rs-core/src/models.rs"
  - "crates/rs-api/src/handlers.rs"
  - "leptos-ui/src/api/status.rs"
  - "leptos-ui/src/store.rs"
  - "leptos-ui/src/components/*banner*.rs"
  - "leptos-ui/src/components/operator_dashboard.rs"
  - "src-tauri/src/commands.rs"
  - "src-tauri/src/state.rs"
  - "e2e/*banner*.spec.ts"
  - "e2e/mock-api.js"
  - "e2e/tauri-mock.js"
---

# Adding a status-driven dashboard banner (the #278 / #354 / #84 pattern)

A new operator-facing banner driven by a `/api/v1/status` field touches EIGHT
places. Miss one and the banner silently no-ops (usually in the Tauri path).
Full checklist — mirror an existing banner (`s3_region_banner`, `ingest_skew_banner`,
`long_stream_banner`):

1. **DTO** — add the field to `rs_core::models::ServiceStatus` with `#[serde(default)]`
   (or `= "default_true"`). Every `ServiceStatus { .. }` literal must add it — grep
   `ServiceStatus {`; only `crates/rs-api/src/handlers.rs::get_status` builds the real
   one (the `rs-service/src/main.rs` ones are the Windows-SCM `ServiceStatus`, unrelated).
2. **HTTP handler** — set it in `get_status` (rs-api `handlers.rs`). Read live config via
   `state.config_live.read()`; a top-level flag reads `state.<atomic>` or computes from
   DB. `handlers.rs` is near the 1000-line cap — keep additions tiny.
3. **Tauri IPC** — MIRROR it in BOTH `src-tauri/src/commands.rs` (`StatusResponse` field +
   `get_status` wiring) AND `src-tauri/src/state.rs` (an accessor). The tray webview uses
   the IPC path, not HTTP — skipping this makes the banner work in the LAN browser but not
   the tray app.
4. **leptos StatusResponse** — `leptos-ui/src/api/status.rs`: add the field (`#[serde(default)]`)
   AND parse it in the browser-HTTP branch (`status["<field>"].as_bool()...`). The Tauri-IPC
   branch uses serde on the invoke result.
5. **store** — `leptos-ui/src/store.rs`: add `RwSignal` field + its default in `new()`.
6. **dashboard** — `operator_dashboard.rs`: import + mount `<XBanner/>` in the banner block,
   AND copy the field into the store in the 2s `_status_poll`. Also mirror in
   `ws.rs::load_initial_state` for immediate initial state.
7. **banner component** — new `components/<x>_banner.rs` (copy `s3_region_banner.rs`); reuse
   an existing CSS class (`banner banner--warn` amber / `banner--critical` red — both already
   in `style.css`; add a new `.banner--*` only if needed). Register it in `components/mod.rs`.
   A bare bool needs no `Memo` — `<Show when=move || sig.get()>` directly.
8. **E2E** — `e2e/mock-api.js`: add a scenario + the field in `buildStatusResponse`. **Also add
   it to `e2e/tauri-mock.js`'s `get_status`** — that object is hand-composed, NOT a verbatim
   relay, so a field missing there silently vanishes under the IPC path every frontend spec
   runs through (#278). New spec `e2e/<x>-banner.spec.ts` (positive shows + negative hidden,
   BOTH ending with a zero-console-error assertion), and add its basename to `testMatch` in
   `playwright-frontend.config.ts`.

Config-backed threshold: add the field to `DeliveryConfig` (or the relevant sub-struct) in
`config.rs` + a `default_*` fn + the `Default` impl, AND classify it in
`config_redact::CONFIG_INVENTORY` (`("<path>", false)` for a non-credential) or the
`config_inventory_is_fully_classified` test fails. Note: the monitor loop reads the config
SNAPSHOT (`orchestrator.config()`), while `get_status` reads `config_live` — a runtime PATCH
diverges them until restart (same as `s3_region_standard`); document "restart to apply".

## dev2 lane build-verify gotchas that cost time here (beyond dev2-build-verify skill)

- **`cp -al` warm-checkout + `rsync` WITHOUT `--delete` leaves STALE files** that break the
  build. Real hit: a stale `leptos-ui/src/api.rs` (from before the `api/` module split)
  coexisting with the synced `api/mod.rs` → `E0761: file for module 'api' found at both`.
  Fix: `rm` the stale file in the lane (it has extra hardlinks; removing the lane's entry
  leaves the source buildcheck untouched).
- **`trunk build` writes to the REPO-ROOT `dist/`, not `leptos-ui/dist/`**, and
  `mock-api.js` serves `path.join(__dirname, "..", "dist")` (repo-root). When "the banner
  isn't in the dist", grep `./dist/*_bg.wasm` (the testid string is greppable in the wasm),
  not `leptos-ui/dist/`.
- The first frontend-E2E run on a loaded dev2 can flake (banner `toBeVisible` times out);
  confirm with `--repeat-each=3` before treating a single failure as a real bug.
