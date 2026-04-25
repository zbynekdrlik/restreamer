# Delete + Cleanup Button: Visible Progress and Error Surfacing — Design Spec

**Issue:** #128 — "delete and cleanup button on dashboard settings not work and not do anything!!!! TDD"

**Date:** 2026-04-25

## Problem

The Events tab in `Settings` exposes two destructive actions per event:

- `Delete + Cleanup` — deletes the event row and all its S3 chunks
- `Clear S3 chunks` — keeps the event row, deletes only its chunks

Both fire long-running async work (S3 deletion can take up to 60s for events with thousands of chunks) but offer the operator zero feedback during that window. The user reports clicking the button and seeing nothing happen, then clicking again and finding the event gone — concluding the button is broken.

## Root cause (confirmed)

`leptos-ui/src/components/confirm_modal.rs:28-34` — `on_confirm_click` runs the user's callback synchronously, then immediately closes the modal:

```rust
let on_confirm_click = move |_| {
    on_confirm.run(());            // synchronous; spawn_local returns immediately
    defer_set_false(show);          // modal closes
};
```

`leptos-ui/src/components/settings.rs:451-472` — both confirm callbacks fire-and-forget the async work and silently swallow errors:

```rust
let on_confirm_delete = Callback::new(move |_: ()| {
    let id = delete_target_id.get();
    spawn_local(async move {
        let _ = api::delete_event(id).await;     // ← error discarded
        if let Ok(events) = api::list_events().await {
            store.events_list.set(events);
        }
        if let Ok(u) = api::get_s3_usage().await {
            s3_usage.set(Some(u));
        }
    });
});
```

Result: modal closes → 10–60s of unchanged UI → events_list silently refreshes when the async block finishes. If the API returned 500/504/409, nothing surfaces.

The backend is correct (`crates/rs-api/src/handlers.rs:474-569`): 60s timeout, parallel-batched S3 deletes (20 concurrent), proper status codes (200/204, 404, 409, 500, 504). No backend changes needed.

## Approach: inline busy state + error banner (single-component scope)

Add two `RwSignal`s inside `EventsManagement`:

- `busy_event_id: RwSignal<Option<i64>>` — shared by both Delete and Clear; only one mutation in flight at a time per event
- `action_error: RwSignal<Option<String>>` — last error text from either action; cleared on next attempt

The two confirm callbacks become:

1. Set `busy_event_id = Some(id)`, `action_error = None`
2. `spawn_local` the API call
3. On `Err(e)`: set `action_error = Some(format!("Delete failed: {e}"))`, do NOT refresh
4. On `Ok(_)`: refresh `events_list` + `s3_usage`
5. **Always** set `busy_event_id = None` at the end of the spawned task (regardless of outcome)

The card UI (`settings.rs` lines ~593-621) gates on `busy_event_id.get() == Some(id)`:

- When busy: render a `<span class="card-busy-text">"Deleting…"</span>` (or `"Clearing chunks…"`) in place of the action buttons, OR keep buttons visible but `disabled=true` with the same label change. Pick one — see "Implementation choice" below.
- When not busy: render the existing two buttons unchanged.

The error banner sits above the events list (next to / below the existing `s3-usage-banner`):

- When `action_error.is_some()`: render an `error-message` div with the text + a small "×" button that sets `action_error = None`
- When `None`: render nothing

Modal closes immediately on confirm — same UX as today. The visible feedback lives on the card itself, which is where the user is already looking.

### Implementation choice — disabled buttons vs. replacement text

Use **disabled buttons + label change**. Reasons:

1. Layout doesn't shift (button widths preserved → no card jitter)
2. Reuses existing `disabled=is_streaming` infrastructure on the same buttons
3. Single line of conditional: `disabled = is_streaming || busy`

The disabled buttons get a CSS rule (`button:disabled` already covers most styling). Add an italic label change: `"Delete + Cleanup"` → `"Deleting…"` and `"Clear S3 chunks"` → `"Clearing…"` while busy.

## Out of scope

- **Modal-stays-open-with-spinner** (alternative B from brainstorming): more code, no measurable UX gain over the inline busy state.
- **Toast/notification component infrastructure**: overkill for one error site. The single-element error banner pattern is already used elsewhere (`s3_usage_error` at lines 522-528).
- **Backend changes**: backend is correct; problem is purely UI.
- **Other action buttons** (Start Delivering, Stop, etc.): different surface, separate concerns. Issue #128 is specifically about Events-tab destructive actions.

## Testing — TDD with Playwright

`e2e/delete-cleanup-button.spec.ts` (new file). Two tests:

### Test 1: success path

1. Mock `GET /api/v1/events` → return one event `{id: 1, name: "test-event", receiving_activated: false, delivering_activated: false, ...}`
2. Mock `GET /api/v1/s3/usage` → return `{total_bytes: 1000, total_objects: 5, by_event: [{event_name: "test-event", bytes: 1000, objects: 5}]}`
3. Mock `DELETE /api/v1/events/1` → 1500ms artificial delay, then 204 No Content
4. After the DELETE returns, mock `GET /api/v1/events` to return `[]` (event is gone)
5. Navigate to `/settings`, click `Events` tab
6. Click `Delete + Cleanup` → modal appears
7. Click confirm in modal
8. **Assert**: modal closes within 200ms
9. **Assert**: `Delete + Cleanup` button label shows "Deleting…" and is disabled
10. **Assert**: `Clear S3 chunks` button is disabled
11. Wait until event card disappears (max 3s timeout)
12. **Assert**: zero browser console errors per airuleset `browser-console-zero-errors`

### Test 2: error path

1. Same setup as Test 1, but mock `DELETE /api/v1/events/1` → 500 Internal Server Error after 500ms
2. Navigate, click Delete + Cleanup, confirm
3. Wait for the 500 to land
4. **Assert**: an error banner appears containing the text "Delete failed"
5. **Assert**: event card still present (not removed)
6. **Assert**: action buttons re-enabled (not stuck in busy state)
7. **Assert**: zero unhandled console errors (the 500 must be CAUGHT, not surfaced as an unhandled rejection)

### Optional Test 3 (stretch): clear-S3 path

Mirror Test 1 for the `Clear S3 chunks` button — confirms the same busy-state pattern applies to both actions. Can be added to the same spec file. If time-boxed, ship the two delete tests first.

## File surface

- `leptos-ui/src/components/settings.rs` — add busy/error signals, wire into UI, modify both confirm callbacks
- `leptos-ui/styles/settings.css` (or wherever `.s3-usage-banner` lives) — `.action-error-banner` class if not reusing
- `e2e/delete-cleanup-button.spec.ts` — new TDD spec
- Version bump: `Cargo.toml`, `src-tauri/Cargo.toml`, `src-tauri/tauri.conf.json`, `leptos-ui/Cargo.toml` (0.3.69 → 0.3.70)

## Acceptance criteria

- [ ] Playwright spec passes locally and in CI
- [ ] On confirm of Delete + Cleanup: button immediately shows "Deleting…" and is disabled until the API call completes
- [ ] On API error: error banner appears with the failure reason; event card remains; buttons re-enable
- [ ] On API success: event removed from list; S3 usage refreshes; no error banner
- [ ] Same behavior for Clear S3 chunks
- [ ] Zero new browser console errors or warnings
- [ ] CI green (all jobs including E2E + deploy)
- [ ] Post-deploy verification on stream.lan via Playwright MCP confirms the new behavior on the live dashboard
