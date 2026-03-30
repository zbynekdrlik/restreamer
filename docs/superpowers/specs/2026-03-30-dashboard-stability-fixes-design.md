# Dashboard Stability Fixes — Design Spec

**Date:** 2026-03-30
**Scope:** Three targeted fixes for the operator dashboard

## Problem Statement

The dashboard has three critical usability bugs:

1. **Add Endpoint selector is unusable** — The inline `<select>` dropdown inside the endpoint tree gets destroyed and re-created every ~2 seconds by WebSocket-driven reactive updates. When the user opens the dropdown and scrolls to select an endpoint, the component re-mounts, resetting the selection to "Choose endpoint..." and closing the dropdown. This has been reported multiple times.

2. **Event selector doesn't lock during active streaming** — When delivery is active, the event dropdown remains enabled. The user can switch to a different event or deselect, losing visibility of the active delivery with no way to access the Stop button. On page load, if an event is actively delivering, it is not auto-selected — the dashboard shows "Idle" while a Hetzner VPS runs in the background costing money.

3. **Endpoint tree DOM thrashing** — The entire endpoint list is wrapped in `{move || { store.delivery.get() ... }}` (line 498 of `operator_dashboard.rs`), which re-creates ALL endpoint DOM nodes every time a WebSocket `DeliveryStatus` event arrives (~2s). This causes visible flickering and poor performance.

## Root Cause Analysis

### Endpoint Selector Re-rendering

File: `leptos-ui/src/components/operator_dashboard.rs`, line 624:

```rust
{move || is_running().then(|| view! {
    <div class="endpoint-branch">
        <span class="branch-connector">{"\u{2514}\u{2500}\u{2500}"}</span>
        <AddEndpointControl />
    </div>
})}
```

`is_running()` calls `store.delivery.get()`, which subscribes to the delivery signal. Every WebSocket `DeliveryStatus` event (every ~2s) triggers re-evaluation. Even though `is_running()` keeps returning `true`, the `.then()` closure creates a new `Option<View>` each time, causing Leptos to unmount and remount `AddEndpointControl`, destroying dropdown state.

### Event Selector Not Locked

File: `leptos-ui/src/components/operator_dashboard.rs`, control bar section. The event `<select>` has no `disabled` attribute bound to delivery state. On mount, no logic checks for an actively delivering event to auto-select it.

### Endpoint Tree DOM Thrashing

File: `leptos-ui/src/components/operator_dashboard.rs`, line 498:

```rust
{move || {
    let delivery = store.delivery.get();
    let eps = delivery.endpoints.clone();
    // ... .map().collect::<Vec<_>>()
}}
```

Every signal change rebuilds the entire endpoint list as a new `Vec<View>`. Leptos cannot diff this — it replaces the entire DOM subtree.

## Design

### Fix 1: Add Endpoint — Modal Dialog

**Remove** the `AddEndpointControl` inline component entirely.

**Add** an `+ Add` button at the bottom of the endpoint tree (inside the tree, but as a simple button — not a reactive closure that rebuilds). Clicking it opens a modal dialog.

**Modal component** (`AddEndpointModal`):
- Mounted at the dashboard root level, outside the endpoint tree — completely immune to endpoint tree re-renders
- Controlled by a `RwSignal<bool>` (`show_add_modal`)
- On open: snapshots available endpoints using `get_untracked()` (non-reactive read)
- Contents:
  - Title: "Add Endpoint"
  - List of available endpoints (not yet active) as clickable rows showing alias and service type
  - Position selector: radio buttons or small select (Live / From Beginning)
  - "Add" button (disabled until an endpoint is selected)
  - "Cancel" button or click-outside to close
- On add: calls `api::delivery_add_endpoint()`, closes modal
- The modal DOM exists independently — no WebSocket event can destroy or reset it

**Button placement**: Replace the current `AddEndpointControl` inline rendering (line 624-629) with a simple `+ Add` button. This button must NOT be inside a reactive closure that depends on `store.delivery.get()`. Use a `Memo` for `is_running` (see Fix 3) so the button only re-renders when running state actually changes (true→false or false→true), not on every delivery data update.

### Fix 2: Event Selector — Lock During Active Streaming

**Auto-select on load**: When the dashboard mounts, check if any event has `delivering_activated == true`. If so, auto-select that event immediately.

**Disable during delivery**: Bind the event `<select>` `disabled` attribute to a derived signal: `move || store.streaming_event.get().map_or(false, |e| e.delivering_activated)`. When delivery is active, the dropdown is grayed out — user cannot switch away from the active event.

**Stop button always accessible**: When an event is delivering, the "Stop Delivering" button must be enabled regardless of other state. The user must always be able to stop delivery.

**Visual indicator**: When the selector is locked, show a small lock icon or "(streaming)" text next to it so the user understands why it's disabled.

### Fix 3: Endpoint Tree — Stable DOM with `<For>`

**Replace** the `{move || { store.delivery.get().endpoints ... .map().collect() }}` pattern with Leptos `<For>` component:

```rust
<For
    each=move || store.delivery.get().endpoints.clone()
    key=|ep| ep.alias.clone()
    children=move |ep| { view! { /* endpoint node */ } }
/>
```

`<For>` diffs by key (alias) and only updates changed nodes. Endpoints that haven't changed keep their DOM intact.

**Memo for `is_running`**: Create a `Memo` that derives a boolean from the delivery status:

```rust
let is_running = Memo::new(move |_| {
    let s = store.delivery.get().status.clone();
    s == "running" || s == "delivering"
});
```

`Memo` only notifies dependents when its output value changes. Since `is_running` returns a boolean, it only triggers re-renders on actual state transitions (idle→running, running→idle), not on every delivery data update.

**`has_endpoints` as Memo**: Same pattern — only triggers when the count goes from 0→N or N→0.

## Files Changed

| File | Change |
|------|--------|
| `leptos-ui/src/components/operator_dashboard.rs` | Remove `AddEndpointControl`, add `AddEndpointModal`, add `+ Add` button, lock event selector, use `<For>` + `Memo` |
| `leptos-ui/style.css` | Add `.modal-overlay`, `.modal-content`, `.add-endpoint-modal`, `.endpoint-row` styles. Remove `.add-endpoint-control`, `.add-endpoint-select`, `.start-position-select` |
| `e2e/frontend.spec.ts` | Add tests for modal, event lock, and DOM stability |

No backend/API changes needed. All fixes are frontend-only.

## Testing (TDD — Tests Written First)

### E2E Tests (Playwright)

1. **Add endpoint modal test**:
   - Navigate to dashboard with active delivery (mock or real)
   - Verify `+ Add` button is visible in endpoint tree
   - Click `+ Add` — verify modal opens with endpoint list
   - Select an endpoint — verify it highlights
   - Select position — verify radio/select works
   - Click Add — verify modal closes and endpoint appears in tree
   - Verify modal does NOT blink/reset during the test (implicit: WebSocket updates are happening)

2. **Event selector lock test**:
   - Start with an actively delivering event
   - Verify the event dropdown shows the correct event name
   - Verify the event dropdown is disabled (cannot interact)
   - Verify "Stop Delivering" button is enabled
   - Stop delivering — verify dropdown becomes enabled again

3. **DOM stability test**:
   - During active delivery with endpoints, take a snapshot of endpoint tree element refs
   - Wait 5 seconds (2-3 WebSocket cycles)
   - Verify the same DOM elements still exist (not destroyed/recreated)
   - Verify endpoint data updates correctly (chunks count increases) without losing DOM identity

## Out of Scope

- Backend API changes
- Endpoint CRUD management (create/edit/delete endpoints)
- Dashboard layout/visual redesign (covered by separate spec)
- Mobile-specific layout fixes
