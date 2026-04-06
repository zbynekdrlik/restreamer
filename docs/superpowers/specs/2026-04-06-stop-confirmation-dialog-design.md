# Stop Confirmation Dialog — Design Spec

**Issue:** #72 — accidental touch on stop button broke live streaming  
**Scope:** Confirmation modal for Stop Delivering + Remove Endpoint (both destructive actions)

## Problem

The "Stop Delivering" button immediately stops all endpoints and destroys the VPS with no confirmation. An accidental tap during a live stream causes an outage. The "Remove Endpoint" button uses a native browser `confirm()` dialog which is inconsistent with the dashboard's design.

## Solution

A reusable `ConfirmModal` Leptos component used by both destructive actions. Centered overlay modal with warning text, context info, Cancel (default focus) and red Confirm button.

## Component: `ConfirmModal`

**Props:**
- `show: RwSignal<bool>` — visibility toggle
- `title: &'static str` — e.g. "Stop Delivering?"
- `message: Signal<String>` — warning text (reactive, includes event/endpoint context)
- `confirm_label: &'static str` — button text, e.g. "Stop Delivering" or "Remove"
- `on_confirm: Callback<()>` — fires when user clicks confirm

**Behavior:**
- Renders overlay (`.modal-overlay`) blocking page interaction
- Cancel button has default focus — Enter key dismisses, not confirms
- Escape key dismisses
- Clicking overlay background dismisses
- Confirm button styled as danger (red border + red background tint)

**File:** `leptos-ui/src/components/confirm_modal.rs` (new file, single component)

## Integration Points

### Stop Delivering (ControlBar)

**File:** `leptos-ui/src/components/operator_dashboard.rs` — `ControlBar` component

Current flow:
```
click Stop → loading=true → api::stop_stream() → done
```

New flow:
```
click Stop → show_confirm=true → modal appears → Cancel: dismiss / Confirm: loading=true → api::stop_stream() → done
```

Modal message includes: event name from `store.selected_event_id`, number of active endpoints.

### Remove Endpoint (EndpointTree)

**File:** `leptos-ui/src/components/operator_dashboard.rs` — `EndpointTree` component

Current flow:
```
click ✕ → window.confirm_with_message() → if true: api::remove_endpoint()
```

New flow:
```
click ✕ → show_confirm=true → modal appears → Cancel: dismiss / Confirm: api::remove_endpoint()
```

Modal message includes: endpoint alias name.

## CSS

**File:** `leptos-ui/style.css`

Reuse existing `.modal-overlay`. New classes:

- `.confirm-modal` — smaller variant of `.add-endpoint-modal` (max-width: 400px, no scroll)
- `.confirm-modal-title` — red-colored title with warning icon
- `.confirm-modal-message` — secondary text with context
- `.confirm-modal-actions` — flex row, right-aligned Cancel + Confirm
- `.confirm-btn-danger` — red border, red tint background, red text (matches `.stop-btn` hover)

## E2E Tests

**File:** `e2e/frontend.spec.ts` (extend existing)

### Test: Stop Delivering confirmation

1. Set up mock API with active delivery state
2. Click "Stop Delivering" button
3. Assert modal appears with correct title and warning text
4. Click "Cancel" — assert modal closes, no API call made
5. Click "Stop Delivering" again
6. In modal, click "Stop Delivering" confirm button
7. Assert `stop-stream` API was called
8. Assert modal closes
9. Assert zero console errors/warnings

### Test: Remove Endpoint confirmation

1. Set up mock API with active endpoints
2. Click remove (✕) button on an endpoint
3. Assert modal appears with endpoint name
4. Click "Cancel" — assert modal closes, endpoint still present
5. Click remove again → click "Remove" confirm button
6. Assert endpoint removal API was called
7. Assert zero console errors/warnings

## Out of Scope

- Type-to-confirm or countdown timer (user chose simple confirm)
- Confirmation for non-destructive actions (start, add endpoint)
- Global confirmation provider/context (each usage owns its own `RwSignal<bool>`)
