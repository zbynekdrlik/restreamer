---
paths:
  - "leptos-ui/src/**"
---

# Leptos UI gotchas

## Controlled `<select>` with a DYNAMIC (reactive) `<option>` list → use `prop:selected` per option

Binding the current value of a `<select>` whose options are rendered from a
signal (e.g. `{move || grants.get().map(|g| view!{<option .../>})}`) is a trap.
Two obvious approaches BOTH fail, and each fails in a *different* case, so a
test that only exercises one case passes while the feature is half-broken (#199):

- **`prop:value=move || val()` on the `<select>`** — reflects a value set
  AFTER the options exist (an async update), but does NOT reflect a value set
  while the options have not rendered yet. Reactive option lists populate a
  tick after their fetch resolves, so opening a form for an already-selected
  row shows the *placeholder* instead of the selected option, and the closure
  does not re-fire when the options later appear (its deps are the value, not
  the list).
- **plain `selected=move || ...` on each `<option>`** — sets the HTML
  *attribute* (`defaultSelected`), which fixes the initial render but does NOT
  update `select.value` when the signal flips AFTER render (an async
  auto-suggest, say), because the attribute is the default, not the live value.

**Robust idiom: `prop:selected=move || current == Some(this_option)` on EVERY
option (including the placeholder).** `prop:selected` sets the DOM *property*,
which the browser reflects into `select.value` both at first render (each option
self-selects as it mounts, regardless of whether the value or the option list
arrived first) AND reactively when the signal changes later. Do NOT combine it
with `prop:value` on the select — property-binding the options alone is enough.

## E2E: an already-linked/selected row's dropdown must be tested on RE-OPEN

The bug above only shows when you OPEN an edit form for a row that already has a
value (the placeholder-instead-of-value symptom). A test that only selects a new
value and saves never sees it. Always add a case that: sets a value, reloads
(fresh store — avoids racing the async post-save refetch), re-opens the form,
and asserts the dropdown shows the persisted value.
