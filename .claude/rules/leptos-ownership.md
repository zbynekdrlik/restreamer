---
paths:
  - "leptos-ui/src/**"
---

# Leptos ownership — timers & detached async must not outlive their owner (#343)

A `#[component]` body runs under a reactive OWNER that the router/`{move || …}`
disposes on a client-side route/tab change. Any timer or `spawn_local` that
captures a component-scoped signal and fires AFTER disposal panics with
`reactive_graph … Tried to access a reactive value that has already been
disposed` (WASM `RuntimeError: unreachable`, surfaced by
`console_error_panic_hook`). This is a hard `browser-console-zero-errors`
violation and does NOT reproduce on a full page load — only on the SPA
transition (so an E2E must navigate via a router `<A>` click, never
`page.goto`).

## Timers — use `utils::interval_until_disposed`, NEVER `std::mem::forget`

- `let _ = Interval::new(...); std::mem::forget(_)` LEAKS the browser timer: it
  keeps firing against disposed signals forever (the #343 "2 errors climbing
  every tick").
- `on_cleanup(move || drop(handle))` does NOT compile — the `!Send` gloo
  `Interval`/`Timeout` handle fails `on_cleanup`'s `Send + Sync` bound
  (`(dyn FnMut() + 'static) cannot be sent between threads safely`).
- Correct idiom: `crate::utils::interval_until_disposed(millis, closure)`. It
  parks the `Interval` in `StoredValue::new_local` — a `Copy` handle with no
  `Drop`, whose parked value lives in the owner's local arena and is dropped
  (running gloo's `Interval::drop` → `clearInterval`) exactly on owner cleanup
  (verified against reactive_graph 0.1.8 `OwnerInner::drop` → `arena.remove`).
  It MUST be called synchronously in a component body (a live owner must be
  current); inside a `spawn_local`/after `.await` it silently degrades to a
  leak — the helper's `debug_assert!(Owner::current().is_some())` catches that.

## Detached async — fallible writes for anything that outlives the owner

A `spawn_local` that writes a **component-scoped** signal AFTER `.await`
(on-mount fetch, a self-rescheduling poll loop) can resolve after disposal.
Route those through the fallible API: `try_set` / `try_update` /
`try_get_untracked` (they no-op / return `None` on a disposed signal). This is
NOT "swallowing the panic" — it is the sanctioned way to touch a signal from a
task that legitimately may outlive it. Root-owned `store.*` signals (created in
`App`, never disposed) stay plain `.set()`. A self-rescheduling `loop { …await… }`
that reads a component signal (e.g. `modal_open.get_untracked()`) is the timer
class in disguise — guard it too. User-action one-shot handlers (Save/Create
click → one RTT) are low-risk and left as plain `.set()`.

## Trap: `components/events.rs` and `log_viewer.rs` are NOT in `components/mod.rs`

They are uncompiled dead twins. The LIVE `EventEndpoints` (Events tab) is
`fn EventEndpoints` in `settings.rs`; the live logs view is elsewhere. Editing
the dead files is a silent no-op — check `mod.rs` before assuming a file is
compiled.
