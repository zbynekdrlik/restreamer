---
paths:
  - "crates/rs-core/src/notify.rs"
  - "crates/rs-core/src/notify_tests.rs"
  - "crates/rs-core/src/notify_scope_tests.rs"
  - "crates/rs-inpoint/src/ingest_report.rs"
  - "crates/rs-inpoint/src/ingest_skew.rs"
  - "crates/rs-delivery/src/rescue_audit.rs"
  - "crates/rs-delivery/src/endpoint_audit.rs"
  - "crates/rs-api/src/delivery_monitor.rs"
---

# Outage notifier — episodes, scopes, and the one invariant (#367)

`rs-core/src/notify.rs::OutageNotifier` turns audit rows into Discord alerts at
the audit writer. Read this before adding an alerting action or changing an
emitter of one.

## The model

- **Episode key = (family, `detail.stage`, endpoint).** A recovery ends only
  its OWN family's episode on its own stage + endpoint; every other episode
  stays open. (One global episode let endpoint A's `AvInvariantRestored`
  silence endpoint B for good.)
- **Families:** `HostInternet`, `VpsReachability`, `S3Upload` (host-level,
  no endpoint), `Rescue` (per endpoint), `IngestSkew` (host-level),
  `AvInvariant` (per stage; push stage also per endpoint).
  `HostInternetRecovered` ends all three host-level families with ONE alert;
  the delivery monitor's `VpsReachable` ends VPS reachability on its own.
- **Unmeasured recoveries close silently:** `IngestSkewRecovered` with
  `state: "reset"` (a session reset cleared the latch, nobody measured the
  source back in sync) ends the episode with NO alert (`recovery_unmeasured`).
- **Scopes:** a lifecycle row closes, SILENTLY, the episodes whose subject it
  ended — no "recovered" alert, but the next onset alerts again:
  - `DeliveryStopped`, `VpsDeleted` → every VPS-side episode (rescue,
    push-stage invariant, VPS reachability). END edges only: a start request
    on a LIVE delivery reuses its instance yet still records
    `DeliveryStarted` and re-emits `VpsReady`;
  - `EndpointAdded/Removed`, `EndpointStartChunkUpdated` → that endpoint's;
  - `RescueActivated` → that endpoint's push-stage invariant (the live pusher
    was dropped for the rescue clip);
  - `EventStarted/Stopped` → S3 upload failures.
- **#311 first:** rows of a CI `E2E-*` event return before ANY state change,
  lifecycle rows included, so a CI delivery never closes a real one's
  episodes.
- **Override rows are not onsets:** `IngestSkewDetected` with
  `state: "override"` (operator force-start) is an audit record of a bypass;
  `onset_suppressed_by_detail` drops it (the latched guard already alerted).

## The invariant (what review B1 caught)

**Every onset must END on every exit path — its paired recovery, or a scope
end.** A keyed episode that never ends dedups every later onset on its key
for the process lifetime: an alert that silently goes quiet. So:

- A new alerting onset needs a family AND a guaranteed end. Check every exit
  of its emitter (stop, drop, reset, reconnect), not only the happy path.
- An emitter whose latch is cleared by a RESET must write the recovery row
  then: the chunker's session reset writes `IngestSkewRecovered`
  (`state: "reset"`) and `AvInvariantRestored` (`ingest_report.rs`
  `publish_reanchor`, `ReanchorEdges`), on all three reset paths
  (`start_new_session`, `reset`, the backward-jump re-anchor).
- An emitter that only LOGS a recovery edge is a gap: the delivery monitor
  only logged "VPS health recovered" until round 2 of the #367 review added
  the `VpsReachable` row.
- Onset and recovery rows of one episode must carry the SAME stage + endpoint.
  Use `rs_core::audit::AV_STAGE_INGEST` / `AV_STAGE_PUSH` and the shared row
  builders, never a literal.
- A new lifecycle action that ends a subject goes into `ends_scope()`, and it
  must reach `observe()`: `is_outage_relevant()` covers `classify()` OR
  `ends_scope()`.

Tests: `notify_tests.rs` (routing, recovery keying) and its child
`notify_scope_tests.rs` (scopes, override). A `RescueActivated` row is ALSO a
live-pusher scope end — in a test, check a push-side episode BEFORE re-entering
rescue on the same endpoint, or the scope end masks what you test.
