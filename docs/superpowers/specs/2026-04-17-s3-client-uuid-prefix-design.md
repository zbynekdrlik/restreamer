# S3 Client-UUID Path Prefix — Design

**Issue:** [#114 Multi-instance isolation: prefix S3 paths with client_uuid to prevent cross-instance collision](https://github.com/zbynekdrlik/restreamer/issues/114)

**Status:** Approved for implementation

---

## Problem

S3 chunk keys are built as `{event_name}/{sequence}.bin`
(`crates/rs-endpoint/src/s3.rs:51`, `crates/rs-delivery/src/s3_fetch.rs:61,94`).
The string passed as `event_identifier` is the human-entered event name
(`crates/rs-endpoint/src/uploader.rs:282` → `ev.name`).

Two Restreamer installations sharing the same S3 bucket that create events
with the same name (e.g. both `"Sunday-Service"`) upload chunks to identical
S3 keys. Writes overwrite each other; delivery VPSes fetch a scrambled mix.

`client_uuid` already exists in `Config` (`crates/rs-core/src/config.rs:8`),
is generated at install time (`scripts/install.ps1:122`), validated as
required (`config.rs:326`), and persisted to the DB
(`rs-runtime/src/orchestrator.rs:122`). It is simply not used in any S3 path
or VPS label.

## Goal

Prefix every chunk S3 key with `client_uuid` so two installations cannot
collide, no matter what event names operators choose. Also label Hetzner
VPSes with `client_uuid` so future orphan detection can filter per
instance.

## Approach

Modify the **value** that flows into `event_identifier` at the two call
sites that build it. The `event_identifier` field is opaque to everything
downstream — rs-delivery's `InitRequest.event_identifier` and the S3 fetch
code treat it as a flat string that's used as an S3 prefix. Making that
string `"{client_uuid}/{event_name}"` yields keys of the form
`{client_uuid}/{event_name}/{seq}.bin` without touching the S3 code, the
`InitRequest` schema, or the delivery binary.

This is the minimum-diff approach: no protocol changes, no multi-layer
plumbing, no schema migrations.

## Changes

### 1. Upload side — thread client_uuid into ChunkUploader

`ChunkUploader::new` (`crates/rs-endpoint/src/uploader.rs:77`) currently takes
`(pool, s3, ws_tx)`. Add a fourth parameter `client_uuid: String`, store it
on the `ChunkUploader` struct (field) and propagate to `WorkerCtx` (struct
around line 16-28) so every upload worker has it. In the upload loop
(around line 280-291):

```rust
let event_id = match db::get_streaming_event_by_id(&pool, chunk.streaming_event_id).await {
    Ok(Some(ev)) => format!("{}/{}", ctx.client_uuid, ev.name),
    ...
};
```

The caller at `crates/rs-runtime/src/orchestrator.rs:463` passes
`self.config.client_uuid.clone()`. Test callers (`uploader.rs:403,488` and
`tests/uploader_integration.rs:146,217,296,360,405`) pass a constant like
`"test-client-uuid".to_string()`.

### 2. Orchestrator side — same prefix when initializing delivery VPS

File: `crates/rs-api/src/delivery.rs` (around line 486)

Today's body JSON builder passes the plain event name as `event_identifier`.
Change to `format!("{}/{}", config.client_uuid, event_name)` so the VPS
fetches from the same prefixed keys the uploader wrote to.

### 3. VPS label — add client_uuid

File: `crates/rs-api/src/delivery.rs` (around line 191-193)

Today:
```rust
let mut labels = HashMap::new();
labels.insert("app".to_string(), "restreamer".to_string());
labels.insert("event_id".to_string(), event_id.to_string());
```

After:
```rust
let mut labels = HashMap::new();
labels.insert("app".to_string(), "restreamer".to_string());
labels.insert("event_id".to_string(), event_id.to_string());
labels.insert("client_uuid".to_string(), config.client_uuid.clone());
```

Hetzner label values are restricted to `[a-zA-Z0-9_.-]` and ≤63 chars. A
standard UUID (36 chars) fits without modification.

### 4. Tests

- **Unit test** in `crates/rs-endpoint/src/s3.rs`: assert
  `chunk_key("abc-uuid/sunday-service", 5) == "abc-uuid/sunday-service/5.bin"`.
  Verifies the existing `chunk_key` function is structurally compatible with
  the new prefix format (it is — it just concatenates strings).
- **Unit test** in the uploader module: with a known `client_uuid` and
  event name, assert the S3 key built for upload matches
  `{uuid}/{name}/{seq}.bin`.
- **Integration test** (rs-endpoint or rs-api, whichever already has an S3
  mock or test bucket): upload a chunk with `client_uuid="A"` and another
  with `client_uuid="B"`, same event name. Assert the two keys are
  disjoint and each instance can only see its own chunks.
- **Existing Playwright E2E** continues to pass — it exercises the full
  RTMP → chunk → S3 → VPS → YouTube pipeline with a single client_uuid.
  No new E2E test needed; the existing one validates end-to-end with the
  new prefix.

## Non-Goals

- **Backward compatibility for old chunks.** The issue explicitly notes
  streams are one-shot — chunks from previous streams are ephemeral. No
  migration, no dual-read path, no grace period. Fresh streams use the new
  format; nothing reads the old format after deployment.
- **Rescue videos.** Already UUID-keyed (`rescue-videos/{uuid}.{ext}`).
  No collision risk, no change.
- **Orphan scanner.** Issue body defers this to a later ticket. VPS labels
  now carry `client_uuid` so a future scanner can filter correctly — that
  future scanner is out of scope here.
- **Tunnel-level leadership election / primary-client exclusion.** Issue
  body marks these "out of scope for MVP".

## Acceptance

- Fresh stream from a client with `client_uuid=A` produces S3 keys
  `A/{event_name}/0.bin`, `A/{event_name}/1.bin`, …
- Simultaneous stream from a different client `B` with the **same** event
  name produces disjoint keys `B/{event_name}/0.bin`, …
- Hetzner VPS for event `N` from client `A` carries label
  `client_uuid=A`.
- Existing E2E remains green (one client, one event — nothing regresses).
- Unit test proves the key composition.
- Integration test proves two client UUIDs produce disjoint key spaces.
