---
paths:
  - "crates/rs-cloud/**"
  - "crates/rs-api/src/delivery*.rs"
  - "crates/rs-api/src/delivery_handlers.rs"
  - "src-tauri/tauri.conf.json"
---

# Hetzner delivery VPS — billing truth, sizing, and the orphan gap

## Query what is ACTUALLY billing — run it ON the box, never copy the token out

The Hetzner token lives in `config.hetzner.api_token` in `C:\ProgramData\Restreamer\config.json`
(env override `RESTREAMER_HETZNER_API_TOKEN`; CI secret `HETZNER_API_TOKEN`). Read it on the box so
the value never enters a transcript — via the `win-stream-snv` MCP `Shell`:

```powershell
$cfg = Get-Content 'C:\ProgramData\Restreamer\config.json' -Raw | ConvertFrom-Json
$h = @{ Authorization = "Bearer $($cfg.hetzner.api_token)" }
(Invoke-RestMethod -Uri 'https://api.hetzner.cloud/v1/servers' -Headers $h).servers |
  Select-Object id,name,status,created,@{n='labels';e={ $_.labels | ConvertTo-Json -Compress }}
```

`$_.labels` is a `PSCustomObject`, **not** a hashtable — `.GetEnumerator()` on it throws. Pipe it
through `ConvertTo-Json -Compress` instead.

A full "am I paying for anything" sweep is five endpoints, not one: `/servers`, `/volumes`,
`/floating_ips`, `/primary_ips`, `/load_balancers` (plus `/images?type=snapshot`, which on this
account holds unrelated `haos-*` / `baking-ai-mgmt-*` snapshots — **not restreamer's, do not delete**).

## Which install owns a given VPS

Every created server carries labels `app=restreamer`, `client_uuid=<creating install>`,
`event_id=<N>`. Match `client_uuid` against the box's own `config.client_uuid` to tell a stream.lan
VPS from a streampp one. The `rs-delivery-evtNNN` name embeds the event id of the *creating*
install's own numbering — event ids are NOT comparable across boxes.

## The orphan gap (#352) — a VPS with no DB row is invisible and bills forever

Every teardown path is keyed on a `delivery_instances` row: `stop_delivery`
(`delivery.rs:791-887`), `cleanup_orphan_delivery_vps` (`delivery_orphan.rs:26-89`, only on
`start_failed` / `stale_row`), and `reconcile_delivery_on_boot` (`delivery_recovery.rs:41-160`,
which re-attaches or no-ops). **Nothing ever asks Hetzner what exists.** No idle timeout, no reaper.
Lose the row (DB reset, reinstall, crash between server-create and row-write) and the server runs
until someone deletes it by hand. The label-selector sweep in `ci.yml` runs only in CI and is scoped
to CI's own `client_uuid` (#137), so it never covers production.

Diagnosis shortcut: `GET /api/v1/delivery/instances` returning **zero rows** while Hetzner shows
servers with this box's `client_uuid` is the orphan signature.

## Sizing is a guess (#353)

`rs_cloud::select_server_type(endpoint_count)` (`crates/rs-cloud/src/lib.rs:102-108`) hardcodes
`0..=2 => cpx22`, `3..=7 => cpx32`, `_ => cpx42`. `hetzner.default_server_type` in config is **dead**
— the creation path never reads it (only `location`, `ssh_key_name`, `snapshot_label`,
`extra_ssh_key_names`), so editing it changes nothing, and `install.ps1:140` still writes the
deprecated `cx23`. No CPU/RAM measurement exists anywhere; the only measured numbers are disk
(~1.8 GB worst case vs the 160 GB a cpx32 ships with) and the 1 Gbit NIC, which is identical across
the cpx line. Do not treat the current tier as validated.

## How the VPS gets its rs-delivery binary (the acquisition chain, #245/#246)

`start_delivery` (`delivery.rs`) → `delivery_binary::ensure_bucket_binary` HEADs the versioned,
immutable key `rs-delivery-{version}` in the CLIENT's OWN S3 bucket. **Present → no network** (cloud-init
`curl`s it from that bucket). **Absent** (typically the client's first event after a version upgrade)
→ upload the bytes to that key, then cloud-init pulls them. Bytes come, in order:

1. **Bundled binary shipped in the Windows install** (#246, the zero-GitHub path): a Tauri
   `bundle.resources` sidecar `rs-delivery-linux` (+ `rs-delivery-linux.sha256`) placed next to the
   exe. rs-api finds it via `current_exe()` (`find_bundled_binary`), sha256-verifies it against the
   sidecar, uploads it. No `github.com`.
2. **GitHub release fallback** (`upload_release_binary`): downloads
   `.../releases/download/restreamer-v{version}/rs-delivery-{version}-linux-amd64` (+ `.sha256`),
   verifies, uploads. Taken when no bundle is present (dev builds, older installs) OR the bundled
   file is unusable (unreadable/empty/sha-mismatch → falls back instead of aborting).

**CI never exercises the bundled/absent path:** `ci.yml build-delivery` `aws s3 cp`s the versioned
key into stream.lan's shared `restreamer-chunks-fsn1` bucket on every push, so stream.lan always
takes the "key present" branch. The GitHub dependency only ever bit a REAL client (own bucket, not
pre-primed) on its first post-upgrade event — that is exactly what #246 removed.

## Tauri `bundle.resources` HARD-FAILS the build (and `cargo check`) on a missing file

A file declared in `bundle.resources` (`src-tauri/tauri.conf.json`) MUST exist at build time, or the
`tauri-build` build script aborts with `resource path '<name>' doesn't exist` — and that build script
runs on **`cargo check` too**, not just `cargo tauri build`. Consequences:

- Both `build-tauri` jobs (ci.yml + release.yml) MUST stage the file (copy the `build-delivery`
  artifact to `src-tauri/rs-delivery-linux` + its `.sha256`) BEFORE `cargo tauri build`. This is a
  strong guard: a broken wiring fails the build LOUD, never ships an installer silently missing the
  binary.
- To `cargo check` src-tauri on dev2 you must stage two placeholders first (`truncate -s 0
  rs-delivery-linux; : > rs-delivery-linux.sha256`) — see the `dev2-build-verify` skill.
- The one silent-failure point is DROPPING the `bundle.resources` declaration itself (Tauri then just
  won't bundle it, no error) — guarded by the `test-integrity` `jq` self-check in ci.yml.
## create_server retries transient errors + adopts-on-409 (#223)

`HetznerClient::create_server` (`crates/rs-cloud/src/hetzner.rs`) retries transient Hetzner API
failures server-side: transport-level `reqwest` errors (`is_timeout`/`is_connect`/`is_request`/
`is_decode`), `429`, `5xx`, and `409`. Capped exponential backoff (1s/3s/9s, `MAX_BACKOFF` 30s),
default 4 attempts; override with `HetznerClient::with_retry(max_attempts, base_backoff)` (tests
pass a ~1ms backoff so they don't sleep whole seconds). Permanent `4xx` (bad token, malformed)
surface immediately.

**Idempotency is via the 409, not a speculative lookup.** A transport error can create the VPS
before the error surfaces, so a blind retry risks a SECOND VPS. Rather than guess, the retry leans
on Hetzner's per-project **name uniqueness**: the deterministic name `rs-delivery-evt{event_id}`
means a retry of an already-created server returns `409`, and on that signal `create_server` looks
the server up by name (`GET /servers?name=`) and ADOPTS it. Adoption is guarded by `is_adoptable`:
it refuses a server that is `status == "deleting"` (the OLD same-named VPS `start_delivery` deletes
right before creating the new one — the #244/#352 cleanup path) or whose labels don't match every
`app`/`event_id`/`client_uuid` we're creating with. A `409` we can't adopt (old VPS still deleting)
is itself treated as transient — a later attempt succeeds once the name frees.

**reqwest 0.12 gotcha:** a truncated/unreadable RESPONSE body is `is_decode()`, NOT `is_body()`
(`is_body` is request-body streaming, unreachable for the in-memory JSON POST). And `Client::new()`
has NO timeout by default — the client is built with `connect_timeout(10s)`+`timeout(30s)` so a
hung request can't block `delivery_start` forever and `is_timeout()` is actually reachable.

**Testing (wiremock, a dev-dependency already in the workspace lock via rs-youtube):** wiremock
serves the **first-registered** matching mock that still has capacity — so to mock "503 then 201"
you mount the 503 mock FIRST with `.up_to_n_times(1)`, then the 201 mock; the retry falls through
to the 201 once the 503 is exhausted. (Adding wiremock to rs-cloud's dev-deps adds one line to
`Cargo.lock` — the `rs-cloud -> wiremock` edge; regenerate the lock or a `--locked` CI job fails.)
