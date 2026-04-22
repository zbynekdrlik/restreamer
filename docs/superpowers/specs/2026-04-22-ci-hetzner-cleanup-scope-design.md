# CI Hetzner Cleanup Scope — Design

**Issue:** #137 — CI pre-flight Hetzner cleanup deletes production VPS from other Restreamer instances (e.g. streampp)

**Goal:** Scope the CI pre-flight Hetzner-VPS cleanup to only delete servers belonging to this Restreamer installation, so a CI run on one installation (e.g. stream.lan) can never delete another installation's active VPS (e.g. streampp).

**Version:** 0.3.67 (strictly greater than 0.3.66 on main)

---

## Incident reference

- **2026-04-22 15:57 UTC** — stream.lan's CI pre-flight step deleted streampp's active production delivery VPS `rs-delivery-evt8` (Hetzner id 127743979) two minutes into streampp's live service. Operator fell back to OBS plugin mid-session.
- Root cause: `.github/workflows/ci.yml:2323-2349` filters by `label_selector=app=restreamer`. That label is generic across every Restreamer installation sharing the Hetzner account.

## Root cause

```powershell
# Current code (ci.yml:2329)
$resp = Invoke-RestMethod -Uri "https://api.hetzner.cloud/v1/servers?label_selector=app=restreamer" ...
foreach ($srv in $resp.servers) {
  DELETE /v1/servers/$($srv.id)
}
```

The label `app=restreamer` is added to **every** VPS our Hetzner account creates. Two (or more) Restreamer installations sharing a single Hetzner API token will each match the other's VPS in this query.

## Existing infrastructure (no new labels needed)

`crates/rs-api/src/delivery.rs:182-185` already applies three labels to every VPS it creates:

```rust
let mut labels = HashMap::new();
labels.insert("app".to_string(), "restreamer".to_string());
labels.insert("event_id".to_string(), event_id.to_string());
labels.insert("client_uuid".to_string(), self.config.client_uuid.clone());
```

`client_uuid` is a UUID generated once per Restreamer installation and persisted in `config.json`. It is unique per installation (stream.lan has one, streampp has another, any future deployment will have its own).

`client_uuid` is exposed over the local API at `GET http://127.0.0.1:8910/api/v1/config` (returns the full `Config` struct, with S3 credentials redacted — `client_uuid` is plain).

## Solution — scope cleanup by `client_uuid`

Change the pre-flight Hetzner cleanup step to:

1. Fetch the local Restreamer's `client_uuid` via `GET /api/v1/config`.
2. Use Hetzner's multi-label filter: `label_selector=app=restreamer,client_uuid=<local-uuid>` (Hetzner's `label_selector` treats commas as AND).
3. Delete only VPS matching both labels — the installation's own VPS.

### Fail-closed on missing client_uuid

If the local API is unreachable, returns no `client_uuid`, or returns an empty string, the CI step **aborts with exit 1** rather than falling back to the old broad filter. Better to leave a genuine orphan VPS than risk deleting another installation's production VPS.

### Sketch of new PowerShell block (replaces ci.yml:2323-2349)

```powershell
# Step 2: Clean up orphaned Hetzner VPS directly (scoped to this installation).
if ($env:HETZNER_API_TOKEN) {
  # Resolve this installation's client_uuid to scope the cleanup.
  $clientUuid = $null
  try {
    $cfg = Invoke-RestMethod -Uri "http://127.0.0.1:8910/api/v1/config" -TimeoutSec 10
    $clientUuid = $cfg.client_uuid
  } catch {
    Write-Host "  Could not read local client_uuid: $_"
  }

  if (-not $clientUuid) {
    Write-Host "ERROR: client_uuid not available — refusing to run Hetzner cleanup (fail-closed to protect other installations)"
    exit 1
  }

  Write-Host "Checking Hetzner API for orphaned rs-delivery VPS for client_uuid=$clientUuid ..."
  try {
    $headers = @{ Authorization = "Bearer $($env:HETZNER_API_TOKEN)" }
    $selector = "app=restreamer,client_uuid=$clientUuid"
    $resp = Invoke-RestMethod -Uri "https://api.hetzner.cloud/v1/servers?label_selector=$selector" `
      -Headers $headers -TimeoutSec 15
    if ($resp.servers -and $resp.servers.Count -gt 0) {
      foreach ($srv in $resp.servers) {
        Write-Host "  Deleting orphaned VPS (this installation): id=$($srv.id) name=$($srv.name) status=$($srv.status) ip=$($srv.public_net.ipv4.ip)"
        try {
          Invoke-RestMethod -Uri "https://api.hetzner.cloud/v1/servers/$($srv.id)" `
            -Method DELETE -Headers $headers -TimeoutSec 15
          Write-Host "  Deleted VPS $($srv.id)"
        } catch {
          Write-Host "  Failed to delete VPS $($srv.id): $_"
        }
      }
      Start-Sleep -Seconds 3
    } else {
      Write-Host "  No orphaned VPS found for this installation"
    }
  } catch {
    Write-Host "  Hetzner API check failed: $_"
  }
}
```

## Regression test

Add a static assertion as a new step in the existing `test-integrity` job (`.github/workflows/ci.yml:420`, runs on `ubuntu-latest` with inline bash). The step scans `.github/workflows/*.yml` for the pattern `label_selector=app=restreamer` and fails if any occurrence is not immediately followed by `,client_uuid=`. Prevents a future copy-paste from reintroducing the broad filter.

```yaml
- name: Scan for unscoped Hetzner label_selector
  run: |
    BAD=$(grep -Prn 'label_selector=app=restreamer(?!,client_uuid=)' .github/workflows/ || true)
    if [ -n "$BAD" ]; then
      echo "ERROR: Hetzner label_selector must include ,client_uuid=<uuid> scope to protect other installations:"
      echo "$BAD"
      exit 1
    fi
    echo "OK: All Hetzner label_selector usages are client_uuid-scoped."
```

The Perl-compatible negative lookahead (`(?!,client_uuid=)`) catches every occurrence of `label_selector=app=restreamer` that is NOT immediately followed by `,client_uuid=`:

- `label_selector=app=restreamer"` — fails (no `,client_uuid=`).
- `label_selector=app=restreamer,client_uuid=...` — passes.
- `label_selector=app=restreamer,event_id=...` — fails (correct: a different scope label also violates the rule).
- `label_selector=app=restreamer` at end-of-line — fails (catches stray unterminated URLs too).

## Out of scope (YAGNI)

- **Per-installation Hetzner label** (e.g. `installation=stream-lan`). `client_uuid` already uniquely identifies each installation; adding another label is redundant.
- **Separate Hetzner projects per installation.** Long-term hardening — unnecessary now that the cleanup is scoped.
- **Retroactive cleanup of pre-0.3.0 VPS** missing `client_uuid` label. All currently-created VPS carry this label; none older than 0.3.0 exist on the account.
- **Additional Rust code changes.** No crate source needs editing — `client_uuid` is already applied and already exposed.

## Verification

1. **Unit-level** — `test-integrity` regression-test assertion passes on the patched ci.yml, fails on the old pattern.
2. **Integration-level** — CI run completes its pre-flight cleanup against a Hetzner account that holds:
   - VPS with `app=restreamer,client_uuid=<stream-lan-uuid>` → deleted (expected).
   - VPS with `app=restreamer,client_uuid=<some-other-uuid>` (e.g. manually created to simulate streampp) → **not** deleted.
3. **Post-deploy** — after merge, a stream.lan CI run that coexists with a streampp VPS (real or synthesized) leaves streampp's VPS intact.

## Files changed

- `.github/workflows/ci.yml` — replace the cleanup block (lines 2323-2349) with the scoped version above.
- `.github/workflows/ci.yml` (test-integrity step) — add the `label_selector` regex assertion.
- `Cargo.toml`, `src-tauri/Cargo.toml`, `src-tauri/tauri.conf.json`, `leptos-ui/Cargo.toml` — bump `0.3.66` → `0.3.67`.

No Rust crates are modified.
