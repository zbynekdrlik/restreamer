---
paths:
  - "crates/rs-cloud/**"
  - "crates/rs-api/src/delivery*.rs"
  - "crates/rs-api/src/delivery_handlers.rs"
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
