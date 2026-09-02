---
paths:
  - "scripts/install.ps1"
  - "crates/rs-core/tests/install_script_defaults.rs"
---

# Installer script (`scripts/install.ps1`) gotchas

`install.ps1` is the manual `irm ... | iex` installer. It self-elevates to admin
near the top, then downloads the release, writes `config.json`, installs the
Tauri NSIS package, registers the `RestreamerGUI` scheduled task, and starts the
app.

## There are TWO independent firewall-setup paths — keep them in sync

Firewall rules for the box exist in **two separate code paths that do NOT share
code and have drifted**:

1. `scripts/install.ps1` — the manual-install path (#108 added rules here:
   `Restreamer-API-8910` TCP 8910, `Restreamer-RTMP-1234` TCP 1234).
2. `.github/workflows/ci.yml` `deploy-stream-lan` job — its OWN inline block
   (`Restreamer API` TCP 8910, `Restreamer HTTPS` TCP 443). Historically had NO
   1234 rule (follow-up #363) and uses SPACE-separated DisplayNames that differ
   from install.ps1's `Restreamer-*` convention, so a box through both paths ends
   up with duplicate 8910 rules.

When you touch firewall behavior, check BOTH paths. Full alignment (names + 1234
in CI) is tracked in #363.

- Scope LAN rules with `-RemoteAddress LocalSubnet` (reachable across the LAN,
  the issue's ask; NOT exposed on a Public network — the API has no LAN-side
  auth, that lives at the Cloudflare Access edge).
- Give each rule a stable `-Name` key (otherwise it is GUID-named) and remove by
  `-DisplayName` before re-adding for idempotency.

## `$ErrorActionPreference = "Stop"` — wrap any fail-prone cmdlet non-fatally

The script sets `$ErrorActionPreference = "Stop"` at the top, so ANY
non-suppressed cmdlet error is TERMINATING and aborts the rest of the install
(e.g. the app never gets its scheduled task). New steps that can legitimately
fail on some boxes (a GPO-managed / locked-down firewall, a missing runtime)
must be wrapped in `try { ... } catch { Write-Err "..."; }` and continue — mirror
the WebView2 block. `-ErrorAction SilentlyContinue` on a `Remove-*` is fine (the
"nothing to remove" case is expected).

## ASCII only — no Unicode / em-dashes in the PowerShell

CI enforces ASCII in CI/PowerShell scripts. Verify with
`grep -nP '[^\x00-\x7F]' scripts/install.ps1` before committing.

## Asserting script SHAPE from Rust: use the skeleton, match exact tokens

`crates/rs-core/tests/install_script_defaults.rs` has a PowerShell-aware,
comment-stripping parser (from #348). Each physical line yields a `CodeLine`
with `.code` (string literals intact) and `.skeleton` (string INTERIORS blanked
to spaces, length-aligned). When asserting a flag/cmdlet is present:

- Match on `.skeleton`, NOT `.code` — else a `Write-Host "New-NetFirewallRule
  ... -LocalPort 8910"` string literal falsely satisfies the check.
- Read a `-DisplayName "value"` (a value that lives in a string) from `.code`,
  since `.skeleton` blanks it.
- Match a numeric flag value as an EXACT token (stop at the first non-digit) so
  `89101` does not match `8910`.
