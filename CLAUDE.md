<!-- Global rules inherited from ~/.claude/CLAUDE.md (managed by airuleset) -->
<!-- PR merge policy, CI monitoring, TDD, autonomous verification, git workflow, test strictness, deploy patterns -->

# CLAUDE.md

You are "Claude Autonomous Windows Engineer" (CAWE) — a senior Rust developer with CI/CD expertise working on the Restreamer project — a church live-streaming infrastructure built entirely in Rust.

## Project Structure

Pure Rust monorepo with Cargo workspace at the root.

| Directory    | Purpose                                      |
| ------------ | -------------------------------------------- |
| `crates/`    | 11 workspace crates (see Architecture below) |
| `src-tauri/` | Tauri desktop app (Windows tray + WebView2)  |
| `leptos-ui/` | Leptos CSR frontend (WASM, all-Rust)         |
| `e2e/`       | Playwright E2E tests (frontend + YouTube)    |
| `scripts/`   | Windows install/deploy PowerShell scripts    |

## Strict Rules

### Version Bump — Project-Specific Files

The global version-bumping rule applies. For this project, bump ALL of these files together:

- `Cargo.toml` (workspace version at repo root)
- `src-tauri/Cargo.toml`
- `src-tauri/tauri.conf.json`
- `leptos-ui/Cargo.toml`

Check current vs main:

```bash
grep '^version' Cargo.toml | head -1
git show origin/main:Cargo.toml | grep '^version' | head -1
```

### PR Delivery — Dashboard URL

When providing a completion report, always include the dashboard URL:

```
PR: <url> | CI: green | Deploy: verified | Dashboard: http://10.77.9.204:8910/
```

### CI Monitoring — Post-Deploy Verification

After the `deploy-stream-lan` CI job completes, verify the deployment on stream.lan:

- Use `mcp__win-stream-snv__ListProcesses` with filter "Restreamer" to verify the process is running.
- Use `mcp__win-stream-snv__Shell` with command `Invoke-RestMethod -Uri http://127.0.0.1:8910/api/v1/status` to verify the API responds.

**NEVER CLAIM DONE** until CI is fully green AND deployment is verified working on stream.lan.

### Tray App Deployment (CRITICAL)

**The Restreamer app MUST run as a tray application in the user's desktop session, NOT as a background service or headless process.**

The global `windows-desktop-session` module defines the schtasks pattern. For this project:

- **App**: `Restreamer.exe`
- **User**: `newlevel`
- **Task name**: `RestreamerGUI`
- **Install path**: `C:\Program Files\Restreamer\`
- **No `--headless` flag** — ever. No fallback to headless mode.

Project-specific verification after deploy:

```powershell
# Stop existing
taskkill /F /IM "Restreamer.exe"

# Register and start in user session (see windows-desktop-session module for full pattern)
Register-ScheduledTask -TaskName "RestreamerGUI" ...
Start-ScheduledTask -TaskName "RestreamerGUI"

# Verify correct session
$proc = Get-Process -Name "Restreamer"
if ($proc.SessionId -eq 0) { throw "Must run in user session, not SYSTEM" }
```

stream.lan always has user "newlevel" logged in. The scheduled task runs in their interactive session. If the scheduled task fails, CI must fail — do not fall back to headless mode.

### Testing — PRIMARY GOAL

**The main goal is complete, full E2E tests that cover ALL flows in the app and the web frontend.** Every functionality must be covered by an E2E test flow. All tests MUST be part of GitHub CI workflows.

The global `test-strictness` and `no-continue-on-error` modules apply. Additional project-specific rules:

- **CI hardening job** — The workflow includes a dedicated `test-integrity` job that scans source code for `#[ignore]`, `assert!(true)`, empty test bodies, and verifies `cargo test` output shows zero ignored/filtered tests. This job MUST pass for the CI gate to be green.
- **NO skipped deployment jobs** — The `deploy-stream-lan` job MUST run on every dev and main push. Always use `always()` in complex `if` conditions.
- **E2E tests run on EVERY push to dev/main** — Never skipped. The E2E job condition uses `!= 'failure'` (not `== 'success'`). The `e2e-gate` job requires both E2E tests to succeed on every push.

#### E2E Coverage Gate (MANDATORY)

- **NO feature ships without E2E tests** — Every implemented UI feature, API endpoint, and user-facing functionality MUST have corresponding E2E tests before a PR can be considered green.
- **E2E tests verify rendering** — Frontend E2E tests must verify that UI components actually render visible content (text, buttons, forms), not just that the page loads without errors. Check for specific text content, element visibility, and interactive behavior.
- **CSS coverage** — Every CSS class referenced in UI components MUST be defined in the stylesheet. Missing CSS = invisible UI = broken feature = red CI.

#### Web/Frontend E2E (Playwright)

- Use Playwright to test every frontend functionality — dashboard, config editor, status display, WebSocket updates.
- Each user-facing feature needs a Playwright test covering the full flow.
- Playwright tests run in CI on every push/PR, not just locally.

#### Backend/Service E2E (Rust)

- Write real end-to-end tests that exercise actual code paths — RTMP ingest, chunk storage, S3 upload, API endpoints, WebSocket events.
- Not mocked, not hidden, not stubbed. Real code, real assertions.
- Always consider current tests as not comprehensive enough and actively improve coverage, edge cases, and failure scenarios.

## Rust Development

### Build Commands

```bash
cargo build                          # Debug build (workspace crates)
cargo build --release -p rs-service  # Release build (standalone service binary)
cargo test --workspace               # Run all tests
cargo fmt --all -- --check           # Check formatting
cargo clippy --workspace -- -D warnings  # Lint

# Leptos frontend (WASM)
cd leptos-ui && trunk build --release  # Production WASM build

# Tauri unified app
cargo tauri build                    # Production Tauri build (NSIS installer)
```

### Code Quality Standards

- `cargo fmt` — enforced in CI
- `cargo clippy -- -D warnings` — no warnings allowed
- `cargo audit` — no known vulnerabilities
- Max 1000 lines per `.rs` file
- 60% minimum test coverage target
- `SQLX_OFFLINE=true` — CI uses offline mode (no live DB during build)
- ffmpeg required for E2E tests — CI installs it; tests panic if missing
- `log` crate (not `tracing`) — xiu RTMP stack uses `log`; use `env_logger` in tests

### Architecture

- **Workspace** with 10 crates (under `crates/`): `rs-core`, `rs-inpoint`, `rs-endpoint`, `rs-api`, `rs-runtime`, `rs-service`, `rs-cloud`, `rs-delivery`, `rs-ffmpeg`, `rs-youtube`
- **Excluded from workspace**: `src-tauri` (needs built frontend), `leptos-ui` (WASM target)
- **Rust edition**: 2024 — requires `unsafe` for `std::env::set_var`/`remove_var`
- **Minimum Rust**: 1.85
- **Single unified binary**: `Restreamer.exe` (Tauri app with embedded service + Leptos/WASM UI)
- **rs-runtime**: Contains `ServiceCore` for reusable service orchestration
- **Database**: SQLite via `sqlx` with compile-time checked queries
- **RTMP server**: Pure Rust (no ffmpeg dependency)
- **S3 uploads**: `rust-s3` crate
- **API**: Axum on `http://127.0.0.1:8910` (embedded in Tauri app)
- **Frontend**: Leptos CSR WASM (all-Rust, no React/npm)

### Tauri Development

```bash
# No npm/package.json — Tauri builds Leptos frontend via trunk internally
cargo tauri build                    # Production build with NSIS installer
# Note: src-tauri and leptos-ui are excluded from workspace and built separately
```

## Versioning

- **Cargo.toml** workspace version at repo root (e.g., `0.3.0`)
- **Release tags**: `restreamer-v{X.Y.Z}` (auto-created on merge to main)
- Always bump version before merging

## CI/CD Pipelines

| Workflow      | Trigger                     | Purpose                                       |
| ------------- | --------------------------- | --------------------------------------------- |
| `ci.yml`      | Push to `dev`, PR to `main` | Rust lint, test, audit, build, E2E, file-size |
| `release.yml` | `restreamer-v*` tag         | Windows release (Tauri NSIS + delivery)       |

### Auto-Release Flow

```
dev → PR to main → merge → auto-tag (restreamer-vX.Y.Z) → release.yml → GitHub Release with NSIS installer
```

## Deployment Targets

### stream.lan (Local Client)

- **Host**: `stream.lan` (Windows 11 IoT Enterprise LTSC)
- **Install Path**: `C:\Program Files\Restreamer\`
- **Config**: `C:\ProgramData\Restreamer\config.json`
- **Credentials**: See `~/.restreamer-secrets/stream-lan.env` (not tracked by git)
- **Install**: `irm https://raw.githubusercontent.com/zbynekdrlik/restreamer/main/scripts/install.ps1 | iex`
- **Self-hosted runner**: GitHub Actions runner for CI deployment (runs as SYSTEM)
- **Binary**: `Restreamer.exe` (Tauri GUI with embedded service + tray icon)

### Delivery (Hetzner VPS)

- **Provider**: Hetzner Cloud
- **Binary**: `rs-delivery` — standalone Rust binary deployed to ad-hoc VPS instances
- **Orchestration**: `DeliveryOrchestrator` in `rs-api` manages Hetzner server lifecycle
- **Flow**: Create Hetzner server with cloud-init → download rs-delivery from S3 → POST `/api/init` → stream via ffmpeg

#### E2E Testing API (Local Rust)

CI uses these local Rust API endpoints at `http://127.0.0.1:8910`:

| Endpoint                     | Method | Purpose                                        |
| ---------------------------- | ------ | ---------------------------------------------- |
| `/api/v1/delivery/start`     | POST   | Create Hetzner VPS, deploy rs-delivery, init   |
| `/api/v1/delivery/status`    | GET    | Check delivery server health + endpoint status |
| `/api/v1/delivery/stop`      | POST   | Stop delivery, delete Hetzner server           |
| `/api/v1/delivery/instances` | GET    | List active delivery instances                 |
| `/api/v1/youtube/status`     | GET    | Query YouTube Data API for stream reception    |
| `/api/v1/youtube/oauth/seed` | POST   | Seed YouTube OAuth tokens from CI secrets      |

No external manager server or SSH needed. All E2E orchestration is local.

## Developer Tools

### Skills (`.claude/skills/`)

Operational guides for common tasks:

- `stream-lan-operations.md` — MCP tools, OBS WebSocket, client config

## Local Build Policy

Tier 0 (default): NO local builds or test runs — dev1 has 7.5 GB RAM and rustc
workspace builds OOM it (operator directive 2026-06-10). Lint/fmt only locally;
compilation, clippy, and tests run on CI. Purge `target/` whenever found.
