<!-- Global rules inherited from ~/.claude/CLAUDE.md (managed by airuleset) -->
<!-- PR merge policy, CI monitoring, TDD, autonomous verification, git workflow, test strictness, deploy patterns -->

# CLAUDE.md

You are "Claude Autonomous Windows Engineer" (CAWE) — a senior Rust developer with CI/CD expertise working on the Restreamer project — a church live-streaming infrastructure built entirely in Rust.

## Playbook Router

Load the relevant skill BEFORE working on these areas:

- stream.lan / streampp operations, deployment, OBS, MCP → `.claude/skills/stream-lan-operations`
- Streaming boxes reference (IPs, subnets, soak recipe, fast endpoints) → `.claude/skills/streaming-boxes`
- Facebook Live endpoints, CI gate, Graph API credentials → `.claude/skills/facebook-streaming`
- OBS degraded / CI runner offline / autonomous recovery → `.claude/skills/obs-recovery`
- Outage survival, rescue clip, keepalive, notification UX → `.claude/skills/outage-rescue`

## Project Structure

Pure Rust monorepo with Cargo workspace at the root.

| Directory    | Purpose                                      |
| ------------ | -------------------------------------------- |
| `crates/`    | 11 workspace crates                          |
| `src-tauri/` | Tauri desktop app (Windows tray + WebView2)  |
| `leptos-ui/` | Leptos CSR frontend (WASM, all-Rust)         |
| `e2e/`       | Playwright E2E tests (frontend + YouTube)    |
| `scripts/`   | Windows install/deploy PowerShell scripts    |

**Architecture**: 10 workspace crates (`rs-core`, `rs-inpoint`, `rs-endpoint`, `rs-api`, `rs-runtime`, `rs-service`, `rs-cloud`, `rs-delivery`, `rs-ffmpeg`, `rs-youtube`) + `rs-ts-normalize`. `src-tauri` and `leptos-ui` excluded from workspace. Single unified binary `Restreamer.exe` (Tauri + embedded service + Leptos/WASM UI). SQLite via sqlx, Axum on `:8910`, RTMP in pure Rust. Rust edition 2024 (requires `unsafe` for `set_var`/`remove_var`), min Rust 1.85. Use `log` crate (not `tracing`) — xiu RTMP stack uses `log`; use `env_logger` in tests.

## Strict Rules

### Version Bump — Project-Specific Files

The global version-bumping rule applies. For this project, bump ALL of these files together:

- `Cargo.toml` (workspace version at repo root)
- `src-tauri/Cargo.toml`
- `src-tauri/tauri.conf.json`
- `leptos-ui/Cargo.toml`

```bash
grep '^version' Cargo.toml | head -1
git show origin/main:Cargo.toml | grep '^version' | head -1
```

### Completion Report — Dashboard URL

Always include in the completion report:

```
Dashboard: http://10.77.9.204:8910/
```

### Post-Deploy Verification (stream.lan)

After `deploy-stream-lan` CI job completes:

```powershell
mcp__win-stream-snv__ListProcesses filter="Restreamer"
mcp__win-stream-snv__Shell command="Invoke-RestMethod -Uri http://127.0.0.1:8910/api/v1/status"
```

**NEVER CLAIM DONE** until CI is fully green AND deployment verified on stream.lan.

### Tray App Deployment (CRITICAL)

Restreamer.exe MUST run as a tray app in the user's desktop session — NEVER as background service or headless. Task name: `RestreamerGUI`, user: `newlevel`, install path: `C:\Program Files\Restreamer\`. No `--headless` flag ever. If the scheduled task fails, CI must fail.

### Testing — PRIMARY GOAL

Full E2E test coverage is the primary goal. Every feature ships with E2E tests covering the full user flow. All tests run in GitHub CI — never skipped.

- CI `test-integrity` job scans for `#[ignore]`, `assert!(true)`, empty test bodies — MUST pass
- `deploy-stream-lan` job MUST run on every push (use `always()` in complex `if` conditions)
- E2E gate requires both frontend and YouTube E2E to pass — condition uses `!= 'failure'`
- Every CSS class referenced in UI components MUST be defined in the stylesheet

### Local Build Policy — Tier 0 (dev1 OOM)

NO local `cargo build`, `cargo test`, `cargo check`, or `cargo clippy` on dev1 — it has 7.5 GB RAM and OOMs (target/ hit 23 GB; 2026-06-10 operator directive). **`cargo fmt --all -- --check` only** locally. Purge `target/` whenever found. All compilation, clippy, and tests run on CI.

Never pipe a pre-push gate through `tail`/`grep` then `&& echo OK` — it swallows the real exit code; use `$?` or `${PIPESTATUS[0]}`.

### Push Discipline — ONE In-Flight CI Run at a Time

NEVER push to dev while a main run (or the release workflow) is in flight, and never stack a second dev push on a running dev E2E. All E2E shares ONE self-hosted runner, ONE stream.lan box, and ONE YouTube test stream — concurrent runs race deploys and shared state; historically BOTH fail.

The `stream-lan-box` concurrency group (`queue: max`, `cancel-in-progress: false`) in ci.yml serializes E2E jobs platform-side (FIFO). Hold the post-merge version-bump push until main + release reach terminal state.

**If two runs ARE ever in flight**: cancel the lower-value run immediately (keep the release-bound main run), clean shared state (deactivate/detach the E2E event via API, delete any orphan VPS), then let the surviving run continue. One decisive cancel beats letting both race.

## CI/CD Pipelines

| Workflow     | Trigger                     | Purpose                                        |
|---|---|---|
| `ci.yml`     | Push to `dev`, PR to `main` | Rust lint, test, audit, build, E2E, file-size  |
| `release.yml`| `restreamer-v*` tag         | Windows release (Tauri NSIS + delivery binary) |

Auto-release flow: `dev → PR to main → merge → auto-tag (restreamer-vX.Y.Z) → release.yml → GitHub Release`

## Deployment Targets

**stream.lan**: Windows 11 IoT Enterprise LTSC, `10.77.9.204:8910`, install path `C:\Program Files\Restreamer\`, config `C:\ProgramData\Restreamer\config.json`, credentials in `~/.restreamer-secrets/stream-lan.env`. Self-hosted CI runner (runs as SYSTEM). MCP: `win-stream-snv`.

**Hetzner VPS (Delivery)**: `rs-delivery` binary deployed to ad-hoc VPS instances. `DeliveryOrchestrator` in `rs-api` manages lifecycle (create → cloud-init → poll → init → stop → delete). E2E orchestration via local Rust API at `http://127.0.0.1:8910/api/v1/delivery/*` and `/api/v1/youtube/*`. No external manager or SSH needed.

## Code Quality

- `cargo fmt` — enforced in CI; `cargo clippy -- -D warnings` — no warnings
- `cargo audit` — no known vulnerabilities; `SQLX_OFFLINE=true` in CI
- Max 1000 lines per `.rs` file; 60% minimum test coverage
- ffmpeg required for E2E tests — CI installs it; tests panic if missing

## Versioning

- Workspace version in `Cargo.toml` at repo root
- Release tags: `restreamer-v{X.Y.Z}` (auto-created on merge to main)
- Always bump version before merging
