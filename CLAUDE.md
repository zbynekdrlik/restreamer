# CLAUDE.md

You are a senior Rust + Python developer with CI/CD expertise working on the Restreamer project — a church live-streaming infrastructure with three Python/Django services and a new Rust local client.

## Project Structure

| Directory             | Language        | Purpose                                           |
| --------------------- | --------------- | ------------------------------------------------- |
| `local-client/`       | Python (Django) | Legacy Windows RTMP client (being replaced)       |
| `local-client-rs/`    | Rust + React    | New Rust local client (Tauri v2 + service binary) |
| `manager-server/`     | Python (Django) | Central management server (Linode VPS)            |
| `delivering-service/` | Python (Django) | Linux re-streaming service                        |

## Strict Rules

### Pull Requests

- **AGENT RESPONSIBILITY**: You are ALWAYS responsible for verifying and delivering a mergeable, green PR with all tests passing. Never hand off broken PRs to the user.
- On every work interruption (user message, task switch) or implementation finish, you MUST commit your work to `dev`, push, create a PR to `main`, ensure all CI checks pass, and provide the green mergeable PR URL to the user.
- Never provide a PR URL that has failing checks or merge conflicts.
- After creating a PR, monitor the CI pipeline status. If checks fail, fix the issues, push fixes, and only then share the final green PR URL.
- **VERIFY BEFORE SHARING**: Before providing ANY PR URL to the user, you MUST run:
  ```bash
  gh api repos/OWNER/REPO/pulls/NUMBER --jq '{mergeable: .mergeable, mergeable_state: .mergeable_state}'
  ```
  The PR is ONLY ready when: `mergeable: true` AND `mergeable_state: "clean"`. If `mergeable_state` is "behind", sync branches first with `git fetch origin && git merge origin/main`. If "blocked" or "dirty", fix the issues. NEVER claim a PR is ready without this verification.
- Every PR MUST include tests covering the implemented changes. No PR is complete without tests.
- NEVER merge a PR. Only the user may merge pull requests. The agent must only create the PR, ensure CI is green, and provide the URL. Merging is exclusively the user's action.

### Testing — PRIMARY GOAL

**The main goal is complete, full E2E tests that cover ALL flows in the app and the web frontend.** Every functionality must be covered by an E2E test flow. All tests MUST be part of GitHub CI workflows.

#### Zero Tolerance Rules

- **NO `#[ignore]`** — Never add `#[ignore]` to any test. Every test runs, every time.
- **NO `#[cfg(skip)]`** or conditional compilation that disables tests.
- **NO false positives** — No `assert!(true)`, no empty test bodies, no tests that pass without exercising real code. Every assertion must verify actual behavior.
- **NO skipped tests** — CI output must show `0 ignored; 0 filtered out` for every test binary.
- **NO mocking real code** — Mocks are ONLY acceptable for external network services (S3, manager HTTP). Internal code paths must be tested with real implementations.
- **CI hardening job** — The workflow includes a dedicated `test-integrity` job that scans source code for `#[ignore]`, `assert!(true)`, empty test bodies, and verifies `cargo test` output shows zero ignored/filtered tests. This job MUST pass for the CI gate to be green.
- **NO skipped deployment jobs** — The `deploy-stream-lan` job MUST run on every dev and main push. If it shows as "skipped", something is wrong with the workflow condition. Always use `always()` in complex `if` conditions to ensure proper evaluation.

#### Web/Frontend E2E (Playwright)

- Use Playwright to test every frontend functionality — dashboard, config editor, status display, WebSocket updates.
- Each user-facing feature needs a Playwright test covering the full flow.
- Playwright tests run in CI on every push/PR, not just locally.

#### Backend/Service E2E (Rust)

- Write real end-to-end tests that exercise actual code paths — RTMP ingest, chunk storage, S3 upload, API endpoints, WebSocket events.
- Not mocked, not hidden, not stubbed. Real code, real assertions.
- Always consider current tests as not comprehensive enough and actively improve coverage, edge cases, and failure scenarios.

#### General

- Every feature, bugfix, and refactor must have corresponding tests that verify actual behavior.
- ALL tests must pass — a passing test suite must reflect genuinely working code.

## Rust Development (`local-client-rs/`)

### Build Commands

```bash
cd local-client-rs
cargo build                          # Debug build (all crates)
cargo build --release -p rs-service  # Release build (service binary only)
cargo test --workspace               # Run all tests
cargo fmt --all -- --check           # Check formatting
cargo clippy --workspace -- -D warnings  # Lint
npx tauri dev                        # Hot-reload Tauri app (dev mode)
npx tauri build                      # Production Tauri build (NSIS installer)
```

### Code Quality Standards

- `cargo fmt` — enforced in CI
- `cargo clippy -- -D warnings` — no warnings allowed
- `cargo audit` — no known vulnerabilities
- Max 1000 lines per `.rs` file
- 60% minimum test coverage target
- TDD approach: write tests alongside features

### Architecture

- **Workspace** with 6 crates: `rs-core`, `rs-inpoint`, `rs-endpoint`, `rs-api`, `rs-service`, `src-tauri`
- **Two binaries**: `restreamer-service.exe` (headless Windows service) + `Restreamer.exe` (Tauri tray app)
- **Database**: SQLite via `sqlx` with compile-time checked queries
- **RTMP server**: Pure Rust (no ffmpeg dependency)
- **S3 uploads**: `rust-s3` crate
- **API**: Axum on `http://127.0.0.1:8910`
- **Tray ↔ Service**: HTTP/WS to localhost (no named pipes)

### Tauri Development

```bash
cd local-client-rs
npm install                          # Install frontend dependencies
npx tauri dev                        # Start dev server + Tauri app
npx tauri build                      # Production build with NSIS installer
```

## Versioning

- **Python**: `VERSION` file at repo root (e.g., `0.1.5`)
- **Rust**: `Cargo.toml` version in `local-client-rs/` (e.g., `0.1.0`)
- **Rust tags**: `local-client-rs-v{X.Y.Z}` (auto-created on merge to main)
- Always bump version before merging

## CI/CD Pipelines

| Workflow            | Trigger                     | Purpose                                    |
| ------------------- | --------------------------- | ------------------------------------------ |
| `ci.yml`            | Push to `dev`, PR to `main` | Python lint + test (all 3 Django services) |
| `version-check.yml` | PR to `main`                | Ensure VERSION is bumped                   |
| `rust-ci.yml`       | Push to `dev`, PR to `main` | Rust lint, test, audit, build, file-size   |
| `rust-release.yml`  | `local-client-rs-v*` tag    | Windows release (service + Tauri NSIS)     |

### Auto-Release Flow

```
dev → PR to main → merge → auto-tag (local-client-rs-vX.Y.Z) → rust-release.yml → GitHub Release with NSIS installer
```

### Branch Policy

- Exactly two branches: `main` (production) and `dev` (development)
- All work on `dev`, PR to `main` for releases
- No feature branches, no direct main pushes

## Deployment Targets

### stream.lan (Local Client)

- **Host**: `stream.lan` (Windows 11 IoT Enterprise LTSC)
- **Install Path**: `C:\Program Files\Restreamer\` (new Rust client)
- **Legacy Path**: `C:\Users\newlevel\restreamer\` (Python client)
- **Config**: `C:\ProgramData\Restreamer\config.json`
- **Credentials**: See `~/.restreamer-secrets/stream-lan.env` (not tracked by git)
- **Install**: `irm https://raw.githubusercontent.com/zbynekdrlik/restreamer/main/local-client-rs/install.ps1 | iex`
- **Self-hosted runner**: GitHub Actions runner for CI deployment (runs as SYSTEM)
- **Tray app**: Launched via Windows Task Scheduler with interactive session

#### Windows GUI App via Task Scheduler (MANDATORY PATTERN)

**To start GUI apps in user's desktop session from CI/service context:**

```powershell
$action = New-ScheduledTaskAction -Execute "C:\Program Files\Restreamer\restreamer-tray.exe"
$trigger = New-ScheduledTaskTrigger -AtLogon -User "newlevel"
$principal = New-ScheduledTaskPrincipal -UserId "newlevel" -LogonType Interactive -RunLevel Limited
$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -ExecutionTimeLimit 0

Unregister-ScheduledTask -TaskName "RestreamerTray" -Confirm:$false -ErrorAction SilentlyContinue
Register-ScheduledTask -TaskName "RestreamerTray" -Action $action -Trigger $trigger -Principal $principal -Settings $settings

Start-ScheduledTask -TaskName "RestreamerTray"
```

**Critical settings:**

- `LogonType Interactive` - runs in desktop session
- `Start-ScheduledTask` cmdlet to trigger (NOT `schtasks /Run`)

### restreamer.newlevel.media (Manager Server)

- **Host**: `restreamer.newlevel.media` (Linode VPS, IP `172.105.95.118`)
- **Install Path**: `/root/kristian/manager-server/restreamer-manager/`
- **Virtualenv**: `/root/.virtualenvs/venv/`
- **Django Admin**: `https://restreamer.newlevel.media/admin/`
- **Process Manager**: tmux session `restreamer`, gunicorn + nginx
- **Celery**: worker on `init_stream_queue`
- **DB**: PostgreSQL 16
- **Credentials**: See `~/.restreamer-secrets/manager-server.env` (not tracked by git)
- **SNV-stream client** is our church streaming client
