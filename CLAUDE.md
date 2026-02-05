# CLAUDE.md

## Strict Rules

### Pull Requests

- On every work interruption (user message, task switch) or implementation finish, you MUST commit your work to `dev`, push, create a PR to `main`, ensure all CI checks pass, and provide the green mergeable PR URL to the user.
- Never provide a PR URL that has failing checks or merge conflicts.
- After creating a PR, monitor the CI pipeline status. If checks fail, fix the issues, push fixes, and only then share the final green PR URL.
- Every PR MUST include tests covering the implemented changes. No PR is complete without tests.
- NEVER merge a PR. Only the user may merge pull requests. The agent must only create the PR, ensure CI is green, and provide the URL. Merging is exclusively the user's action.

### Testing

- Always write real end-to-end (E2E) tests — not mocked, not hidden, not stubbed. Tests must exercise the actual code paths.
- Always consider your current test implementations as not comprehensive enough and actively look for ways to improve coverage, edge cases, and failure scenarios.
- Prefer integration and E2E tests over unit tests with heavy mocking. Mocks are only acceptable for external API calls and third-party services.
- Every feature, bugfix, and refactor must have corresponding tests that verify the actual behavior.
- ALL tests must pass — never skip, ignore, or disable tests. Never produce false-positive green results that hide real issues. A passing test suite must reflect genuinely working code.

### Deployment Target: stream.lan (Local Client)

- **Host**: `stream.lan` (Windows 11 IoT Enterprise LTSC)
- **Install Path**: `C:\Users\newlevel\restreamer\`
- **Legacy Path**: `C:\Users\newlevel\Desktop\restreamer\` (backup as `restreamer_old_backup`)
- **Credentials**: See `~/.restreamer-secrets/stream-lan.env` (not tracked by git)

### Deployment Target: restreamer.newlevel.media (Manager Server)

- **Host**: `restreamer.newlevel.media` (Linode VPS, IP `172.105.95.118`)
- **Install Path**: `/root/kristian/manager-server/restreamer-manager/`
- **Virtualenv**: `/root/.virtualenvs/venv/`
- **Django Admin**: `https://restreamer.newlevel.media/admin/`
- **Process Manager**: tmux session `restreamer`, gunicorn + nginx
- **Celery**: worker on `init_stream_queue`
- **DB**: PostgreSQL 16
- **Credentials**: See `~/.restreamer-secrets/manager-server.env` (not tracked by git)
- **SNV-stream client** is our church streaming client
