---
paths:
  - "src-tauri/tauri.linux.conf.json"
  - "src-tauri/linux/**"
  - "crates/rs-runtime/src/self_upgrade*.rs"
  - "crates/rs-core/src/paths.rs"
  - "crates/rs-runtime/src/www_dir.rs"
  - ".github/workflows/release.yml"
---

# Linux build (stream-pp, Ubuntu 26.04 openbox kiosk) — #323

- **Package:** `Restreamer_<ver>_amd64.deb`, built by `build-tauri-linux` (ci.yml and
  release.yml) with `cargo tauri build --bundles deb`. `tauri.linux.conf.json` is merged
  over `tauri.conf.json` on Linux only, so the Windows NSIS is untouched.
- **Layout:**
  - `/usr/bin/Restreamer`
  - resources under `/usr/lib/Restreamer/` (`www/`, `rs-delivery-linux`)
  - `/usr/lib/systemd/user/restreamer.service`
  - Tauri writes the dpkg paths without a leading `./`.
  - `www_dir::www_candidates` and `delivery_binary::bundled_candidates` both look at
    `<exe_dir>/../lib/Restreamer/`.
- **Runs as** a systemd `--user` service running the GUI, NOT `--headless` (#393). The
  dashboard window is the operator's always-present window: Alt-Tab with OBS, maximized,
  and every close (X, Alt-F4, WM close) is refused, never hidden, because the kiosk has
  no tray. `Restart=always` brings it back if the process dies.
  - The kiosk's openbox autostart (camera-box) imports DISPLAY/XAUTHORITY into the user
    manager and then runs `systemctl --user start restreamer`.
  - Enable it once with `systemctl --user enable --now restreamer`.
  - Probe the GUI without touching production: run a 2nd instance with throwaway
    `RESTREAMER_CONFIG_DIR`/`RESTREAMER_DATA_DIR` and DISPLAY/XAUTHORITY from
    `systemctl --user show-environment`. Its service fails on the busy ports, but the
    window renders the live dashboard. `wmctrl -l` lists it, and `ffmpeg -f x11grab -i :0
    -frames:v 1` takes a screenshot.
- **Paths** come from `rs_core::paths` only:
  - config: `~/.config/restreamer/config.json`
  - data, DB, chunks, logs: `~/.local/share/restreamer`
  - overrides: `RESTREAMER_CONFIG_DIR` and `RESTREAMER_DATA_DIR`, then
    `XDG_CONFIG_HOME` and `XDG_DATA_HOME`.
  - Never hardcode a path again. `/etc` and `/var/lib` are root-owned and a desktop app
    cannot write them.
- **A config file must exist** (#112). The minimum is `client_uuid` plus
  `s3.{bucket,region,endpoint,access_key_id,secret_access_key}`. Each box needs its OWN
  `client_uuid`.
- **stream-pp upgrades itself (#323, from 0.29.53).** The `.deb` ships a ROOT timer,
  `restreamer-upgrade.timer` (every 15 min), enabled by its `postinst`. It runs
  `Restreamer --upgrade-check` (`crates/rs-runtime/src/self_upgrade.rs`):
  - it compares the latest GitHub release with the INSTALLED package (`dpkg-query`
    `${db:Status-Want} ${db:Status-Status} ${Version}`), never with its own compiled
    version: once dpkg replaced `/usr/bin/Restreamer`, a compiled-version check would
    call a half-done install (unpacked, missing Depends) "up to date" forever. A
    package on hold or being removed (Status-Want `hold`/`deinstall`/`purge`) is left
    alone; a newer one is never downgraded; the release's `.deb`/`.sig` are looked up
    only when an install is due; it restarts only after dpkg reports the new version
    configured;
  - a GUI whose `/proc/<pid>/exe` ends in ` (deleted)` has a restart pending, and the
    next idle check does it;
  - it checks "idle" (no `rtmp_connected`, no event receiving or delivering) before the
    download, then does the slow work (download, verify, `apt-get --download-only` for
    new Depends), checks again, installs, and restarts at once: an old binary serving
    the new package's files would upload the new bundled VPS binary under the old
    version. Both apt calls pass `-o DPkg::Lock::Timeout=10` (stream-pp's own apt
    config says 600); a lock another install holds is a deferral to the next tick
    (`dpkg_locked`: "is another process using it?", never "are you root?"). So the
    gap between the last idle check and the restart is at most ~10 s of lock wait
    plus the apt/dpkg run. A REFUSED status port counts as idle only while no GUI process runs
    (the app is down; the release may fix it); a timeout, an HTTP error, or a refusal
    while the GUI runs (another `api.port`/`api.bind`) is an error. The status
    request has its own 10 s bound;
  - it installs only a `.deb` whose `.deb.sig` verifies with `UPDATER_PUBKEY` (pinned to
    `tauri.conf.json`'s updater pubkey by a test) AND whose signed comment names that
    exact file (`file:Restreamer_<v>_amd64.deb`): a renamed old package cannot pose as
    a newer release;
  - it installs with `apt-get install -y -f --no-remove` (a new Depends gets
    installed; `-f` repairs a hand `dpkg -i` that left the package unpacked, which
    otherwise blocks every later apt install; `--no-remove` never removes another
    package unattended), then
    `systemctl --user -M <gui user>@ daemon-reload` + `restart restreamer`. The GUI user is the owner of the `Restreamer` process that is
    not root and not a CLI mode (`--upgrade-check`, `--verify-update`, `--restart-app`).

  - **Pausing it (e.g. before an event): `systemctl stop` only, never `disable`.** A stopped
    timer starts again at the next boot. On 25.10 a session ran `disable --now` plus a
    transient `systemd-run --on-calendar` re-enable; the box rebooted, the transient unit
    was gone, and stream-pp sat on 0.29.57 with the timer off until 27.10 (#323).

  Read it with `journalctl -u restreamer-upgrade` (one line per check) and
  `systemctl list-timers restreamer-upgrade.timer`. The release signs the `.deb` with
  `TAURI_SIGNING_PRIVATE_KEY` (tauri-cli reads the password from the env; never pass
  `-p`) and verifies it with the built binary (`Restreamer --verify-update <deb> <sig>`).
  **The signing key (2026-10-20, owner decision on #323):** the pair from bf7bb18f was
  created with an empty password, and the minisign of the time stored it unencrypted
  but flagged "Sc", so tauri-cli 2.x failed with "Wrong password for that key" for any
  password. The SAME pair (pubkey unchanged) was re-encrypted with a real password
  (minisign 0.9.1 `SecretKey::encrypt` + `raw_scrypt_params`, done in Python) and both
  secrets replaced. The re-encrypted key and its password are the dev1 key files
  `tauri-updater-signing-key` and `tauri-updater-signing-password` in the secrets
  store. `~/.tauri/restreamer.key` (mode 600) holds the same re-encrypted key; no
  unencrypted copy is kept. Never read them inline.
  Dev CI does the same, installs the `.deb` on the runner, runs the real check (and
  again under `apt-mark hold`: left alone), and drives the root restart (`Restreamer --restart-app`, hidden) against a stand-in user
  unit under a lingering user manager. A release now has 5 assets.
  `self_upgrade_host.rs` (dpkg, apt, systemctl, CLI) is mutation-excluded glue.
- **Upgrading a Linux box by hand** (stream-pp `10.76.8.204`, stream-snv `10.77.9.204`,
  Linux since 22.10.2026). Since 24.10.2026 (owner) `newlevel` has PASSWORDLESS sudo on both.
  Go through the box's MCP only, never ssh (#422, owner 24.10.2026: "potom aky ma vlastne mcp
  vyznam?!"). Scripts and one-liners use `remoteos-call` (dev1 `~/.local/bin`, the project's
  MCP config):
  - `remoteos-call linux-stream-snv put <deb> /tmp/<deb>` stages the CI `.deb` (artifact
    `restreamer-linux-deb`) or a release asset;
  - `remoteos-call linux-stream-snv Shell 'sha256sum /tmp/<deb> && sudo -n dpkg -i /tmp/<deb>
    && systemctl --user daemon-reload && systemctl --user restart restreamer'`. The exit code is
    the remote one, and `--timeout 300` covers dpkg.
  - Verify the dashboard DOM version, `dpkg-query -W restreamer` and `wmctrl -l` (the
    `Restreamer Dashboard` window, `DISPLAY=:0`).
  - SSH is break-glass ONLY, for when the box's MCP server itself is down. The old dev1 ->
    box `python3 -m http.server` hop and the `stream-pp-sudo` password (`sudo -S` over ssh,
    before 24.10) are retired.
- **Watching the release run:** `release.yml` runs list with `headBranch == "main"`, not
  the `restreamer-v*` tag. A waiter that filters on the tag never matches and sits to its
  timeout (cost ~1.5 h on 0.29.47). Pick the newest Release run created after the main
  merge run, or poll `gh release view restreamer-v<X.Y.Z>` for the assets.
- **Smoke test without the box:** install the `.deb` in a `ubuntu:26.04` container on
  dev2, start `Restreamer --headless` as a normal user, and expect 200 from
  `/api/v1/status` and `/` (recipe: build-deb + deb-smoke scripts, 2026-10-11).
- No ingest priority boost on Linux: `ingest_priority` is `Unsupported` there. Watch
  `logs/stall.log` on the first productions.
