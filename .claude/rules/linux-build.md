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
    `${db:Status-Abbrev}`), never with its own compiled version: once dpkg replaced
    `/usr/bin/Restreamer`, a compiled-version check would call a half-done install
    (unconfigured, missing Depends) "up to date" forever;
  - a GUI whose `/proc/<pid>/exe` ends in ` (deleted)` has a restart pending, and the
    next idle check does it;
  - it checks "idle" (no `rtmp_connected`, no event receiving or delivering) before the
    download, again before the install and again before the restart. Nothing listening
    on 127.0.0.1:8910 counts as idle: the app is down, and the release may fix that;
  - it installs only a `.deb` whose `.deb.sig` verifies with `UPDATER_PUBKEY` (pinned to
    `tauri.conf.json`'s updater pubkey by a test) AND whose signed comment names that
    exact file (`file:Restreamer_<v>_amd64.deb`): a renamed old package cannot pose as
    a newer release;
  - it installs with `apt-get install -y -o DPkg::Lock::Timeout=300 <deb>` (a new
    Depends gets installed), then `systemctl --user -M <gui user>@ daemon-reload` +
    `restart restreamer`. The GUI user is the owner of the `Restreamer` process that is
    not root and not a CLI mode (`--upgrade-check`, `--verify-update`, `--restart-app`).

  Read it with `journalctl -u restreamer-upgrade` (one line per check) and
  `systemctl list-timers restreamer-upgrade.timer`. The release signs the `.deb` with
  `TAURI_SIGNING_PRIVATE_KEY` (tauri-cli reads the password from the env; never pass
  `-p`) and verifies it with the built binary (`Restreamer --verify-update <deb> <sig>`).
  Dev CI does the same, installs the `.deb` on the runner, runs the real check, and
  drives the root restart (`Restreamer --restart-app`, hidden) against a stand-in user
  unit under a lingering user manager. A release now has 5 assets.
  `self_upgrade_host.rs` (dpkg, apt, systemctl, CLI) is mutation-excluded glue.
- **Upgrading stream-pp by hand** (the first 0.29.53 install, or when the timer is
  broken) (`newlevel@10.76.8.204`): `newlevel` has no passwordless
  sudo there. The owner provided the sudo password on 2026-10-19 (#323: the
  streampp box drifts behind releases). It is the dev1 key file `stream-pp-sudo`
  in the secrets store, and it is used only through
  `airuleset.py secret exec --file <it> --env PPPW -- bash <script>`. The script pipes
  `${PPPW%$'\n'}` into `ssh newlevel@10.76.8.204 "sudo -S -k -p '' dpkg -i /tmp/<deb>"`
  (the file ends in a newline, so strip it). Never read the file inline; a hook blocks
  it. Then run `systemctl --user daemon-reload` (the package ships the unit file) and
  `systemctl --user restart restreamer` through the `linux-stream-pp` MCP. Verify the
  version on the dashboard (`http://10.76.8.204:8910/`), `dpkg-query -W restreamer`,
  and `wmctrl -l` (the `Restreamer Dashboard` window, with `DISPLAY=:0`) (0.29.51).
  The repo is public, so stage the `.deb` with `curl -sSL -o /tmp/<deb>
  https://github.com/zbynekdrlik/restreamer/releases/download/restreamer-v<X.Y.Z>/<deb>`
  run on the box (MCP Shell), and compare its sha256 with a `gh release download` copy
  on dev1 before handing it over (0.29.47).
- **Watching the release run:** `release.yml` runs list with `headBranch == "main"`, not
  the `restreamer-v*` tag. A waiter that filters on the tag never matches and sits to its
  timeout (cost ~1.5 h on 0.29.47). Pick the newest Release run created after the main
  merge run, or poll `gh release view restreamer-v<X.Y.Z>` for the assets.
- **Smoke test without the box:** install the `.deb` in a `ubuntu:26.04` container on
  dev2, start `Restreamer --headless` as a normal user, and expect 200 from
  `/api/v1/status` and `/` (recipe: build-deb + deb-smoke scripts, 2026-10-11).
- No ingest priority boost on Linux: `ingest_priority` is `Unsupported` there. Watch
  `logs/stall.log` on the first productions.
