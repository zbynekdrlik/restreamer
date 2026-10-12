---
paths:
  - "src-tauri/tauri.linux.conf.json"
  - "src-tauri/linux/**"
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
- **Runs as** a systemd `--user` service with `--headless`. The kiosk has no system tray,
  and its openbox autostart runs `systemctl --user start restreamer`. Enable it once with
  `systemctl --user enable --now restreamer`. The Windows rule "tray app, never
  `--headless`" stays Windows-only.
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
- **Smoke test without the box:** install the `.deb` in a `ubuntu:26.04` container on
  dev2, start `Restreamer --headless` as a normal user, and expect 200 from
  `/api/v1/status` and `/` (recipe: build-deb + deb-smoke scripts, 2026-10-11).
- No ingest priority boost on Linux: `ingest_priority` is `Unsupported` there. Watch
  `logs/stall.log` on the first productions.
