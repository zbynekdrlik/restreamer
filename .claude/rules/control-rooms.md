---
paths:
  - "crates/rs-api/src/control_room/**"
  - "leptos-ui/src/components/control_room.rs"
  - "leptos-ui/src/api/control_rooms.rs"
  - "e2e/control-rooms.spec.ts"
---

# Control rooms: the YT/FB tabs in the dashboard (#388)

- **Shape.** One box-hosted Chrome (debugging port `127.0.0.1:9231`, user-data-dir
  `<data_dir>/browser-profiles/control-rooms`). It starts on the first `open`, and it
  survives a Restreamer restart on purpose: the next start reuses it, logins included.
  - Windows: a real window parked at -32000,-32000.
  - Linux: Chrome draws on Restreamer's own `Xvfb :93`, so the kiosk screen and tint2
    never show it. The .deb `Depends: xvfb`.
  - Linux under systemd (`INVOCATION_ID` set): Chrome and Xvfb start through
    `systemd-run --user --scope --collect`. The unit's `KillMode=control-group`
    otherwise kills them on every `systemctl --user restart restreamer`, and all
    room tabs close (stream-pp, 0.29.41). The handover run exits at once and needs
    no scope. Do NOT set `KillMode=process` on the unit: Restreamer's ffmpeg
    children must die with it.
  - If systemd-run itself fails before the program is up (no user manager or
    bus: its own `Failed to connect to bus` / `Failed to start transient scope
    unit` lines), a warning carries the line and the program starts directly.
    Any other non-zero exit is the program's own (with `--scope` systemd-run
    becomes it): it stays scoped, and is never restarted in the service cgroup. Chrome and Xvfb stderr go to
    `<data_dir>/browser-profiles/{chrome,xvfb}.stderr.log` (truncated per start;
    read them first when a room never comes up).
  - `RESTREAMER_CHROME` overrides the binary. stream-pp uses the system
    `google-chrome-stable`, installed by camera-box setup-strih step 16j.
- **Memory (stream-pp OOM, 2026-10-14).** Each room tab costs ~500 MB; 7 tabs (4.4 GB)
  plus no swap on the box made the OOM killer hit strih OBS and Chrome, and
  restreamer.service (`OOMPolicy=stop`) died with its in-cgroup Chrome. Now:
  - Chrome's scope carries `CHROME_SCOPE_PROPS`: MemoryHigh=3G, MemoryMax=3584M,
    OOMPolicy=continue. An OOM stays inside Chrome (a renderer dies);
  - at most `MAX_LIVE_ROOMS` (4) room tabs are open. `open()` updates `TabMap.recent`
    and closes the least recently viewed beyond that. A tab Chrome refuses to close
    stays tracked and is closed on a later view.
- **Each room reopens at its last page.** The relay reports every main-frame URL:
  `Page.frameNavigated` plus `Page.navigatedWithinDocument` (YT Studio is an SPA; a page
  target's id is its main frame id). `remember_url` keeps only `https://studio.youtube.com/*`
  and `facebook.com/live/producer*`, never sign-in or picker pages. The URL is stored in
  `control-room-tabs.json` (`url`) and dropped when the account is edited.
- **One Google profile = ONE active YouTube channel identity.** A second brand channel
  in the same profile gets "Select a channel" or "Oops, you don't have permission"
  (probed 2026-10-13).
  - So every destination has its own profile (`dest-<id>`), FB too. A shared FB
    account profile made one room's sign-in change every FB room (2026-10-14). A room
    still open in a profile it no longer owns is closed and reopened in its own.
  - A missing profile is CLONED from a signed-in profile of the same account and
    service (`acct-*`, or another room's `dest-*`; caches skipped). On YouTube the
    operator then only clicks "Switch account" and picks the channel.
- **Account mapping** lives box-local in `<data_dir>/control-rooms.json`, keyed by
  endpoint id. It holds emails, so it never goes in the repo. Set it with
  `PUT /api/v1/control-rooms/{id}/account`, or from the room's "Zmeniť účet".
- **Linux cookies need `--password-store=basic`.** The kiosk has no keyring. A profile
  copied between user-data-dirs keeps its SID.
- **A profile's own startup window adds `about:blank` / `chrome://newtab/` tabs** next
  to the requested URL. `new_target` skips them; never take the first new page blindly.
- **Clone BEFORE `ensure_running`.** Chrome starts in its `Local State`
  `profile.last_used` and creates that directory when it is missing. A clone
  checked after the start then sees the directory and skips, and the room opens
  signed out (stream-pp, 2026-10-14).
- **Only `acct-*` profiles carry a session.** On stream-pp the spike's `dest-*`
  were never signed in. To check, count the `SID` (Google) or `c_user` (FB) rows
  in `<profile>/Cookies` with python's sqlite3 (the box has no `sqlite3` CLI).
- **Popups.** Studio "Switch account" opens `youtube.com/account` in a new tab.
  The relay polls `Target.getTargets` for tabs whose `openerId` is the room tab,
  navigates the room tab there, and closes the popup.
- **The screencast stops on a cross-site navigation** (renderer swap). The relay
  restarts `Page.startScreencast` on every main-frame `Page.frameNavigated`.
- **Relay.** Dashboard to Chrome carries only the whitelisted mouse/key fields, paste,
  `{t:copy}`, `history.back()`, http(s) `Page.navigate`, and `{t:size,w,h}`. Anything
  else is dropped. Frames are JPEG binary messages.
- **Copy out of a room.** A copy inside the box Chrome lands in Xvfb's clipboard, which
  the operator never sees. The relay installs `copy_hook_source()` (new documents AND the
  open one) plus the `Runtime.addBinding` `__restreamerCopy`. The hook reports a `copy`
  event (page data, else the selection) and `navigator.clipboard.writeText` (Studio's
  "Copy" buttons). `Runtime.bindingCalled` then goes to the dashboard as `{t:clip,text}`
  (max 64 KB). Ctrl+C on the canvas sends `{t:copy}`: `execCommand('copy')` with
  `userGesture: true`. The dashboard tries `navigator.clipboard` (secure context only;
  `http://<box-ip>` is not one), then `execCommand`, and always shows the text with a
  "Kopírovať" button: a real click works where the automatic copy is refused.
- **The page viewport follows the dashboard canvas.** The canvas fills the window below
  it; its content size goes as `{t:size}` to `Emulation.setDeviceMetricsOverride`
  (scaled down to fit 2560x1600 keeping its aspect ratio, at least 320x240, re-applied
  after a navigation). One viewer per room: with two, the last resize wins. The screencast's
  maxWidth/maxHeight is that maximum, so frames arrive 1:1 with the canvas: big AND
  sharp. The Chrome window and Xvfb stay 1600x900; the emulated viewport may exceed them.
- **Tests.**
  - `control_room/*_tests.rs` use a fake CDP websocket page and a real axum relay.
  - The Playwright spec uses the mock's `/api/v1/control-rooms*` routes and a second
    noServer WebSocketServer, `/api/v1/control-rooms/ws`. Its fixture frame is
    `e2e/fixtures/control-room-frame.jpg`; an inline base64 JPEG trips the secret scanner.
- **dev2 gotcha.** `trunk build` writes to the repo-root `dist/`, not `leptos-ui/dist/`.
  An `rsync --delete` of the repo to dev2 wipes it, and the mock then serves a stale or
  missing UI. Re-run `trunk build` before Playwright.
