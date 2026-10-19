---
paths:
  - "e2e/**"
  - "leptos-ui/index.html"
---

# Browser warnings the console listener cannot see (`browser-log.spec.ts`)

- **Chrome's own warnings go to the CDP `Log` domain**, not to `console`. These are
  deprecations, interventions and failed loads.
  - In 0.29.49 the dashboard logged `<meta name="apple-mobile-web-app-capable"> is
    deprecated` on every load (seen on stream-pp, Chrome 155).
  - All 170+ E2E tests passed anyway.
- **Neither of Playwright's own browsers logs that deprecation:**
  - not the bundled Chromium (145, headless shell or `channel: "chromium"`);
  - not Chrome stable launched by Playwright (`channel: "chrome"`), whose default
    flags suppress it.
  - Chrome started plainly, as the boxes start it, does log it.
  - So `browser-log.spec.ts` spawns `google-chrome --headless=new
    --remote-debugging-port=0`, reads the port from `DevToolsActivePort`, attaches
    with `chromium.connectOverCDP`, and collects `console` AND `Log.entryAdded`.
  - ubuntu-latest ships Chrome. `CHROME_BIN` overrides the binary.
- **`--password-store=basic` is required.** Without a desktop session, a fresh
  profile's first request waits ~25 s, the D-Bus timeout, for a keyring. The
  request timing shows it queued BEFORE `domainLookupStart`; `--no-proxy-server`
  does not help.
- **Close it with `Browser.close` over a browser CDP session, then wait for `exit`.**
  `proc.kill()` leaves child processes writing the profile, and `rmSync` fails
  with `ENOTEMPTY`. `browser.close()` on a `connectOverCDP` browser only
  disconnects.
- Chrome's Issues panel items (`Audits.issueAdded`, e.g. a form field without
  id/name) are not console output and are not asserted.
