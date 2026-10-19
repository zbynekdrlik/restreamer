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
  - Arm a SIGKILL timer (2 s) before sending `Browser.close`, since a hung Chrome
    never answers it.
    - The timer kills the whole process GROUP: spawn with `detached: true`, then
      `process.kill(-pid, "SIGKILL")`. Killing only the main pid leaves children
      writing the profile.
    - Verified on dev2 by SIGSTOPing Chrome: everything was gone and the profile
      removed after 2.6 s.
    - After the wait, the group is killed in any case, since a child may outlive
      the main process. A `process.once("exit")` hook in the worker reaps the
      detached group if `afterEach` never runs.
    - `afterEach` shares the test's budget, so the describe sets a 60 s timeout:
      ~5 s of test plus at most ~6 s of stop and rm retries.
  - Skip the exit wait when `pid` is undefined (spawn failed) or
    `exitCode`/`signalCode` is set: an `exit` listener added after the exit never
    fires.
  - Spawn synchronously (`launchChrome`) and assign the handle BEFORE the first
    await (`connectChrome`). Stop Chrome in `test.afterEach`, not in a `finally`
    in the test body: on a test timeout Playwright abandons the body, and only a
    handle `afterEach` can already see gets cleaned.
- **The observation window:** `networkidle` on every route.
  - On `/` the spec also waits for BOTH responses of the ControlBar's next poll
    (`/status` and `/chunks/stats`, fetched together), then one
    `requestAnimationFrame`, so the render the poll drives lands inside the
    window.
  - A second `waitForLoadState("networkidle")` is a no-op: it resolves at once
    once the event has fired for the document.
  - `/settings` has no periodic fetch. Waiting for `/status` there times out
    (#360: the poll lives on the operator route only).
- **Three canaries keep the assertion honest**, each marked `browser-log-canary` in
  its text or URL:
  - a `console.warn` (the console path);
  - an image load from `127.0.0.1:9`, which Chrome logs as a network error (the
    Log error path);
  - a `page.route`-fulfilled http page carrying only the deprecated apple meta.
    Chrome logs the exact 0.29.49 warning for it (the Log warning path).
  - The spec asserts that all three arrived, then that every non-canary entry
    list is empty.
  - Canary attempts that did NOT log anything: a 404 (the mock answers unknown
    paths with `index.html` and 200), a synchronous XHR, injecting the meta into
    the live DOM, `setContent`, and a `data:` URL.
- Chrome's Issues panel items (`Audits.issueAdded`, e.g. a form field without
  id/name) are not console output and are not asserted.
