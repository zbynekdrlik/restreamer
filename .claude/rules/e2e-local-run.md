---
paths:
  - "e2e/**"
  - "playwright-frontend.config.ts"
---

# Running the frontend Playwright suite locally (project account boxes)

- Build the UI first: `cd leptos-ui && BUILD_VERSION=<v>-dev BUILD_TIMESTAMP="..." trunk build --release`
  (writes the REPO-ROOT `dist/`, which `mock-api.js` serves).
- Start the mock: `cd e2e && RESTREAMER_TEST_HOOKS=1 node mock-api.js` (fixed port 8910;
  check `ss -ltn | grep 8910` first, a sibling lane's mock may hold it).
- **`npx playwright install` is blocked on project accounts** (#1201): browsers live in
  the system-wide `/opt/ms-playwright`. A plain `npm install` resolves `@playwright/test`
  `^1.50` to the NEWEST release, whose browser revision is usually not there yet
  ("Executable doesn't exist at .../chromium_headless_shell-1255"). Install the version
  that matches an installed revision without touching `package.json`:
  `npm install --no-save @playwright/test@1.64.0` (revision 1248, 26.11.2026). The
  revision is in `node_modules/playwright-core/browsers.json`; compare it with
  `ls /opt/ms-playwright`. Delete the generated `e2e/package-lock.json` (not tracked).
- Run: `npx playwright test --config playwright-frontend.config.ts <spec basename>`;
  `--repeat-each 8` on a new spec catches a render race before CI does.
