import { defineConfig } from "@playwright/test";

/**
 * Playwright config for YouTube Studio E2E verification.
 *
 * Uses a persistent Chrome profile on stream.lan so the Google OAuth session
 * persists between CI runs.  First-time setup: run `npm run setup-yt-profile`
 * to open a headed browser and log into YouTube Studio manually.
 */
export default defineConfig({
  testDir: ".",
  testMatch: "youtube-studio-check.spec.ts",
  // 5 min: the base health/preview flow (up to 6x10s retries + host-health
  // poll) plus the #249 picture gate (~60s readiness + 3 samples 10s apart).
  timeout: 300_000,
  retries: 0,
  workers: 1,
  reporter: [["list"]],
  use: {
    // Persistent context is configured inside the test itself
    // because Playwright's launchPersistentContext API requires
    // it to be called in the test body, not in config.
    headless: !process.env.HEADED,
    viewport: { width: 1280, height: 720 },
    // SECRET HYGIENE (#249): the Live Control Room page carries the stream-key
    // panel. NEVER let Playwright auto-capture the full page — the default
    // only-on-failure full-page screenshot / video / trace would archive the
    // stream key into CI artifacts. Only the explicit element/canvas captures
    // in the spec are ever written.
    screenshot: "off",
    video: "off",
    trace: "off",
  },
});
