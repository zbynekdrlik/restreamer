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
  timeout: 120_000,
  retries: 0,
  workers: 1,
  reporter: [["list"]],
  use: {
    // Persistent context is configured inside the test itself
    // because Playwright's launchPersistentContext API requires
    // it to be called in the test body, not in config.
    headless: !process.env.HEADED,
    viewport: { width: 1280, height: 720 },
  },
});
