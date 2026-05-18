import { defineConfig } from "@playwright/test";

/**
 * Playwright config for Facebook Live Producer E2E verification.
 *
 * Uses a persistent Chrome profile on stream.lan so the Facebook session
 * persists between CI runs. First-time setup: run
 * `scripts\setup-fb-profile.ps1` (HEADED) to open a headed browser and
 * log into Facebook manually.
 *
 * The spec runs for up to 35 minutes (30 min soak + setup overhead), so
 * the per-test timeout is generous.
 */
export default defineConfig({
  testDir: ".",
  testMatch: "fb-live-producer-check.spec.ts",
  timeout: 35 * 60 * 1000,
  retries: 0,
  workers: 1,
  reporter: [["list"]],
  use: {
    headless: !process.env.HEADED,
    viewport: { width: 1280, height: 720 },
  },
});
