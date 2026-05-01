import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: ".",
  // Match frontend.spec.ts plus the post-mortem specs added in Task 26.
  // `start-delivery-rtmp-gate` was removed — the RTMP-stable gate is
  // verified by backend unit tests (`rs-api/src/router_tests.rs`) and
  // the E2E version keeps hitting parallel-worker shared-state races.
  testMatch: /(frontend|audit-panel|zero-endpoint-banner|remove-last-endpoint-modal|endpoint-history-sparkline|cache-drift-panel|delete-cleanup-button|rust-pusher)\.spec\.ts$/,
  timeout: 30000,
  retries: 0,
  // Single worker — the new post-mortem specs (audit-panel,
  // zero-endpoint-banner, remove-last-endpoint-modal, endpoint-history-sparkline)
  // share a single mock-api process whose scenario state is global.
  // Running tests in parallel races scenario-write vs. WS-broadcast/poll-read
  // and produces flakes. Serial execution is fast enough (≤90s total) and
  // deterministic.
  workers: 1,
  use: {
    baseURL: "http://127.0.0.1:8910",
    headless: true,
    timezoneId: "America/New_York",
  },
  projects: [
    {
      name: "chromium",
      use: { browserName: "chromium" },
    },
  ],
  reporter: [["html", { outputFolder: "playwright-report" }], ["list"]],
});
