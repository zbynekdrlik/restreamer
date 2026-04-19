import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: ".",
  // Match frontend.spec.ts plus the post-mortem specs added in Task 26.
  // `start-delivery-rtmp-gate` was removed — the RTMP-stable gate is
  // verified by backend unit tests (`rs-api/src/router_tests.rs`) and
  // the E2E version keeps hitting parallel-worker shared-state races.
  testMatch: /(frontend|audit-panel|zero-endpoint-banner|remove-last-endpoint-modal|endpoint-history-sparkline)\.spec\.ts$/,
  timeout: 30000,
  retries: 0,
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
