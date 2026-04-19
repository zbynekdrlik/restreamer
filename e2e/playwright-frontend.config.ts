import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: ".",
  // Match frontend.spec.ts plus all post-mortem specs added in Task 26
  // (audit-panel, zero-endpoint-banner, remove-last-endpoint-modal,
  // start-delivery-rtmp-gate, endpoint-history-sparkline).
  testMatch: /(frontend|audit-panel|zero-endpoint-banner|remove-last-endpoint-modal|start-delivery-rtmp-gate|endpoint-history-sparkline)\.spec\.ts$/,
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
