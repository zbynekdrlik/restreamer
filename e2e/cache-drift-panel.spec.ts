import { test, expect } from "@playwright/test";
import * as fs from "fs";
import * as path from "path";

// Inject Tauri mock so the Leptos app runs in browser mode (fetch via HTTP,
// not Tauri IPC), matching every other frontend spec in this suite.
const tauriMockScript = fs.readFileSync(
  path.join(__dirname, "tauri-mock.js"),
  "utf-8",
);

// Chromium-level warnings that are not application bugs.
const ALLOWED_CONSOLE = [
  /integrity.*attribute.*currently ignored.*subresource integrity/i,
];

test("pacing panel renders three series sections with clean console", async ({
  page,
  request,
}) => {
  const consoleMessages: string[] = [];
  page.on("console", (msg) => {
    if (msg.type() === "error" || msg.type() === "warning") {
      consoleMessages.push(`[${msg.type()}] ${msg.text()}`);
    }
  });

  await page.addInitScript(tauriMockScript);
  await request.post("http://127.0.0.1:8910/api/v1/__reset");

  await page.goto("/");

  // The pacing panel is always rendered in the sidebar, event_id=0 shows
  // the placeholder. Select event 1 so event_id > 0 and the panel fetches.
  await page.locator("select.event-selector").selectOption("1");

  // Wait for the panel to be present in the DOM.
  await expect(page.getByTestId("pacing-panel")).toBeVisible();

  // All three series sections must be present once data loads (empty series
  // from mock → panel shows "0 samples" for each).
  await expect(page.getByTestId("producer-rate")).toBeVisible({ timeout: 5000 });
  await expect(page.getByTestId("consumer-rate")).toBeVisible({ timeout: 5000 });
  await expect(page.getByTestId("clock-skew")).toBeVisible({ timeout: 5000 });

  // Zero browser console errors / warnings (subresource integrity exempted).
  const real = consoleMessages.filter(
    (m) => !ALLOWED_CONSOLE.some((r) => r.test(m)),
  );
  expect(real).toEqual([]);
});
