import { test, expect } from "@playwright/test";
import * as fs from "fs";
import * as path from "path";

// Inject Tauri mock so the Leptos app is in "Tauri mode" for invoke().
const tauriMockScript = fs.readFileSync(
  path.join(__dirname, "tauri-mock.js"),
  "utf-8",
);

// Chromium-level warnings that are not application bugs.
const ALLOWED_CONSOLE = [
  /integrity.*attribute.*currently ignored.*subresource integrity/i,
];

test("audit panel shows rows from WebSocket after operator action", async ({
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

  // The AuditPanel is always rendered inside the sidebar of the operator
  // dashboard. Wait for it to exist.
  await expect(page.locator(".audit-panel")).toBeVisible();

  // Trigger an operator action that the mock-api turns into an
  // AuditAppended WS broadcast. We POST a new event directly to the
  // mock API — the mock emits `event_started` in response.
  const res = await request.post("http://127.0.0.1:8910/api/v1/events", {
    data: { name: "e2e-audit-test" },
  });
  expect(res.ok()).toBeTruthy();

  // The audit-panel list should gain a row containing 'event_started'.
  await expect(
    page.locator(".audit-panel .audit-row", { hasText: "event_started" }),
  ).toBeVisible({ timeout: 5000 });

  const real = consoleMessages.filter(
    (m) => !ALLOWED_CONSOLE.some((r) => r.test(m)),
  );
  expect(real).toEqual([]);
});
