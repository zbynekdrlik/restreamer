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

// D1 — Template-level rescue editor surfaces a calm informational hint when
// no custom URL is set, so operators can see at a glance that the built-in
// default rescue is protecting the stream. When a URL is typed, the hint
// flips to "Custom rescue video active".
test("template with no rescue URL shows built-in default hint", async ({
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

  // Templates view lives inside /settings under the Templates tab.
  await page.goto("/settings");
  await expect(page.locator(".settings-page")).toBeVisible({ timeout: 10000 });

  await page.locator(".settings-tabs button:has-text('Templates')").click();
  await expect(page.locator(".templates-tab")).toBeVisible({ timeout: 5000 });

  // Wait for at least one template card to render (mock seeds 2 templates,
  // both with rescue_video_url unset → both should show the default hint).
  await expect(
    page.locator(".templates-tab .items-list .settings-card"),
  ).toHaveCount(2, { timeout: 5000 });

  const defaultHint = page
    .locator('[data-testid="rescue-default-hint"]')
    .first();
  await expect(defaultHint).toBeVisible();
  await expect(defaultHint).toContainText("Using built-in default");

  // No custom hints when nothing is filled in.
  await expect(
    page.locator('[data-testid="rescue-custom-hint"]'),
  ).toHaveCount(0);

  // Zero browser console errors / warnings (per browser-console-zero-errors).
  const real = consoleMessages.filter(
    (m) => !ALLOWED_CONSOLE.some((r) => r.test(m)),
  );
  expect(real).toEqual([]);
});

test("template with custom rescue URL shows custom hint instead", async ({
  page,
  request,
}) => {
  await page.addInitScript(tauriMockScript);
  await request.post("http://127.0.0.1:8910/api/v1/__reset");

  await page.goto("/settings");
  await expect(page.locator(".settings-page")).toBeVisible({ timeout: 10000 });

  await page.locator(".settings-tabs button:has-text('Templates')").click();
  await expect(page.locator(".templates-tab")).toBeVisible({ timeout: 5000 });

  await expect(
    page.locator(".templates-tab .items-list .settings-card"),
  ).toHaveCount(2, { timeout: 5000 });

  // Type a URL into the first template's rescue input — triggers reactive switch.
  const firstInput = page
    .locator(".templates-tab .items-list .settings-card")
    .first()
    .locator("input.rescue-video-input");
  await firstInput.fill("https://example.com/custom.flv");

  // First card now shows the custom hint (default hint must be absent on that card).
  const firstCard = page
    .locator(".templates-tab .items-list .settings-card")
    .first();
  await expect(
    firstCard.locator('[data-testid="rescue-custom-hint"]'),
  ).toBeVisible();
  await expect(
    firstCard.locator('[data-testid="rescue-custom-hint"]'),
  ).toContainText("Custom rescue video active");
  await expect(
    firstCard.locator('[data-testid="rescue-default-hint"]'),
  ).toHaveCount(0);

  // Second card (untouched) still shows the default hint.
  const secondCard = page
    .locator(".templates-tab .items-list .settings-card")
    .nth(1);
  await expect(
    secondCard.locator('[data-testid="rescue-default-hint"]'),
  ).toBeVisible();
});
