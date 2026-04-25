import { test, expect } from "@playwright/test";
import * as fs from "fs";
import * as path from "path";

// Inject Tauri mock so the Leptos app runs in "Tauri mode" for invoke().
const tauriMockScript = fs.readFileSync(
  path.join(__dirname, "tauri-mock.js"),
  "utf-8",
);

// Chromium-level warnings that are not application bugs.
const ALLOWED_CONSOLE = [
  /integrity.*attribute.*currently ignored.*subresource integrity/i,
];

// The mock-api seeds two events on /__reset; we target id=1 ("Sunday Service").
const TARGET_EVENT_NAME = "Sunday Service";

test("delete + cleanup shows busy state and removes event on success", async ({
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

  // Delay the DELETE response so the busy state is observable.
  await page.route("**/api/v1/events/1", async (route) => {
    if (route.request().method() === "DELETE") {
      await new Promise((r) => setTimeout(r, 1500));
      await route.continue();
    } else {
      await route.continue();
    }
  });

  await page.goto("/settings");
  await page.locator(".settings-tabs .tab", { hasText: "Events" }).click();

  const card = page.locator(".settings-card", { hasText: TARGET_EVENT_NAME });
  const deleteBtn = card.locator("button.btn-danger");
  const clearBtn = card.locator("button.btn-secondary");

  await expect(deleteBtn).toBeEnabled();
  await deleteBtn.click();

  // Confirm in modal
  await page.locator(".confirm-modal .confirm-btn-danger").click();

  // Modal closes immediately
  await expect(page.locator(".confirm-modal")).toHaveCount(0);

  // Busy state: label flips to "Deleting…", both card buttons disabled
  await expect(deleteBtn).toHaveText(/Deleting/, { timeout: 1000 });
  await expect(deleteBtn).toBeDisabled();
  await expect(clearBtn).toBeDisabled();

  // After the delay + DELETE + list refresh, the card is gone
  await expect(card).toHaveCount(0, { timeout: 5000 });

  const real = consoleMessages.filter(
    (m) => !ALLOWED_CONSOLE.some((r) => r.test(m)),
  );
  expect(real).toEqual([]);
});

test("delete + cleanup shows error banner on API failure", async ({
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

  // Force a 500 response on DELETE
  await page.route("**/api/v1/events/1", async (route) => {
    if (route.request().method() === "DELETE") {
      await new Promise((r) => setTimeout(r, 200));
      await route.fulfill({ status: 500, body: "internal server error" });
    } else {
      await route.continue();
    }
  });

  await page.goto("/settings");
  await page.locator(".settings-tabs .tab", { hasText: "Events" }).click();

  const card = page.locator(".settings-card", { hasText: TARGET_EVENT_NAME });
  const deleteBtn = card.locator("button.btn-danger");

  await deleteBtn.click();
  await page.locator(".confirm-modal .confirm-btn-danger").click();

  // Error banner appears with "Delete failed"
  await expect(
    page.locator(".error-message", { hasText: /Delete failed/i }),
  ).toBeVisible({ timeout: 3000 });

  // Event card still present
  await expect(card).toBeVisible();

  // Button re-enabled and label restored
  await expect(deleteBtn).toBeEnabled();
  await expect(deleteBtn).toHaveText(/Delete \+ Cleanup/);

  // Filter out the expected 500-response noise from console.
  // The application MUST handle the error gracefully — the only allowed
  // console output is fetch's own network logging for the 500.
  const real = consoleMessages.filter(
    (m) =>
      !ALLOWED_CONSOLE.some((r) => r.test(m)) &&
      !/500/.test(m) &&
      !/Failed to load/i.test(m),
  );
  expect(real).toEqual([]);
});

test("clear S3 chunks shows busy state and keeps event after success", async ({
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

  // Delay the clear-s3 POST so the busy state is observable.
  await page.route("**/api/v1/events/1/clear-s3", async (route) => {
    if (route.request().method() === "POST") {
      await new Promise((r) => setTimeout(r, 1500));
      await route.continue();
    } else {
      await route.continue();
    }
  });

  await page.goto("/settings");
  await page.locator(".settings-tabs .tab", { hasText: "Events" }).click();

  const card = page.locator(".settings-card", { hasText: TARGET_EVENT_NAME });
  const clearBtn = card.locator("button.btn-secondary");
  const deleteBtn = card.locator("button.btn-danger");

  await expect(clearBtn).toBeEnabled();
  await clearBtn.click();

  // Confirm in the clear-s3 modal (same modal class, different confirm label
  // text but same confirm-btn-danger selector).
  await page.locator(".confirm-modal .confirm-btn-danger").click();

  // Modal closes immediately
  await expect(page.locator(".confirm-modal")).toHaveCount(0);

  // Busy state: clear button label flips to "Clearing…", both card buttons disabled
  await expect(clearBtn).toHaveText(/Clearing/, { timeout: 1000 });
  await expect(clearBtn).toBeDisabled();
  await expect(deleteBtn).toBeDisabled();

  // After the delay + POST + s3_usage refresh, the card stays (clear does
  // not remove the event), but the busy label clears.
  await expect(clearBtn).toHaveText(/Clear S3 chunks/, { timeout: 5000 });
  await expect(clearBtn).toBeEnabled();
  await expect(card).toBeVisible();

  const real = consoleMessages.filter(
    (m) => !ALLOWED_CONSOLE.some((r) => r.test(m)),
  );
  expect(real).toEqual([]);
});
