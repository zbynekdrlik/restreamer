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

test("audit panel shows timestamps in browser-local time, not raw UTC", async ({
  page,
  request,
}) => {
  // #153: audit_panel.rs rendered the raw `ts.split('T')[1]` substring
  // (i.e. UTC) instead of converting through js_sys::Date to the browser's
  // local timezone — a regression of the #50 fix, silently dropped when
  // AuditPanel replaced ActivityFeed (PR c8951ba8). The Playwright config
  // pins the browser to America/New_York, which never equals UTC, so a
  // raw-UTC render is reliably distinguishable from a correct local render.
  const consoleMessages: string[] = [];
  page.on("console", (msg) => {
    if (msg.type() === "error" || msg.type() === "warning") {
      consoleMessages.push(`[${msg.type()}] ${msg.text()}`);
    }
  });

  await page.addInitScript(tauriMockScript);
  await request.post("http://127.0.0.1:8910/api/v1/__reset");
  await page.goto("/");
  await expect(page.locator(".audit-panel")).toBeVisible();

  const fixedTs = "2026-01-15T09:03:37.000Z";
  await request.post("http://127.0.0.1:8910/api/v1/_test/ws-broadcast", {
    data: {
      type: "AuditAppended",
      data: {
        id: 999901,
        ts: fixedTs,
        source: "operator",
        severity: "info",
        event_id: null,
        instance_id: null,
        endpoint: null,
        action: "tz_regression_probe",
        detail: {},
      },
    },
  });

  const row = page.locator(".audit-panel .audit-row", {
    hasText: "tz_regression_probe",
  });
  await expect(row).toBeVisible({ timeout: 5000 });

  // Expected = the browser's LOCAL hh:mm:ss for the fixture, computed the
  // same way the component does (getHours/getMinutes/getSeconds) — NOT the
  // raw UTC substring ("09:03:37") the bug renders.
  const expected = await page.evaluate((ts: string) => {
    const d = new Date(ts);
    const pad = (n: number) => String(n).padStart(2, "0");
    return `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
  }, fixedTs);

  await expect(row.locator(".audit-row__time")).toHaveText(expected);

  const real = consoleMessages.filter(
    (m) => !ALLOWED_CONSOLE.some((r) => r.test(m)),
  );
  expect(real).toEqual([]);
});

test("audit panel groups a repeat burst and un-groups when toggled off", async ({
  page,
  request,
}) => {
  // #169: a wall of near-identical endpoint_rtmp_push_died rows during a
  // mass-death should collapse into ONE grouped row (count + first->last span)
  // when "Group bursts" is on (default), and expand back to the raw rows when
  // the operator toggles it off.
  const consoleMessages: string[] = [];
  page.on("console", (msg) => {
    if (msg.type() === "error" || msg.type() === "warning") {
      consoleMessages.push(`[${msg.type()}] ${msg.text()}`);
    }
  });

  await page.addInitScript(tauriMockScript);
  await request.post("http://127.0.0.1:8910/api/v1/__reset");
  await page.goto("/");
  await expect(page.locator(".audit-panel")).toBeVisible();

  // Three FB-Zbynek push-died rows 20s apart — inside the 60s grouping window.
  const burst = [
    "2026-01-15T08:00:00.000Z",
    "2026-01-15T08:00:20.000Z",
    "2026-01-15T08:00:40.000Z",
  ];
  for (let i = 0; i < burst.length; i++) {
    await request.post("http://127.0.0.1:8910/api/v1/_test/ws-broadcast", {
      data: {
        type: "AuditAppended",
        data: {
          id: 770001 + i,
          ts: burst[i],
          source: "vps",
          severity: "warn",
          event_id: null,
          instance_id: null,
          endpoint: "FB-Zbynek",
          action: "endpoint_rtmp_push_died",
          detail: { lifetime_secs: 30 },
        },
      },
    });
  }

  const deadRows = page.locator(".audit-panel .audit-row", {
    hasText: "endpoint_rtmp_push_died",
  });

  // Group bursts ON (default): the 3 rows collapse into 1 row with a "3" count.
  await expect(deadRows).toHaveCount(1, { timeout: 5000 });
  await expect(deadRows.first().locator(".audit-row__count")).toContainText(
    "3",
  );

  // Toggle Group bursts OFF -> the raw 3 rows reappear, no count badge.
  await page
    .locator(".audit-panel__group-toggle input[type=checkbox]")
    .uncheck();
  await expect(deadRows).toHaveCount(3, { timeout: 5000 });
  await expect(page.locator(".audit-panel .audit-row__count")).toHaveCount(0);

  const real = consoleMessages.filter(
    (m) => !ALLOWED_CONSOLE.some((r) => r.test(m)),
  );
  expect(real).toEqual([]);
});
