import { test, expect, Page } from "@playwright/test";

// #199: link/unlink an OAuth grant to an endpoint from the edit-endpoint
// dialog. Runs against the mock backend (mock-api.js) used by frontend.spec.
//
// The real `POST /endpoints/{id}/link-oauth` returns 204 No Content; the mock
// mirrors that. The dropdown appears only for YT_RTMP endpoints.

const GRANTS = [
  { id: 10, label: "main", channel_id: "UCmain", connected_at: "2026-03-01T00:00:00Z" },
  { id: 20, label: "backup", channel_id: "UCbackup", connected_at: "2026-03-02T00:00:00Z" },
];

// YouTube Main endpoint (mock seed id=1) has stream_key "xxxx-xxxx-xxxx".
const YT_KEY = "xxxx-xxxx-xxxx";

// Chromium-level warnings that are not application bugs (crbug.com/981419) —
// same allow-list every other frontend spec uses.
const ALLOWED_CONSOLE = [
  /integrity.*attribute.*currently ignored.*subresource integrity/i,
];

function expectCleanConsole(msgs: string[]) {
  expect(msgs.filter((m) => !ALLOWED_CONSOLE.some((r) => r.test(m)))).toEqual(
    [],
  );
}

async function reset(page: Page) {
  await page.request.post("/api/v1/__reset");
}

async function seedGrants(
  page: Page,
  grants: unknown[],
  streamKeys: unknown[],
) {
  await page.request.post("/api/v1/_test/seed-oauth-grants", {
    data: { grants, stream_keys: streamKeys },
  });
}

async function openConfigTab(page: Page) {
  await page.goto("/settings");
  await page.locator(".settings-tabs button:has-text('Config')").click();
  await expect(page.locator(".endpoints-tab")).toBeVisible({ timeout: 10000 });
  await page.waitForTimeout(500);
}

function collectConsoleErrors(page: Page): string[] {
  const msgs: string[] = [];
  page.on("console", (m) => {
    if (m.type() === "error" || m.type() === "warning") {
      msgs.push(`[${m.type()}] ${m.text()}`);
    }
  });
  return msgs;
}

test.describe("Endpoint OAuth grant link/unlink (#199)", () => {
  test("YT dialog shows OAuth dropdown; select + save links the grant (204) and persists", async ({
    page,
  }) => {
    const consoleErrors = collectConsoleErrors(page);
    await reset(page);
    // No stream-key map => no auto-suggest; dropdown defaults to (unlink).
    await seedGrants(page, GRANTS, []);
    await openConfigTab(page);

    const section = page.locator(".endpoints-tab");
    // Edit the YouTube Main (YT_RTMP) endpoint — first card.
    await section
      .locator(".endpoint-card")
      .first()
      .locator('button:has-text("Edit")')
      .click();
    await expect(section.locator(".endpoint-edit-form")).toBeVisible({
      timeout: 5000,
    });

    // Dropdown present, with (unlink) + both grants.
    const select = section.getByTestId("edit-oauth-select");
    await expect(select).toBeVisible();
    await expect(select.locator("option")).toHaveCount(3);
    await expect(select).toHaveValue(""); // unlinked initially

    // Select grant 20 and save; assert the link-oauth POST fired with 204 and
    // the right oauth_id.
    await select.selectOption("20");
    const [linkResp] = await Promise.all([
      page.waitForResponse(
        (r) =>
          r.url().includes("/endpoints/1/link-oauth") &&
          r.request().method() === "POST",
      ),
      section.locator('button:has-text("Save")').click(),
    ]);
    expect(linkResp.status()).toBe(204);
    expect(linkResp.request().postDataJSON()).toEqual({ oauth_id: 20 });
    await expect(section.locator(".endpoint-edit-form")).toBeHidden({
      timeout: 5000,
    });

    // Persistence: the dashboard's own data source (GET /endpoints) now reports
    // youtube_oauth_id=20 for the endpoint.
    const eps = await (await page.request.get("/api/v1/endpoints")).json();
    expect(eps.find((e: { id: number }) => e.id === 1).youtube_oauth_id).toBe(
      20,
    );

    expectCleanConsole(consoleErrors);
  });

  test("Unlink option clears the linkage back to NULL", async ({ page }) => {
    const consoleErrors = collectConsoleErrors(page);
    await reset(page);
    await seedGrants(page, GRANTS, []);
    await openConfigTab(page);

    const section = page.locator(".endpoints-tab");
    // First link grant 10.
    await section
      .locator(".endpoint-card")
      .first()
      .locator('button:has-text("Edit")')
      .click();
    await section.getByTestId("edit-oauth-select").selectOption("10");
    await Promise.all([
      page.waitForResponse(
        (r) =>
          r.url().includes("/endpoints/1/link-oauth") &&
          r.request().method() === "POST",
      ),
      section.locator('button:has-text("Save")').click(),
    ]);
    // Wait for the post-save GET /endpoints refetch so the store is fresh
    // before re-opening (save closes the form BEFORE the refetch lands).
    await page.waitForResponse(
      (r) =>
        /\/api\/v1\/endpoints$/.test(r.url()) && r.request().method() === "GET",
    );
    await expect(section.locator(".endpoint-edit-form")).toBeHidden({
      timeout: 5000,
    });

    // Re-open, choose (unlink), save — assert POST body oauth_id:null.
    await section
      .locator(".endpoint-card")
      .first()
      .locator('button:has-text("Edit")')
      .click();
    await expect(section.getByTestId("edit-oauth-select")).toHaveValue("10");
    await section.getByTestId("edit-oauth-select").selectOption("");
    const [unlinkResp] = await Promise.all([
      page.waitForResponse(
        (r) =>
          r.url().includes("/endpoints/1/link-oauth") &&
          r.request().method() === "POST",
      ),
      section.locator('button:has-text("Save")').click(),
    ]);
    expect(unlinkResp.status()).toBe(204);
    expect(unlinkResp.request().postDataJSON()).toEqual({ oauth_id: null });

    expectCleanConsole(consoleErrors);
  });

  test("dropdown is NOT shown for a non-YT (Facebook) endpoint", async ({
    page,
  }) => {
    const consoleErrors = collectConsoleErrors(page);
    await reset(page);
    await seedGrants(page, GRANTS, []);
    await openConfigTab(page);

    const section = page.locator(".endpoints-tab");
    // Second card is Facebook Page (service_type FB).
    await section
      .locator(".endpoint-card")
      .nth(1)
      .locator('button:has-text("Edit")')
      .click();
    await expect(section.locator(".endpoint-edit-form")).toBeVisible({
      timeout: 5000,
    });
    await expect(section.getByTestId("edit-oauth-select")).toHaveCount(0);

    expectCleanConsole(consoleErrors);
  });

  test("auto-suggest pre-selects the grant that uniquely owns the stream key", async ({
    page,
  }) => {
    const consoleErrors = collectConsoleErrors(page);
    await reset(page);
    // Grant 10 owns the YT endpoint's stream key; grant 20 owns another.
    await seedGrants(page, GRANTS, [
      { oauth_id: 10, stream_names: [YT_KEY] },
      { oauth_id: 20, stream_names: ["some-other-key"] },
    ]);
    await openConfigTab(page);

    const section = page.locator(".endpoints-tab");
    await section
      .locator(".endpoint-card")
      .first()
      .locator('button:has-text("Edit")')
      .click();
    await expect(section.locator(".endpoint-edit-form")).toBeVisible({
      timeout: 5000,
    });
    // Auto-suggest pre-selected grant 10.
    await expect(section.getByTestId("edit-oauth-select")).toHaveValue("10");
    await expect(section.getByTestId("oauth-suggest-hint")).toHaveCount(0);

    expectCleanConsole(consoleErrors);
  });

  test("auto-suggest shows the hint when no grant owns the stream key", async ({
    page,
  }) => {
    const consoleErrors = collectConsoleErrors(page);
    await reset(page);
    // Neither grant owns the YT endpoint's stream key.
    await seedGrants(page, GRANTS, [
      { oauth_id: 10, stream_names: ["nope-1"] },
      { oauth_id: 20, stream_names: ["nope-2"] },
    ]);
    await openConfigTab(page);

    const section = page.locator(".endpoints-tab");
    await section
      .locator(".endpoint-card")
      .first()
      .locator('button:has-text("Edit")')
      .click();
    await expect(section.locator(".endpoint-edit-form")).toBeVisible({
      timeout: 5000,
    });
    await expect(section.getByTestId("edit-oauth-select")).toHaveValue("");
    await expect(section.getByTestId("oauth-suggest-hint")).toBeVisible();

    expectCleanConsole(consoleErrors);
  });
});
