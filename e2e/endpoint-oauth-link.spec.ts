import { test, expect, Page } from "@playwright/test";

// #199: link/unlink an OAuth grant to an endpoint from the edit-endpoint
// dialog. Runs against the mock backend (mock-api.js) used by frontend.spec.
//
// The real `POST /endpoints/{id}/link-oauth` returns 204 No Content; the mock
// mirrors that. The dropdown appears only for YT_RTMP endpoints. Auto-suggest
// is computed server-side: `GET /endpoints/{id}/oauth-suggest` returns
// { oauth_id, owners, probed_ok }, seeded per-endpoint by the test fixture.

const GRANTS = [
  { id: 10, label: "main", channel_id: "UCmain", connected_at: "2026-03-01T00:00:00Z" },
  { id: 20, label: "backup", channel_id: "UCbackup", connected_at: "2026-03-02T00:00:00Z" },
];

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

// `suggest` maps endpointId -> { oauth_id, owners, probed_ok }.
async function seedGrants(
  page: Page,
  grants: unknown[],
  suggest: Record<string, unknown> = {},
) {
  await page.request.post("/api/v1/_test/seed-oauth-grants", {
    data: { grants, suggest },
  });
}

async function openConfigTab(page: Page) {
  // Register the grants-fetch waiter BEFORE navigating so it catches the mount
  // fetch whenever it fires (on load or on the Config click), never missing it.
  const grantsLoaded = page.waitForResponse((r) =>
    /\/api\/v1\/youtube\/oauths$/.test(r.url()),
  );
  await page.goto("/settings");
  await page.locator(".settings-tabs button:has-text('Config')").click();
  await expect(page.locator(".endpoints-tab")).toBeVisible({ timeout: 10000 });
  await grantsLoaded; // dropdown grants are loaded before we open an edit form
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

// Open the edit form for the Nth endpoint card (0-based).
async function editCard(page: Page, n: number) {
  const section = page.locator(".endpoints-tab");
  await section
    .locator(".endpoint-card")
    .nth(n)
    .locator('button:has-text("Edit")')
    .click();
  await expect(section.locator(".endpoint-edit-form")).toBeVisible({
    timeout: 5000,
  });
  return section;
}

test.describe("Endpoint OAuth grant link/unlink (#199)", () => {
  test("YT dialog shows OAuth dropdown; select + save links the grant (204) and persists", async ({
    page,
  }) => {
    const consoleErrors = collectConsoleErrors(page);
    await reset(page);
    await seedGrants(page, GRANTS); // no suggest => dropdown defaults to (unlink)
    await openConfigTab(page);

    const section = await editCard(page, 0); // YouTube Main (YT_RTMP)

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
    await seedGrants(page, GRANTS);
    await openConfigTab(page);

    let section = await editCard(page, 0);
    // Link grant 10.
    await section.getByTestId("edit-oauth-select").selectOption("10");
    await Promise.all([
      page.waitForResponse(
        (r) =>
          r.url().includes("/endpoints/1/link-oauth") &&
          r.request().method() === "POST",
      ),
      section.locator('button:has-text("Save")').click(),
    ]);

    // Reload for a deterministic fresh store (the mock has the link persisted),
    // then re-open (linked to 10, so no auto-suggest fires), choose (unlink),
    // save — assert POST body oauth_id:null.
    await openConfigTab(page);
    section = await editCard(page, 0);
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

    const eps = await (await page.request.get("/api/v1/endpoints")).json();
    expect(
      eps.find((e: { id: number }) => e.id === 1).youtube_oauth_id,
    ).toBeNull();

    expectCleanConsole(consoleErrors);
  });

  test("dropdown is NOT shown for a non-YT (Facebook) endpoint", async ({
    page,
  }) => {
    const consoleErrors = collectConsoleErrors(page);
    await reset(page);
    await seedGrants(page, GRANTS);
    await openConfigTab(page);

    const section = await editCard(page, 1); // Facebook Page (FB)
    await expect(section.getByTestId("edit-oauth-select")).toHaveCount(0);

    expectCleanConsole(consoleErrors);
  });

  test("a save with no OAuth change does NOT emit a link-oauth request", async ({
    page,
  }) => {
    const consoleErrors = collectConsoleErrors(page);
    let linkPosts = 0;
    page.on("request", (r) => {
      if (r.url().includes("/link-oauth") && r.method() === "POST") linkPosts++;
    });
    await reset(page);
    await seedGrants(page, GRANTS); // no suggest => stays (unlink)
    await openConfigTab(page);

    const section = await editCard(page, 0);
    // Change only the alias, leave the OAuth dropdown untouched.
    const aliasInput = section.locator('.edit-row:has(label:text("Alias")) input');
    await aliasInput.clear();
    await aliasInput.fill("YouTube Renamed");
    await Promise.all([
      page.waitForResponse(
        (r) => r.url().includes("/endpoints/1") && r.request().method() === "PUT",
      ),
      page.waitForResponse(
        (r) =>
          /\/api\/v1\/endpoints$/.test(r.url()) &&
          r.request().method() === "GET",
      ),
      section.locator('button:has-text("Save")').click(),
    ]);
    await expect(section.locator(".endpoint-edit-form")).toBeHidden({
      timeout: 5000,
    });
    expect(linkPosts).toBe(0);

    expectCleanConsole(consoleErrors);
  });

  test("auto-suggest pre-selects the grant that uniquely owns the stream key", async ({
    page,
  }) => {
    const consoleErrors = collectConsoleErrors(page);
    await reset(page);
    await seedGrants(page, GRANTS, {
      "1": { oauth_id: 10, owners: 1, probed_ok: true },
    });
    await openConfigTab(page);

    const section = page.locator(".endpoints-tab");
    await Promise.all([
      page.waitForResponse((r) => /\/endpoints\/1\/oauth-suggest/.test(r.url())),
      section
        .locator(".endpoint-card")
        .first()
        .locator('button:has-text("Edit")')
        .click(),
    ]);
    await expect(section.locator(".endpoint-edit-form")).toBeVisible({
      timeout: 5000,
    });
    await expect(section.getByTestId("edit-oauth-select")).toHaveValue("10");
    await expect(section.getByTestId("oauth-suggest-hint")).toHaveCount(0);

    expectCleanConsole(consoleErrors);
  });

  test("auto-suggest does NOT override an already-linked endpoint", async ({
    page,
  }) => {
    const consoleErrors = collectConsoleErrors(page);
    await reset(page);
    // The server would suggest grant 10, but the endpoint is already linked
    // to 20 — the existing link must win (no probe, no override).
    await seedGrants(page, GRANTS, {
      "1": { oauth_id: 10, owners: 1, probed_ok: true },
    });
    await openConfigTab(page);

    // Link endpoint 1 to grant 20.
    let section = await editCard(page, 0);
    await section.getByTestId("edit-oauth-select").selectOption("20");
    await Promise.all([
      page.waitForResponse(
        (r) =>
          r.url().includes("/endpoints/1/link-oauth") &&
          r.request().method() === "POST",
      ),
      section.locator('button:has-text("Save")').click(),
    ]);

    // Reload for a deterministic fresh store, then watch for an (erroneous)
    // oauth-suggest probe while re-opening the now-linked endpoint.
    await openConfigTab(page);
    let suggestFired = false;
    page.on("request", (r) => {
      if (r.url().includes("/oauth-suggest")) suggestFired = true;
    });
    section = await editCard(page, 0);
    // It stays linked to 20 (the existing link wins over the server suggestion).
    await expect(section.getByTestId("edit-oauth-select")).toHaveValue("20");
    // Give any (erroneous) probe a chance to fire, then assert it did not.
    await page.waitForTimeout(300);
    expect(suggestFired).toBe(false);

    expectCleanConsole(consoleErrors);
  });

  test("ambiguous (>1 owner) does not pre-select and shows no hint", async ({
    page,
  }) => {
    const consoleErrors = collectConsoleErrors(page);
    await reset(page);
    await seedGrants(page, GRANTS, {
      "1": { oauth_id: null, owners: 2, probed_ok: true },
    });
    await openConfigTab(page);

    const section = page.locator(".endpoints-tab");
    await Promise.all([
      page.waitForResponse((r) => /\/endpoints\/1\/oauth-suggest/.test(r.url())),
      section
        .locator(".endpoint-card")
        .first()
        .locator('button:has-text("Edit")')
        .click(),
    ]);
    await expect(section.locator(".endpoint-edit-form")).toBeVisible({
      timeout: 5000,
    });
    await expect(section.getByTestId("edit-oauth-select")).toHaveValue("");
    await expect(section.getByTestId("oauth-suggest-hint")).toHaveCount(0);

    expectCleanConsole(consoleErrors);
  });

  test("auto-suggest shows the hint when no grant owns the stream key", async ({
    page,
  }) => {
    const consoleErrors = collectConsoleErrors(page);
    await reset(page);
    await seedGrants(page, GRANTS, {
      "1": { oauth_id: null, owners: 0, probed_ok: true },
    });
    await openConfigTab(page);

    const section = page.locator(".endpoints-tab");
    await Promise.all([
      page.waitForResponse((r) => /\/endpoints\/1\/oauth-suggest/.test(r.url())),
      section
        .locator(".endpoint-card")
        .first()
        .locator('button:has-text("Edit")')
        .click(),
    ]);
    await expect(section.locator(".endpoint-edit-form")).toBeVisible({
      timeout: 5000,
    });
    await expect(section.getByTestId("edit-oauth-select")).toHaveValue("");
    await expect(section.getByTestId("oauth-suggest-hint")).toBeVisible();

    expectCleanConsole(consoleErrors);
  });
});
