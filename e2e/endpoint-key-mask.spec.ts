import { test, expect, Page } from "@playwright/test";

// #391: the endpoint read routes serve the stream key masked (`***` + last 4).
// The edit form therefore starts with an EMPTY key field (placeholder shows
// the mask); saving without typing a key must keep the stored key, and typing
// a new key must replace it. Runs against mock-api.js, which mirrors the real
// masking + keep-on-empty/masked PUT contract.

const ALLOWED_CONSOLE = [
  /integrity.*attribute.*currently ignored.*subresource integrity/i,
];

function collectConsoleErrors(page: Page): string[] {
  const msgs: string[] = [];
  page.on("console", (m) => {
    if (m.type() === "error" || m.type() === "warning") {
      msgs.push(`[${m.type()}] ${m.text()}`);
    }
  });
  return msgs;
}

function expectCleanConsole(msgs: string[]) {
  expect(msgs.filter((m) => !ALLOWED_CONSOLE.some((r) => r.test(m)))).toEqual(
    [],
  );
}

async function openFirstEndpointEdit(page: Page) {
  await page.goto("/settings");
  await page.locator(".settings-tabs button:has-text('Config')").click();
  const section = page.locator(".endpoints-tab");
  await expect(section.locator(".endpoint-card").first()).toBeVisible({
    timeout: 10000,
  });
  await section
    .locator(".endpoint-card")
    .first()
    .locator('button:has-text("Edit")')
    .click();
  await expect(section.locator(".endpoint-edit-form")).toBeVisible({
    timeout: 5000,
  });
  return section;
}

async function storedKey(page: Page, id: number): Promise<string> {
  const r = await page.request.get(`/api/v1/_test/stored-key/${id}`);
  return (await r.json()).stream_key;
}

async function keyUpdates(page: Page, id: number) {
  const r = await page.request.get("/api/v1/_test/change-key-ops");
  const ops: { op: string; id: number }[] = await r.json();
  return ops.filter((o) => o.op === "update" && o.id === id);
}

test.describe("Endpoint stream key is never served in plaintext (#391)", () => {
  test.beforeEach(async ({ page }) => {
    await page.request.post("/api/v1/__reset");
  });

  test("the list carries only the mask; saving without a key keeps the stored key", async ({
    page,
  }) => {
    const msgs = collectConsoleErrors(page);

    const list = await (await page.request.get("/api/v1/endpoints")).json();
    expect(list[0].stream_key).toBe("***xxxx");
    expect(JSON.stringify(list)).not.toContain("xxxx-xxxx-xxxx");

    const section = await openFirstEndpointEdit(page);
    const keyInput = section.getByTestId("edit-key-input");
    await expect(keyInput).toHaveValue("");
    await expect(keyInput).toHaveAttribute("placeholder", "unchanged (***xxxx)");

    const putBody = page.waitForRequest(
      (r) => r.method() === "PUT" && /\/api\/v1\/endpoints\/1$/.test(r.url()),
    );
    await section
      .locator('.edit-row:has(label:text("Alias")) input')
      .fill("YouTube Renamed");
    await section.locator(".edit-actions button.save").click();
    const sent = (await putBody).postDataJSON();
    expect(sent.alias).toBe("YouTube Renamed");
    expect(sent).not.toHaveProperty("stream_key");

    await expect(section.locator(".endpoint-edit-form")).toBeHidden({
      timeout: 5000,
    });
    await expect(section.locator(".endpoint-card").first()).toContainText(
      "YouTube Renamed",
    );
    expect(await storedKey(page, 1)).toBe("xxxx-xxxx-xxxx");
    expect(await keyUpdates(page, 1)).toEqual([]);

    expectCleanConsole(msgs);
  });

  test("typing a new key replaces the stored key", async ({ page }) => {
    const msgs = collectConsoleErrors(page);

    const section = await openFirstEndpointEdit(page);
    await section.getByTestId("edit-key-input").fill("new-stream-key-mnop");
    await section.locator(".edit-actions button.save").click();
    await expect(section.locator(".endpoint-edit-form")).toBeHidden({
      timeout: 5000,
    });

    expect(await storedKey(page, 1)).toBe("new-stream-key-mnop");
    expect(await keyUpdates(page, 1)).toHaveLength(1);
    const list = await (await page.request.get("/api/v1/endpoints")).json();
    expect(list[0].stream_key).toBe("***mnop");

    expectCleanConsole(msgs);
  });
});
