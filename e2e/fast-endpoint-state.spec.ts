import { test, expect } from "@playwright/test";
import * as fs from "fs";
import * as path from "path";

// Inject the Tauri mock so the Leptos app runs in browser mode (fetch over
// HTTP, not Tauri IPC), matching every other frontend spec in this suite.
const tauriMockScript = fs.readFileSync(
  path.join(__dirname, "tauri-mock.js"),
  "utf-8",
);

// Chromium-level warnings that are not application bugs.
const ALLOWED_CONSOLE = [
  /integrity.*attribute.*currently ignored.*subresource integrity/i,
];

const MOCK = "http://127.0.0.1:8910";

function collectConsole(page: import("@playwright/test").Page): string[] {
  const messages: string[] = [];
  page.on("console", (msg) => {
    if (msg.type() === "error" || msg.type() === "warning") {
      messages.push(`[${msg.type()}] ${msg.text()}`);
    }
  });
  return messages;
}

function realErrors(messages: string[]): string[] {
  return messages.filter((m) => !ALLOWED_CONSOLE.some((r) => r.test(m)));
}

async function openFastScenario(
  page: import("@playwright/test").Page,
  request: import("@playwright/test").APIRequestContext,
) {
  await page.addInitScript(tauriMockScript);
  await request.post(`${MOCK}/api/v1/__reset`);
  await request.post(`${MOCK}/api/v1/_test/scenario`, {
    data: { scenario: "fast-endpoint" },
  });
  await page.goto("/");
  const fast = page.locator(".endpoint-node", { hasText: "KS-PP-TEST" });
  const normal = page.locator(".endpoint-node", { hasText: "YT_PP_4K_RTMP" });
  await expect(fast).toBeVisible({ timeout: 10000 });
  await expect(normal).toBeVisible();
  return { fast, normal };
}

// #437 (PP live 25.11.2026): the operator must SEE whether a fast stream is
// clean: the skips ahead, whether the backup ingest keeps up, the target and
// the lag behind live.
test("an endpoint node shows its skips ahead and its backup ingest state", async ({
  page,
  request,
}) => {
  const consoleMessages = collectConsole(page);
  const { fast, normal } = await openFastScenario(page, request);

  await expect(fast.getByTestId("endpoint-skips")).toHaveText("preskočené: 25");
  await expect(fast.getByTestId("endpoint-backup")).toHaveText(
    "záloha: zaostáva (vypadlo 75)",
  );
  await expect(fast.getByTestId("fast-delay-state")).toHaveText(
    "cieľ: 9 s · za živým: 28 s",
  );
  // The clean normal endpoint: backup OK, no skips label at all.
  await expect(normal.getByTestId("endpoint-backup")).toHaveText("záloha: OK");
  await expect(normal.getByTestId("endpoint-skips")).toHaveCount(0);
  await expect(normal.getByTestId("fast-delay-control")).toHaveCount(0);

  expect(realErrors(consoleMessages)).toEqual([]);
});

test("the operator sets a fast endpoint's read-delay target live", async ({
  page,
  request,
}) => {
  const consoleMessages = collectConsole(page);
  const { fast } = await openFastScenario(page, request);

  // A value outside 5..=120 is refused in the UI and never sent.
  await fast.getByTestId("fast-delay-input").fill("4");
  await fast.getByTestId("fast-delay-set").click();
  await expect(fast.getByTestId("fast-delay-result")).toHaveText(
    "zadaj 5 až 120 s",
  );
  let sent = await (await request.get(`${MOCK}/api/v1/_test/fast-delay-requests`)).json();
  expect(sent).toEqual([]);

  // 30 s: above the 28 s lag, so the 62 s skip loop of 25.11. stops.
  await fast.getByTestId("fast-delay-input").fill("30");
  await fast.getByTestId("fast-delay-set").click();
  await expect(fast.getByTestId("fast-delay-result")).toHaveText(
    "nastavené: 9 s → 30 s",
  );
  sent = await (await request.get(`${MOCK}/api/v1/_test/fast-delay-requests`)).json();
  expect(sent).toEqual([{ event_id: 1, alias: "KS-PP-TEST", target_secs: 30 }]);

  // The backend holds the new target: a reload shows it on the node.
  await page.reload();
  const reloaded = page.locator(".endpoint-node", { hasText: "KS-PP-TEST" });
  await expect(reloaded.getByTestId("fast-delay-state")).toHaveText(
    "cieľ: 30 s · za živým: 28 s",
    { timeout: 10000 },
  );

  expect(realErrors(consoleMessages)).toEqual([]);
});
