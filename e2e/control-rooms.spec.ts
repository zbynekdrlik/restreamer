import { test, expect } from "@playwright/test";
import * as fs from "fs";
import * as path from "path";

// #388: control-room tabs in the operator dashboard. With an active event the
// dashboard shows one tab per YT/FB destination of that event; a tab opens the
// destination's control room (a screencast of the box-hosted Chrome) and
// forwards mouse/keyboard back. The mock API serves the destinations and a fake
// screencast websocket (one solid red 16x16 JPEG frame).

const tauriMockScript = fs.readFileSync(path.join(__dirname, "tauri-mock.js"), "utf-8");
const API = "http://127.0.0.1:8910/api/v1";
const ALLOWED_CONSOLE = [/integrity.*attribute.*currently ignored.*subresource integrity/i];

const ROOMS = {
  event: { id: 7, name: "Nedeľa" },
  destinations: [
    {
      endpoint_id: 11, alias: "YT NLCH 4K", service_type: "YT_RTMP", account: "yt-owner@example.com",
      sure: true, channel: "UC1", channel_name: "NEW LEVEL CHURCH", profile: "acct-yt-owner-example-com",
      url: "https://studio.youtube.com/channel/UC1/livestreaming", target_id: null, signed_in: true,
    },
    {
      endpoint_id: 12, alias: "FB-GoodFest", service_type: "FB", account: null, sure: false,
      channel_name: null, profile: "dest-12", url: "https://www.facebook.com/live/producer/",
      target_id: null, signed_in: null,
    },
  ],
};

test.beforeEach(async ({ request }) => {
  await request.post(`${API}/__reset`);
});

function collectConsole(page: import("@playwright/test").Page): string[] {
  const out: string[] = [];
  page.on("console", (msg) => {
    if (msg.type() === "error" || msg.type() === "warning") out.push(`[${msg.type()}] ${msg.text()}`);
  });
  return out;
}

const unexpected = (msgs: string[]) => msgs.filter((m) => !ALLOWED_CONSOLE.some((re) => re.test(m)));

test("no destinations: no tab strip, the dashboard is unchanged", async ({ page }) => {
  const consoleMessages = collectConsole(page);
  await page.addInitScript(tauriMockScript);
  await page.goto("/");
  await expect(page.locator(".operator-dashboard__layout")).toBeVisible();
  await expect(page.getByTestId("control-room-tabs")).toHaveCount(0);
  expect(unexpected(consoleMessages)).toEqual([]);
});

test("active event: tabs per destination, a room shows the screencast and forwards input", async ({
  page,
  request,
}) => {
  const consoleMessages = collectConsole(page);
  await request.post(`${API}/_test/control-rooms`, { data: ROOMS });
  await page.addInitScript(tauriMockScript);
  await page.goto("/");

  const tabs = page.getByTestId("control-room-tabs");
  await expect(tabs).toBeVisible();
  await expect(tabs).toContainText("Event: Nedeľa");
  await expect(page.getByTestId("cr-tab-11")).toContainText("YT NLCH 4K");
  await expect(page.getByTestId("cr-tab-11")).toHaveAttribute("title", "yt-owner@example.com");
  await expect(page.getByTestId("cr-tab-12")).toContainText("FB-GoodFest");
  await expect(page.getByTestId("cr-tab-12")).toHaveAttribute("title", "účet nezistený");

  // Open the YT room: the dashboard hides, the room opens and paints a frame.
  await page.getByTestId("cr-tab-11").click();
  await expect(page.locator(".operator-dashboard__home")).toBeHidden();
  const room = page.getByTestId("control-room");
  await expect(room).toBeVisible();
  await expect(page.getByTestId("cr-room-alias")).toHaveText("YT NLCH 4K");
  await expect(page.getByTestId("cr-room-account")).toHaveText("yt-owner@example.com");
  await expect(page.getByTestId("cr-room-status")).toHaveText("pripojené");
  await expect(page.getByTestId("cr-url")).toHaveValue("https://studio.youtube.com/channel/UC1/livestreaming");
  const canvas = page.getByTestId("cr-canvas");
  await expect(canvas).toHaveAttribute("data-frames", /^[1-9]/);
  const pixel = await canvas.evaluate((c: HTMLCanvasElement) => {
    const d = c.getContext("2d")!.getImageData(c.width / 2, c.height / 2, 1, 1).data;
    return [d[0], d[1], d[2]];
  });
  expect(pixel[0]).toBeGreaterThan(200); // the red test frame was drawn
  expect(pixel[2]).toBeLessThan(60);

  // Mouse and keyboard go back to the box-hosted Chrome.
  await canvas.click({ position: { x: 20, y: 20 } });
  await page.keyboard.type("ok");
  await expect
    .poll(async () => {
      const s = await (await request.get(`${API}/_test/control-rooms`)).json();
      return s.inputs.map((i: { type?: string; text?: string }) => `${i.type}:${i.text ?? ""}`);
    })
    .toEqual(expect.arrayContaining(["mousePressed:", "mouseReleased:", "keyDown:o", "keyDown:k"]));
  const state = await (await request.get(`${API}/_test/control-rooms`)).json();
  expect(state.opens).toEqual([11]);
  expect(state.inputs.every((i: { target: string }) => i.target === "T11")).toBe(true);

  // Back to the dashboard.
  await page.getByTestId("cr-tab-dashboard").click();
  await expect(page.locator(".operator-dashboard__home")).toBeVisible();
  await expect(room).toHaveCount(0);

  expect(unexpected(consoleMessages)).toEqual([]);
});

test("the account of a destination is set from its room", async ({ page, request }) => {
  const consoleMessages = collectConsole(page);
  await request.post(`${API}/_test/control-rooms`, { data: ROOMS });
  await page.addInitScript(tauriMockScript);
  await page.goto("/");
  await page.getByTestId("cr-tab-12").click();
  await expect(page.getByTestId("cr-room-account")).toHaveText("účet nezistený");
  await page.getByRole("button", { name: "Zmeniť účet" }).click();
  await page.getByTestId("cr-account-input").fill("fb-owner@example.com");
  await page.getByTestId("cr-account-save").click();
  await expect
    .poll(async () => (await (await request.get(`${API}/_test/control-rooms`)).json()).accounts["12"])
    .toEqual({ account: "fb-owner@example.com", channel: null, sure: true });
  // The tab strip picks the new account up on its next refresh.
  await page.reload();
  await expect(page.getByTestId("cr-tab-12")).toHaveAttribute("title", "fb-owner@example.com");

  // Editing a YouTube room's account keeps the channel it opens.
  await page.getByTestId("cr-tab-11").click();
  await page.getByRole("button", { name: "Zmeniť účet" }).click();
  await page.getByTestId("cr-account-input").fill("yt-new@example.com");
  await page.getByTestId("cr-account-save").click();
  await expect
    .poll(async () => (await (await request.get(`${API}/_test/control-rooms`)).json()).accounts["11"])
    .toEqual({ account: "yt-new@example.com", channel: "UC1", sure: true });
  expect(unexpected(consoleMessages)).toEqual([]);
});
