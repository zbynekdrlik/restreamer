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

test("a room fills the window and the box-hosted page gets the canvas size", async ({ page, request }) => {
  const consoleMessages = collectConsole(page);
  await request.post(`${API}/_test/control-rooms`, { data: ROOMS });
  await page.setViewportSize({ width: 1920, height: 1080 });
  await page.addInitScript(tauriMockScript);
  await page.goto("/");

  // The dashboard uses the whole window width, not a centred 1200 px column.
  const app = await page.locator(".app").boundingBox();
  expect(app!.x).toBeLessThan(1);
  expect(app!.width).toBeGreaterThan(1850);

  await page.getByTestId("cr-tab-11").click();
  await expect(page.getByTestId("cr-room-status")).toHaveText("pripojené");
  const canvas = page.getByTestId("cr-canvas");
  const sizes = async () => {
    const s = await (await request.get(`${API}/_test/control-rooms`)).json();
    return s.inputs.filter((i: { t?: string }) => i.t === "size") as { w: number; h: number }[];
  };

  // The canvas fills the window below the room header, and the page in the
  // box-hosted Chrome is told exactly that size.
  await expect.poll(async () => (await sizes()).length).toBeGreaterThan(0);
  let box = (await canvas.boundingBox())!;
  expect(box.width).toBeGreaterThan(1850);
  expect(box.y + box.height).toBeLessThanOrEqual(1080);
  expect(box.y + box.height).toBeGreaterThan(1040);
  let last = (await sizes()).at(-1)!;
  const inner = await canvas.evaluate((c: HTMLCanvasElement) => [c.clientWidth, c.clientHeight]);
  expect([last.w, last.h]).toEqual(inner);

  // A smaller window: the canvas shrinks with it and the page follows.
  await page.setViewportSize({ width: 1280, height: 800 });
  await expect.poll(async () => (await sizes()).at(-1)!.w).toBeLessThan(1280);
  box = (await canvas.boundingBox())!;
  expect(box.y + box.height).toBeLessThanOrEqual(800);
  last = (await sizes()).at(-1)!;
  const inner2 = await canvas.evaluate((c: HTMLCanvasElement) => [c.clientWidth, c.clientHeight]);
  expect([last.w, last.h]).toEqual(inner2);

  // Clicks still land on the page at the right spot: the centre of the
  // canvas maps to the centre of the (16x16) frame.
  await canvas.click({ position: { x: box.width / 2, y: box.height / 2 } });
  await expect
    .poll(async () => {
      const s = await (await request.get(`${API}/_test/control-rooms`)).json();
      return s.inputs.filter((i: { type?: string }) => i.type === "mousePressed").length;
    })
    .toBeGreaterThan(0);
  const press = (await (await request.get(`${API}/_test/control-rooms`)).json()).inputs.find(
    (i: { type?: string }) => i.type === "mousePressed",
  );
  expect(press.x).toBeCloseTo(8, 0);
  expect(press.y).toBeCloseTo(8, 0);

  expect(unexpected(consoleMessages)).toEqual([]);
});

test("a copy in the room reaches the operator's clipboard; Ctrl+C asks the page to copy", async ({
  page,
  request,
  context,
}) => {
  const consoleMessages = collectConsole(page);
  await context.grantPermissions(["clipboard-read", "clipboard-write"]);
  await request.post(`${API}/_test/control-rooms`, { data: ROOMS });
  await page.addInitScript(tauriMockScript);
  await page.goto("/");
  await page.getByTestId("cr-tab-11").click();
  await expect(page.getByTestId("cr-room-status")).toHaveText("pripojené");

  // Ctrl+C in the room: the box-hosted page runs its own copy command.
  const canvas = page.getByTestId("cr-canvas");
  await canvas.click({ position: { x: 20, y: 20 } });
  await page.keyboard.press("Control+c");
  await expect
    .poll(async () => {
      const s = await (await request.get(`${API}/_test/control-rooms`)).json();
      return s.inputs.filter((i: { t: string }) => i.t === "copy").length;
    })
    .toBe(1);

  // The page reports what it copied (a selection, a "Copy" button in Studio).
  const url = "rtmp://a.rtmp.youtube.com/live2";
  const sent = await request.post(`${API}/_test/control-rooms/send`, {
    data: { target: "T11", msg: { t: "clip", text: url } },
  });
  expect((await sent.json()).sent).toBe(1);
  await expect(page.getByTestId("cr-clip")).toHaveValue(url);
  await expect.poll(() => page.evaluate(() => navigator.clipboard.readText())).toBe(url);

  // The bar's own button copies it again (a real click: works where the
  // automatic copy is refused).
  await page.evaluate(() => navigator.clipboard.writeText("something else"));
  await page.getByTestId("cr-clip-copy").click();
  await expect.poll(() => page.evaluate(() => navigator.clipboard.readText())).toBe(url);

  expect(unexpected(consoleMessages)).toEqual([]);
});

// #399: thumbnails into a room. A page's file chooser shows a bar with the
// operator's own file picker; a file dropped on the room or an image pasted
// into it goes to the page as {t:"file"} with its bytes.
test("files go into a room: the page's file chooser, a drop and a pasted image", async ({ page, request }) => {
  const consoleMessages = collectConsole(page);
  await request.post(`${API}/_test/control-rooms`, { data: ROOMS });
  await page.addInitScript(tauriMockScript);
  await page.goto("/");
  await page.getByTestId("cr-tab-11").click();
  await expect(page.getByTestId("cr-room-status")).toHaveText("pripojené");
  const files = async () => {
    const s = await (await request.get(`${API}/_test/control-rooms`)).json();
    return s.inputs
      .filter((i: { t: string }) => i.t === "file")
      .map((i: { name: string; b64: string; x?: number; y?: number }) => ({
        name: i.name,
        b64: i.b64,
        placed: typeof i.x === "number" && typeof i.y === "number",
      }));
  };
  const send = (msg: object) => request.post(`${API}/_test/control-rooms/send`, { data: { target: "T11", msg } });

  // "Upload thumbnail" in Studio: the page asks for a file.
  await expect(page.getByTestId("cr-file-bar")).toHaveCount(0);
  await send({ t: "file-chooser", mode: "selectSingle" });
  await expect(page.getByTestId("cr-file-bar")).toBeVisible();
  const png = Buffer.from([0x89, 0x50, 0x4e, 0x47, 1, 2, 3]);
  await page.getByTestId("cr-file-input").setInputFiles({ name: "thumb.png", mimeType: "image/png", buffer: png });
  await expect.poll(files).toEqual([{ name: "thumb.png", b64: png.toString("base64"), placed: true }]);
  await send({ t: "file-done", name: "thumb.png", how: "chooser" });
  await expect(page.getByTestId("cr-room-status")).toHaveText("vložené: thumb.png");
  await expect(page.getByTestId("cr-file-bar")).toHaveCount(0);

  // A file dropped on the room.
  await page.getByTestId("cr-canvas").evaluate((c) => {
    const dt = new DataTransfer();
    dt.items.add(new File([new Uint8Array([1, 2])], "drop.jpg", { type: "image/jpeg" }));
    const r = c.getBoundingClientRect();
    for (const type of ["dragover", "drop"]) {
      c.dispatchEvent(new DragEvent(type, { dataTransfer: dt, clientX: r.left + 30, clientY: r.top + 30, bubbles: true, cancelable: true }));
    }
  });
  await expect.poll(async () => (await files()).map((f: { name: string }) => f.name)).toEqual(["thumb.png", "drop.jpg"]);

  // An image pasted from the operator's clipboard.
  await page.getByTestId("cr-canvas").focus();
  await page.evaluate(() => {
    const dt = new DataTransfer();
    dt.items.add(new File([new Uint8Array([9])], "paste.png", { type: "image/png" }));
    document.dispatchEvent(new ClipboardEvent("paste", { clipboardData: dt, bubbles: true, cancelable: true }));
  });
  await expect.poll(async () => (await files()).map((f: { name: string }) => f.name)).toEqual([
    "thumb.png",
    "drop.jpg",
    "paste.png",
  ]);
  expect((await files()).every((f: { placed: boolean }) => f.placed)).toBe(true);

  // The relay refused one: the operator reads why.
  await send({ t: "file-error", error: "súbor je väčší ako 20 MB" });
  await expect(page.getByTestId("cr-room-status")).toHaveText("súbor sa nevložil: súbor je väčší ako 20 MB");

  expect(unexpected(consoleMessages)).toEqual([]);
});

test("a room's page is reloaded from its button, F5 and Ctrl+R", async ({ page, request }) => {
  const consoleMessages = collectConsole(page);
  await request.post(`${API}/_test/control-rooms`, { data: ROOMS });
  await page.addInitScript(tauriMockScript);
  await page.goto("/");
  await page.getByTestId("cr-tab-11").click();
  await expect(page.getByTestId("cr-room-status")).toHaveText("pripojené");
  const reloads = async () => {
    const s = await (await request.get(`${API}/_test/control-rooms`)).json();
    return s.inputs.filter((i: { t: string }) => i.t === "reload").length;
  };
  const keys = async () => {
    const s = await (await request.get(`${API}/_test/control-rooms`)).json();
    return s.inputs.filter((i: { t: string; key?: string }) => i.t === "k" && (i.key === "F5" || i.key === "r")).length;
  };

  await page.getByTestId("cr-reload").click();
  await expect.poll(reloads).toBe(1);
  await expect(page.getByTestId("cr-room-status")).toHaveText("obnovujem stránku…");
  await page.getByTestId("cr-canvas").click({ position: { x: 20, y: 20 } });
  await page.keyboard.press("F5");
  await expect.poll(reloads).toBe(2);
  await page.keyboard.press("Control+r");
  await expect.poll(reloads).toBe(3);
  // They reload the room's page, not the dashboard, and are not sent as keys.
  await expect(page.getByTestId("control-room")).toBeVisible();
  expect(await keys()).toBe(0);
  expect(unexpected(consoleMessages)).toEqual([]);
});
