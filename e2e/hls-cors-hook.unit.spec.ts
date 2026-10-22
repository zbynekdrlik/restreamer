// #415: the page hook that lets YouTube Studio's live preview play in a
// control room. Chrome 151+ plays HLS natively and refuses segments that come
// from another origin than the manifest unless the media element asked in
// CORS mode ("insecure media request"). Studio hands its manifest
// (www.youtube.com/api/manifest/hls_*) to a <video> without a crossorigin
// attribute, and the segments come from *.c.youtube.com, so the preview never
// played on stream-pp (2026-10-21).
//
// This runs the very file the relay installs in every room page
// (crates/rs-api/src/control_room/hls_cors_hook.js) in a real browser, on a
// routed studio.youtube.com page: no YouTube, no network.
import { test, expect, Page } from "@playwright/test";
import * as fs from "fs";
import * as path from "path";

const HOOK = fs.readFileSync(
  path.join(__dirname, "..", "crates", "rs-api", "src", "control_room", "hls_cors_hook.js"),
  "utf-8",
);
const HLS = "https://www.youtube.com/api/manifest/hls_variant/expire/1/index.m3u8";
const MP4 = "https://www.youtube.com/clip.mp4";

function collectConsole(page: Page): string[] {
  const out: string[] = [];
  page.on("console", (m) => {
    if (m.type() === "error" || m.type() === "warning") out.push(`[${m.type()}] ${m.text()}`);
  });
  page.on("pageerror", (e) => out.push(`[pageerror] ${e.message}`));
  return out;
}

async function openWithHook(page: Page, url: string) {
  // The probe elements never load (preload none), but answer anything anyway.
  await page.route("https://www.youtube.com/**", (r) => r.fulfill({ status: 204, body: "" }));
  await page.route(url, (r) =>
    r.fulfill({ status: 200, contentType: "text/html", body: "<!doctype html><title>room</title>" }),
  );
  await page.addInitScript(HOOK);
  await page.goto(url);
}

type Probe = { how: "property" | "attribute"; src: string; preset?: string };

// A media element the way a page sets its source; returns its CORS mode.
const crossOriginOf = (page: Page, p: Probe) =>
  page.evaluate(({ how, src, preset }) => {
    const v = document.createElement("video");
    v.preload = "none";
    if (preset) v.setAttribute("crossorigin", preset);
    if (how === "property") v.src = src;
    else v.setAttribute("src", src);
    if (v.getAttribute("src") !== src) throw new Error("the source was not set");
    return v.crossOrigin;
  }, p);

test("on YouTube an HLS manifest is loaded with credentials in CORS mode", async ({ page }) => {
  const consoleMessages = collectConsole(page);
  await openWithHook(page, "https://studio.youtube.com/video/x/livestreaming");

  expect(await crossOriginOf(page, { how: "property", src: HLS })).toBe("use-credentials");
  expect(await crossOriginOf(page, { how: "attribute", src: HLS })).toBe("use-credentials");
  // Anything but an HLS manifest is left as the page set it.
  expect(await crossOriginOf(page, { how: "property", src: MP4 })).toBeNull();
  expect(await crossOriginOf(page, { how: "attribute", src: MP4 })).toBeNull();
  // A page that chose a mode itself keeps it.
  expect(await crossOriginOf(page, { how: "property", src: HLS, preset: "anonymous" })).toBe("anonymous");
  // Installing it twice (new document + the open one) changes nothing.
  await page.evaluate(HOOK);
  expect(await crossOriginOf(page, { how: "property", src: HLS })).toBe("use-credentials");

  expect(consoleMessages).toEqual([]);
});

// The relay also runs the hook in a page that is already open (a room's
// first document, or a tab that outlived a Restreamer restart): a preview
// whose manifest was set before the hook came is repaired and reloaded.
test("a preview set before the hook arrived is repaired", async ({ page }) => {
  const consoleMessages = collectConsole(page);
  await page.route("https://www.youtube.com/**", (r) => r.fulfill({ status: 204, body: "" }));
  await page.route("https://studio.youtube.com/**", (r) =>
    r.fulfill({ status: 200, contentType: "text/html", body: "<!doctype html><title>room</title>" }),
  );
  await page.goto("https://studio.youtube.com/video/x/livestreaming");
  await page.evaluate(
    ({ hls, mp4 }) => {
      for (const [id, src, preset] of [["hls", hls, ""], ["mp4", mp4, ""], ["own", hls, "anonymous"]]) {
        const v = document.createElement("video");
        v.id = id;
        v.preload = "none";
        if (preset) v.setAttribute("crossorigin", preset);
        v.src = src;
        document.body.appendChild(v);
      }
    },
    { hls: HLS, mp4: MP4 },
  );
  await page.evaluate(HOOK);
  const modes = await page.evaluate(() =>
    ["hls", "mp4", "own"].map((id) => (document.getElementById(id) as HTMLVideoElement).crossOrigin),
  );
  expect(modes).toEqual(["use-credentials", null, "anonymous"]);

  expect(consoleMessages).toEqual([]);
});

// The mode follows the source: an element the hook marked that then gets
// anything but an HLS manifest is back to the page's own (no) mode.
test("an element reused for another source drops the hook's mode", async ({ page }) => {
  const consoleMessages = collectConsole(page);
  await openWithHook(page, "https://studio.youtube.com/video/x/livestreaming");
  const modes = await page.evaluate(
    ({ hls, mp4 }) => {
      const v = document.createElement("video");
      v.preload = "none";
      v.src = hls;
      const first = v.crossOrigin;
      v.src = mp4;
      const second = v.crossOrigin;
      v.setAttribute("src", hls);
      const third = v.crossOrigin;
      v.setAttribute("src", mp4);
      return [first, second, third, v.crossOrigin];
    },
    { hls: HLS, mp4: MP4 },
  );
  expect(modes).toEqual(["use-credentials", null, "use-credentials", null]);

  expect(consoleMessages).toEqual([]);
});

test("other sites are left alone", async ({ page }) => {
  const consoleMessages = collectConsole(page);
  await openWithHook(page, "https://www.facebook.com/live/producer/");

  expect(await crossOriginOf(page, { how: "property", src: HLS })).toBeNull();
  expect(await crossOriginOf(page, { how: "attribute", src: HLS })).toBeNull();

  expect(consoleMessages).toEqual([]);
});
