import { test, expect, chromium, type Browser } from "@playwright/test";
import { spawn, type ChildProcess } from "child_process";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";

const tauriMockScript = fs.readFileSync(
  path.join(__dirname, "tauri-mock.js"),
  "utf-8",
);

// Chrome's own warnings (deprecations, interventions, failed loads) go to
// the CDP Log domain, not to `console`, so page.on("console") never sees
// them. On stream-pp 0.29.49 the dashboard logged a deprecated
// `apple-mobile-web-app-capable` meta warning that every E2E missed.
//
// The browser is plain Google Chrome stable, started here and driven over
// CDP: neither Playwright's bundled Chromium (145) nor Chrome launched by
// Playwright (its default flags) logs that deprecation, Chrome started as
// the boxes start it does. ubuntu-latest ships Chrome; without it this fails.
const CHROME = process.env.CHROME_BIN ?? "google-chrome";
const BASE = "http://127.0.0.1:8910";
// Every canary carries this marker (in its text or its URL).
const CANARY = "browser-log-canary";

interface Chrome {
  proc: ChildProcess;
  dir: string;
  browser?: Browser;
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

// Synchronous, so the caller holds the handle before anything can time out.
function launchChrome(): Chrome {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "browser-log-"));
  const proc = spawn(
    CHROME,
    [
      "--headless=new",
      "--remote-debugging-port=0",
      `--user-data-dir=${dir}`,
      "--no-first-run",
      // Without a desktop session the first request otherwise waits ~25 s
      // (the D-Bus timeout) for a keyring to encrypt cookies with.
      "--password-store=basic",
      "about:blank",
    ],
    // Its own process group, so a hung Chrome dies with all its children.
    { stdio: "ignore", detached: true },
  );
  return { proc, dir };
}

async function connectChrome(chrome: Chrome): Promise<Browser> {
  const { proc, dir } = chrome;
  const spawnError = new Promise<never>((_, reject) =>
    proc.once("error", (e) => reject(new Error(`${CHROME}: ${e.message}`))),
  );
  // Chrome writes the port it picked to DevToolsActivePort.
  const portFile = path.join(dir, "DevToolsActivePort");
  const port = await Promise.race([
    spawnError,
    (async () => {
      for (let i = 0; i < 100; i++) {
        if (fs.existsSync(portFile)) {
          const first = fs.readFileSync(portFile, "utf-8").split("\n")[0];
          if (first) return first;
        }
        await sleep(100);
      }
      throw new Error(`${CHROME} wrote no DevToolsActivePort within 10 s`);
    })(),
  ]);
  chrome.browser = await chromium.connectOverCDP(`http://127.0.0.1:${port}`);
  return chrome.browser;
}

// Browser.close lets every Chrome process finish writing the profile. A
// Chrome that is not gone within 2 s (hung, or never connected: at once)
// is killed with its whole process group. The stop takes at most ~6 s
// (4 s wait + rm retries); the describe's timeout reserves room for it.
async function stopChrome(chrome: Chrome): Promise<void> {
  const { proc, dir, browser } = chrome;
  try {
    const running =
      proc.pid !== undefined && proc.exitCode === null && proc.signalCode === null;
    if (running) {
      const exited = new Promise<void>((r) => proc.once("exit", () => r()));
      const kill = setTimeout(() => {
        try {
          process.kill(-proc.pid!, "SIGKILL");
        } catch {
          // The group is already gone.
        }
      }, browser ? 2000 : 0);
      void browser
        ?.newBrowserCDPSession()
        .then((s) => s.send("Browser.close"))
        .catch(() => {});
      await Promise.race([exited, sleep(4000)]);
      clearTimeout(kill);
    }
  } finally {
    fs.rmSync(dir, {
      recursive: true,
      force: true,
      maxRetries: 10,
      retryDelay: 200,
    });
  }
}

test.describe("Browser log", () => {
  // ~10 s of test plus up to ~6 s of teardown in afterEach, which shares
  // the test's budget: 60 s keeps a slow runner from cutting the cleanup.
  test.describe.configure({ timeout: 60_000 });
  let chrome: Chrome | undefined;

  test.beforeEach(async ({ request }) => {
    await request.post(`${BASE}/api/v1/__reset`);
  });

  // Runs after a test timeout too, unlike a finally in the test body.
  test.afterEach(async () => {
    const c = chrome;
    chrome = undefined;
    if (c) await stopChrome(c);
  });

  test("the dashboard makes Chrome log no warning or error", async () => {
    chrome = launchChrome();
    const browser = await connectChrome(chrome);
    const context = browser.contexts()[0];
    await context.addInitScript(tauriMockScript);
    const page = context.pages()[0] ?? (await context.newPage());
    const entries: string[] = [];
    page.on("console", (msg) => {
      if (msg.type() === "error" || msg.type() === "warning") {
        entries.push(`[console ${msg.type()}] ${msg.location().url} ${msg.text()}`);
      }
    });
    const cdp = await context.newCDPSession(page);
    cdp.on("Log.entryAdded", ({ entry }) => {
      if (entry.level === "warning" || entry.level === "error") {
        entries.push(`[log ${entry.level} ${entry.source}] ${entry.url ?? ""} ${entry.text}`);
      }
    });
    await cdp.send("Log.enable");

    // `polls`: the operator route's ControlBar polls /status every 2 s;
    // /settings has no periodic fetch.
    for (const { route, polls } of [
      { route: "/", polls: true },
      { route: "/settings", polls: false },
    ]) {
      await page.goto(`${BASE}${route}`);
      await expect(page.locator('[data-testid="version"]')).toBeVisible();
      await page.waitForLoadState("networkidle");
      if (polls) {
        // Observation window past the first render: the next /status poll
        // and the render it drives land inside it.
        await page.waitForResponse((r) => r.url() === `${BASE}/api/v1/status`, {
          timeout: 5000,
        });
        await page.waitForLoadState("networkidle");
      }
    }

    // Canaries prove each listener path was live through the whole window:
    // a console.warn (console), a failed image load (Log error), and a page
    // with the deprecated meta (Log warning, the exact 0.29.49 message).
    await page.evaluate((marker) => {
      console.warn(marker);
      const img = new Image();
      img.src = `http://127.0.0.1:9/${marker}.png`;
    }, CANARY);
    await page.route(`${BASE}/${CANARY}`, (r) =>
      r.fulfill({
        contentType: "text/html",
        body: '<html><head><meta name="apple-mobile-web-app-capable" content="yes"></head><body></body></html>',
      }),
    );
    await page.goto(`${BASE}/${CANARY}`);
    await expect
      .poll(() => [
        entries.some((e) => e.startsWith("[console warning]") && e.includes(CANARY)),
        entries.some((e) => e.startsWith("[log error network]") && e.includes(CANARY)),
        entries.some(
          (e) =>
            e.startsWith("[log warning other]") &&
            e.includes(`${BASE}/${CANARY}`) &&
            e.includes("apple-mobile-web-app-capable"),
        ),
      ])
      .toEqual([true, true, true]);

    expect(entries.filter((e) => !e.includes(CANARY))).toEqual([]);
  });
});
