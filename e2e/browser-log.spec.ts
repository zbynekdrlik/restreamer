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

interface Chrome {
  proc: ChildProcess;
  dir: string;
  browser?: Browser;
}

// Browser.close lets every Chrome process finish writing the profile; a
// Chrome that does not exit within 5 s (hung, or never connected) is killed.
async function stopChrome(chrome: Chrome): Promise<void> {
  const { proc, dir, browser } = chrome;
  try {
    if (proc.exitCode === null && proc.signalCode === null) {
      const exited = new Promise<void>((r) => proc.once("exit", () => r()));
      const kill = setTimeout(() => proc.kill("SIGKILL"), browser ? 5000 : 0);
      await browser
        ?.newBrowserCDPSession()
        .then((s) => s.send("Browser.close"))
        .catch(() => {});
      await exited;
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

async function startChrome(): Promise<Chrome> {
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
    { stdio: "ignore" },
  );
  const chrome: Chrome = { proc, dir };
  try {
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
          await new Promise((r) => setTimeout(r, 100));
        }
        throw new Error(`${CHROME} wrote no DevToolsActivePort within 10 s`);
      })(),
    ]);
    chrome.browser = await chromium.connectOverCDP(`http://127.0.0.1:${port}`);
    return chrome;
  } catch (e) {
    await stopChrome(chrome);
    throw e;
  }
}

test.describe("Browser log", () => {
  let chrome: Chrome | undefined;

  test.beforeEach(async ({ request }) => {
    await request.post("http://127.0.0.1:8910/api/v1/__reset");
  });

  // Runs after a test timeout too, unlike a finally in the test body.
  test.afterEach(async () => {
    if (chrome) await stopChrome(chrome);
    chrome = undefined;
  });

  test("the dashboard makes Chrome log no warning or error", async () => {
    chrome = await startChrome();
    const context = chrome.browser!.contexts()[0];
    await context.addInitScript(tauriMockScript);
    const page = context.pages()[0] ?? (await context.newPage());
    const entries: string[] = [];
    page.on("console", (msg) => {
      if (msg.type() === "error" || msg.type() === "warning") {
        entries.push(`[console ${msg.type()}] ${msg.text()}`);
      }
    });
    const cdp = await context.newCDPSession(page);
    cdp.on("Log.entryAdded", ({ entry }) => {
      if (entry.level === "warning" || entry.level === "error") {
        entries.push(`[log ${entry.level} ${entry.source}] ${entry.url ?? ""} ${entry.text}`);
      }
    });
    await cdp.send("Log.enable");

    for (const route of ["/", "/settings"]) {
      await page.goto(`http://127.0.0.1:8910${route}`);
      await expect(page.locator('[data-testid="version"]')).toBeVisible();
      // Warnings after the first render (late fetches, the WS) count too.
      await page.waitForLoadState("networkidle");
    }
    const found = [...entries];

    // Canaries: both listeners are still live at the end of the window. A
    // console.warn reaches `console`; an image from a closed port makes
    // Chrome log a failed load in the Log domain.
    await page.evaluate(() => {
      console.warn("browser-log canary");
      const img = new Image();
      img.src = "http://127.0.0.1:9/browser-log-canary.png";
    });
    await expect
      .poll(() => [
        entries.some((e) => e.startsWith("[console warning] browser-log canary")),
        entries.some((e) => e.startsWith("[log error network]") && e.includes("browser-log-canary")),
      ])
      .toEqual([true, true]);

    expect(found).toEqual([]);
  });
});
