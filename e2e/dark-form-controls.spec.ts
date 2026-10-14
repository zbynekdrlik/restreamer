import { test, expect } from "@playwright/test";
import * as fs from "fs";
import * as path from "path";

const tauriMockScript = fs.readFileSync(
  path.join(__dirname, "tauri-mock.js"),
  "utf-8",
);

// stream-pp (Linux, WebKitGTK operator window, 2026-10-14): every <select> showed a WHITE
// box with the theme's light text, so the operator could not read the selected event.
// WebKitGTK draws a native-appearance control with the system (light) GTK theme and
// ignores our dark background, while the text colour still comes from our CSS. So the
// controls must opt out of the native look (`appearance: none`), the page must declare
// `color-scheme: dark` (spinners, popups, scrollbars), and every <option> carries the
// dark background explicitly (the popup list does not inherit the select's).
function luminance(rgb: string): number {
  const m = rgb.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)(?:,\s*([\d.]+))?\)/);
  if (!m) throw new Error(`unparsed colour ${rgb}`);
  if (m[4] !== undefined && parseFloat(m[4]) === 0) return NaN; // transparent
  return (0.2126 * +m[1] + 0.7152 * +m[2] + 0.0722 * +m[3]) / 255;
}

test.describe("Form controls stay dark on every platform", () => {
  test.beforeEach(async ({ page, request }) => {
    await page.addInitScript(tauriMockScript);
    await request.post("http://127.0.0.1:8910/api/v1/__reset");
  });

  test("selects and their options: no native look, dark background, light text", async ({
    page,
  }) => {
    const consoleMsgs: string[] = [];
    page.on("console", (m) => {
      if (m.type() === "error" || m.type() === "warning") consoleMsgs.push(m.text());
    });

    let checked = 0;
    for (const route of ["/", "/settings"]) {
      await page.goto(route);
      await expect(page.locator("select").first()).toBeVisible();

      const scheme = await page.evaluate(
        () => getComputedStyle(document.documentElement).colorScheme,
      );
      expect(scheme, `${route}: :root color-scheme`).toContain("dark");

      const controls = await page.locator("select").evaluateAll((els) =>
        els.map((el) => {
          const s = getComputedStyle(el);
          return {
            label: (el as HTMLSelectElement).className || el.outerHTML.slice(0, 60),
            appearance: s.appearance,
            bg: s.backgroundColor,
            fg: s.color,
            options: Array.from((el as HTMLSelectElement).options).map((o) => {
              const os = getComputedStyle(o);
              return { text: o.text, bg: os.backgroundColor, fg: os.color };
            }),
          };
        }),
      );
      for (const c of controls) {
        checked++;
        expect(c.appearance, `${route} select ${c.label}: appearance`).toBe("none");
        expect(luminance(c.bg), `${route} select ${c.label}: background ${c.bg}`).toBeLessThan(0.3);
        expect(luminance(c.fg), `${route} select ${c.label}: text ${c.fg}`).toBeGreaterThan(0.6);
        for (const o of c.options) {
          expect(luminance(o.bg), `${route} option "${o.text}": background ${o.bg}`).toBeLessThan(0.3);
          expect(luminance(o.fg), `${route} option "${o.text}": text ${o.fg}`).toBeGreaterThan(0.6);
        }
      }
    }
    expect(checked, "selects found on / and /settings").toBeGreaterThan(1);
    expect(consoleMsgs).toEqual([]);
  });
});
