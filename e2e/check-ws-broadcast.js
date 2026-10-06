#!/usr/bin/env node
// #377 static guard: every frontend E2E WebSocket push must go through
// e2e/lib/ws.ts AFTER waitForWsClient().
//
// The mock API drops a WebSocket push (`_test/ws-broadcast`,
// `_test/emit-metrics-sample`) that arrives before the page's socket is
// broadcast-ready, so a spec that pushes straight after a DOM wait flakes
// ~1/160 (main run 37520022262). This check fails the build when an e2e
// TypeScript file:
//   (a) names a mock push route itself instead of calling the lib/ws helper;
//   (b) calls a push helper before waitForWsClient() — counted since the last
//       page.goto()/page.reload() of the same test (a reload opens a NEW
//       socket), and reset at every test/hook start and every statement at or
//       outside the test's own indent, so a wait never leaks into a later
//       helper (top-level or inside a describe);
//   (c) calls a push helper without `await` (a floating push races the test);
//   (d) imports a lib/ws helper under an alias (the scan matches by name).
// Comments and string contents never count as calls; a route string inside a
// comment is fine.
//
// Usage:
//   node check-ws-broadcast.js              scan e2e/**/*.ts (exit 1 on violation)
//   node check-ws-broadcast.js --self-test  prove the guard catches known-bad shapes
"use strict";

const fs = require("fs");
const path = require("path");

const RAW_ROUTES = ["_test/ws-broadcast", "_test/emit-metrics-sample"];
const PUSH_HELPERS = ["broadcast", "broadcastMetricsSamples"];
const WAIT_HELPER = "waitForWsClient";
const HELPER_FILE = path.join("lib", "ws.ts");
const SKIP_DIRS = new Set(["node_modules", "playwright-report", "test-results"]);

const TEST_START_RE = /^\s*test(?:\.(?:only|skip|fixme|beforeEach|afterEach|beforeAll|afterAll))?\(/;
// Ordered token scan inside a line: navigation resets, the wait arms, a push consumes.
const TOKEN_RE = new RegExp(
  `\\bpage\\.(?:goto|reload|goBack|goForward)\\(|\\b${WAIT_HELPER}\\(|\\b(?:${PUSH_HELPERS.join("|")})\\(`,
  "g",
);
const ALIAS_RE = new RegExp(
  `import\\s*(?:type\\s*)?\\{[^}]*\\b(?:${[WAIT_HELPER, ...PUSH_HELPERS].join("|")})\\s+as\\b`,
);

/**
 * Split a source into per-line views with comments removed:
 *   code — string contents KEPT (to find route literals),
 *   bare — string contents blanked (to find calls),
 *   lead — the line starts outside any comment/string (column-0 detection).
 * Tracks block comments and template literals across lines.
 */
function lexLines(source) {
  const out = [];
  let inBlock = false;
  let quote = null; // '"', "'", or '`' while inside a string
  for (const line of source.split("\n")) {
    const lead = !inBlock && quote === null;
    let code = "";
    let bare = "";
    for (let i = 0; i < line.length; i++) {
      const c = line[i];
      const n = line[i + 1];
      if (inBlock) {
        if (c === "*" && n === "/") {
          inBlock = false;
          i++;
        }
        continue;
      }
      if (quote !== null) {
        code += c;
        bare += " ";
        if (c === "\\") {
          code += n === undefined ? "" : n;
          bare += n === undefined ? "" : " ";
          i++;
        } else if (c === quote) {
          quote = null;
          bare = bare.slice(0, -1) + c;
        }
        continue;
      }
      if (c === "/" && n === "/") break;
      if (c === "/" && n === "*") {
        inBlock = true;
        i++;
        continue;
      }
      if (c === '"' || c === "'" || c === "`") quote = c;
      code += c;
      bare += c;
    }
    // A plain string cannot span lines; only a template literal can.
    if (quote === '"' || quote === "'") quote = null;
    out.push({ code, bare, lead });
  }
  return out;
}

/**
 * Return violations for one source file: [{line, reason}] (1-based lines).
 */
function findViolations(source) {
  const violations = [];
  const lines = source.split("\n");
  const lexed = lexLines(source);
  if (ALIAS_RE.test(lexed.map((l) => l.code).join("\n"))) {
    violations.push({ line: 1, reason: `a lib/ws helper is imported under an alias -- import it by its own name` });
  }
  let waited = false;
  let testIndent = 0; // indent of the most recent test/hook start
  lexed.forEach(({ code, bare, lead }, idx) => {
    const lineNo = idx + 1;
    const raw = lines[idx];
    const indent = raw.length - raw.trimStart().length;
    if (TEST_START_RE.test(raw)) {
      waited = false;
      testIndent = indent;
    } else if (lead && bare.trim() !== "" && indent <= testIndent) {
      // A statement at (or outside) the test's own level -- its closing `});`,
      // a sibling helper inside a describe, a top-level helper: nothing from
      // the previous test carries over.
      waited = false;
    }
    for (const route of RAW_ROUTES) {
      if (code.includes(route)) {
        violations.push({
          line: lineNo,
          reason: `raw use of ${route} -- call the e2e/lib/ws.ts helper after ${WAIT_HELPER}()`,
        });
      }
    }
    for (const m of bare.matchAll(TOKEN_RE)) {
      const tok = m[0];
      if (tok.startsWith("page.")) {
        waited = false;
        continue;
      }
      if (tok === `${WAIT_HELPER}(`) {
        waited = true;
        continue;
      }
      if (!/\bawait\s+$/.test(bare.slice(0, m.index))) {
        violations.push({ line: lineNo, reason: `${tok}) must be awaited` });
      }
      if (!waited) {
        violations.push({
          line: lineNo,
          reason: `${tok}) before ${WAIT_HELPER}() since the last navigation in this test`,
        });
      }
    }
  });
  return violations;
}

function listTsFiles(dir, rel = "") {
  const files = [];
  for (const ent of fs.readdirSync(path.join(dir, rel), { withFileTypes: true })) {
    const r = path.join(rel, ent.name);
    if (ent.isDirectory()) {
      if (!SKIP_DIRS.has(ent.name)) files.push(...listTsFiles(dir, r));
    } else if (ent.name.endsWith(".ts") && r !== HELPER_FILE) {
      files.push(r);
    }
  }
  return files.sort();
}

function scan(dir) {
  const files = listTsFiles(dir);
  let total = 0;
  let pushes = 0;
  const pushRe = new RegExp(`\\bawait (?:${PUSH_HELPERS.join("|")})\\(`, "g");
  for (const f of files) {
    const src = fs.readFileSync(path.join(dir, f), "utf8");
    pushes += (src.match(pushRe) || []).length;
    for (const v of findViolations(src)) {
      console.error(`ERROR: ${f}:${v.line}: ${v.reason}`);
      total += 1;
    }
  }
  if (total > 0) {
    console.error(`ws-broadcast guard (#377): ${total} violation(s) in ${files.length} file(s).`);
    return 1;
  }
  console.log(
    `OK: ws-broadcast guard (#377): ${files.length} e2e .ts files, ${pushes} WebSocket pushes, all awaited after ${WAIT_HELPER}().`,
  );
  return 0;
}

const T = (...lines) => lines.join("\n");
const OPEN = 'test("x", async ({ page, request }) => {';
const SELF_TEST_CASES = [
  {
    name: "raw ws-broadcast POST",
    want: 1,
    src: T(OPEN, '  await page.goto("/");', '  await request.post("http://127.0.0.1:8910/api/v1/_test/ws-broadcast", { data: {} });', "});"),
  },
  {
    name: "raw emit-metrics-sample POST",
    want: 1,
    src: T(OPEN, '  await page.goto("/");', "  await waitForWsClient(page, request);", '  await request.post("/api/v1/_test/emit-metrics-sample", { data: {} });', "});"),
  },
  {
    name: "broadcast without wait",
    want: 1,
    src: T(OPEN, '  await page.goto("/");', '  await broadcast(request, { type: "X" });', "});"),
  },
  {
    name: "metrics push without wait",
    want: 1,
    src: T(OPEN, '  await page.goto("/");', '  await broadcastMetricsSamples(request, "yt1", 5);', "});"),
  },
  {
    name: "wait in a previous test does not carry over",
    want: 1,
    src: T(OPEN, '  await page.goto("/");', "  await waitForWsClient(page, request);", '  await broadcast(request, { type: "X" });', "});",
      OPEN, '  await page.goto("/");', '  await broadcast(request, { type: "X" });', "});"),
  },
  {
    name: "wait does not leak into a top-level helper",
    want: 1,
    src: T(OPEN, '  await page.goto("/");', "  await waitForWsClient(page, request);", "});",
      "async function sendIt(request) {", '  await broadcast(request, { type: "X" });', "}"),
  },
  {
    name: "wait does not leak into a helper nested in a describe",
    want: 1,
    src: T('test.describe("d", () => {', "  " + OPEN, '    await page.goto("/");', "    await waitForWsClient(page, request);", "  });",
      "  async function sendIt(request) {", '    await broadcast(request, { type: "X" });', "  }", "});"),
  },
  {
    name: "reload after wait needs a new wait",
    want: 1,
    src: T(OPEN, '  await page.goto("/");', "  await waitForWsClient(page, request);", "  await page.reload();", '  await broadcast(request, { type: "X" });', "});"),
  },
  {
    name: "wait in a trailing comment does not count",
    want: 1,
    src: T(OPEN, '  await page.goto("/"); // then waitForWsClient(page, request);', '  await broadcast(request, { type: "X" });', "});"),
  },
  {
    name: "wait inside a block comment does not count",
    want: 1,
    src: T(OPEN, '  await page.goto("/");', "  /*", "  await waitForWsClient(page, request);", "  */", '  await broadcast(request, { type: "X" });', "});"),
  },
  {
    name: "wait inside a string does not count",
    want: 1,
    src: T(OPEN, '  await page.goto("/");', '  console.log("await waitForWsClient(page, request)");', '  await broadcast(request, { type: "X" });', "});"),
  },
  {
    name: "un-awaited broadcast",
    want: 1,
    src: T(OPEN, '  await page.goto("/");', "  await waitForWsClient(page, request);", '  void broadcast(request, { type: "X" });', "});"),
  },
  {
    name: "aliased import",
    want: 1,
    src: T('import { broadcast as send, waitForWsClient } from "./lib/ws";', OPEN, '  await page.goto("/");', "  await waitForWsClient(page, request);", '  await send(request, { type: "X" });', "});"),
  },
  {
    name: "correct usage (loop, comments, URL strings)",
    want: 0,
    src: T('import { broadcast, waitForWsClient } from "./lib/ws";', "", "test.describe(\"d\", () => {", "  " + OPEN,
      '    await request.post("http://127.0.0.1:8910/api/v1/__reset"); // a // in a string is not a comment',
      '    await page.goto("/");', "    // a comment naming _test/ws-broadcast is fine", "    /* so is _test/emit-metrics-sample */",
      "    await waitForWsClient(page, request);", "    for (const t of [1, 2]) {", '      await broadcast(request, { type: `X${t}` });', "    }", "  });", "});"),
  },
];

function selfTest() {
  let failed = 0;
  for (const c of SELF_TEST_CASES) {
    const got = findViolations(c.src);
    const ok = got.length === c.want;
    console.log(`${ok ? "PASS" : "FAIL"}: self-test "${c.name}": ${got.length} violation(s), want ${c.want}`);
    if (!ok) {
      for (const v of got) console.log(`    line ${v.line}: ${v.reason}`);
      failed += 1;
    }
  }
  return failed === 0 ? 0 : 1;
}

if (require.main === module) {
  const code = process.argv.includes("--self-test") ? selfTest() : scan(__dirname);
  process.exit(code);
}

module.exports = { findViolations };
