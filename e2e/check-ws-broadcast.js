#!/usr/bin/env node
// #377 static guard: every frontend E2E WebSocket broadcast must go through
// e2e/lib/ws.ts AFTER waitForWsClient().
//
// The mock API drops a `_test/ws-broadcast` that arrives before the page's
// WebSocket is connected (and its connect-time snapshot sent), so a spec that
// broadcasts straight after a DOM wait flakes ~1/160 (main run 37520022262).
// This check fails the build when a spec:
//   (a) POSTs `_test/ws-broadcast` itself instead of calling broadcast(), or
//   (b) calls broadcast() in a test before waitForWsClient() — counted since
//       the test's last page.goto()/page.reload() (a reload opens a NEW socket).
//
// Usage:
//   node check-ws-broadcast.js              scan e2e/*.spec.ts (exit 1 on violation)
//   node check-ws-broadcast.js --self-test  prove the guard catches known-bad shapes
"use strict";

const fs = require("fs");
const path = require("path");

const RAW_ROUTE = "_test/ws-broadcast";
const TEST_START_RE = /^\s*test(?:\.(?:only|skip|fixme|beforeEach|afterEach|beforeAll|afterAll))?\(/;
// Ordered token scan inside a line: navigation resets, wait arms, broadcast consumes.
const TOKEN_RE = /\bpage\.(?:goto|reload|goBack|goForward)\(|\bwaitForWsClient\(|\bbroadcast\(/g;

function isCommentLine(line) {
  const t = line.trim();
  return t.startsWith("//") || t.startsWith("/*") || t.startsWith("*");
}

/**
 * Return violations for one spec source: [{line, reason}] (1-based lines).
 */
function findViolations(source) {
  const violations = [];
  const lines = source.split("\n");
  let waited = false;
  lines.forEach((line, idx) => {
    const lineNo = idx + 1;
    if (TEST_START_RE.test(line)) waited = false;
    if (isCommentLine(line)) return;
    if (line.includes(RAW_ROUTE)) {
      violations.push({
        line: lineNo,
        reason: `raw POST to ${RAW_ROUTE} -- use broadcast() from ./lib/ws after waitForWsClient()`,
      });
    }
    for (const m of line.matchAll(TOKEN_RE)) {
      const tok = m[0];
      if (tok.startsWith("page.")) {
        waited = false;
      } else if (tok === "waitForWsClient(") {
        waited = true;
      } else if (!waited) {
        violations.push({
          line: lineNo,
          reason: "broadcast() before waitForWsClient() since the last navigation in this test",
        });
      }
    }
  });
  return violations;
}

function scan(dir) {
  const specs = fs
    .readdirSync(dir)
    .filter((f) => f.endsWith(".spec.ts"))
    .sort();
  let total = 0;
  let broadcasts = 0;
  for (const f of specs) {
    const src = fs.readFileSync(path.join(dir, f), "utf8");
    broadcasts += (src.match(/\bawait broadcast\(/g) || []).length;
    for (const v of findViolations(src)) {
      console.error(`ERROR: ${f}:${v.line}: ${v.reason}`);
      total += 1;
    }
  }
  if (total > 0) {
    console.error(`ws-broadcast guard (#377): ${total} violation(s) in ${specs.length} spec file(s).`);
    return 1;
  }
  console.log(`OK: ws-broadcast guard (#377): ${specs.length} spec files, ${broadcasts} broadcast() calls, all after waitForWsClient().`);
  return 0;
}

function selfTest() {
  const cases = [
    {
      name: "raw POST",
      want: 1,
      src: [
        'test("x", async ({ page, request }) => {',
        '  await page.goto("/");',
        '  await request.post("http://127.0.0.1:8910/api/v1/_test/ws-broadcast", { data: {} });',
        "});",
      ].join("\n"),
    },
    {
      name: "broadcast without wait",
      want: 1,
      src: [
        'test("x", async ({ page, request }) => {',
        '  await page.goto("/");',
        '  await broadcast(request, { type: "X" });',
        "});",
      ].join("\n"),
    },
    {
      name: "wait in a previous test does not carry over",
      want: 1,
      src: [
        'test("a", async ({ page, request }) => {',
        '  await page.goto("/");',
        "  await waitForWsClient(page, request);",
        '  await broadcast(request, { type: "X" });',
        "});",
        'test("b", async ({ page, request }) => {',
        '  await page.goto("/");',
        '  await broadcast(request, { type: "X" });',
        "});",
      ].join("\n"),
    },
    {
      name: "reload after wait needs a new wait",
      want: 1,
      src: [
        'test("x", async ({ page, request }) => {',
        '  await page.goto("/");',
        "  await waitForWsClient(page, request);",
        "  await page.reload();",
        '  await broadcast(request, { type: "X" });',
        "});",
      ].join("\n"),
    },
    {
      name: "correct usage",
      want: 0,
      src: [
        'test("x", async ({ page, request }) => {',
        '  await page.goto("/");',
        "  // a comment naming _test/ws-broadcast is fine",
        "  await waitForWsClient(page, request);",
        "  for (const t of [1, 2]) {",
        '    await broadcast(request, { type: "X" });',
        "  }",
        "});",
      ].join("\n"),
    },
  ];
  let failed = 0;
  for (const c of cases) {
    const got = findViolations(c.src).length;
    const ok = got === c.want;
    console.log(`${ok ? "PASS" : "FAIL"}: self-test "${c.name}": ${got} violation(s), want ${c.want}`);
    if (!ok) failed += 1;
  }
  return failed === 0 ? 0 : 1;
}

if (require.main === module) {
  const code = process.argv.includes("--self-test") ? selfTest() : scan(__dirname);
  process.exit(code);
}

module.exports = { findViolations };
