// #377 — race-free WebSocket broadcasts for the frontend E2E specs.
//
// The mock API (`e2e/mock-api.js`) fans a `POST /api/v1/_test/ws-broadcast`
// out ONLY to the WebSocket clients connected at that instant. The Leptos
// client opens its socket asynchronously after first render, so "the DOM is
// visible" does NOT imply "the page is subscribed": a broadcast that wins
// that race is silently lost and the assertion times out (main run
// 37520022262: expected "RTMP Only", got "Disconnected").
//
// Contract for every spec:
//   1. navigate (page.goto / page.reload)
//   2. `await waitForWsClient(page, request)` — once, before the first broadcast
//   3. `await broadcast(request, msg)` — never a raw POST to the route
// `e2e/check-ws-broadcast.js` (run in the frontend-e2e CI job) fails the build
// when a spec breaks this contract.
import { expect, type APIRequestContext, type Page } from "@playwright/test";

export const MOCK_API = "http://127.0.0.1:8910/api/v1";

export interface WaitForWsClientOptions {
  /** Minimum number of ready clients (default 1). */
  min?: number;
  /** Total time to wait, in ms (default 10000). */
  timeout?: number;
}

/**
 * Wait until the mock API has at least `min` WebSocket clients that can
 * safely receive a broadcast: the socket is OPEN AND its connect-time
 * snapshot was already sent (so the snapshot cannot overwrite the test's
 * message). Call once after navigation, before the first `broadcast`.
 */
export async function waitForWsClient(
  page: Page,
  request: APIRequestContext,
  { min = 1, timeout = 10_000 }: WaitForWsClientOptions = {},
): Promise<void> {
  let last = { count: 0, open: 0 };
  await expect
    .poll(
      async () => {
        if (page.isClosed()) {
          throw new Error("waitForWsClient: page closed before its WebSocket connected");
        }
        const res = await request.get(`${MOCK_API}/_test/ws-clients`);
        expect(res.ok(), `GET /_test/ws-clients -> HTTP ${res.status()}`).toBe(true);
        last = await res.json();
        return last.count;
      },
      {
        message: `page WebSocket never became broadcast-ready (wanted >= ${min} ready clients, last ${JSON.stringify(last)})`,
        timeout,
      },
    )
    .toBeGreaterThanOrEqual(min);
}

/**
 * Broadcast one WebSocket event through the mock API. Fails immediately if
 * the mock delivered it to zero clients (a lost message), instead of letting
 * the caller's UI assertion time out with a misleading value.
 */
export async function broadcast(
  request: APIRequestContext,
  msg: { type: string; [field: string]: unknown },
): Promise<void> {
  const res = await request.post(`${MOCK_API}/_test/ws-broadcast`, { data: msg });
  expect(res.ok(), `POST /_test/ws-broadcast -> HTTP ${res.status()}`).toBe(true);
  const body = await res.json();
  expect(
    body.delivered,
    `ws-broadcast ${msg.type} reached no WebSocket client — call waitForWsClient() first`,
  ).toBeGreaterThanOrEqual(1);
}
