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
//   3. `await broadcast(request, msg)` — never a raw POST to the mock route
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

interface WsClientsReport {
  count: number;
  open: number;
  snapshot_sent?: number;
  page_load?: number;
  initial_load_done?: boolean;
}

/**
 * Wait until the mock API reports at least `min` broadcast-ready WebSocket
 * clients. The mock counts a client as ready when it is OPEN, belongs to the
 * latest page load, its connect-time snapshot was already sent, and the
 * page's initial HTTP state load finished — so neither the snapshot nor an
 * initial fetch can overwrite what the test broadcasts next. See
 * `GET /api/v1/_test/ws-clients` in mock-api.js. Call once after navigation,
 * before the first `broadcast`.
 */
export async function waitForWsClient(
  page: Page,
  request: APIRequestContext,
  { min = 1, timeout = 10_000 }: WaitForWsClientOptions = {},
): Promise<void> {
  let last: WsClientsReport = { count: 0, open: 0 };
  try {
    await expect
      .poll(
        async () => {
          if (page.isClosed()) {
            throw new Error("page closed before its WebSocket became broadcast-ready");
          }
          const res = await request.get(`${MOCK_API}/_test/ws-clients`);
          if (!res.ok()) {
            throw new Error(`GET /_test/ws-clients -> HTTP ${res.status()}`);
          }
          last = (await res.json()) as WsClientsReport;
          return last.count;
        },
        { timeout },
      )
      .toBeGreaterThanOrEqual(min);
  } catch (err) {
    // The poll's own message is built once, up front — add the LAST report
    // so a failure says which readiness condition never held.
    throw new Error(
      `waitForWsClient: page WebSocket never became broadcast-ready ` +
        `(wanted >= ${min} ready clients, last /_test/ws-clients ${JSON.stringify(last)}): ` +
        `${err instanceof Error ? err.message : String(err)}`,
    );
  }
}

async function postAndExpectDelivered(
  request: APIRequestContext,
  route: string,
  data: unknown,
  what: string,
): Promise<void> {
  const res = await request.post(`${MOCK_API}/${route}`, { data });
  expect(res.ok(), `POST /${route} -> HTTP ${res.status()}`).toBe(true);
  const body = await res.json();
  expect(
    body.delivered,
    `${what} reached no WebSocket client — call waitForWsClient() first`,
  ).toBeGreaterThanOrEqual(1);
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
  await postAndExpectDelivered(request, "_test/ws-broadcast", msg, `ws-broadcast ${msg.type}`);
}

/**
 * Have the mock broadcast `count` MetricsSample events for `alias`
 * (`POST /_test/emit-metrics-sample`). Same delivery contract as `broadcast`.
 */
export async function broadcastMetricsSamples(
  request: APIRequestContext,
  alias: string,
  count: number,
): Promise<void> {
  await postAndExpectDelivered(
    request,
    "_test/emit-metrics-sample",
    { alias, count },
    `emit-metrics-sample ${alias}`,
  );
}
