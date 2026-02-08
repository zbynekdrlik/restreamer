import type { ServiceStatus, ChunkRecord, ChunkStats } from "../types";

const BASE_URL = "http://127.0.0.1:8910/api/v1";

async function fetchJson<T>(path: string): Promise<T> {
  const response = await fetch(`${BASE_URL}${path}`);
  if (!response.ok) {
    throw new Error(`API error: ${response.status} ${response.statusText}`);
  }
  return response.json();
}

async function postAction(path: string): Promise<void> {
  const response = await fetch(`${BASE_URL}${path}`, { method: "POST" });
  if (!response.ok) {
    throw new Error(`API error: ${response.status} ${response.statusText}`);
  }
}

async function deleteResource(path: string): Promise<void> {
  const response = await fetch(`${BASE_URL}${path}`, { method: "DELETE" });
  if (!response.ok) {
    throw new Error(`API error: ${response.status} ${response.statusText}`);
  }
}

export const api = {
  getStatus: () => fetchJson<ServiceStatus>("/status"),
  getChunks: (offset = 0, limit = 50) =>
    fetchJson<ChunkRecord[]>(`/chunks?offset=${offset}&limit=${limit}`),
  getChunkStats: () => fetchJson<ChunkStats>("/chunks/stats"),
  deleteChunks: () => deleteResource("/chunks"),
  deleteStreamingEvent: () => deleteResource("/streaming-event"),
  restartInpoint: () => postAction("/actions/restart-inpoint"),
  restartEndpoint: () => postAction("/actions/restart-endpoint"),
  toggleReceiving: () => postAction("/actions/toggle-receiving"),
  toggleDelivering: () => postAction("/actions/toggle-delivering"),
};

export function createWebSocket(): WebSocket {
  return new WebSocket("ws://127.0.0.1:8910/api/v1/ws");
}
