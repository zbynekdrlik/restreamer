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

async function patchJson<T>(path: string, body: unknown): Promise<T> {
  const response = await fetch(`${BASE_URL}${path}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!response.ok) {
    throw new Error(`API error: ${response.status} ${response.statusText}`);
  }
  return response.json();
}

export interface Config {
  client_uuid: string;
  manager_url: string;
  api: { bind: string; port: number };
  inpoint: { rtmp_bind: string; rtmp_port: number; chunk_duration_ms: number };
  s3: {
    bucket: string;
    region: string;
    endpoint: string;
    access_key_id: string;
    secret_access_key: string;
  };
}

export interface LogEntry {
  level: string;
  target: string;
  message: string;
}

export interface LogsResponse {
  entries: LogEntry[];
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
  getConfig: () => fetchJson<Config>("/config"),
  patchConfig: (updates: Partial<Config>) =>
    patchJson<Config>("/config", updates),
  getLogsInpoint: (limit = 100) =>
    fetchJson<LogsResponse>(`/logs/inpoint?limit=${limit}`),
  getLogsEndpoint: (limit = 100) =>
    fetchJson<LogsResponse>(`/logs/endpoint?limit=${limit}`),
};

export function createWebSocket(): WebSocket {
  return new WebSocket("ws://127.0.0.1:8910/api/v1/ws");
}
