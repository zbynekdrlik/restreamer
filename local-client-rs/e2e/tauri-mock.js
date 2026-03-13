// Tauri invoke mock for browser injection.
// Stubs window.__TAURI__.core.invoke so the Leptos frontend works outside Tauri.
// The frontend uses Tauri invoke for: get_status, get_chunk_stats, get_streaming_event, get_logs

const MOCK_API = "http://127.0.0.1:8910";

const mockResponses = {
  get_status: async () => {
    const [statusResp, chunkResp] = await Promise.all([
      fetch(`${MOCK_API}/api/v1/status`),
      fetch(`${MOCK_API}/api/v1/chunks/stats`),
    ]);
    const status = await statusResp.json();
    const chunk_stats = await chunkResp.json();
    const inpoint_connected = status.inpoint?.details?.rtmp_connected || false;
    const data = {
      streaming_event: status.streaming_event || null,
      chunk_stats,
      inpoint_connected,
    };
    return { success: true, data, error: null };
  },
  get_chunk_stats: async () => {
    const resp = await fetch(`${MOCK_API}/api/v1/chunks/stats`);
    const data = await resp.json();
    return { success: true, data, error: null };
  },
  get_streaming_event: async () => {
    const resp = await fetch(`${MOCK_API}/api/v1/status`);
    const data = await resp.json();
    return {
      success: true,
      data: data.streaming_event || null,
      error: null,
    };
  },
  get_logs: async (_args) => {
    const resp = await fetch(`${MOCK_API}/api/v1/logs`);
    const data = await resp.json();
    return { success: true, data, error: null };
  },
};

window.__TAURI__ = {
  core: {
    invoke: async (cmd, args) => {
      const handler = mockResponses[cmd];
      if (handler) {
        return await handler(args);
      }
      console.warn(`[tauri-mock] Unknown command: ${cmd}`);
      return { success: false, data: null, error: `Unknown command: ${cmd}` };
    },
  },
};
