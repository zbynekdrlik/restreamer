// Mock API server for frontend E2E tests.
// Provides fake responses for all API endpoints used by the Leptos WASM frontend.
// Also serves the WASM dist/ and WebSocket for unified testing.

const express = require("express");
const cors = require("cors");
const path = require("path");

const app = express();
app.use(cors());
app.use(express.json());

// Serve the WASM frontend from dist/ for unified WebSocket + static serving
const distDir = path.join(__dirname, "..", "dist");
app.use(express.static(distDir));

// --- Mock data ---

const statusResponse = {
  inpoint: {
    state: "idle",
    details: { rtmp_connected: false },
  },
  streaming_event: {
    id: 1,
    name: "Sunday Service",
    received_bytes: 52428800,
    receiving_activated: false,
    delivering_activated: false,
    cache_delay_secs: null,
  },
  chunk_stats: {
    total_chunks: 42,
    pending_chunks: 3,
    sent_chunks: 39,
    in_process_chunks: 0,
    total_bytes: 52428800,
    buffer_duration_secs: 126.5,
  },
};

let events = [
  {
    id: 1,
    name: "Sunday Service",
    received_bytes: 52428800,
    receiving_activated: false,
    delivering_activated: false,
    cache_delay_secs: null,
  },
  {
    id: 2,
    name: "Wednesday Bible Study",
    received_bytes: 0,
    receiving_activated: false,
    delivering_activated: false,
    cache_delay_secs: 300,
  },
];

let endpoints = [
  {
    id: 1,
    alias: "YouTube Main",
    service_type: "YT_HLS",
    stream_key: "xxxx-xxxx-xxxx",
    enabled: true,
    position_last: 0,
    delivered_bytes: 1048576,
    is_fast: false,
    created_at: "2026-03-01T00:00:00Z",
    updated_at: "2026-03-09T10:00:00Z",
  },
  {
    id: 2,
    alias: "Facebook Page",
    service_type: "FB",
    stream_key: "fb-key-123",
    enabled: false,
    position_last: 0,
    delivered_bytes: 0,
    is_fast: true,
    created_at: "2026-03-05T00:00:00Z",
    updated_at: "2026-03-09T10:00:00Z",
  },
];

// Event-endpoint assignments (M2M)
let eventEndpoints = {
  1: [1], // Event 1 has YouTube Main assigned
  2: [],
};

// --- Status endpoint (Tauri invoke mock handled client-side) ---
app.get("/api/v1/status", (_req, res) => {
  res.json(statusResponse);
});

// --- Events endpoints ---
app.get("/api/v1/events", (_req, res) => {
  res.json(events);
});

app.post("/api/v1/events", (req, res) => {
  const newEvent = {
    id: events.length + 1,
    name: req.body.name || "New Event",
    received_bytes: 0,
    receiving_activated: false,
    delivering_activated: false,
    cache_delay_secs: null,
  };
  events.push(newEvent);
  eventEndpoints[newEvent.id] = [];
  res.json(newEvent);
});

app.get("/api/v1/events/:id", (req, res) => {
  const evt = events.find((e) => e.id === parseInt(req.params.id));
  if (evt) {
    res.json(evt);
  } else {
    res.status(404).json({ error: "not found" });
  }
});

app.delete("/api/v1/events/:id", (req, res) => {
  const id = parseInt(req.params.id);
  events = events.filter((e) => e.id !== id);
  delete eventEndpoints[id];
  res.status(204).send();
});

app.post("/api/v1/events/:id/activate", (req, res) => {
  const evt = events.find((e) => e.id === parseInt(req.params.id));
  if (evt) {
    evt.receiving_activated = true;
    res.json({ status: "ok" });
  } else {
    res.status(404).json({ error: "not found" });
  }
});

app.post("/api/v1/events/:id/start-delivering", (req, res) => {
  const evt = events.find((e) => e.id === parseInt(req.params.id));
  if (evt) {
    evt.delivering_activated = true;
    res.json({ status: "ok" });
  } else {
    res.status(404).json({ error: "not found" });
  }
});

app.post("/api/v1/events/:id/deactivate", (req, res) => {
  const evt = events.find((e) => e.id === parseInt(req.params.id));
  if (evt) {
    evt.receiving_activated = false;
    evt.delivering_activated = false;
    res.json({ status: "ok" });
  } else {
    res.status(404).json({ error: "not found" });
  }
});

app.post("/api/v1/events/:id/start-stream", (req, res) => {
  const id = parseInt(req.params.id);
  const evt = events.find((e) => e.id === id);
  if (!evt) {
    return res.status(404).json({ error: "not found" });
  }
  // Check for conflict - another active event
  const conflict = events.find(
    (e) => e.id !== id && (e.receiving_activated || e.delivering_activated),
  );
  if (conflict) {
    return res.status(409).json({ error: "another event is active" });
  }
  evt.receiving_activated = true;
  evt.delivering_activated = true;

  // Broadcast activity feed and pipeline state via WebSocket
  broadcastWs({
    type: "ActivityFeed",
    data: {
      timestamp: new Date().toISOString(),
      severity: "info",
      message: `Stream started: ${evt.name}`,
      source: "system",
    },
  });
  broadcastWs({
    type: "PipelineState",
    data: {
      state: "buffering",
      event_id: id,
      event_name: evt.name,
      buffer_progress: 0.0,
      target_delay_secs: 120,
      current_delay_secs: 0.0,
      session_start: null,
      predicted: false,
    },
  });

  res.json({ status: "ok" });
});

app.post("/api/v1/events/:id/stop-stream", (req, res) => {
  const evt = events.find((e) => e.id === parseInt(req.params.id));
  if (evt) {
    evt.receiving_activated = false;
    evt.delivering_activated = false;

    broadcastWs({
      type: "ActivityFeed",
      data: {
        timestamp: new Date().toISOString(),
        severity: "info",
        message: `Stream stopped: ${evt.name}`,
        source: "system",
      },
    });
    broadcastWs({
      type: "PipelineState",
      data: {
        state: "idle",
        event_id: null,
        event_name: null,
        buffer_progress: 0.0,
        target_delay_secs: 0,
        current_delay_secs: 0.0,
        session_start: null,
        predicted: false,
      },
    });

    res.json({ status: "ok" });
  } else {
    res.status(404).json({ error: "not found" });
  }
});

app.patch("/api/v1/events/:id", (req, res) => {
  const evt = events.find((e) => e.id === parseInt(req.params.id));
  if (!evt) {
    return res.status(404).json({ error: "not found" });
  }
  if (req.body.name) evt.name = req.body.name;
  if (req.body.cache_delay_secs !== undefined)
    evt.cache_delay_secs = req.body.cache_delay_secs;
  res.json({ status: "ok" });
});

// --- Event-Endpoint M2M ---
app.get("/api/v1/events/:id/endpoints", (req, res) => {
  const id = parseInt(req.params.id);
  const epIds = eventEndpoints[id] || [];
  const eps = endpoints.filter((e) => epIds.includes(e.id));
  res.json(eps);
});

app.post("/api/v1/events/:eventId/endpoints/:endpointId", (req, res) => {
  const eventId = parseInt(req.params.eventId);
  const endpointId = parseInt(req.params.endpointId);
  if (!eventEndpoints[eventId]) {
    eventEndpoints[eventId] = [];
  }
  if (!eventEndpoints[eventId].includes(endpointId)) {
    eventEndpoints[eventId].push(endpointId);
  }
  res.status(201).json({ status: "ok" });
});

app.delete("/api/v1/events/:eventId/endpoints/:endpointId", (req, res) => {
  const eventId = parseInt(req.params.eventId);
  const endpointId = parseInt(req.params.endpointId);
  if (eventEndpoints[eventId]) {
    eventEndpoints[eventId] = eventEndpoints[eventId].filter(
      (id) => id !== endpointId,
    );
  }
  res.status(204).send();
});

// --- Endpoints API ---
app.get("/api/v1/endpoints", (_req, res) => {
  res.json(endpoints);
});

app.post("/api/v1/endpoints", (req, res) => {
  const newEp = {
    id: endpoints.length + 1,
    alias: req.body.alias || "New Endpoint",
    service_type: req.body.service_type || "YT_HLS",
    stream_key: req.body.stream_key || "",
    enabled: true,
    position_last: 0,
    delivered_bytes: 0,
    is_fast: false,
    created_at: new Date().toISOString(),
    updated_at: new Date().toISOString(),
  };
  endpoints.push(newEp);
  res.json(newEp);
});

app.put("/api/v1/endpoints/:id", (req, res) => {
  const ep = endpoints.find((e) => e.id === parseInt(req.params.id));
  if (!ep) {
    return res.status(404).json({ error: "not found" });
  }
  if (req.body.alias !== undefined) ep.alias = req.body.alias;
  if (req.body.service_type !== undefined)
    ep.service_type = req.body.service_type;
  if (req.body.stream_key !== undefined) ep.stream_key = req.body.stream_key;
  if (req.body.enabled !== undefined) ep.enabled = req.body.enabled;
  if (req.body.is_fast !== undefined) ep.is_fast = req.body.is_fast;
  res.json({ status: "ok" });
});

app.delete("/api/v1/endpoints/:id", (req, res) => {
  endpoints = endpoints.filter((e) => e.id !== parseInt(req.params.id));
  res.json({ status: "ok" });
});

// --- Chunks endpoints (used by dashboard/chunk_list) ---
app.get("/api/v1/chunks/stats", (_req, res) => {
  res.json(statusResponse.chunk_stats);
});

// --- Cached delivery status (for instant initial load) ---
let cachedDelivery = {
  instance_name: "",
  status: "none",
  server_ip: null,
  endpoint_count: 0,
  endpoints: [],
};

app.get("/api/v1/delivery/status/cached", (_req, res) => {
  res.json(cachedDelivery);
});

// --- Logs endpoint ---
app.get("/api/v1/logs", (_req, res) => {
  res.json([
    {
      level: "INFO",
      target: "rs_inpoint",
      message: "RTMP server started on port 1234",
    },
    {
      level: "WARN",
      target: "rs_endpoint",
      message: "S3 upload retrying after timeout",
    },
    {
      level: "ERROR",
      target: "rs_runtime",
      message: "Connection lost",
    },
    {
      level: "INFO",
      target: "rs_inpoint",
      message: "Stream connected: live/test",
    },
    {
      level: "INFO",
      target: "rs_endpoint",
      message: "Chunk uploaded: chunk_042.ts",
    },
  ]);
});

// Test-only: broadcast arbitrary WebSocket events for E2E pipeline state tests
app.post("/api/v1/_test/ws-broadcast", (req, res) => {
  broadcastWs(req.body);
  res.json({ status: "ok" });
});

// SPA fallback: serve index.html for any non-API route that wasn't matched by static
app.get("*", (req, res) => {
  res.sendFile(path.join(distDir, "index.html"));
});

// --- WebSocket endpoint (broadcasts delivery status) ---
const { WebSocketServer } = require("ws");

const PORT = 8910;
const server = app.listen(PORT, () => {
  console.log(`Mock API server running on http://127.0.0.1:${PORT}`);
});

const wss = new WebSocketServer({ server, path: "/api/v1/ws" });

// Broadcast a message to all connected WebSocket clients
function broadcastWs(message) {
  const data = JSON.stringify(message);
  wss.clients.forEach((client) => {
    if (client.readyState === 1) {
      client.send(data);
    }
  });
}

wss.on("connection", (ws) => {
  console.log("[ws] Client connected");

  // Immediately send a delivery status event for E2E testing
  const deliveryEvent = {
    type: "DeliveryStatus",
    data: {
      instance_name: "rs-delivery-evt1",
      status: "running",
      server_ip: "1.2.3.4",
      endpoint_count: 2,
      endpoints: [
        {
          alias: "YouTube Main",
          alive: true,
          current_chunk_id: 142,
          bytes_processed_total: 1073741824,
          chunks_processed: 1847,
          chunk_delay_secs: 3.2,
          stall_reason: null,
          ffmpeg_restart_count: 0,
          last_error: null,
        },
        {
          alias: "Facebook Page",
          alive: true,
          current_chunk_id: 140,
          bytes_processed_total: 943718400,
          chunks_processed: 1620,
          chunk_delay_secs: 45.0,
          stall_reason: "chunk_gap",
          ffmpeg_restart_count: 3,
          last_error: "S3 fetch timeout",
        },
      ],
    },
  };

  // Update cache and send after a brief delay
  cachedDelivery = deliveryEvent.data;
  setTimeout(() => {
    if (ws.readyState === ws.OPEN) {
      ws.send(JSON.stringify(deliveryEvent));
    }
  }, 200);

  ws.on("close", () => {
    console.log("[ws] Client disconnected");
  });
});
