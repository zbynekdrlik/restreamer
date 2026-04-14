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
    rescue_video_url: null,
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
    created_from: null,
    rescue_video_url: null,
  },
  {
    id: 2,
    name: "Wednesday Bible Study",
    received_bytes: 0,
    receiving_activated: false,
    delivering_activated: false,
    cache_delay_secs: 300,
    created_from: null,
    rescue_video_url: null,
  },
];

let templates = [
  {
    id: 1,
    name: "sunday-service",
    cache_delay_secs: 120,
  },
  {
    id: 2,
    name: "wednesday-study",
    cache_delay_secs: null,
  },
];

let templateEndpoints = {
  1: [1], // sunday-service has YouTube Main
  2: [],
};

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

// Store initial data snapshots for test reset
const initialEvents = JSON.parse(JSON.stringify(events));
const initialEndpoints = JSON.parse(JSON.stringify(endpoints));
const initialTemplates = JSON.parse(JSON.stringify(templates));
const initialEventEndpoints = JSON.parse(JSON.stringify(eventEndpoints));
const initialTemplateEndpoints = JSON.parse(JSON.stringify(templateEndpoints));

// --- Status endpoint (Tauri invoke mock handled client-side) ---
app.get("/api/v1/status", (_req, res) => {
  res.json(statusResponse);
});

// --- Events endpoints ---
app.get("/api/v1/events", (_req, res) => {
  res.json(events);
});

app.post("/api/v1/events", (req, res) => {
  let name;
  let createdFrom = null;
  let cacheDelaySecs = null;

  if (req.body.template_id) {
    const tmpl = templates.find((t) => t.id === parseInt(req.body.template_id));
    if (!tmpl) {
      return res.status(404).json({ error: "template not found" });
    }
    const dateStr = new Date().toISOString().split("T")[0];
    let candidate = `${tmpl.name}-${dateStr}`;
    // Deduplicate: if name exists, append -2, -3, etc.
    let suffix = 2;
    while (events.some((e) => e.name === candidate)) {
      candidate = `${tmpl.name}-${dateStr}-${suffix}`;
      suffix++;
    }
    name = candidate;
    createdFrom = tmpl.name;
    cacheDelaySecs = tmpl.cache_delay_secs;
  } else {
    name = req.body.name || "New Event";
  }

  const newEvent = {
    id: events.length + 1,
    name,
    received_bytes: 0,
    receiving_activated: false,
    delivering_activated: false,
    cache_delay_secs: cacheDelaySecs,
    created_from: createdFrom,
    rescue_video_url: null,
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
      target_delay_secs: 120,
      session_start: null,
      local_buffer_chunks: 0,
      s3_queue_chunks: 0,
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
        target_delay_secs: 0,
        session_start: null,
        local_buffer_chunks: 0,
        s3_queue_chunks: 0,
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
  if (req.body.rescue_video_url !== undefined)
    evt.rescue_video_url = req.body.rescue_video_url;
  res.json({ status: "ok" });
});

// --- Templates API ---
app.get("/api/v1/templates", (_req, res) => res.json(templates));

app.post("/api/v1/templates", (req, res) => {
  const t = {
    id: templates.length + 1,
    name: req.body.name,
    cache_delay_secs: req.body.cache_delay_secs || null,
  };
  templates.push(t);
  templateEndpoints[t.id] = [];
  res.status(201).json({ id: t.id });
});

app.get("/api/v1/templates/:id", (req, res) => {
  const t = templates.find((t) => t.id === parseInt(req.params.id));
  if (!t) return res.status(404).json({ error: "not found" });
  res.json(t);
});

app.patch("/api/v1/templates/:id", (req, res) => {
  const t = templates.find((t) => t.id === parseInt(req.params.id));
  if (!t) return res.status(404).json({ error: "not found" });
  if (req.body.name) t.name = req.body.name;
  if (req.body.cache_delay_secs !== undefined)
    t.cache_delay_secs = req.body.cache_delay_secs;
  res.json(t);
});

app.delete("/api/v1/templates/:id", (req, res) => {
  const id = parseInt(req.params.id);
  templates = templates.filter((t) => t.id !== id);
  delete templateEndpoints[id];
  res.status(204).send();
});

app.get("/api/v1/templates/:id/endpoints", (req, res) => {
  const id = parseInt(req.params.id);
  const epIds = templateEndpoints[id] || [];
  res.json(endpoints.filter((e) => epIds.includes(e.id)));
});

app.post("/api/v1/templates/:tid/endpoints/:eid", (req, res) => {
  const tid = parseInt(req.params.tid);
  const eid = parseInt(req.params.eid);
  if (!templateEndpoints[tid]) templateEndpoints[tid] = [];
  if (!templateEndpoints[tid].includes(eid)) templateEndpoints[tid].push(eid);
  res.status(201).send();
});

app.delete("/api/v1/templates/:tid/endpoints/:eid", (req, res) => {
  const tid = parseInt(req.params.tid);
  const eid = parseInt(req.params.eid);
  if (templateEndpoints[tid]) {
    templateEndpoints[tid] = templateEndpoints[tid].filter((e) => e !== eid);
  }
  res.status(204).send();
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

// Delivery endpoint add/remove. The real backend has these for adding /
// removing endpoints from an actively delivering event. Mock as no-ops
// (200) so frontend tests that fire these requests don't get a 404
// response which the browser would log as a console error and trip the
// global afterEach console-clean assertion.
app.post("/api/v1/delivery/endpoints/add", (_req, res) => {
  res.json({ status: "ok" });
});
app.post("/api/v1/delivery/endpoints/remove", (_req, res) => {
  res.json({ status: "ok" });
});

// S3 usage + per-event clear stubs for the new Settings tab UI. Both
// no-op so frontend tests that visit /settings don't 404.
app.get("/api/v1/s3/usage", (_req, res) => {
  res.json({ total_bytes: 0, total_objects: 0, by_event: [] });
});
app.post("/api/v1/events/:id/clear-s3", (_req, res) => {
  res.json({ deleted: 0 });
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

// --- YouTube status endpoint ---
app.get("/api/v1/youtube/status", (_req, res) => {
  res.json({
    authenticated: true,
    stream_receiving: true,
    broadcast_testing: false,
    broadcast_statuses: [],
    stream_count: 1,
    streams: [
      {
        title: "Live Stream",
        stream_status: "active",
        health_status: "good",
        configuration_issues: [],
        cdn_resolution: "1080p",
        cdn_frame_rate: "30fps",
        cdn_ingestion_type: "hls",
      },
    ],
    error: null,
  });
});

// Test-only: reset all mock data to initial state between tests
app.post("/api/v1/__reset", (_req, res) => {
  events = JSON.parse(JSON.stringify(initialEvents));
  endpoints = JSON.parse(JSON.stringify(initialEndpoints));
  templates = JSON.parse(JSON.stringify(initialTemplates));
  eventEndpoints = JSON.parse(JSON.stringify(initialEventEndpoints));
  templateEndpoints = JSON.parse(JSON.stringify(initialTemplateEndpoints));
  res.json({ reset: true });
});

// Test-only: broadcast arbitrary WebSocket events for E2E pipeline state tests
app.post("/api/v1/_test/ws-broadcast", (req, res) => {
  broadcastWs(req.body);
  res.json({ status: "ok" });
});

// Test-only: simulate VPS disconnect — cache bar drains at real-time rate
let disconnectTimer = null;
app.post("/api/v1/_test/simulate-disconnect", (req, res) => {
  const { start_delay = 120, target_delay = 120, drain_rate = 2 } = req.body;
  let currentDelay = start_delay;
  if (disconnectTimer) clearInterval(disconnectTimer);
  disconnectTimer = setInterval(() => {
    currentDelay = Math.max(0, currentDelay - drain_rate);
    const state = currentDelay <= 0 ? "buffer_exhausted" : "streaming";
    broadcastWs({
      type: "PipelineState",
      data: {
        state,
        event_id: 1,
        event_name: "Test Event",
        target_delay_secs: target_delay,
        session_start: null,
        local_buffer_chunks: Math.floor(currentDelay / 2),
        s3_queue_chunks: 0,
      },
    });
    if (currentDelay <= 0) {
      clearInterval(disconnectTimer);
      disconnectTimer = null;
    }
  }, 2000);
  res.json({ status: "ok", start_delay: currentDelay });
});

app.post("/api/v1/_test/simulate-reconnect", (req, res) => {
  if (disconnectTimer) {
    clearInterval(disconnectTimer);
    disconnectTimer = null;
  }
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
          is_fast: false,
          delivery_mode: "normal",
          rescue_eta_secs: null,
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
          is_fast: false,
          delivery_mode: "normal",
          rescue_eta_secs: null,
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
