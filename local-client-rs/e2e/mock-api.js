// Mock API server for frontend E2E tests.
// Provides fake responses for all API endpoints used by the Leptos WASM frontend.

const express = require("express");
const cors = require("cors");

const app = express();
app.use(cors());
app.use(express.json());

// --- Mock data ---

const statusResponse = {
  streaming_event: {
    id: 1,
    name: "Sunday Service",
    received_bytes: 52428800,
    receiving_activated: true,
    delivering_activated: false,
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
    receiving_activated: true,
    delivering_activated: false,
  },
  {
    id: 2,
    name: "Wednesday Bible Study",
    received_bytes: 0,
    receiving_activated: false,
    delivering_activated: false,
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

app.delete("/api/v1/endpoints/:id", (req, res) => {
  endpoints = endpoints.filter((e) => e.id !== parseInt(req.params.id));
  res.json({ status: "ok" });
});

// --- Chunks endpoints (used by dashboard/chunk_list) ---
app.get("/api/v1/chunks/stats", (_req, res) => {
  res.json(statusResponse.chunk_stats);
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
      message: "Connection lost to manager server",
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

const PORT = 8910;
app.listen(PORT, () => {
  console.log(`Mock API server running on http://127.0.0.1:${PORT}`);
});
