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
    identifier: "Sunday Service",
    short_description: "Weekly Sunday Service Stream",
    date_of_event: "2026-03-09",
    server_ip: "172.105.95.118",
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
    identifier: "Sunday Service",
    short_description: "Weekly Sunday Service Stream",
    date_of_event: "2026-03-09",
    server_ip: "172.105.95.118",
    received_bytes: 52428800,
    receiving_activated: true,
    delivering_activated: false,
  },
  {
    id: 2,
    identifier: "Wednesday Bible Study",
    short_description: "Midweek study",
    date_of_event: "2026-03-12",
    server_ip: "172.105.95.118",
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

let schedules = [
  {
    id: 1,
    event_id: 1,
    start_time: "2026-03-16T09:00:00Z",
    repeat_interval: "weekly",
    last_run_at: "2026-03-09T09:00:00Z",
    next_run_at: "2026-03-16T09:00:00Z",
    enabled: true,
  },
  {
    id: 2,
    event_id: 2,
    start_time: "2026-03-12T19:00:00Z",
    repeat_interval: null,
    last_run_at: null,
    next_run_at: "2026-03-12T19:00:00Z",
    enabled: false,
  },
];

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
    identifier: req.body.identifier || "New Event",
    short_description: null,
    date_of_event: new Date().toISOString().split("T")[0],
    server_ip: "",
    received_bytes: 0,
    receiving_activated: false,
    delivering_activated: false,
  };
  events.push(newEvent);
  res.json(newEvent);
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

// --- Schedules API ---
app.get("/api/v1/schedules", (_req, res) => {
  res.json(schedules);
});

app.delete("/api/v1/schedules/:id", (req, res) => {
  schedules = schedules.filter((s) => s.id !== parseInt(req.params.id));
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
