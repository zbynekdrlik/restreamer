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

// Scenario selection — set by `POST /api/v1/_test/scenario` before page.goto.
// Supported values:
//   - "default" (initial state)
//   - "zero-endpoints"  — delivering_activated=true, endpoints=[]
//   - "last-endpoint"   — delivering_activated=true, 1 endpoint configured
//   - "rtmp-gate-tick"  — rtmp_stable_secs ticks from 0 upward on each /status poll
let scenario = "default";
let rtmpStableSecs = 999; // default: stream has been stable plenty long
let rtmpTickStartMs = null; // when the tick scenario started
// Explicit override set via POST /api/v1/_test/set-rtmp-stable-secs.
// When non-null, this takes precedence over the time-based tick and the
// default `rtmpStableSecs`. This is the race-free way for tests to
// control the RTMP-stable gate: parallel workers that reset scenario
// state cannot clobber a value another worker has pinned, because each
// test sets this explicitly between assertions.
let rtmpStableSecsOverride = null;

function currentRtmpStableSecs() {
  if (rtmpStableSecsOverride !== null) {
    return rtmpStableSecsOverride;
  }
  if (scenario === "rtmp-gate-tick") {
    if (rtmpTickStartMs === null) rtmpTickStartMs = Date.now();
    // Advance 15 simulated seconds per real second so the button
    // enables within ~1s of real time.
    const elapsedMs = Date.now() - rtmpTickStartMs;
    return Math.floor((elapsedMs / 1000) * 15);
  }
  return rtmpStableSecs;
}

function buildStatusResponse() {
  // The DEFAULT scenario simulates an IDLE dashboard (no RTMP connection,
  // no streaming event active) so the pipeline tests for "Disconnected" /
  // "RTMP Idle" / gray dots pass. Non-default scenarios opt into an active
  // RTMP stream by setting `scenario` via /api/v1/_test/scenario.
  //
  // `rtmp_stable_secs` is kept at its non-zero default (999s) even when
  // `rtmp_connected` is false so legacy tests that click `.start-btn`
  // without a WebSocket broadcast still pass the RTMP-stable gate. The
  // backend would never return this combination, but the Start button's
  // gate only reads `rtmp_stable_secs`, not `rtmp_connected`, so this
  // "loose" mock keeps pre-gate tests green without forcing every
  // `.start-btn` test to first broadcast an InpointStatus event.
  const rtmpActive =
    scenario === "zero-endpoints" ||
    scenario === "last-endpoint" ||
    scenario === "rtmp-gate-tick";
  return {
    inpoint: {
      state: rtmpActive ? "connected" : "idle",
      details: {
        rtmp_connected: rtmpActive,
        rtmp_stable_secs: currentRtmpStableSecs(),
      },
    },
    streaming_event: currentStreamingEvent(),
    chunk_stats: {
      total_chunks: 42,
      pending_chunks: 3,
      sent_chunks: 39,
      in_process_chunks: 0,
      total_bytes: 52428800,
      buffer_duration_secs: 126.5,
    },
  };
}

function currentStreamingEvent() {
  if (scenario === "zero-endpoints" || scenario === "last-endpoint") {
    return {
      id: 1,
      name: "Sunday Service",
      received_bytes: 52428800,
      receiving_activated: true,
      delivering_activated: true,
      cache_delay_secs: 120,
      rescue_video_url: null,
    };
  }
  return {
    id: 1,
    name: "Sunday Service",
    received_bytes: 52428800,
    receiving_activated: false,
    delivering_activated: false,
    cache_delay_secs: null,
    rescue_video_url: null,
  };
}

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
    service_type: "YT_RTMP",
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
  res.json(buildStatusResponse());
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
  // Emit audit row so audit-panel E2E can assert a visible entry.
  broadcastAudit("event_started", "operator", "info", null, {
    event_id: newEvent.id,
    event_name: newEvent.name,
  });
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
    service_type: req.body.service_type || "YT_RTMP",
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
  res.json(buildStatusResponse().chunk_stats);
});

// --- Upload telemetry (issue #118, #65; state classifier #168) ---
let uploadStats = {
  chunks_per_sec: 2.5,
  median_ms: 180,
  p95_ms: 540,
  error_rate: 0,
  in_flight: 3,
  adaptive_target: 8,
  permanent_recent: 0,
  state: { kind: "healthy" },
  render: {
    class: "ok",
    label: "Uploads OK",
    tooltip: "S3 uploads steady, no failures in the last minute.",
  },
};
app.get("/api/v1/uploads/stats", (_req, res) => {
  res.json(uploadStats);
});

let uploadRecent = [
  { chunk_id: 101, event_identifier: "sunday-service", sequence_number: 42, size_bytes: 102400, attempts: 1, duration_ms: 180, status: "sent",     last_error: null,      first_attempt_at: 1735000000000, completed_at: 1735000000180 },
  { chunk_id: 100, event_identifier: "sunday-service", sequence_number: 41, size_bytes: 102400, attempts: 2, duration_ms: 450, status: "retrying", last_error: "timeout", first_attempt_at: 1734999999800, completed_at: null },
  { chunk_id: 99,  event_identifier: "sunday-service", sequence_number: 40, size_bytes: 102400, attempts: 1, duration_ms: 150, status: "sent",     last_error: null,      first_attempt_at: 1734999999000, completed_at: 1734999999150 },
];
app.get("/api/v1/uploads/recent", (_req, res) => {
  res.json(uploadRecent);
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

// Diagnostics: pacing time-series (empty series for all E2E tests — no real
// chunks exist in the mock, but the endpoint must return a valid response so
// the panel renders without errors).
app.get("/api/v1/diagnostics/pacing", (_req, res) => {
  res.json({
    producer_rate: [],
    consumer_rate: [],
    clock_skew: [],
  });
});

// Test-only: reset all mock data to initial state between tests
app.post("/api/v1/__reset", (_req, res) => {
  events = JSON.parse(JSON.stringify(initialEvents));
  endpoints = JSON.parse(JSON.stringify(initialEndpoints));
  templates = JSON.parse(JSON.stringify(initialTemplates));
  eventEndpoints = JSON.parse(JSON.stringify(initialEventEndpoints));
  templateEndpoints = JSON.parse(JSON.stringify(initialTemplateEndpoints));
  scenario = "default";
  rtmpStableSecs = 999;
  rtmpTickStartMs = null;
  rtmpStableSecsOverride = null;
  auditIdCounter = 0;
  res.json({ reset: true });
});

// Test-only: set the active scenario. Tests call this BEFORE page.goto().
// Body: { scenario: "default" | "zero-endpoints" | "last-endpoint" | "rtmp-gate-tick" }
app.post("/api/v1/_test/scenario", (req, res) => {
  scenario = req.body.scenario || "default";
  if (scenario === "rtmp-gate-tick") {
    rtmpStableSecs = 0;
    rtmpTickStartMs = null;
  } else {
    rtmpStableSecs = 999;
    rtmpTickStartMs = null;
  }
  // Reshape endpoint/event state to match the scenario.
  if (scenario === "zero-endpoints") {
    events = events.map((e) =>
      e.id === 1
        ? { ...e, receiving_activated: true, delivering_activated: true }
        : e,
    );
    eventEndpoints[1] = [];
    cachedDelivery = {
      instance_name: "rs-delivery-evt1",
      status: "running",
      server_ip: "1.2.3.4",
      endpoint_count: 0,
      endpoints: [],
    };
  } else if (scenario === "last-endpoint") {
    events = events.map((e) =>
      e.id === 1
        ? { ...e, name: "test-event", receiving_activated: true, delivering_activated: true }
        : e,
    );
    eventEndpoints[1] = [1];
    cachedDelivery = {
      instance_name: "rs-delivery-evt1",
      status: "running",
      server_ip: "1.2.3.4",
      endpoint_count: 1,
      endpoints: [
        {
          alias: "yt1",
          alive: true,
          current_chunk_id: 142,
          bytes_processed_total: 1073741824,
          chunks_processed: 1847,
          chunk_delay_secs: 3.2,
          stall_reason: null,
          ffmpeg_restart_count: 0,
          // reconnect_count mirrors ffmpeg_restart_count for PusherKind::Rust
          // endpoints (#103). Always present so the dashboard can read it
          // uniformly regardless of which pusher the endpoint uses.
          reconnect_count: 0,
          last_error: null,
          is_fast: false,
          delivery_mode: "normal",
          rescue_eta_secs: null,
        },
      ],
    };
  }
  res.json({ scenario });
});

// Test-only: explicit override for `rtmp_stable_secs` returned by /status.
// Takes precedence over the scenario's time-based tick. Pass `secs: null`
// or omit the body to clear the override. This lets tests pin an exact
// value for the RTMP-stable gate without racing against a shared ticker
// that other parallel workers can reset by changing the scenario.
//
// Body: { secs: number | null }
app.post("/api/v1/_test/set-rtmp-stable-secs", (req, res) => {
  const { secs } = req.body || {};
  if (secs === null || secs === undefined) {
    rtmpStableSecsOverride = null;
  } else {
    rtmpStableSecsOverride = Number(secs);
  }
  res.json({ rtmp_stable_secs_override: rtmpStableSecsOverride });
});

// Audit row broadcaster used by operator action handlers below.
// Mock audit query endpoint to match the real backend. Returns an empty
// rows list — tests that want pre-populated audit data can drive it via
// `/api/v1/_test/ws-broadcast`. Without this route the frontend's
// `AuditPanel` mount-time backfill `GET /api/v1/audit?limit=50` returns
// HTML (Express 404) → JSON parse error → console warning → every
// Playwright test's zero-warnings assertion trips.
app.get("/api/v1/audit", (_req, res) => {
  res.json({ rows: [] });
});

let auditIdCounter = 0;
function broadcastAudit(action, source = "operator", severity = "info", endpoint = null, detail = {}) {
  auditIdCounter += 1;
  broadcastWs({
    type: "AuditAppended",
    data: {
      id: auditIdCounter,
      ts: new Date().toISOString(),
      source,
      severity,
      event_id: null,
      instance_id: null,
      endpoint,
      action,
      detail,
    },
  });
}

// Test-only: emit a single MetricsSample for the first endpoint so the
// sparkline has ≥2 points to draw.
app.post("/api/v1/_test/emit-metrics-sample", (req, res) => {
  const alias = req.body.alias || "yt1";
  const count = req.body.count || 5;
  const base_ts = Date.now();
  for (let i = 0; i < count; i++) {
    broadcastWs({
      type: "MetricsSample",
      data: {
        ts_ms: base_ts + i * 1000,
        event_id: 1,
        instance_id: 1,
        alias,
        chunk_delay_secs: 3.0 + i * 0.5,
        current_chunk_id: 100 + i,
        chunks_processed: 1800 + i,
        alive: true,
      },
    });
  }
  res.json({ emitted: count });
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

  // Choose the initial delivery payload based on the active scenario.
  let deliveryData;
  if (scenario === "zero-endpoints") {
    deliveryData = {
      instance_name: "rs-delivery-evt1",
      status: "running",
      server_ip: "1.2.3.4",
      endpoint_count: 0,
      endpoints: [],
    };
  } else if (scenario === "last-endpoint") {
    deliveryData = {
      instance_name: "rs-delivery-evt1",
      status: "running",
      server_ip: "1.2.3.4",
      endpoint_count: 1,
      endpoints: [
        {
          alias: "yt1",
          alive: true,
          current_chunk_id: 142,
          bytes_processed_total: 1073741824,
          chunks_processed: 1847,
          chunk_delay_secs: 3.2,
          stall_reason: null,
          ffmpeg_restart_count: 0,
          // reconnect_count mirrors ffmpeg_restart_count for PusherKind::Rust
          // endpoints (#103). Always present so the dashboard can read it
          // uniformly regardless of which pusher the endpoint uses.
          reconnect_count: 0,
          last_error: null,
          is_fast: false,
          delivery_mode: "normal",
          rescue_eta_secs: null,
        },
      ],
    };
  } else {
    deliveryData = {
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
    };
  }

  // For scenarios that need an "active" pipeline (zero-endpoints,
  // last-endpoint), also emit a PipelineState event so the frontend
  // knows we're in streaming/buffering and shows the banner/controls.
  const pipelineData =
    scenario === "zero-endpoints" || scenario === "last-endpoint"
      ? {
          state: "streaming",
          event_id: 1,
          event_name:
            scenario === "last-endpoint" ? "test-event" : "Sunday Service",
          target_delay_secs: 120,
          session_start: new Date().toISOString(),
          local_buffer_chunks: 10,
          s3_queue_chunks: 5,
          cache_duration_secs: 118.0,
        }
      : null;

  // Update cache and send after a brief delay
  cachedDelivery = deliveryData;
  setTimeout(() => {
    if (ws.readyState === ws.OPEN) {
      ws.send(JSON.stringify({ type: "DeliveryStatus", data: deliveryData }));
      if (pipelineData) {
        ws.send(JSON.stringify({ type: "PipelineState", data: pipelineData }));
      }
    }
  }, 200);

  ws.on("close", () => {
    console.log("[ws] Client disconnected");
  });
});
