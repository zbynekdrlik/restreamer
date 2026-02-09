/**
 * Shared test fixtures for Playwright E2E tests.
 *
 * These provide mock API responses that mirror the real Rust backend's
 * JSON schema, allowing the frontend to be tested without a running service.
 */

export const API_BASE = "http://127.0.0.1:8910/api/v1";

export const mockStatus = {
  inpoint: { state: "running", details: {} },
  endpoint: { state: "running", details: {} },
  poller: { state: "running", details: {} },
  streaming_event: {
    id: 1,
    identifier: "evt-test-001",
    short_description: "Sunday Service",
    date_of_event: "2026-02-09T10:00:00Z",
    server_ip: "172.105.95.118",
    received_bytes: 52428800,
    receiving_activated: true,
    delivering_activated: true,
  },
};

export const mockStatusDisconnected = {
  inpoint: { state: "stopped", details: {} },
  endpoint: { state: "error", details: {} },
  poller: { state: "stopped", details: {} },
  streaming_event: null,
};

export const mockChunks = [
  {
    id: 1,
    streaming_event_id: 1,
    chunk_file_path: "/tmp/chunks/chunk_001.ts",
    data_size: 1048576,
    created_at: "2026-02-09T10:00:05Z",
    md5: "d41d8cd98f00b204e9800998ecf8427e",
    in_process: false,
    sent: true,
  },
  {
    id: 2,
    streaming_event_id: 1,
    chunk_file_path: "/tmp/chunks/chunk_002.ts",
    data_size: 2097152,
    created_at: "2026-02-09T10:00:10Z",
    md5: "098f6bcd4621d373cade4e832627b4f6",
    in_process: true,
    sent: false,
  },
  {
    id: 3,
    streaming_event_id: 1,
    chunk_file_path: "/tmp/chunks/chunk_003.ts",
    data_size: 524288,
    created_at: "2026-02-09T10:00:15Z",
    md5: "5d41402abc4b2a76b9719d911017c592",
    in_process: false,
    sent: false,
  },
];

export const mockChunkStats = {
  total_chunks: 3,
  pending_chunks: 1,
  sent_chunks: 1,
  in_process_chunks: 1,
  total_bytes: 3670016,
  buffer_duration_secs: 15,
};

export const mockConfig = {
  client_uuid: "test-uuid-00000000",
  manager_url: "https://restreamer.newlevel.media",
  api: { bind: "127.0.0.1", port: 8910 },
  inpoint: { rtmp_bind: "0.0.0.0", rtmp_port: 1935, chunk_duration_ms: 5000 },
  s3: {
    bucket: "restreamer-chunks",
    region: "eu-central-1",
    endpoint: "https://s3.eu-central-1.amazonaws.com",
    access_key_id: "***",
    secret_access_key: "***",
  },
};

export const mockLogsInpoint = {
  entries: [
    {
      level: "INFO",
      target: "rs_inpoint::rtmp",
      message: "RTMP server started on 0.0.0.0:1935",
    },
    {
      level: "INFO",
      target: "rs_inpoint::chunker",
      message: "Chunk 001 written (1048576 bytes)",
    },
  ],
};

export const mockLogsEndpoint = {
  entries: [
    {
      level: "INFO",
      target: "rs_endpoint::uploader",
      message: "Uploaded chunk 001 to S3",
    },
    {
      level: "WARN",
      target: "rs_endpoint::s3",
      message: "Upload retry attempt 2/10",
    },
  ],
};

export const mockWsEvents = {
  inpointStatus: {
    type: "InpointStatus",
    data: {
      state: "running",
      rtmp_connected: true,
      received_bytes: 52428800,
      chunk_count: 3,
    },
  },
  chunkReceived: {
    type: "ChunkReceived",
    data: { id: 4, data_size: 1048576, md5: "abc123def456" },
  },
  chunkUploaded: {
    type: "ChunkUploaded",
    data: { chunk_id: 1 },
  },
  error: {
    type: "Error",
    data: { service: "endpoint", message: "S3 connection timeout" },
  },
};
