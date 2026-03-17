# Restreamer

Church live-streaming system: captures RTMP from OBS, chunks MPEG-TS, uploads to S3,
and re-streams to YouTube/Facebook via dynamically provisioned Hetzner VPS instances.

## Architecture

```
OBS/vMix → RTMP → Restreamer (Windows) → S3 → Hetzner VPS (rs-delivery) → YouTube/Facebook
```

## Workspace Crates

| Crate           | Purpose                                          |
| --------------- | ------------------------------------------------ |
| rs-core         | Config, database (SQLite), models                |
| rs-inpoint      | RTMP server, MPEG-TS chunking                    |
| rs-endpoint     | S3 upload                                        |
| rs-api          | Axum HTTP API, WebSocket, delivery orchestration |
| rs-runtime      | Service orchestration                            |
| rs-service      | Standalone Windows Service binary                |
| rs-delivery     | Delivery relay binary (deployed to Hetzner VPS)  |
| rs-ffmpeg       | FFmpeg process wrapper                           |
| rs-ts-normalize | MPEG-TS timestamp normalization                  |
| rs-cloud        | Hetzner Cloud API client                         |
| rs-youtube      | YouTube OAuth & stream verification              |

## Additional Components

| Directory  | Purpose                                     |
| ---------- | ------------------------------------------- |
| src-tauri/ | Tauri desktop app (Windows tray + WebView2) |
| leptos-ui/ | Leptos CSR frontend (WASM, all-Rust)        |
| e2e/       | Playwright E2E tests                        |
| scripts/   | Windows install/deploy PowerShell scripts   |
