# Restreamer

Church live-streaming system that captures RTMP input (OBS/vMix), chunks it, uploads to S3, and re-streams to YouTube/Facebook/Vimeo via dynamically provisioned cloud instances.

## Architecture

```
[OBS/vMix] --RTMP--> [local-client]        --chunks-->  [S3 + manager-server API]
                      (Windows, FFmpeg,                   (restreamer.newlevel.media)
                       Django, port 1234)                         |
                                                   [provisions Linode instance]
                                                                  |
                                                   [delivering-service on Linode]
                                                                  |
                                                   [FFmpeg re-streams to YT/FB/Vimeo]
```

## Components

| Directory             | Description                                                            | Runs On                   |
| --------------------- | ---------------------------------------------------------------------- | ------------------------- |
| `local-client/`       | Windows endpoint — captures RTMP, chunks & uploads to S3               | Windows                   |
| `delivering-service/` | Re-streaming service — downloads chunks from S3, re-streams via FFmpeg | Linux (Linode)            |
| `manager-server/`     | Central management — provisions instances, YouTube OAuth, web UI       | Linux (production server) |

## Quick Start (local-client)

1. Clone this repo
2. Copy `local-client/.env.example` to `local-client/.env` and fill in credentials
3. Run `local-client\scripts\setup.bat` (as Administrator)
4. Django admin available at `http://127.0.0.1:8571/admin/`

See each component's directory for detailed setup instructions.

## Environment Variables

See `.env.example` at the repo root for all required environment variables across all components.
