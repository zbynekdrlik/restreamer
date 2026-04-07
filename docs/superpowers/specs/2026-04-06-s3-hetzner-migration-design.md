# S3 Migration: Linode → Hetzner Object Storage

**Issue:** #38
**Goal:** Move all S3 storage from Linode Object Storage to Hetzner Object Storage (nbg1 region), co-located with the delivery VPS.

## Current State

| Component | Value | Provider |
|-----------|-------|----------|
| stream.lan config endpoint | `eu-central-1.linodeobjects.com` | Linode |
| CI `rs-delivery` upload | `eu-central-1.linodeobjects.com` | Linode |
| GitHub Secrets (`S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`) | Linode credentials | Linode |
| Code default (`config.rs`) | `fsn1.your-objectstorage.com` | Hetzner (placeholder) |
| VPS default location | `nbg1` | Hetzner |

## Target State

All S3 operations use Hetzner Object Storage in **nbg1 (Nuremberg)** — same region as the delivery VPS for minimum latency. User is in Slovakia; nbg1 is the closest Hetzner datacenter.

| Component | New Value |
|-----------|-----------|
| S3 endpoint | `https://nbg1.your-objectstorage.com` |
| S3 region | `nbg1` |
| Bucket name | `restreamer-chunks` (unchanged) |
| Credentials | New Hetzner S3 access key pair |

## Changes Required

### 1. Manual: Hetzner Console Setup

User creates via Hetzner Cloud Console:
- Object Storage bucket `restreamer-chunks` in nbg1
- S3 credentials (access key + secret key)

### 2. Code: Update Defaults (`crates/rs-core/src/config.rs`)

- Change S3 endpoint default from `https://fsn1.your-objectstorage.com` to `https://nbg1.your-objectstorage.com`
- Change S3 region default from `eu-central-1` to `nbg1`

### 3. CI: Update rs-delivery Upload (`.github/workflows/ci.yml`)

- Line 321: Change `--endpoint-url https://eu-central-1.linodeobjects.com` to `--endpoint-url https://nbg1.your-objectstorage.com`
- Add `--region nbg1` flag

### 4. GitHub Secrets: Replace Credentials

- `S3_ACCESS_KEY_ID` → Hetzner access key
- `S3_SECRET_ACCESS_KEY` → Hetzner secret key

### 5. stream.lan Config: Update S3 Section

Update `C:\ProgramData\Restreamer\config.json`:
```json
{
  "s3": {
    "bucket": "restreamer-chunks",
    "region": "nbg1",
    "endpoint": "https://nbg1.your-objectstorage.com",
    "access_key_id": "<hetzner-key>",
    "secret_access_key": "<hetzner-secret>"
  }
}
```

### 6. Version Bump

0.3.22 → 0.3.23 across all version files.

## What Does NOT Change

- S3 API calls — Hetzner Object Storage is S3-compatible
- Bucket name — `restreamer-chunks`
- VPS cloud-init — reads S3 config from app config dynamically
- VPS location — already `nbg1`
- Delivery binary — uses env vars from cloud-init

## Verification

1. CI uploads `rs-delivery` binary to Hetzner S3 successfully
2. Start a stream → RTMP → chunks upload to Hetzner S3
3. Delivery VPS downloads chunks from Hetzner S3 → streams to endpoints
4. All existing E2E tests pass

## Data Migration

No data migration needed. Old Linode chunks from past streams are not needed. Fresh start on Hetzner.

## Rollback

If Hetzner S3 has issues: revert code changes, restore Linode credentials in GitHub Secrets and stream.lan config.
