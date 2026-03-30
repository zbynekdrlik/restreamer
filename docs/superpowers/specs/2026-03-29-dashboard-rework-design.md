# Dashboard Rework: Terminal HUD, PWA, Mobile-First

**Issue:** #54 — rework dashboard to be more modern, PWA, responsive, with tree-based endpoint visualization

**Date:** 2026-03-29

---

## 1. Visual Language: Terminal / HUD

Dark mission-control aesthetic with monospace typography.

| Token | Value |
|-------|-------|
| `--bg-primary` | `#0a0a0a` |
| `--bg-card` | `#111111` |
| `--border` | `#333333` |
| `--text-primary` | `#ffffff` |
| `--text-secondary` | `#555555` |
| `--status-ok` | `#00ff88` |
| `--status-warn` | `#facc15` |
| `--status-error` | `#dc3545` |
| `--font-mono` | `"JetBrains Mono", "Fira Code", "Cascadia Code", monospace` |
| `--border-radius` | `2px` |
| `--border-width` | `2px solid` |

- All labels: uppercase, `letter-spacing: 2px`, `font-size: 10-11px`
- Cards: `#111` background, `2px solid #333` border, sharp corners
- Status indicators: colored borders on cards, not background fills
- Active/healthy borders use status color instead of `#333`
- Animations: `pulse` on active status dots only

## 2. Layout: Single Column Top-to-Bottom Pipeline

Full-page vertical flow. Each pipeline stage is a full-width row.

```
[HEADER]  Restreamer | WS ● | gear icon → /settings
──────────────────────────────────────────────────
[CONTROL BAR]  Event: [dropdown]  [START] [STOP]  ● STREAMING  02:14:33
──────────────────────────────────────────────────
● OBS ─────────────────────────── 1080p60 connected
    │
● RTMP ────────────────────────── 4.2 Mbps | 142 chunks
    │
● BUFFER ──────────────────────── 24.3s / 30s target
    │  [████████████████░░░░] 81%
    │
● S3 → VPS ────────────────────── running | 12 Mbps out | 3 eps
    │
    ├── ● YT-Main
    ├── ● FB-Stream                +18s ▲
    └── ● YT-Monitor              STALLED | ffmpeg x3
```

### Pipeline Nodes

Each node is a full-width row:
- **Left**: status dot (colored circle) + node label (uppercase, bold, monospace)
- **Right**: key metric(s) inline
- **Connector**: vertical line (`│`) between nodes, tree connectors (`├──`, `└──`) for endpoints

### Endpoint Nodes (Anomaly-Only Display)

Endpoints show minimal info by default:
- **Always**: alias + colored status dot (green/yellow/red)
- **Only when anomalous**:
  - Delay: shown only when it differs significantly from other endpoints (e.g. fast endpoint lagging, or cached endpoint falling behind)
  - Error/stall: shown only when there's an active issue (stall reason, ffmpeg restart count)
  - When healthy and in sync: just alias + green dot

### Bitrate Display

Bitrate (e.g. `12 Mbps`) is displayed on the **S3 → VPS** node since it's shared across all endpoints. Not repeated per endpoint.

### Controls on Endpoints

- **Remove button** (×): visible per endpoint when delivery is running
- **Add Endpoint**: dropdown below endpoint tree, with start position selector (Live / From Beginning)

## 3. Removed: Activity Feed

Activity feed is removed from the dashboard. Not used by the operator.

## 4. Settings Page

- Stays at `/settings` as a separate route
- Receives the same HUD restyle (monospace, dark, sharp corners, same CSS variables)
- No functional changes to OBS settings, events, or endpoint management
- Visual consistency with dashboard

## 5. PWA: Mobile-First Monitoring

### Use Case

Phone is the **primary monitoring device** during live streams. The operator keeps the dashboard on their phone to avoid covering OBS on the desktop. Desktop (stream.lan) is the fallback for when something goes wrong.

### PWA Configuration

- **manifest.json**: `display: standalone`, `theme_color: #0a0a0a`, `background_color: #0a0a0a`
- **Icons**: 192x192 and 512x512 PNG (generate from Restreamer logo or simple "RS" monogram)
- **Service worker**: registration only, no asset caching (all data is live/real-time)
- **Relative paths** in manifest — works on any domain

### Touch & Mobile

- Minimum touch target: 44x44px
- Start/Stop buttons: large, prominent
- Event selector: native `<select>` for mobile UX
- No hover-dependent interactions

## 6. Responsive Breakpoints

| Breakpoint | Target | Behavior |
|------------|--------|----------|
| `< 480px` | Phone | Compact nodes, smaller font (11px body), tighter padding, abbreviated labels |
| `< 768px` | Tablet | Standard single column, normal spacing |
| `> 768px` | Desktop | Wider nodes, metrics spread horizontally, more breathing room |

Single column layout at all breakpoints — no layout changes needed, just spacing and font size adjustments.

## 7. HTTPS & Domain Setup

### Domain

`streamsnv.newlevel.media` — Cloudflare-managed DNS under `newlevel.media` zone.

### Architecture (Same as reaperiem)

```
Remote user → https://streamsnv.newlevel.media
  → Cloudflare Edge (HTTPS termination)
  → cloudflared tunnel on stream.lan
  → http://localhost:8910

Local user → https://streamsnv.newlevel.media (or http://10.77.9.204:8910)
  → Direct LAN access or tunnel
```

### Cloudflare Tunnel Setup (stream.lan)

1. Install `cloudflared` on stream.lan (Windows)
2. Create tunnel: `cloudflared tunnel create restreamer`
3. Route DNS: `cloudflared tunnel route dns restreamer streamsnv.newlevel.media`
4. Config at `C:\Users\newlevel\.cloudflared\config.yml`:
   ```yaml
   tunnel: restreamer
   credentials-file: C:\Users\newlevel\.cloudflared\<tunnel-id>.json

   ingress:
     - hostname: streamsnv.newlevel.media
       service: http://localhost:8910
     - service: http_status:404
   ```
5. Install as Windows service: `cloudflared service install` + `cloudflared service start`

**Cloudflare credentials** (same account as reaperiem):
- API Token: `Nyw0JCox1ft-bmPK3mZAuvUbE_02K4aT1XO9RrxE`
- Account ID: `8f3efbc0edbe05bd6fdcab10cd63876a`
- Zone ID: `b9019ca528e573e62c2a110a45f45c74`

### TLS Certificates (Let's Encrypt)

Generate via certbot with Cloudflare DNS-01 challenge:

```bash
certbot certonly \
  --dns-cloudflare \
  --dns-cloudflare-credentials /path/to/cloudflare.ini \
  -d streamsnv.newlevel.media
```

- `fullchain.pem` → GitHub Secret `TLS_CERT_PEM`
- `privkey.pem` → GitHub Secret `TLS_KEY_PEM`
- CI deploys to `C:\ProgramData\Restreamer\cert.pem` and `key.pem`

### Axum HTTPS Integration

Config additions to `config.json`:
```json
{
  "tls": true,
  "https_port": 443,
  "tls_cert": "cert.pem",
  "tls_key": "key.pem",
  "https_domain": "streamsnv.newlevel.media"
}
```

Axum changes:
- Bind additional HTTPS listener on port 443 when `tls: true`
- HTTP→HTTPS redirect middleware (respects `x-forwarded-proto: https` from Cloudflare)
- LAN/WAN detection via `cf-connecting-ip` header

## 8. File Changes Summary

### New Files

| File | Purpose |
|------|---------|
| `leptos-ui/manifest.json` | PWA manifest |
| `leptos-ui/sw.js` | Service worker (registration only) |
| `leptos-ui/icon-192.png` | PWA icon 192x192 |
| `leptos-ui/icon-512.png` | PWA icon 512x512 |
| `docs/cloudflare-tunnel-setup.md` | Tunnel setup guide for stream.lan |

### Modified Files

| File | Change |
|------|--------|
| `leptos-ui/style.css` | Full restyle: HUD theme, new CSS variables, responsive breakpoints |
| `leptos-ui/index.html` | Add manifest link, theme-color meta, SW registration, icon links |
| `leptos-ui/src/components/operator_dashboard.rs` | Rewrite layout: single column flow, pipeline nodes, endpoint tree, remove activity feed |
| `leptos-ui/src/components/header.rs` | HUD restyle |
| `leptos-ui/src/components/settings.rs` | HUD restyle |
| `leptos-ui/src/store.rs` | Remove `activity_feed` signal |
| `leptos-ui/src/ws.rs` | Remove ActivityFeed handler |
| `crates/rs-api/src/state.rs` | Add TLS config fields |
| `crates/rs-core/src/config.rs` | Add `tls`, `https_port`, `tls_cert`, `tls_key`, `https_domain` fields |
| `crates/rs-service/src/main.rs` or equivalent | HTTPS listener + redirect middleware |
| `e2e/frontend.spec.ts` | Update selectors for new layout, remove activity feed tests, add pipeline tree tests |
| `.github/workflows/ci.yml` | Deploy TLS certs from secrets |

### Removed

| Item | Reason |
|------|--------|
| Activity feed component + CSS | Not used |
| `ActivityFeed` WS event handling in frontend | Not used |
| Two-column endpoint grid layout | Replaced by tree |
| `dashboard.rs` (old unused component) | Dead code cleanup |

## 9. Testing

### E2E (Playwright)

- Pipeline flow renders all nodes (OBS, RTMP, Buffer, S3/VPS) with correct status
- Endpoint tree shows endpoints branching from VPS node
- Endpoint anomaly display: healthy endpoint shows only alias+dot, unhealthy shows error
- Control bar: event selector, start/stop, state badge, timer
- Settings page renders with HUD style
- Mobile viewport (375px): all nodes visible, no horizontal scroll
- PWA manifest is served at `/manifest.json`
- Console zero errors/warnings

### Unit Tests

- Config parsing with new TLS fields
- HTTPS redirect middleware (with and without `x-forwarded-proto`)
- LAN/WAN detection logic

## 10. Out of Scope

- Offline support / asset caching in service worker
- Light theme / theme switching
- Dashboard drag-and-drop reordering
- Activity feed (explicitly removed, not deferred)
- Backend ActivityFeed WS event removal (backend can keep emitting; frontend ignores)
