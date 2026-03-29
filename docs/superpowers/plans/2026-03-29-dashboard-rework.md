# Dashboard Rework: Terminal HUD, PWA, Mobile-First

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rework the Restreamer dashboard into a terminal/HUD-style, mobile-first PWA with single-column pipeline flow, anomaly-only endpoint display, and HTTPS via Cloudflare Tunnel.

**Architecture:** Replace the current flexbox/grid dashboard with a single-column top-to-bottom pipeline visualization using monospace HUD aesthetic. Add HTTPS support via axum-server+rustls for PWA installability, with Cloudflare Tunnel for remote access. Remove unused activity feed.

**Tech Stack:** Leptos 0.7 CSR (WASM), Axum 0.8, axum-server 0.8 (tls-rustls), rustls 0.23, Trunk

---

## File Structure

### New Files

| File | Responsibility |
|------|---------------|
| `leptos-ui/manifest.json` | PWA manifest (standalone, theme #0a0a0a) |
| `leptos-ui/sw.js` | Service worker (registration only, no caching) |
| `leptos-ui/icon-192.png` | PWA icon 192x192 |
| `leptos-ui/icon-512.png` | PWA icon 512x512 |
| `docs/cloudflare-tunnel-setup.md` | Tunnel setup guide for stream.lan |

### Modified Files

| File | Change |
|------|--------|
| `Cargo.toml` | Version bump 0.3.14→0.3.15, add axum-server + rustls workspace deps |
| `src-tauri/Cargo.toml` | Version bump |
| `src-tauri/tauri.conf.json` | Version bump |
| `leptos-ui/Cargo.toml` | Version bump |
| `crates/rs-core/src/config.rs` | Add TLS config fields to ApiConfig |
| `crates/rs-api/src/lib.rs` | Add HTTPS listener + redirect middleware |
| `crates/rs-api/Cargo.toml` | Add axum-server, rustls deps |
| `leptos-ui/index.html` | Add manifest, theme-color, SW registration, icons |
| `leptos-ui/style.css` | Full restyle: HUD theme variables + responsive |
| `leptos-ui/src/components/operator_dashboard.rs` | Rewrite: single-column pipeline flow + endpoint tree |
| `leptos-ui/src/components/header.rs` | HUD restyle |
| `leptos-ui/src/components/settings.rs` | HUD restyle |
| `leptos-ui/src/store.rs` | Remove activity_feed signal |
| `leptos-ui/src/ws.rs` | Remove ActivityFeed handler in frontend |
| `e2e/frontend.spec.ts` | Update selectors, remove activity feed tests, add pipeline tree tests |

### Deleted Files

| File | Reason |
|------|--------|
| `leptos-ui/src/components/dashboard.rs` | Old unused component (OperatorDashboard is the active one) |

---

## Task 1: Version Bump

**Files:**
- Modify: `Cargo.toml` (line 3)
- Modify: `src-tauri/Cargo.toml` (line 3)
- Modify: `src-tauri/tauri.conf.json` (version field)
- Modify: `leptos-ui/Cargo.toml` (line 3)

- [ ] **Step 1: Bump all version files from 0.3.14 to 0.3.15**

```bash
cd /home/newlevel/devel/restreamer
sed -i '0,/^version = "0.3.14"/s//version = "0.3.15"/' Cargo.toml
sed -i 's/^version = "0.3.14"/version = "0.3.15"/' src-tauri/Cargo.toml
sed -i 's/^version = "0.3.14"/version = "0.3.15"/' leptos-ui/Cargo.toml
sed -i 's/"version": "0.3.14"/"version": "0.3.15"/' src-tauri/tauri.conf.json
```

- [ ] **Step 2: Verify versions match**

```bash
grep '^version' Cargo.toml | head -1
grep '^version' src-tauri/Cargo.toml
grep '^version' leptos-ui/Cargo.toml
grep '"version"' src-tauri/tauri.conf.json
```

Expected: all show `0.3.15`

- [ ] **Step 3: Commit**

```bash
git add Cargo.toml src-tauri/Cargo.toml src-tauri/tauri.conf.json leptos-ui/Cargo.toml
git commit -m "chore: bump version to 0.3.15 for dashboard rework"
```

---

## Task 2: TLS Config Fields + HTTPS Listener

**Files:**
- Modify: `Cargo.toml` (workspace dependencies)
- Modify: `crates/rs-api/Cargo.toml`
- Modify: `crates/rs-core/src/config.rs`
- Modify: `crates/rs-api/src/lib.rs`
- Test: `crates/rs-core/src/config.rs` (inline tests)
- Test: `crates/rs-api/src/lib.rs` (inline tests)

- [ ] **Step 1: Write failing test for TLS config parsing**

In `crates/rs-core/src/config.rs`, add to the existing `#[cfg(test)] mod tests` block:

```rust
#[test]
fn tls_config_defaults() {
    let json = r#"{
        "client_uuid": "test",
        "s3": { "bucket": "b", "region": "r", "endpoint": "e", "access_key_id": "a", "secret_access_key": "s" },
        "delivery": { "snapshot_label": "test" },
        "api": {}
    }"#;
    let config: Config = serde_json::from_str(json).unwrap();
    assert!(!config.api.tls);
    assert_eq!(config.api.https_port, 443);
    assert_eq!(config.api.tls_cert, "cert.pem");
    assert_eq!(config.api.tls_key, "key.pem");
    assert!(config.api.https_domain.is_none());
}

#[test]
fn tls_config_explicit() {
    let json = r#"{
        "client_uuid": "test",
        "s3": { "bucket": "b", "region": "r", "endpoint": "e", "access_key_id": "a", "secret_access_key": "s" },
        "delivery": { "snapshot_label": "test" },
        "api": {
            "tls": true,
            "https_port": 8443,
            "tls_cert": "my-cert.pem",
            "tls_key": "my-key.pem",
            "https_domain": "streamsnv.newlevel.media"
        }
    }"#;
    let config: Config = serde_json::from_str(json).unwrap();
    assert!(config.api.tls);
    assert_eq!(config.api.https_port, 8443);
    assert_eq!(config.api.tls_cert, "my-cert.pem");
    assert_eq!(config.api.tls_key, "my-key.pem");
    assert_eq!(config.api.https_domain.as_deref(), Some("streamsnv.newlevel.media"));
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cargo test -p rs-core tls_config -- --nocapture
```

Expected: FAIL — `ApiConfig` doesn't have `tls`, `https_port`, etc.

- [ ] **Step 3: Add TLS fields to ApiConfig**

In `crates/rs-core/src/config.rs`, modify the `ApiConfig` struct:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiConfig {
    #[serde(default = "default_api_port")]
    pub port: u16,
    #[serde(default = "default_api_bind")]
    pub bind: String,
    #[serde(default)]
    pub tls: bool,
    #[serde(default = "default_https_port")]
    pub https_port: u16,
    #[serde(default = "default_tls_cert")]
    pub tls_cert: String,
    #[serde(default = "default_tls_key")]
    pub tls_key: String,
    #[serde(default)]
    pub https_domain: Option<String>,
}
```

Add default functions:

```rust
fn default_https_port() -> u16 {
    443
}

fn default_tls_cert() -> String {
    "cert.pem".to_string()
}

fn default_tls_key() -> String {
    "key.pem".to_string()
}
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cargo test -p rs-core tls_config -- --nocapture
```

Expected: PASS

- [ ] **Step 5: Add workspace dependencies for HTTPS**

In root `Cargo.toml` under `[workspace.dependencies]`:

```toml
axum-server = { version = "0.8", features = ["tls-rustls"] }
rustls = { version = "0.23", features = ["ring"] }
```

In `crates/rs-api/Cargo.toml` under `[dependencies]`:

```toml
axum-server = { workspace = true }
rustls = { workspace = true }
```

- [ ] **Step 6: Write HTTPS redirect middleware test**

In `crates/rs-api/src/lib.rs`, add a test module:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::util::ServiceExt;

    #[tokio::test]
    async fn https_redirect_skips_when_forwarded_proto_https() {
        let app = axum::Router::new()
            .route("/test", axum::routing::get(|| async { "ok" }))
            .layer(axum::middleware::from_fn(move |req, next| {
                https_redirect(req, next, "streamsnv.newlevel.media".to_string())
            }));

        let req = Request::builder()
            .uri("/test")
            .header("host", "streamsnv.newlevel.media")
            .header("x-forwarded-proto", "https")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn https_redirect_redirects_http_domain_request() {
        let app = axum::Router::new()
            .route("/test", axum::routing::get(|| async { "ok" }))
            .layer(axum::middleware::from_fn(move |req, next| {
                https_redirect(req, next, "streamsnv.newlevel.media".to_string())
            }));

        let req = Request::builder()
            .uri("/test")
            .header("host", "streamsnv.newlevel.media")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::PERMANENT_REDIRECT);
        assert_eq!(
            resp.headers().get("location").unwrap(),
            "https://streamsnv.newlevel.media/test"
        );
    }

    #[tokio::test]
    async fn https_redirect_passes_through_ip_requests() {
        let app = axum::Router::new()
            .route("/test", axum::routing::get(|| async { "ok" }))
            .layer(axum::middleware::from_fn(move |req, next| {
                https_redirect(req, next, "streamsnv.newlevel.media".to_string())
            }));

        let req = Request::builder()
            .uri("/test")
            .header("host", "10.77.9.204:8910")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }
}
```

- [ ] **Step 7: Run tests to verify they fail**

```bash
cargo test -p rs-api -- https_redirect --nocapture
```

Expected: FAIL — `https_redirect` function doesn't exist

- [ ] **Step 8: Implement HTTPS redirect middleware and serve function changes**

In `crates/rs-api/src/lib.rs`, add the redirect middleware:

```rust
async fn https_redirect(
    req: axum::extract::Request,
    next: axum::middleware::Next,
    domain: String,
) -> axum::response::Response {
    use axum::response::IntoResponse;

    let forwarded_proto = req
        .headers()
        .get("x-forwarded-proto")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("");
    if forwarded_proto == "https" {
        return next.run(req).await;
    }

    let host = req
        .headers()
        .get("host")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("");
    let host_name = host.split(':').next().unwrap_or("");
    if host_name == domain {
        let path = req
            .uri()
            .path_and_query()
            .map(|pq| pq.as_str())
            .unwrap_or("/");
        let location = format!("https://{domain}{path}");
        axum::response::Redirect::permanent(&location).into_response()
    } else {
        next.run(req).await
    }
}
```

Modify the `serve()` function to accept config and optionally start HTTPS:

```rust
pub async fn serve(
    state: AppState,
    addr: SocketAddr,
) -> anyhow::Result<(SocketAddr, JoinHandle<()>)> {
    let config = state.config.clone();
    let mut app = router::build_router(state);

    // Add HTTPS redirect middleware if domain is configured
    if config.api.tls {
        if let Some(ref domain) = config.api.https_domain {
            let domain = domain.clone();
            app = app.layer(axum::middleware::from_fn(move |req, next| {
                let domain = domain.clone();
                https_redirect(req, next, domain)
            }));
        }
    }

    // Start HTTPS listener if TLS is enabled
    if config.api.tls {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let config_dir = std::path::Path::new(&config.api.tls_cert).parent()
            .unwrap_or(std::path::Path::new("."));
        // For Restreamer: certs live next to config at C:\ProgramData\Restreamer\
        let cert_path = if std::path::Path::new(&config.api.tls_cert).is_absolute() {
            std::path::PathBuf::from(&config.api.tls_cert)
        } else {
            // Default: C:\ProgramData\Restreamer\ on Windows
            rs_core::config::Config::default_path()
                .parent()
                .unwrap_or(std::path::Path::new("."))
                .join(&config.api.tls_cert)
        };
        let key_path = if std::path::Path::new(&config.api.tls_key).is_absolute() {
            std::path::PathBuf::from(&config.api.tls_key)
        } else {
            rs_core::config::Config::default_path()
                .parent()
                .unwrap_or(std::path::Path::new("."))
                .join(&config.api.tls_key)
        };

        if cert_path.exists() && key_path.exists() {
            match axum_server::tls_rustls::RustlsConfig::from_pem_file(&cert_path, &key_path).await {
                Ok(rustls_config) => {
                    let https_addr = SocketAddr::from(([0, 0, 0, 0], config.api.https_port));
                    let https_app = app.clone();
                    tokio::spawn(async move {
                        info!("HTTPS server listening on {https_addr}");
                        if let Err(e) = axum_server::bind_rustls(https_addr, rustls_config)
                            .serve(https_app.into_make_service())
                            .await
                        {
                            tracing::error!("HTTPS server failed: {e}");
                        }
                    });
                }
                Err(e) => {
                    tracing::error!("Failed to load TLS certificates from {}: {e}", cert_path.display());
                }
            }
        } else {
            tracing::warn!("TLS enabled but cert files not found: cert={} key={}", cert_path.display(), key_path.display());
        }
    }

    let listener = TcpListener::bind(addr).await?;
    let local_addr = listener.local_addr()?;
    info!("API server listening on {local_addr}");

    let handle = tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            tracing::error!("API server error: {e}");
        }
    });

    Ok((local_addr, handle))
}
```

- [ ] **Step 9: Run tests to verify they pass**

```bash
cargo test -p rs-api -- https_redirect --nocapture
cargo test -p rs-core tls_config -- --nocapture
```

Expected: all PASS

- [ ] **Step 10: Run clippy and fmt**

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
```

- [ ] **Step 11: Commit**

```bash
git add crates/rs-core/src/config.rs crates/rs-api/src/lib.rs crates/rs-api/Cargo.toml Cargo.toml
git commit -m "feat: add TLS config fields and HTTPS listener with redirect middleware"
```

---

## Task 3: PWA Assets (manifest, service worker, icons, index.html)

**Files:**
- Create: `leptos-ui/manifest.json`
- Create: `leptos-ui/sw.js`
- Create: `leptos-ui/icon-192.png`
- Create: `leptos-ui/icon-512.png`
- Modify: `leptos-ui/index.html`

- [ ] **Step 1: Create manifest.json**

```json
{
  "name": "Restreamer",
  "short_name": "Restreamer",
  "description": "Live streaming operations dashboard",
  "start_url": "/",
  "scope": "/",
  "display": "standalone",
  "background_color": "#0a0a0a",
  "theme_color": "#0a0a0a",
  "orientation": "portrait",
  "icons": [
    {
      "src": "/icon-192.png",
      "sizes": "192x192",
      "type": "image/png",
      "purpose": "any maskable"
    },
    {
      "src": "/icon-512.png",
      "sizes": "512x512",
      "type": "image/png",
      "purpose": "any maskable"
    }
  ]
}
```

- [ ] **Step 2: Create sw.js**

```javascript
// Restreamer Service Worker — PWA shell only, no asset caching.
// All data is live/real-time via WebSocket. No offline use case.

self.addEventListener("install", () => {
  self.skipWaiting();
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches
      .keys()
      .then((names) => Promise.all(names.map((name) => caches.delete(name))))
      .then(() => self.clients.claim()),
  );
});
```

- [ ] **Step 3: Generate PWA icons**

Generate simple "RS" monogram icons using ImageMagick (available on the dev machine):

```bash
cd /home/newlevel/devel/restreamer/leptos-ui
convert -size 192x192 xc:'#0a0a0a' -fill '#00ff88' -font 'DejaVu-Sans-Bold' -pointsize 80 -gravity center -annotate 0 'RS' icon-192.png
convert -size 512x512 xc:'#0a0a0a' -fill '#00ff88' -font 'DejaVu-Sans-Bold' -pointsize 200 -gravity center -annotate 0 'RS' icon-512.png
```

If ImageMagick is not available, create minimal 1-color PNG placeholders with Python:

```bash
python3 -c "
import struct, zlib
def png(w,h,r,g,b,path):
    raw=b''
    for _ in range(h): raw+=b'\x00'+bytes([r,g,b])*w
    def chunk(t,d): return struct.pack('>I',len(d))+t+d+struct.pack('>I',zlib.crc32(t+d)&0xffffffff)
    with open(path,'wb') as f:
        f.write(b'\x89PNG\r\n\x1a\n')
        f.write(chunk(b'IHDR',struct.pack('>IIBBBBB',w,h,8,2,0,0,0)))
        f.write(chunk(b'IDAT',zlib.compress(raw)))
        f.write(chunk(b'IEND',b''))
png(192,192,0,255,136,'icon-192.png')
png(512,512,0,255,136,'icon-512.png')
"
```

- [ ] **Step 4: Update index.html**

Replace `leptos-ui/index.html` with:

```html
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
    <title>Restreamer</title>
    <link data-trunk rel="rust" data-wasm-opt="z" />
    <link data-trunk rel="css" href="style.css" />
    <link data-trunk rel="copy-file" href="manifest.json" />
    <link data-trunk rel="copy-file" href="sw.js" />
    <link data-trunk rel="copy-file" href="icon-192.png" />
    <link data-trunk rel="copy-file" href="icon-512.png" />
    <link rel="manifest" href="/manifest.json" />
    <link rel="icon" href="/icon-192.png" />
    <link rel="apple-touch-icon" href="/icon-192.png" />
    <meta name="theme-color" content="#0a0a0a" />
    <meta name="apple-mobile-web-app-capable" content="yes" />
    <meta name="apple-mobile-web-app-status-bar-style" content="black" />
</head>
<body>
    <noscript>This app requires JavaScript and WebAssembly.</noscript>
    <script>
    if ('serviceWorker' in navigator) {
        navigator.serviceWorker.register('/sw.js');
    }
    </script>
</body>
</html>
```

- [ ] **Step 5: Commit**

```bash
git add leptos-ui/manifest.json leptos-ui/sw.js leptos-ui/icon-192.png leptos-ui/icon-512.png leptos-ui/index.html
git commit -m "feat: add PWA manifest, service worker, icons, and updated index.html"
```

---

## Task 4: CSS Restyle — HUD Theme

**Files:**
- Modify: `leptos-ui/style.css` (full rewrite of variables + component styles)

- [ ] **Step 1: Replace CSS variables and base styles**

Replace the `:root` block and body styles at the top of `style.css` with:

```css
:root {
    --bg-primary: #0a0a0a;
    --bg-card: #111111;
    --border: #333333;
    --border-active: #00ff88;
    --text-primary: #ffffff;
    --text-secondary: #555555;
    --text-label: #888888;
    --status-ok: #00ff88;
    --status-warn: #facc15;
    --status-error: #dc3545;
    --font-mono: "JetBrains Mono", "Fira Code", "Cascadia Code", "Courier New", monospace;
    --radius: 2px;
    --border-w: 2px;
    --spacing-xs: 4px;
    --spacing-sm: 8px;
    --spacing-md: 16px;
    --spacing-lg: 24px;
    --spacing-xl: 32px;
}
```

Replace body styles:

```css
* { box-sizing: border-box; margin: 0; padding: 0; }

body {
    font-family: var(--font-mono);
    background: var(--bg-primary);
    color: var(--text-primary);
    font-size: 13px;
    line-height: 1.5;
    -webkit-font-smoothing: antialiased;
}
```

- [ ] **Step 2: Restyle header**

```css
.app-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: var(--spacing-sm) var(--spacing-md);
    border-bottom: var(--border-w) solid var(--border);
    background: var(--bg-card);
}

.app-title {
    color: var(--status-ok);
    font-weight: 700;
    font-size: 14px;
    text-transform: uppercase;
    letter-spacing: 2px;
}

.header-right {
    display: flex;
    align-items: center;
    gap: var(--spacing-sm);
}

.ws-indicator {
    width: 8px;
    height: 8px;
    border-radius: 50%;
    background: var(--status-ok);
}

.ws-indicator.disconnected {
    background: var(--status-error);
}

.header-nav-btn {
    background: none;
    border: var(--border-w) solid var(--border);
    color: var(--text-secondary);
    padding: var(--spacing-xs) var(--spacing-sm);
    font-family: var(--font-mono);
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 1px;
    cursor: pointer;
    border-radius: var(--radius);
}

.header-nav-btn:hover {
    border-color: var(--text-primary);
    color: var(--text-primary);
}
```

- [ ] **Step 3: Restyle control bar**

```css
.control-bar {
    display: flex;
    align-items: center;
    gap: var(--spacing-md);
    padding: var(--spacing-md);
    border-bottom: var(--border-w) solid var(--border);
    flex-wrap: wrap;
}

.event-selector {
    background: var(--bg-card);
    border: var(--border-w) solid var(--border);
    color: var(--text-primary);
    font-family: var(--font-mono);
    font-size: 12px;
    padding: var(--spacing-xs) var(--spacing-sm);
    border-radius: var(--radius);
    min-width: 150px;
}

.start-btn, .stop-btn {
    font-family: var(--font-mono);
    font-size: 11px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 1px;
    padding: var(--spacing-sm) var(--spacing-md);
    border-radius: var(--radius);
    cursor: pointer;
    border: var(--border-w) solid;
    min-height: 44px;
    min-width: 44px;
}

.start-btn {
    background: var(--status-ok);
    color: #000;
    border-color: var(--status-ok);
}

.start-btn:hover { opacity: 0.85; }

.start-btn:disabled {
    background: var(--border);
    border-color: var(--border);
    color: var(--text-secondary);
    cursor: not-allowed;
}

.stop-btn {
    background: transparent;
    color: var(--status-error);
    border-color: var(--status-error);
}

.stop-btn:hover { background: rgba(220, 53, 69, 0.15); }

.state-badge {
    font-size: 10px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 2px;
    padding: var(--spacing-xs) var(--spacing-sm);
    border: var(--border-w) solid;
    border-radius: var(--radius);
}

.state-badge.idle { color: var(--text-secondary); border-color: var(--border); }
.state-badge.buffering { color: var(--status-warn); border-color: var(--status-warn); }
.state-badge.streaming { color: var(--status-ok); border-color: var(--status-ok); }
.state-badge.stopping { color: var(--status-error); border-color: var(--status-error); }
.state-badge.buffer_exhausted { color: var(--status-error); border-color: var(--status-error); }

.session-timer {
    color: var(--text-secondary);
    font-size: 14px;
    font-weight: 700;
    font-variant-numeric: tabular-nums;
}
```

- [ ] **Step 4: Add pipeline flow styles (new single-column layout)**

```css
/* Pipeline: single column top-to-bottom flow */
.pipeline {
    padding: var(--spacing-md);
}

.pipeline-node {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: var(--spacing-sm) var(--spacing-md);
    background: var(--bg-card);
    border: var(--border-w) solid var(--border);
    border-radius: var(--radius);
}

.pipeline-node.active { border-color: var(--status-ok); }
.pipeline-node.warning { border-color: var(--status-warn); }
.pipeline-node.error { border-color: var(--status-error); }

.pipeline-node-left {
    display: flex;
    align-items: center;
    gap: var(--spacing-sm);
}

.pipeline-node-label {
    font-weight: 700;
    font-size: 12px;
    text-transform: uppercase;
    letter-spacing: 2px;
}

.pipeline-node.active .pipeline-node-label { color: var(--status-ok); }
.pipeline-node.warning .pipeline-node-label { color: var(--status-warn); }
.pipeline-node.error .pipeline-node-label { color: var(--status-error); }
.pipeline-node .pipeline-node-label { color: var(--text-secondary); }

.pipeline-node-metric {
    color: var(--text-secondary);
    font-size: 11px;
}

.status-dot {
    width: 8px;
    height: 8px;
    border-radius: 50%;
    background: var(--text-secondary);
    flex-shrink: 0;
}

.status-dot.active { background: var(--status-ok); }
.status-dot.warning { background: var(--status-warn); }
.status-dot.error { background: var(--status-error); }
.status-dot.active { animation: pulse 2s infinite; }

@keyframes pulse {
    0%, 100% { opacity: 1; }
    50% { opacity: 0.5; }
}

/* Connector line between pipeline nodes */
.pipeline-connector {
    display: flex;
    justify-content: center;
    padding: 2px 0;
    color: var(--border);
    font-size: 14px;
    line-height: 1;
    user-select: none;
}

/* Buffer progress bar inside pipeline */
.buffer-bar {
    height: 4px;
    background: var(--border);
    border-radius: var(--radius);
    margin-top: var(--spacing-xs);
    overflow: hidden;
}

.buffer-bar-fill {
    height: 100%;
    border-radius: var(--radius);
    transition: width 0.5s ease;
}

.buffer-bar-fill.healthy { background: var(--status-ok); }
.buffer-bar-fill.warning { background: var(--status-warn); }
.buffer-bar-fill.critical { background: var(--status-error); }
```

- [ ] **Step 5: Add endpoint tree styles**

```css
/* Endpoint tree branching from VPS node */
.endpoint-tree {
    padding-left: var(--spacing-lg);
}

.endpoint-branch {
    display: flex;
    align-items: center;
    gap: var(--spacing-sm);
    padding: var(--spacing-xs) 0;
}

.branch-connector {
    color: var(--border);
    font-family: var(--font-mono);
    font-size: 14px;
    user-select: none;
    flex-shrink: 0;
    width: 24px;
}

.endpoint-node {
    display: flex;
    align-items: center;
    gap: var(--spacing-sm);
    padding: var(--spacing-xs) var(--spacing-sm);
    background: var(--bg-card);
    border: var(--border-w) solid var(--border);
    border-radius: var(--radius);
    flex: 1;
    min-height: 36px;
}

.endpoint-node.ok { border-color: var(--status-ok); }
.endpoint-node.warning { border-color: var(--status-warn); }
.endpoint-node.error { border-color: var(--status-error); }

.endpoint-alias {
    font-weight: 700;
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 1px;
}

.endpoint-node.ok .endpoint-alias { color: var(--status-ok); }
.endpoint-node.warning .endpoint-alias { color: var(--status-warn); }
.endpoint-node.error .endpoint-alias { color: var(--status-error); }

.endpoint-anomaly {
    color: var(--text-secondary);
    font-size: 10px;
    margin-left: auto;
}

.btn-remove-endpoint {
    background: none;
    border: none;
    color: var(--text-secondary);
    font-family: var(--font-mono);
    font-size: 14px;
    cursor: pointer;
    padding: 2px 6px;
    min-width: 44px;
    min-height: 44px;
    display: flex;
    align-items: center;
    justify-content: center;
}

.btn-remove-endpoint:hover { color: var(--status-error); }

/* Add endpoint control */
.add-endpoint-control {
    display: flex;
    align-items: center;
    gap: var(--spacing-sm);
    padding: var(--spacing-sm) 0;
    padding-left: var(--spacing-lg);
}

.add-endpoint-select, .start-position-select {
    background: var(--bg-card);
    border: var(--border-w) solid var(--border);
    color: var(--text-primary);
    font-family: var(--font-mono);
    font-size: 11px;
    padding: var(--spacing-xs) var(--spacing-sm);
    border-radius: var(--radius);
    min-height: 44px;
}
```

- [ ] **Step 6: Add responsive breakpoints**

```css
/* Settings page HUD restyle */
.settings-page {
    padding: var(--spacing-md);
}

.settings-section {
    margin-bottom: var(--spacing-lg);
}

.settings-card {
    background: var(--bg-card);
    border: var(--border-w) solid var(--border);
    border-radius: var(--radius);
    padding: var(--spacing-md);
}

.settings-card h3 {
    color: var(--status-ok);
    font-size: 12px;
    text-transform: uppercase;
    letter-spacing: 2px;
    margin-bottom: var(--spacing-md);
}

.settings-card input, .settings-card select {
    background: var(--bg-primary);
    border: var(--border-w) solid var(--border);
    color: var(--text-primary);
    font-family: var(--font-mono);
    font-size: 12px;
    padding: var(--spacing-sm);
    border-radius: var(--radius);
    width: 100%;
}

.settings-card button {
    background: var(--bg-card);
    border: var(--border-w) solid var(--border);
    color: var(--text-primary);
    font-family: var(--font-mono);
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 1px;
    padding: var(--spacing-sm) var(--spacing-md);
    border-radius: var(--radius);
    cursor: pointer;
    min-height: 44px;
}

.settings-card button:hover {
    border-color: var(--text-primary);
}

/* Responsive: phone */
@media (max-width: 480px) {
    body { font-size: 11px; }
    .control-bar { flex-direction: column; align-items: stretch; gap: var(--spacing-sm); }
    .pipeline-node { padding: var(--spacing-xs) var(--spacing-sm); }
    .pipeline-node-label { font-size: 10px; letter-spacing: 1px; }
    .endpoint-tree { padding-left: var(--spacing-md); }
    .add-endpoint-control { padding-left: var(--spacing-md); flex-wrap: wrap; }
}

/* Responsive: tablet and below */
@media (max-width: 768px) {
    .control-bar { flex-wrap: wrap; }
    .app-header { padding: var(--spacing-xs) var(--spacing-sm); }
}
```

- [ ] **Step 7: Remove old CSS that is no longer needed**

Remove these class blocks from style.css (they belong to the old layout):
- `.pipeline-flow` (old horizontal pipeline)
- `.pipeline-arrow` (old arrows between horizontal nodes)
- `.endpoint-groups` (old two-column grid)
- `.endpoint-card` (old card style — replaced by `.endpoint-node`)
- `.cache-bar` (replaced by inline `.buffer-bar`)
- `.activity-feed`, `.activity-entry`, `.activity-time` (removed feature)
- Old responsive breakpoints that reference removed classes

Keep all form/input styles and any utility classes still referenced by settings page.

- [ ] **Step 8: Commit**

```bash
git add leptos-ui/style.css
git commit -m "feat: restyle dashboard with terminal HUD theme, responsive breakpoints"
```

---

## Task 5: Rewrite operator_dashboard.rs — Pipeline Flow + Endpoint Tree

**Files:**
- Modify: `leptos-ui/src/components/operator_dashboard.rs` (major rewrite)
- Modify: `leptos-ui/src/store.rs` (remove activity_feed)
- Modify: `leptos-ui/src/ws.rs` (remove ActivityFeed frontend handler)
- Delete: `leptos-ui/src/components/dashboard.rs`
- Modify: `leptos-ui/src/components/mod.rs` (remove dashboard export)

This is the largest task. The component structure changes from:

**Old:** ControlBar → PipelineFlow (horizontal) → CacheBar → EndpointGroups (2-col grid) → ActivityFeed

**New:** ControlBar → Pipeline (vertical nodes with connectors) → EndpointTree (branching) → AddEndpointControl

- [ ] **Step 1: Remove activity_feed from store.rs**

In `leptos-ui/src/store.rs`, remove the `activity_feed` field from `DashboardStore` and its initialization.

- [ ] **Step 2: Remove ActivityFeed handler from ws.rs**

In `leptos-ui/src/ws.rs`, remove the match arm that handles `WsEvent::ActivityFeed` and updates the store's activity_feed signal. Keep the backend WsEvent variant definition (it's in rs-core, not frontend).

- [ ] **Step 3: Delete dashboard.rs and remove from mod.rs**

```bash
rm leptos-ui/src/components/dashboard.rs
```

In `leptos-ui/src/components/mod.rs`, remove the `pub mod dashboard;` line.

- [ ] **Step 4: Rewrite operator_dashboard.rs — ControlBar**

Replace the ControlBar component. Keep the same logic (event selector, start/stop, state badge, timer) but use new CSS classes:

```rust
#[component]
fn ControlBar() -> impl IntoView {
    let store = use_context::<DashboardStore>().expect("DashboardStore");
    // ... existing signal logic for event selector, start/stop ...

    view! {
        <div class="control-bar">
            <select class="event-selector"
                on:change=move |ev| { /* existing event selection logic */ }
            >
                <option value="">"Select Event"</option>
                <For each=move || store.events_list.get()
                    key=|e| e.id
                    let:event
                >
                    <option value={event.id.to_string()}>{&event.name}</option>
                </For>
            </select>
            <button class="start-btn" /* existing click handler */ >"START"</button>
            <button class="stop-btn" /* existing click handler */ >"STOP"</button>
            <span class="state-badge" class:idle=/* ... */ class:streaming=/* ... */ >
                {move || pipeline_state_label()}
            </span>
            <span class="session-timer">{move || format_timer()}</span>
        </div>
    }
}
```

- [ ] **Step 5: Rewrite operator_dashboard.rs — Pipeline nodes**

Replace PipelineFlow + CacheBar with vertical Pipeline component:

```rust
#[component]
fn Pipeline() -> impl IntoView {
    let store = use_context::<DashboardStore>().expect("DashboardStore");

    // Derive node states from store signals
    let obs_status = move || store.obs_status.get();
    let inpoint = move || store.inpoint_connected.get();
    let chunk_stats = move || store.chunk_stats.get();
    let pipeline = move || store.pipeline_state.get();
    let delivery = move || store.delivery.get();

    view! {
        <div class="pipeline">
            // OBS Node
            <div class="pipeline-node" class:active=move || obs_status().connected>
                <div class="pipeline-node-left">
                    <div class="status-dot" class:active=move || obs_status().connected></div>
                    <span class="pipeline-node-label">"OBS"</span>
                </div>
                <span class="pipeline-node-metric">
                    {move || if obs_status().connected { "connected" } else { "disconnected" }}
                </span>
            </div>
            <div class="pipeline-connector">"\u{2502}"</div>

            // RTMP Node
            <div class="pipeline-node" class:active=move || inpoint()>
                <div class="pipeline-node-left">
                    <div class="status-dot" class:active=move || inpoint()></div>
                    <span class="pipeline-node-label">"RTMP"</span>
                </div>
                <span class="pipeline-node-metric">
                    {move || {
                        let stats = chunk_stats();
                        if stats.local_chunks > 0 {
                            format!("{} chunks", stats.local_chunks)
                        } else {
                            "idle".to_string()
                        }
                    }}
                </span>
            </div>
            <div class="pipeline-connector">"\u{2502}"</div>

            // Buffer Node (with inline progress bar)
            <div class="pipeline-node" class:active=move || pipeline().buffer_progress > 0.0
                 class:warning=move || pipeline().buffer_progress > 0.0 && pipeline().buffer_progress < 0.8>
                <div class="pipeline-node-left">
                    <div class="status-dot"
                        class:active=move || pipeline().buffer_progress >= 0.8
                        class:warning=move || pipeline().buffer_progress > 0.0 && pipeline().buffer_progress < 0.8
                    ></div>
                    <span class="pipeline-node-label">"BUFFER"</span>
                </div>
                <span class="pipeline-node-metric">
                    {move || format!("{:.0}s / {:.0}s",
                        pipeline().current_delay_secs,
                        pipeline().target_delay_secs)}
                </span>
            </div>
            // Inline buffer bar
            <Show when=move || pipeline().buffer_progress > 0.0>
                <div style="padding: 0 16px;">
                    <div class="buffer-bar">
                        <div class="buffer-bar-fill"
                            class:healthy=move || pipeline().buffer_progress >= 0.8
                            class:warning=move || pipeline().buffer_progress >= 0.5 && pipeline().buffer_progress < 0.8
                            class:critical=move || pipeline().buffer_progress < 0.5
                            style:width=move || format!("{}%", (pipeline().buffer_progress * 100.0).min(100.0))
                        ></div>
                    </div>
                </div>
            </Show>
            <div class="pipeline-connector">"\u{2502}"</div>

            // S3 → VPS Node
            <div class="pipeline-node"
                class:active=move || delivery().status == "running" || delivery().status == "delivering">
                <div class="pipeline-node-left">
                    <div class="status-dot"
                        class:active=move || delivery().status == "running" || delivery().status == "delivering"
                    ></div>
                    <span class="pipeline-node-label">"S3 \u{2192} VPS"</span>
                </div>
                <span class="pipeline-node-metric">
                    {move || {
                        let d = delivery();
                        if d.endpoint_count > 0 {
                            format!("{} | {} eps", d.status, d.endpoint_count)
                        } else {
                            d.status.clone()
                        }
                    }}
                </span>
            </div>

            // Endpoint tree
            <EndpointTree />
        </div>
    }
}
```

- [ ] **Step 6: Rewrite operator_dashboard.rs — EndpointTree**

```rust
#[component]
fn EndpointTree() -> impl IntoView {
    let store = use_context::<DashboardStore>().expect("DashboardStore");
    let delivery = move || store.delivery.get();
    let endpoints = move || delivery().endpoints;
    let is_delivering = move || {
        let status = delivery().status;
        status == "running" || status == "delivering"
    };

    view! {
        <div class="endpoint-tree">
            <For each=move || {
                let eps = endpoints();
                eps.into_iter().enumerate().collect::<Vec<_>>()
            }
                key=|(_, ep)| ep.alias.clone()
                let:item
            >
                {
                    let (idx, ep) = item;
                    let is_last = move || idx == endpoints().len().saturating_sub(1);
                    let connector = move || if is_last() { "\u{2514}\u{2500}\u{2500}" } else { "\u{251C}\u{2500}\u{2500}" };
                    let status_class = if !ep.alive {
                        "error"
                    } else if ep.stall_reason.is_some() {
                        "warning"
                    } else {
                        "ok"
                    };
                    let anomaly = {
                        let mut parts = Vec::new();
                        if ep.chunk_delay_secs > 30.0 {
                            parts.push(format!("+{:.0}s", ep.chunk_delay_secs));
                        }
                        if let Some(ref reason) = ep.stall_reason {
                            parts.push(reason.clone());
                        }
                        if ep.ffmpeg_restart_count > 0 {
                            parts.push(format!("ffmpeg x{}", ep.ffmpeg_restart_count));
                        }
                        if !ep.alive && ep.stall_reason.is_none() {
                            parts.push("DEAD".to_string());
                        }
                        parts.join(" | ")
                    };
                    let alias = ep.alias.clone();

                    view! {
                        <div class="endpoint-branch">
                            <span class="branch-connector">{connector}</span>
                            <div class={format!("endpoint-node {status_class}")}>
                                <div class={format!("status-dot {status_class}")}></div>
                                <span class="endpoint-alias">{&alias}</span>
                                <Show when=move || !anomaly.is_empty()>
                                    <span class="endpoint-anomaly">{&anomaly}</span>
                                </Show>
                                <Show when=is_delivering>
                                    <button class="btn-remove-endpoint"
                                        on:click=move |_| { /* call remove endpoint API */ }
                                    >"\u{00D7}"</button>
                                </Show>
                            </div>
                        </div>
                    }
                }
            </For>
        </div>

        // Add endpoint control
        <Show when=is_delivering>
            <AddEndpointControl />
        </Show>
    }
}
```

- [ ] **Step 7: Keep AddEndpointControl (minor restyle only)**

The existing `AddEndpointControl` component logic is correct. Only update its CSS classes to match the new theme (already handled in Task 4 CSS).

- [ ] **Step 8: Update the main OperatorDashboard component**

```rust
#[component]
pub fn OperatorDashboard() -> impl IntoView {
    // Keep existing store initialization and HTTP polling logic
    // ...

    view! {
        <div class="operator-dashboard">
            <ControlBar />
            <Pipeline />
        </div>
    }
}
```

Remove the `ActivityFeed` component entirely.

- [ ] **Step 9: Verify it compiles**

```bash
cd /home/newlevel/devel/restreamer/leptos-ui && trunk build 2>&1 | tail -20
```

Expected: successful build (warnings OK for now, errors must be fixed)

- [ ] **Step 10: Commit**

```bash
git add leptos-ui/src/
git commit -m "feat: rewrite dashboard as single-column pipeline flow with endpoint tree"
```

---

## Task 6: Restyle Header and Settings

**Files:**
- Modify: `leptos-ui/src/components/header.rs`
- Modify: `leptos-ui/src/components/settings.rs`

- [ ] **Step 1: Update header.rs CSS classes**

The header component structure stays the same, just update class names to match new CSS. Ensure the header uses `app-header`, `app-title`, `header-right`, `ws-indicator`, `header-nav-btn` classes.

- [ ] **Step 2: Update settings.rs CSS classes**

Update class names to use `settings-page`, `settings-section`, `settings-card`. No functional changes — just visual consistency with HUD theme.

- [ ] **Step 3: Verify build**

```bash
cd /home/newlevel/devel/restreamer/leptos-ui && trunk build 2>&1 | tail -20
```

- [ ] **Step 4: Commit**

```bash
git add leptos-ui/src/components/header.rs leptos-ui/src/components/settings.rs
git commit -m "feat: restyle header and settings page with HUD theme"
```

---

## Task 7: E2E Tests

**Files:**
- Modify: `e2e/frontend.spec.ts`

- [ ] **Step 1: Remove activity feed tests**

Remove the entire "Activity Feed" describe block and any assertions about `.activity-feed`, `.activity-entry`, `.activity-time`.

- [ ] **Step 2: Update pipeline flow tests**

Replace horizontal pipeline tests with vertical pipeline node tests:

```typescript
test('pipeline nodes render in vertical flow', async ({ page }) => {
    const consoleMessages: string[] = [];
    page.on('console', (msg) => {
        if (msg.type() === 'error' || msg.type() === 'warning') {
            consoleMessages.push(`[${msg.type()}] ${msg.text()}`);
        }
    });

    await page.goto('/');

    // Pipeline nodes exist
    const nodes = page.locator('.pipeline-node');
    await expect(nodes).toHaveCount(4); // OBS, RTMP, Buffer, S3→VPS

    // Node labels
    await expect(page.locator('.pipeline-node-label').nth(0)).toHaveText('OBS');
    await expect(page.locator('.pipeline-node-label').nth(1)).toHaveText('RTMP');
    await expect(page.locator('.pipeline-node-label').nth(2)).toHaveText('BUFFER');
    await expect(page.locator('.pipeline-node-label').nth(3)).toContainText('VPS');

    // Connectors between nodes
    const connectors = page.locator('.pipeline-connector');
    await expect(connectors).toHaveCount(3);

    expect(consoleMessages).toEqual([]);
});
```

- [ ] **Step 3: Update endpoint tree tests**

```typescript
test('endpoint tree shows branches when delivering', async ({ page }) => {
    const consoleMessages: string[] = [];
    page.on('console', (msg) => {
        if (msg.type() === 'error' || msg.type() === 'warning') {
            consoleMessages.push(`[${msg.type()}] ${msg.text()}`);
        }
    });

    await page.goto('/');

    // Broadcast delivery status with endpoints
    await page.evaluate(() => {
        window.__test_ws_send(JSON.stringify({
            type: 'DeliveryStatus',
            instance_name: 'test-vps',
            status: 'delivering',
            server_ip: '1.2.3.4',
            endpoint_count: 2,
            endpoints: [
                { alias: 'YT-Main', alive: true, current_chunk_id: 100, bytes_processed_total: 1000000, chunks_processed: 100, chunk_delay_secs: 12.0, stall_reason: null, ffmpeg_restart_count: 0, last_error: null, is_fast: false },
                { alias: 'FB-Stream', alive: true, current_chunk_id: 95, bytes_processed_total: 800000, chunks_processed: 95, chunk_delay_secs: 45.0, stall_reason: null, ffmpeg_restart_count: 0, last_error: null, is_fast: false }
            ]
        }));
    });

    // Endpoint branches appear
    const branches = page.locator('.endpoint-branch');
    await expect(branches).toHaveCount(2);

    // Aliases visible
    await expect(page.locator('.endpoint-alias').nth(0)).toHaveText('YT-Main');
    await expect(page.locator('.endpoint-alias').nth(1)).toHaveText('FB-Stream');

    // Tree connectors
    await expect(page.locator('.branch-connector').nth(0)).toHaveText('├──');
    await expect(page.locator('.branch-connector').nth(1)).toHaveText('└──');

    expect(consoleMessages).toEqual([]);
});

test('endpoint shows anomaly only when unhealthy', async ({ page }) => {
    const consoleMessages: string[] = [];
    page.on('console', (msg) => {
        if (msg.type() === 'error' || msg.type() === 'warning') {
            consoleMessages.push(`[${msg.type()}] ${msg.text()}`);
        }
    });

    await page.goto('/');

    await page.evaluate(() => {
        window.__test_ws_send(JSON.stringify({
            type: 'DeliveryStatus',
            instance_name: 'test-vps',
            status: 'delivering',
            server_ip: '1.2.3.4',
            endpoint_count: 2,
            endpoints: [
                { alias: 'YT-Main', alive: true, current_chunk_id: 100, bytes_processed_total: 1000000, chunks_processed: 100, chunk_delay_secs: 12.0, stall_reason: null, ffmpeg_restart_count: 0, last_error: null, is_fast: false },
                { alias: 'YT-Monitor', alive: false, current_chunk_id: 80, bytes_processed_total: 500000, chunks_processed: 80, chunk_delay_secs: 0.0, stall_reason: 'chunk_miss', ffmpeg_restart_count: 3, last_error: null, is_fast: true }
            ]
        }));
    });

    // Healthy endpoint: no anomaly text
    const healthyNode = page.locator('.endpoint-node').nth(0);
    await expect(healthyNode.locator('.endpoint-anomaly')).toHaveCount(0);

    // Unhealthy endpoint: shows anomaly
    const unhealthyNode = page.locator('.endpoint-node').nth(1);
    await expect(unhealthyNode.locator('.endpoint-anomaly')).toContainText('DEAD');
    await expect(unhealthyNode.locator('.endpoint-anomaly')).toContainText('ffmpeg x3');

    expect(consoleMessages).toEqual([]);
});
```

- [ ] **Step 4: Add mobile viewport test**

```typescript
test('mobile viewport renders without horizontal scroll', async ({ page }) => {
    const consoleMessages: string[] = [];
    page.on('console', (msg) => {
        if (msg.type() === 'error' || msg.type() === 'warning') {
            consoleMessages.push(`[${msg.type()}] ${msg.text()}`);
        }
    });

    await page.setViewportSize({ width: 375, height: 812 }); // iPhone SE
    await page.goto('/');

    // Page should not have horizontal scroll
    const scrollWidth = await page.evaluate(() => document.documentElement.scrollWidth);
    const clientWidth = await page.evaluate(() => document.documentElement.clientWidth);
    expect(scrollWidth).toBeLessThanOrEqual(clientWidth);

    // Pipeline nodes should be visible
    await expect(page.locator('.pipeline-node').first()).toBeVisible();

    expect(consoleMessages).toEqual([]);
});
```

- [ ] **Step 5: Add PWA manifest test**

```typescript
test('PWA manifest is served', async ({ page }) => {
    const response = await page.goto('/manifest.json');
    expect(response?.status()).toBe(200);
    const manifest = await response?.json();
    expect(manifest.name).toBe('Restreamer');
    expect(manifest.display).toBe('standalone');
    expect(manifest.theme_color).toBe('#0a0a0a');
});
```

- [ ] **Step 6: Update all existing tests that reference changed selectors**

Go through every remaining test and update:
- `.pipeline-flow` → `.pipeline`
- `.pipeline-node` (old horizontal) → `.pipeline-node` (same class, different layout)
- `.endpoint-card` → `.endpoint-node`
- `.cache-bar` → `.buffer-bar`
- Remove any `.activity-feed` references
- Update pipeline node count expectations (was 5 horizontal, now 4 vertical — OBS, RTMP, Buffer, S3→VPS)

- [ ] **Step 7: Commit**

```bash
git add e2e/frontend.spec.ts
git commit -m "test: update E2E tests for HUD pipeline layout and endpoint tree"
```

---

## Task 8: Cloudflare Tunnel Setup Documentation

**Files:**
- Create: `docs/cloudflare-tunnel-setup.md`

- [ ] **Step 1: Write tunnel setup guide**

```markdown
# Cloudflare Tunnel Setup for stream.lan

Domain: `streamsnv.newlevel.media`
Machine: stream.lan (Windows 11 IoT Enterprise LTSC)

## Prerequisites

- Cloudflare account with `newlevel.media` zone
- API Token: stored in password manager (same token as iem.newlevel.media)
- Account ID: `8f3efbc0edbe05bd6fdcab10cd63876a`
- Zone ID: `b9019ca528e573e62c2a110a45f45c74`

## Install cloudflared

```powershell
Invoke-WebRequest -Uri "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-windows-amd64.msi" -OutFile "$env:TEMP\cloudflared.msi"
msiexec /i "$env:TEMP\cloudflared.msi" /quiet
```

## Create Tunnel

```powershell
cloudflared tunnel login
cloudflared tunnel create restreamer
cloudflared tunnel route dns restreamer streamsnv.newlevel.media
```

## Configure

Create `C:\Users\newlevel\.cloudflared\config.yml`:

```yaml
tunnel: restreamer
credentials-file: C:\Users\newlevel\.cloudflared\<tunnel-id>.json

ingress:
  - hostname: streamsnv.newlevel.media
    service: http://localhost:8910
  - service: http_status:404
```

## Install as Service

```powershell
cloudflared service install
cloudflared service start
```

## TLS Certificates (Let's Encrypt)

Generate on any Linux machine with certbot:

```bash
pip install certbot-dns-cloudflare

# Create cloudflare.ini with API token
echo "dns_cloudflare_api_token = YOUR_TOKEN" > cloudflare.ini
chmod 600 cloudflare.ini

certbot certonly \
  --dns-cloudflare \
  --dns-cloudflare-credentials cloudflare.ini \
  -d streamsnv.newlevel.media

# Certs at /etc/letsencrypt/live/streamsnv.newlevel.media/
```

Upload to GitHub Secrets:
- `TLS_CERT_PEM` ← contents of `fullchain.pem`
- `TLS_KEY_PEM` ← contents of `privkey.pem`

CI deploys these to `C:\ProgramData\Restreamer\cert.pem` and `key.pem`.

## Verify

```
curl -I https://streamsnv.newlevel.media/api/v1/status
```
```

- [ ] **Step 2: Commit**

```bash
git add docs/cloudflare-tunnel-setup.md
git commit -m "docs: add Cloudflare Tunnel setup guide for streamsnv.newlevel.media"
```

---

## Task 9: CI — Deploy TLS Certificates

**Files:**
- Modify: `.github/workflows/ci.yml`

- [ ] **Step 1: Add TLS cert deployment to the deploy-stream-lan job**

In the deploy step that copies files to stream.lan, add cert deployment:

```yaml
- name: Deploy TLS certificates
  if: env.TLS_CERT_PEM != '' && env.TLS_KEY_PEM != ''
  env:
    TLS_CERT_PEM: ${{ secrets.TLS_CERT_PEM }}
    TLS_KEY_PEM: ${{ secrets.TLS_KEY_PEM }}
  shell: powershell
  run: |
    $configDir = "C:\ProgramData\Restreamer"
    [System.IO.File]::WriteAllText("$configDir\cert.pem", $env:TLS_CERT_PEM)
    [System.IO.File]::WriteAllText("$configDir\key.pem", $env:TLS_KEY_PEM)
    Write-Host "TLS certificates deployed to $configDir"
```

- [ ] **Step 2: Add TLS config to stream.lan config.json**

The config.json on stream.lan needs to be updated with TLS fields. This can be done via MCP tools or the deploy script:

```json
{
  "api": {
    "port": 8910,
    "bind": "0.0.0.0",
    "tls": true,
    "https_port": 443,
    "tls_cert": "cert.pem",
    "tls_key": "key.pem",
    "https_domain": "streamsnv.newlevel.media"
  }
}
```

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: deploy TLS certificates for HTTPS/PWA support"
```

---

## Task 10: Local Checks + Push

- [ ] **Step 1: Run all local checks**

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

- [ ] **Step 2: Fix any issues found**

- [ ] **Step 3: Push to dev and monitor CI**

```bash
git push origin dev
gh run list --branch dev --limit 3
```

Monitor until all jobs pass.

---

## Verification

1. **Desktop browser** — open `http://10.77.9.204:8910/`, verify HUD theme with monospace fonts, single-column pipeline, endpoint tree
2. **Mobile browser** — open same URL on phone (375px viewport), verify no horizontal scroll, touch targets ≥ 44px
3. **PWA install** — after HTTPS is configured, open `https://streamsnv.newlevel.media/` on phone, verify install prompt appears
4. **Pipeline status** — start a stream, verify OBS → RTMP → Buffer → S3/VPS nodes update with real-time data
5. **Endpoint tree** — verify endpoints branch from VPS node, healthy endpoints show only alias + green dot, unhealthy show anomaly text
6. **Settings page** — navigate to `/settings`, verify HUD theme consistency
7. **Console zero errors** — no browser console errors or warnings on any page
