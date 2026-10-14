//! Test-only fake of Chrome's debugging port (#388): `/json/version`, the
//! browser websocket (`Target.getTargets`) and page websockets
//! (`Network.getCookies`, `Target.closeTarget` drops the target, everything
//! else answered `{}`). Every request is
//! recorded, and the target list and cookies can be changed while it runs.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, State};
use axum::response::IntoResponse;
use axum::routing::get;
use futures::StreamExt;
use serde_json::{Value, json};

#[derive(Default)]
pub struct FakeState {
    /// `Target.getTargets` infos.
    pub targets: Vec<Value>,
    /// target id -> cookie names `Network.getCookies` reports there.
    pub cookies: HashMap<String, Vec<String>>,
    /// (socket path, request) of every CDP request received.
    pub requests: Vec<(String, Value)>,
    /// Target ids whose `Target.closeTarget` fails with a CDP error.
    pub refuse_close: Vec<String>,
}

#[derive(Clone)]
pub struct FakeChrome {
    pub addr: String,
    pub state: Arc<Mutex<FakeState>>,
}

pub fn page(id: &str, ctx: &str, url: &str) -> Value {
    json!({"targetId": id, "type": "page", "url": url, "title": id, "browserContextId": ctx})
}

impl FakeChrome {
    /// Serve on a free port.
    pub async fn start() -> Self {
        Self::start_on(tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap()).await
    }

    pub async fn start_on(listener: tokio::net::TcpListener) -> Self {
        let addr = listener.local_addr().unwrap().to_string();
        let state = Arc::new(Mutex::new(FakeState::default()));
        let app = axum::Router::new()
            .route("/json/version", get(version))
            .route("/devtools/{kind}/{id}", get(socket))
            .with_state((addr.clone(), Arc::clone(&state)));
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Self { addr, state }
    }

    pub fn set_targets(&self, targets: Vec<Value>) {
        self.state.lock().unwrap().targets = targets;
    }

    pub fn methods(&self) -> Vec<String> {
        let s = self.state.lock().unwrap();
        s.requests
            .iter()
            .map(|(_, r)| r["method"].as_str().unwrap_or("").to_string())
            .collect()
    }
}

type Shared = (String, Arc<Mutex<FakeState>>);

async fn version(State((addr, _)): State<Shared>) -> impl IntoResponse {
    axum::Json(
        json!({"Browser": "Chrome/fake", "webSocketDebuggerUrl": format!("ws://{addr}/devtools/browser/b1")}),
    )
}

async fn socket(
    State((_, state)): State<Shared>,
    Path((kind, id)): Path<(String, String)>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    ws.on_upgrade(move |s| serve_socket(s, state, format!("{kind}/{id}"), id))
}

async fn serve_socket(mut ws: WebSocket, state: Arc<Mutex<FakeState>>, path: String, id: String) {
    while let Some(Ok(Message::Text(text))) = ws.next().await {
        let req: Value = serde_json::from_str(&text).unwrap();
        let refused = req["method"] == "Target.closeTarget" && {
            let s = state.lock().unwrap();
            s.refuse_close
                .iter()
                .any(|t| req["params"]["targetId"] == *t)
        };
        if refused {
            state
                .lock()
                .unwrap()
                .requests
                .push((path.clone(), req.clone()));
            let reply = json!({"id": req["id"], "error": {"code": -32000, "message": "refused"}});
            if ws
                .send(Message::Text(reply.to_string().into()))
                .await
                .is_err()
            {
                break;
            }
            continue;
        }
        let result = {
            let mut s = state.lock().unwrap();
            s.requests.push((path.clone(), req.clone()));
            match req["method"].as_str() {
                Some("Target.getTargets") => json!({"targetInfos": s.targets}),
                Some("Target.closeTarget") => {
                    let id = req["params"]["targetId"].clone();
                    s.targets.retain(|t| t["targetId"] != id);
                    json!({"success": true})
                }
                Some("Network.getCookies") => {
                    let names = s.cookies.get(&id).cloned().unwrap_or_default();
                    json!({"cookies": names.iter().map(|n| json!({"name": n, "value": "x"})).collect::<Vec<_>>()})
                }
                _ => json!({}),
            }
        };
        let reply = json!({"id": req["id"], "result": result});
        if ws
            .send(Message::Text(reply.to_string().into()))
            .await
            .is_err()
        {
            break;
        }
    }
}

/// A fake `chrome`/`Xvfb` executable (unix): appends its arguments (one run
/// per line) to `<dir>/<name>.args`, writes `$DISPLAY` to `<dir>/<name>.display`,
/// touches `touch` if given, and exits with `exit_code`.
#[cfg(unix)]
pub fn fake_bin(
    dir: &std::path::Path,
    name: &str,
    exit_code: i32,
    touch: Option<&std::path::Path>,
) -> std::path::PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let p = dir.join(name);
    let args = dir.join(format!("{name}.args"));
    let disp = dir.join(format!("{name}.display"));
    let touch = touch
        .map(|t| format!("touch '{}'\n", t.display()))
        .unwrap_or_default();
    std::fs::write(
        &p,
        format!(
            "#!/bin/sh\necho \"$*\" >> '{}'\nprintf '%s' \"$DISPLAY\" > '{}'\n{touch}exit {exit_code}\n",
            args.display(),
            disp.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o755)).unwrap();
    p
}
