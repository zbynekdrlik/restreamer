//! Screencast relay between a dashboard tab and one Chrome page (#388).
//!
//! Chrome -> dashboard: JPEG frames as binary messages, plus text messages
//! `{"t":"meta","w","h"}` (frame size changed), `{"t":"url","url"}` (the
//! page navigated) and `{"t":"clip","text"}` (the page copied text).
//! Dashboard -> Chrome: `{"t":"m"}` mouse, `{"t":"k"}` key,
//! `{"t":"paste","text"}`, `{"t":"copy"}`, `{"t":"nav","url"}`, `{"t":"back"}`, and
//! `{"t":"size","w","h"}` (the canvas size in CSS pixels: the page viewport
//! follows it, so the room fills the operator's window at full sharpness).
//! Anything else is dropped, and only the listed fields are forwarded.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use axum::extract::ws::{Message as AxMessage, WebSocket};
use base64::Engine;
use futures::{SinkExt, StreamExt};
use serde_json::{Map, Value, json};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message as TgMessage;

use super::cdp::{self, Cdp, TargetInfo};

const MOUSE_FIELDS: &[&str] = &[
    "type",
    "x",
    "y",
    "button",
    "clickCount",
    "modifiers",
    "deltaX",
    "deltaY",
];
const KEY_FIELDS: &[&str] = &[
    "type",
    "key",
    "code",
    "text",
    "windowsVirtualKeyCode",
    "modifiers",
];

/// The largest viewport a room asks Chrome for. Screencast frames follow
/// the viewport, up to this size.
pub const MAX_VIEW_W: u32 = 2560;
pub const MAX_VIEW_H: u32 = 1600;
/// The smallest: below it YT Studio and Live Producer are unusable anyway.
pub const MIN_VIEW_W: u32 = 320;
pub const MIN_VIEW_H: u32 = 240;

/// The page viewport for a dashboard canvas of `w` x `h` CSS pixels.
pub fn viewport(w: f64, h: f64) -> Value {
    // `as` saturates, and NaN becomes 0: the clamp then gives the minimum.
    let (w, h) = (w.round() as u32 as f64, h.round() as u32 as f64);
    // Too big: scale down to fit, keeping the aspect ratio, so the frames
    // are never drawn stretched on a 4K or ultrawide canvas.
    let fit = (f64::from(MAX_VIEW_W) / w)
        .min(f64::from(MAX_VIEW_H) / h)
        .min(1.0);
    let w = ((w * fit).round() as u32).clamp(MIN_VIEW_W, MAX_VIEW_W);
    let h = ((h * fit).round() as u32).clamp(MIN_VIEW_H, MAX_VIEW_H);
    json!({ "width": w, "height": h, "deviceScaleFactor": 1, "mobile": false })
}

/// The page binding the copy hook reports copied text through.
pub const CLIP_BINDING: &str = "__restreamerCopy";
/// The longest copied text relayed to the dashboard, in bytes.
pub const CLIP_MAX: usize = 64 * 1024;

/// The script that reports the page's copies through [`CLIP_BINDING`]: a
/// `copy` event (Ctrl+C, a page's `execCommand('copy')`) and
/// `navigator.clipboard.writeText` (YT Studio's "Copy" buttons). The copy
/// lands in the box Chrome's own clipboard, which the operator never sees.
pub fn copy_hook_source() -> String {
    const HOOK: &str = r#"(() => {
  if (window.__restreamerCopyHooked) return;
  window.__restreamerCopyHooked = true;
  const report = (t) => { try { if (typeof t === 'string' && t) BINDING(t); } catch (_) {} };
  const selected = () => {
    const a = document.activeElement;
    if (a && (a.tagName === 'INPUT' || a.tagName === 'TEXTAREA') && typeof a.selectionStart === 'number') {
      return a.value.slice(a.selectionStart, a.selectionEnd);
    }
    return String(window.getSelection() || '');
  };
  // A page handler may set its own data during the event: read it after the
  // event reached the window; a handler that stops it gets the selection.
  let pending = null;
  document.addEventListener('copy', () => {
    const sel = selected();
    pending = sel;
    setTimeout(() => { if (pending !== null) { pending = null; report(sel); } }, 0);
  }, true);
  window.addEventListener('copy', (e) => {
    const data = e.clipboardData && e.clipboardData.getData('text/plain');
    const sel = pending;
    pending = null;
    report(data || sel || selected());
  });
  const c = navigator.clipboard;
  if (c && c.writeText) {
    const write = c.writeText.bind(c);
    c.writeText = (t) => { report(String(t)); return write(t); };
  }
})()"#;
    HOOK.replace("BINDING", CLIP_BINDING)
}

/// The CDP method that sizes the page viewport.
pub const SET_VIEWPORT: &str = "Emulation.setDeviceMetricsOverride";

fn pick(v: &Value, fields: &[&str]) -> Value {
    let mut out = Map::new();
    for f in fields {
        if let Some(x) = v.get(*f).filter(|x| !x.is_null()) {
            out.insert((*f).to_string(), x.clone());
        }
    }
    Value::Object(out)
}

/// Translate one dashboard message into a CDP (method, params), or `None`
/// when it is not an allowed input.
pub fn client_to_cdp(v: &Value) -> Option<(&'static str, Value)> {
    match v.get("t")?.as_str()? {
        "m" => Some(("Input.dispatchMouseEvent", pick(v, MOUSE_FIELDS))),
        "k" => Some(("Input.dispatchKeyEvent", pick(v, KEY_FIELDS))),
        "paste" => Some((
            "Input.insertText",
            json!({ "text": v.get("text")?.as_str()? }),
        )),
        "nav" => {
            let url = v.get("url")?.as_str()?.trim();
            let url = if url.contains("://") {
                url.to_string()
            } else {
                format!("https://{url}")
            };
            (url.starts_with("https://") || url.starts_with("http://"))
                .then(|| ("Page.navigate", json!({ "url": url })))
        }
        "back" => Some((
            "Runtime.evaluate",
            json!({ "expression": "history.back()" }),
        )),
        // The page's own copy command, as a user gesture: the hook reports
        // what it copied.
        "copy" => Some((
            "Runtime.evaluate",
            json!({ "expression": "document.execCommand('copy')", "userGesture": true }),
        )),
        "size" => Some((
            SET_VIEWPORT,
            viewport(v.get("w")?.as_f64()?, v.get("h")?.as_f64()?),
        )),
        _ => None,
    }
}

/// What to send the dashboard for one CDP message from the page.
#[derive(Debug, PartialEq)]
pub enum PageOut {
    /// A frame: its size, JPEG bytes, and the session id to ack.
    Frame {
        w: u32,
        h: u32,
        jpeg: Vec<u8>,
        session_id: i64,
    },
    /// The main frame navigated.
    Url(String),
    /// The page copied this text.
    Clip(String),
    Ignore,
}

pub fn page_to_client(m: &Value) -> PageOut {
    match m.get("method").and_then(Value::as_str) {
        Some("Page.screencastFrame") => {
            let p = &m["params"];
            let md = &p["metadata"];
            let jpeg = p["data"]
                .as_str()
                .and_then(|d| base64::engine::general_purpose::STANDARD.decode(d).ok());
            match (jpeg, p["sessionId"].as_i64()) {
                (Some(jpeg), Some(session_id)) => PageOut::Frame {
                    w: md["deviceWidth"].as_f64().unwrap_or(0.0).round() as u32,
                    h: md["deviceHeight"].as_f64().unwrap_or(0.0).round() as u32,
                    jpeg,
                    session_id,
                },
                _ => PageOut::Ignore,
            }
        }
        Some("Runtime.bindingCalled") if m["params"]["name"] == CLIP_BINDING => {
            match m["params"]["payload"].as_str() {
                Some(t) if !t.is_empty() && t.len() <= CLIP_MAX => PageOut::Clip(t.into()),
                _ => PageOut::Ignore,
            }
        }
        Some("Page.frameNavigated") if m["params"]["frame"].get("parentId").is_none() => {
            m["params"]["frame"]["url"]
                .as_str()
                .map_or(PageOut::Ignore, |u| PageOut::Url(u.into()))
        }
        _ => PageOut::Ignore,
    }
}

/// The URL of a same-document navigation (YT Studio's page switches) of the
/// main frame. A page target's id is its main frame's id.
pub fn same_document_url(m: &Value, main_frame: &str) -> Option<String> {
    if m.get("method").and_then(Value::as_str) != Some("Page.navigatedWithinDocument") {
        return None;
    }
    let p = &m["params"];
    (p["frameId"].as_str() == Some(main_frame))
        .then(|| p["url"].as_str().map(str::to_string))
        .flatten()
}

/// The tabs the room tab `room` opened (`window.open`, a `target=_blank`
/// link) that already carry an http(s) URL: the room shows them in its own
/// tab, since the viewer only ever sees that one.
pub fn popups_of(pages: &[TargetInfo], room: &str) -> Vec<TargetInfo> {
    pages
        .iter()
        .filter(|t| {
            t.opener_id.as_deref() == Some(room)
                && (t.url.starts_with("https://") || t.url.starts_with("http://"))
        })
        .cloned()
        .collect()
}

/// How often the relay looks for tabs the room opened.
const POPUP_POLL: std::time::Duration = std::time::Duration::from_secs(1);

fn screencast_params() -> Value {
    json!({ "format": "jpeg", "quality": 75, "maxWidth": MAX_VIEW_W, "maxHeight": MAX_VIEW_H })
}

/// Told every main-frame URL the page reaches.
pub type UrlSink = Arc<dyn Fn(String) + Send + Sync>;

/// [`relay_with`], keeping no URLs.
pub async fn relay(client: WebSocket, cdp: Cdp, target_id: String, start_url: String) {
    relay_with(client, cdp, target_id, start_url, Arc::new(|_| {})).await;
}

/// Relay `client` to the page `target_id` until either side closes.
pub async fn relay_with(
    client: WebSocket,
    cdp: Cdp,
    target_id: String,
    start_url: String,
    on_url: UrlSink,
) {
    let page = match tokio_tungstenite::connect_async(cdp.page_ws_url(&target_id)).await {
        Ok((ws, _)) => ws,
        Err(e) => {
            tracing::warn!("control rooms: cannot attach to tab {target_id}: {e}");
            return;
        }
    };
    tracing::info!("control rooms: viewer attached to tab {target_id}");
    let (mut page_tx, mut page_rx) = page.split();
    let (mut client_tx, mut client_rx) = client.split();
    let seq = Arc::new(AtomicU64::new(0));
    // The viewport the viewer asked for, re-applied after a navigation.
    let view: Arc<std::sync::Mutex<Option<Value>>> = Arc::default();

    // One writer per socket: the reader tasks hand their messages over.
    let (to_page, mut to_page_rx) = mpsc::unbounded_channel::<String>();
    let page_writer = tokio::spawn(async move {
        while let Some(text) = to_page_rx.recv().await {
            if page_tx.send(TgMessage::Text(text.into())).await.is_err() {
                break;
            }
        }
    });
    let call = {
        let to_page = to_page.clone();
        let seq = Arc::clone(&seq);
        move |method: &str, params: Value| {
            let id = seq.fetch_add(1, Ordering::Relaxed) + 1;
            let _ = to_page.send(cdp::request(id, method, params));
        }
    };

    call("Page.enable", json!({}));
    // Copies in the page come back through a binding (Runtime.bindingCalled).
    let hook = copy_hook_source();
    call("Runtime.enable", json!({}));
    call("Runtime.addBinding", json!({ "name": CLIP_BINDING }));
    call(
        "Page.addScriptToEvaluateOnNewDocument",
        json!({ "source": hook }),
    );
    call("Runtime.evaluate", json!({ "expression": hook }));
    call("Page.bringToFront", json!({}));
    call("Page.startScreencast", screencast_params());

    let (to_client, mut to_client_rx) = mpsc::unbounded_channel::<AxMessage>();
    let _ = to_client.send(AxMessage::Text(
        json!({ "t": "url", "url": start_url }).to_string().into(),
    ));
    let client_writer = tokio::spawn(async move {
        while let Some(m) = to_client_rx.recv().await {
            if client_tx.send(m).await.is_err() {
                break;
            }
        }
    });

    let from_page = {
        let call = call.clone();
        let to_client = to_client.clone();
        let view = Arc::clone(&view);
        let main_frame = target_id.clone();
        async move {
            let mut last = (0, 0);
            while let Some(Ok(msg)) = page_rx.next().await {
                let TgMessage::Text(text) = msg else { continue };
                let Ok(v) = serde_json::from_str::<Value>(&text) else {
                    continue;
                };
                if let Some(u) = same_document_url(&v, &main_frame) {
                    on_url(u.clone());
                    let m = json!({ "t": "url", "url": u }).to_string();
                    if to_client.send(AxMessage::Text(m.into())).is_err() {
                        break;
                    }
                    continue;
                }
                match page_to_client(&v) {
                    PageOut::Frame {
                        w,
                        h,
                        jpeg,
                        session_id,
                    } => {
                        if (w, h) != last {
                            last = (w, h);
                            let meta = json!({ "t": "meta", "w": w, "h": h }).to_string();
                            if to_client.send(AxMessage::Text(meta.into())).is_err() {
                                break;
                            }
                        }
                        if to_client.send(AxMessage::Binary(jpeg.into())).is_err() {
                            break;
                        }
                        call(
                            "Page.screencastFrameAck",
                            json!({ "sessionId": session_id }),
                        );
                    }
                    PageOut::Url(u) => {
                        // A cross-site navigation swaps the renderer, and the
                        // screencast stops with the old one: start it again,
                        // in the viewer's viewport.
                        let size = view.lock().unwrap_or_else(|p| p.into_inner()).clone();
                        if let Some(size) = size {
                            call(SET_VIEWPORT, size);
                        }
                        call("Page.startScreencast", screencast_params());
                        on_url(u.clone());
                        let m = json!({ "t": "url", "url": u }).to_string();
                        if to_client.send(AxMessage::Text(m.into())).is_err() {
                            break;
                        }
                    }
                    PageOut::Clip(text) => {
                        tracing::info!(
                            "control rooms: tab {main_frame} copied {} characters",
                            text.chars().count()
                        );
                        let m = json!({ "t": "clip", "text": text }).to_string();
                        if to_client.send(AxMessage::Text(m.into())).is_err() {
                            break;
                        }
                    }
                    PageOut::Ignore => {}
                }
            }
        }
    };
    let follow_popups = {
        let cdp = cdp.clone();
        let room = target_id.clone();
        let call = call.clone();
        async move {
            loop {
                tokio::time::sleep(POPUP_POLL).await;
                let pages = match cdp.pages().await {
                    Ok(p) => p,
                    Err(e) => {
                        tracing::debug!("control rooms: tab list for popups failed: {e:#}");
                        continue;
                    }
                };
                for p in popups_of(&pages, &room) {
                    tracing::info!(
                        "control rooms: tab {room} opened {}; showing it in the room tab",
                        p.url
                    );
                    call("Page.navigate", json!({ "url": p.url }));
                    if let Err(e) = cdp
                        .browser_call("Target.closeTarget", json!({ "targetId": p.target_id }))
                        .await
                    {
                        tracing::warn!(
                            "control rooms: closing popup {} failed: {e:#}",
                            p.target_id
                        );
                    }
                }
            }
        }
    };
    let tab = target_id.clone();
    let from_client = async move {
        // A Close frame ends the stream, which ends this loop.
        while let Some(Ok(msg)) = client_rx.next().await {
            if let AxMessage::Text(text) = msg
                && let Some((method, params)) = serde_json::from_str::<Value>(&text)
                    .ok()
                    .as_ref()
                    .and_then(client_to_cdp)
            {
                if method == SET_VIEWPORT {
                    tracing::info!(
                        "control rooms: tab {tab} viewport {}x{}",
                        params["width"],
                        params["height"]
                    );
                    *view.lock().unwrap_or_else(|p| p.into_inner()) = Some(params.clone());
                    call(method, params);
                    // The frames follow the new size from the next start on.
                    call("Page.startScreencast", screencast_params());
                } else {
                    call(method, params);
                }
            }
        }
    };
    tokio::select! {
        _ = from_page => {}
        _ = from_client => {}
        _ = follow_popups => {}
    }
    drop(to_page);
    drop(to_client);
    page_writer.abort();
    client_writer.abort();
    tracing::info!("control rooms: viewer detached from tab {target_id}");
}

#[cfg(test)]
#[path = "relay_tests.rs"]
mod tests;
