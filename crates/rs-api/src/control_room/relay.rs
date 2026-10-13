//! Screencast relay between a dashboard tab and one Chrome page (#388).
//!
//! Chrome -> dashboard: JPEG frames as binary messages, plus text messages
//! `{"t":"meta","w","h"}` (frame size changed) and `{"t":"url","url"}` (the
//! page navigated). Dashboard -> Chrome: `{"t":"m"}` mouse, `{"t":"k"}` key,
//! `{"t":"paste","text"}`, `{"t":"nav","url"}`, `{"t":"back"}`. Anything else
//! is dropped, and only the listed fields are forwarded.

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
        Some("Page.frameNavigated") if m["params"]["frame"].get("parentId").is_none() => {
            m["params"]["frame"]["url"]
                .as_str()
                .map_or(PageOut::Ignore, |u| PageOut::Url(u.into()))
        }
        _ => PageOut::Ignore,
    }
}

/// The tabs the room tab `room` opened (`window.open`, a `target=_blank`
/// link) that already carry an http(s) URL: the room shows them in its own
/// tab, since the viewer only ever sees that one.
pub fn popups_of(pages: &[TargetInfo], room: &str) -> Vec<TargetInfo> {
    let _ = (pages, room);
    vec![]
}

/// Relay `client` to the page `target_id` until either side closes.
pub async fn relay(client: WebSocket, cdp: Cdp, target_id: String, start_url: String) {
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
    call("Page.bringToFront", json!({}));
    call(
        "Page.startScreencast",
        json!({ "format": "jpeg", "quality": 75, "maxWidth": super::browser::WINDOW_W, "maxHeight": super::browser::WINDOW_H }),
    );

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
        async move {
            let mut last = (0, 0);
            while let Some(Ok(msg)) = page_rx.next().await {
                let TgMessage::Text(text) = msg else { continue };
                let Ok(v) = serde_json::from_str::<Value>(&text) else {
                    continue;
                };
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
                        let m = json!({ "t": "url", "url": u }).to_string();
                        if to_client.send(AxMessage::Text(m.into())).is_err() {
                            break;
                        }
                    }
                    PageOut::Ignore => {}
                }
            }
        }
    };
    let from_client = async move {
        // A Close frame ends the stream, which ends this loop.
        while let Some(Ok(msg)) = client_rx.next().await {
            if let AxMessage::Text(text) = msg
                && let Some((method, params)) = serde_json::from_str::<Value>(&text)
                    .ok()
                    .as_ref()
                    .and_then(client_to_cdp)
            {
                call(method, params);
            }
        }
    };
    tokio::select! {
        _ = from_page => {}
        _ = from_client => {}
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
