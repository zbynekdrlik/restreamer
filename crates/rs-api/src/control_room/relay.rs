//! Screencast relay between a dashboard tab and one Chrome page (#388).
//!
//! Chrome -> dashboard: JPEG frames as binary messages, plus text messages
//! `{"t":"meta","w","h"}` (frame size changed), `{"t":"url","url"}` (the
//! page navigated) and `{"t":"clip","text"}` (the page copied text).
//! Dashboard -> Chrome: `{"t":"m"}` mouse, `{"t":"k"}` key,
//! `{"t":"paste","text"}`, `{"t":"copy"}`, `{"t":"nav","url"}`, `{"t":"back"}`,
//! `{"t":"reload","hard"?}`, `{"t":"drawn"}` (the last frame was drawn: only
//! then does Chrome get its ack and send the next), and
//! `{"t":"size","w","h"}` (the canvas size in CSS pixels: the page viewport
//! follows it, so the room fills the operator's window at full sharpness).
//! Anything else is dropped, and only the listed fields are forwarded.
//!
//! Popups: a tab the page opens is moved into the room tab (#399), except a
//! popup that can still reach its opener (a Google sign-in or "Overte svoju
//! totožnosť"): the room shows that popup until it closes, so the page that
//! opened it can take the proof back (#405).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use axum::extract::ws::{Message as AxMessage, WebSocket};
use base64::Engine;
use futures::{SinkExt, StreamExt};
use serde_json::{Map, Value, json};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message as TgMessage;

use super::cdp::{self, Cdp, TargetInfo};
use super::kiosk_picker::{BoxDialog, PickTarget, is_own_press, opens_box_dialog, spawn_pick};
use super::page_hooks::{copy_hook_source, hls_cors_hook_source};
use super::popups::{PopupEnd, PopupMove, popup_end, popup_message, popup_moves, switch_notices};
use super::visibility::{ShownTabs, hide_if_visible, hide_others};

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

/// The largest file the operator can put into a room (a thumbnail is a few MB).
pub const FILE_MAX: usize = 20 * 1024 * 1024;
/// How long an uploaded file stays on the box.
pub const UPLOAD_MAX_AGE: std::time::Duration = std::time::Duration::from_secs(3600);

/// A file the dashboard sent into the room, and where it was dropped.
#[derive(Debug, PartialEq)]
pub struct Upload {
    pub name: String,
    pub bytes: Vec<u8>,
    pub x: f64,
    pub y: f64,
}

/// A safe file name for an uploaded file: one path component of ASCII
/// letters, digits, `.`, `_` and `-`, no leading dot, at most 80 bytes with
/// the end (the extension) kept.
pub fn upload_name(name: &str) -> String {
    let safe: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
                c
            } else {
                '_'
            }
        })
        .collect();
    let safe = safe.trim_start_matches('.');
    // ASCII only by now, so any byte offset is a char boundary.
    let safe = &safe[safe.len().saturating_sub(80)..];
    if safe.is_empty() {
        "file".into()
    } else {
        safe.into()
    }
}

/// The file in a dashboard `{t:"file"}` message.
pub fn decode_file(v: &Value) -> Result<Upload, String> {
    let b64 = v["b64"].as_str().ok_or("chýba obsah súboru")?;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(b64)
        .map_err(|e| format!("poškodený súbor ({e})"))?;
    if bytes.is_empty() {
        return Err("prázdny súbor".into());
    }
    if bytes.len() > FILE_MAX {
        return Err(format!("súbor je väčší ako {} MB", FILE_MAX >> 20));
    }
    Ok(Upload {
        name: upload_name(v["name"].as_str().unwrap_or_default()),
        bytes,
        x: v["x"].as_f64().unwrap_or(0.0),
        y: v["y"].as_f64().unwrap_or(0.0),
    })
}

/// The CDP calls that put the file at `path` into the page: into the pending
/// file chooser `chooser`, else dropped on the page at (x, y).
pub fn file_actions(
    path: &str,
    chooser: Option<i64>,
    x: f64,
    y: f64,
) -> Vec<(&'static str, Value)> {
    if let Some(node) = chooser {
        return vec![(
            "DOM.setFileInputFiles",
            json!({ "files": [path], "backendNodeId": node }),
        )];
    }
    let data = json!({ "items": [], "files": [path], "dragOperationsMask": 1 });
    ["dragEnter", "dragOver", "drop"]
        .into_iter()
        .map(|t| {
            (
                "Input.dispatchDragEvent",
                json!({ "type": t, "x": x, "y": y, "data": data }),
            )
        })
        .collect()
}

/// An upload of this age is past `max_age` (exactly `max_age` still stays).
pub fn is_expired(age: std::time::Duration, max_age: std::time::Duration) -> bool {
    age > max_age
}

/// Delete uploaded files older than `max_age`. A missing directory is fine.
pub fn prune_uploads(dir: &std::path::Path, max_age: std::time::Duration) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for e in entries.flatten() {
        let old = e
            .metadata()
            .and_then(|m| m.modified())
            .ok()
            .and_then(|t| t.elapsed().ok())
            .is_some_and(|age| is_expired(age, max_age));
        if old && let Err(err) = std::fs::remove_file(e.path()) {
            tracing::warn!(
                "control rooms: cannot delete old upload {:?}: {err}",
                e.path()
            );
        }
    }
}

/// Write an uploaded file into `dir` under a unique name.
fn store_upload(dir: &std::path::Path, up: &Upload) -> std::io::Result<std::path::PathBuf> {
    std::fs::create_dir_all(dir)?;
    prune_uploads(dir, UPLOAD_MAX_AGE);
    let path = dir.join(format!("{}-{}", uuid::Uuid::new_v4().simple(), up.name));
    std::fs::write(&path, &up.bytes)?;
    Ok(path)
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
        // F5 as a key never reloads (Chrome handles it in the browser UI).
        "reload" => Some((
            "Page.reload",
            json!({ "ignoreCache": v.get("hard").and_then(Value::as_bool).unwrap_or(false) }),
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
    /// The page opened a file chooser (`node`: its `<input type=file>`).
    FileChooser {
        node: Option<i64>,
        mode: String,
    },
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
        Some("Page.fileChooserOpened") => PageOut::FileChooser {
            node: m["params"]["backendNodeId"].as_i64(),
            mode: m["params"]["mode"].as_str().unwrap_or_default().into(),
        },
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

/// Put the file of a dashboard `{t:"file"}` message into the page (#399):
/// into the pending file chooser, else dropped where it was let go. Returns
/// the reply for the dashboard, `file-done` or `file-error`.
fn put_file(
    v: &Value,
    uploads: &std::path::Path,
    chooser: &std::sync::Mutex<Option<i64>>,
    call: &impl Fn(&str, Value),
    tab: &str,
) -> Value {
    let up = match decode_file(v) {
        Ok(up) => up,
        Err(e) => {
            tracing::warn!("control rooms: tab {tab}: file refused: {e}");
            return json!({ "t": "file-error", "error": e });
        }
    };
    let path = match store_upload(uploads, &up) {
        Ok(p) => p,
        Err(e) => {
            tracing::error!(
                "control rooms: tab {tab}: cannot store {} in {uploads:?}: {e}",
                up.name
            );
            return json!({ "t": "file-error", "error": format!("súbor sa nedal uložiť ({e})") });
        }
    };
    let node = chooser.lock().unwrap_or_else(|p| p.into_inner()).take();
    let how = if node.is_some() { "chooser" } else { "drop" };
    tracing::info!(
        "control rooms: tab {tab}: {} ({} bytes) into the page by {how}",
        up.name,
        up.bytes.len()
    );
    for (method, params) in file_actions(&path.to_string_lossy(), node, up.x, up.y) {
        call(method, params);
    }
    json!({ "t": "file-done", "name": up.name, "how": how })
}

/// [`relay_room`], with uploads in the system temp directory.
pub async fn relay_with(
    client: WebSocket,
    cdp: Cdp,
    target_id: String,
    start_url: String,
    on_url: UrlSink,
) {
    let uploads = std::env::temp_dir().join("restreamer-room-uploads");
    relay_room(
        client,
        cdp,
        target_id,
        start_url,
        on_url,
        uploads,
        ShownTabs::default(),
        None,
    )
    .await;
}

/// The `{"t":"url"}` message telling the dashboard which page it sees.
fn url_message(url: &str) -> AxMessage {
    AxMessage::Text(json!({ "t": "url", "url": url }).to_string().into())
}

/// The JSON `msg` as a text message to the dashboard.
fn text_message(msg: &Value) -> AxMessage {
    AxMessage::Text(msg.to_string().into())
}

/// Why one [`relay_tab`] session ended.
enum TabEnd {
    /// The viewer left, or the room tab is gone: the relay is over.
    Over,
    /// The shown tab opened a popup that reports back to it: show that.
    Popup(TargetInfo),
    /// The shown popup is done: closed (`None`), or landed on a room page
    /// that the tab under it takes over.
    PopupDone(Option<String>),
    /// The viewer asked to leave the shown popup (a popup that never closes
    /// itself, a popped-out chat).
    LeavePopup,
}

/// Relay `client` to the page `target_id` until either side closes. Files
/// the operator puts into the room are written to `uploads`. A popup that
/// reports back to the tab that opened it (a sign-in, "Overte svoju
/// totožnosť") is shown instead of that tab until it closes, with the tab
/// kept alive behind it (#405).
#[allow(clippy::too_many_arguments)]
pub async fn relay_room(
    client: WebSocket,
    cdp: Cdp,
    target_id: String,
    start_url: String,
    on_url: UrlSink,
    uploads: std::path::PathBuf,
    shown_tabs: ShownTabs,
    dialog: Option<BoxDialog>,
) {
    let (mut client_tx, mut client_rx) = client.split();
    let (to_client, mut to_client_rx) = mpsc::unbounded_channel::<AxMessage>();
    let _ = to_client.send(url_message(&start_url));
    let client_writer = tokio::spawn(async move {
        while let Some(m) = to_client_rx.recv().await {
            if client_tx.send(m).await.is_err() {
                break;
            }
        }
    });
    // The viewport the viewer asked for, re-applied after a navigation and
    // on every tab the room shows.
    let view: Arc<std::sync::Mutex<Option<Value>>> = Arc::default();
    // A popup's pages (a sign-in, /reauth) are never the room's last page.
    let no_urls: UrlSink = Arc::new(|_| {});
    // The popups shown over the room tab, innermost last.
    let mut popups: Vec<String> = Vec::new();
    // Popups the room is done with: never shown or moved again, even when
    // Chrome refused to close one (no show/hand-over loop).
    let mut done: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut navigate_to: Option<String> = None;
    loop {
        let shown = TabSession {
            cdp: &cdp,
            tab: popups.last().map_or(target_id.as_str(), String::as_str),
            is_popup: !popups.is_empty(),
            to_client: &to_client,
            view: &view,
            on_url: if popups.is_empty() {
                on_url.clone()
            } else {
                no_urls.clone()
            },
            uploads: &uploads,
            skip: &done,
            shown_tabs: &shown_tabs,
            dialog: &dialog,
        };
        let end = relay_tab(shown, &mut client_rx, navigate_to.take()).await;
        let left = match end {
            TabEnd::Over => break,
            TabEnd::Popup(p) => {
                tracing::info!(
                    "control rooms: room {target_id}: {} opened {} as a popup that reports back to it; showing the popup until it closes",
                    popups.last().map_or(target_id.as_str(), String::as_str),
                    p.url
                );
                let _ = to_client.send(url_message(&p.url));
                let _ = to_client.send(text_message(&popup_message(true)));
                popups.push(p.target_id);
                continue;
            }
            TabEnd::PopupDone(landed) => {
                let p = popups.pop().unwrap_or_default();
                match &landed {
                    Some(url) => {
                        tracing::info!(
                            "control rooms: popup {p} reached {url}; the tab under it takes it over"
                        );
                        close_popup(&cdp, &p).await;
                    }
                    None => tracing::info!("control rooms: popup {p} closed"),
                }
                navigate_to = landed;
                p
            }
            TabEnd::LeavePopup => {
                let p = popups.pop().unwrap_or_default();
                tracing::info!("control rooms: the viewer left popup {p}; closing it");
                close_popup(&cdp, &p).await;
                p
            }
        };
        done.insert(left);
        let now = popups.last().map_or(target_id.as_str(), String::as_str);
        tracing::info!("control rooms: room {target_id}: showing tab {now} again");
        let _ = to_client.send(text_message(&popup_message(!popups.is_empty())));
        let url = match &navigate_to {
            Some(u) => Some(u.clone()),
            None => cdp
                .pages()
                .await
                .ok()
                .and_then(|ps| ps.into_iter().find(|t| t.target_id == now))
                .map(|t| t.url),
        };
        if let Some(u) = url {
            let _ = to_client.send(url_message(&u));
        }
    }
    drop(to_client);
    client_writer.abort();
    tracing::info!("control rooms: viewer detached from tab {target_id}");
}

/// Close a popup the room is done with; a refusal is only logged (the room
/// never shows it again either way).
async fn close_popup(cdp: &Cdp, popup: &str) {
    if let Err(e) = cdp
        .browser_call("Target.closeTarget", json!({ "targetId": popup }))
        .await
    {
        tracing::warn!("control rooms: closing popup {popup} failed: {e:#}");
    }
}

/// The tab one [`relay_tab`] session shows, and what it shares with the
/// sessions before and after it.
struct TabSession<'a> {
    cdp: &'a Cdp,
    /// The room tab, or the popup of it shown now.
    tab: &'a str,
    is_popup: bool,
    to_client: &'a mpsc::UnboundedSender<AxMessage>,
    view: &'a Arc<std::sync::Mutex<Option<Value>>>,
    on_url: UrlSink,
    uploads: &'a std::path::Path,
    /// Popups the room is done with.
    skip: &'a std::collections::HashSet<String>,
    /// The tabs every relay shows now; the others get hidden (#407).
    shown_tabs: &'a ShownTabs,
    /// The box's own file dialog, for a viewer on the box (#414).
    dialog: &'a Option<BoxDialog>,
}

/// Relay the viewer to one tab until the viewer leaves, the tab goes away,
/// or the room switches between its tab and a popup. `navigate_to` is
/// loaded into the tab first (a popup's room page, handed over).
async fn relay_tab(
    shown: TabSession<'_>,
    client_rx: &mut futures::stream::SplitStream<WebSocket>,
    navigate_to: Option<String>,
) -> TabEnd {
    let TabSession {
        cdp,
        tab,
        is_popup,
        to_client,
        view,
        on_url,
        uploads,
        skip,
        shown_tabs,
        dialog,
    } = shown;
    let page = match tokio_tungstenite::connect_async(cdp.page_ws_url(tab)).await {
        Ok((ws, _)) => ws,
        Err(e) => {
            tracing::warn!("control rooms: cannot attach to tab {tab}: {e}");
            // A popup that cannot be shown counts as done.
            return if is_popup {
                TabEnd::PopupDone(None)
            } else {
                TabEnd::Over
            };
        }
    };
    tracing::info!("control rooms: viewer attached to tab {tab}");
    // #407: counted as shown, so no other relay hides it.
    let shown_guard = shown_tabs.show(tab);
    let leaving = tab.to_string();
    let (mut page_tx, mut page_rx) = page.split();
    let seq = Arc::new(AtomicU64::new(0));
    // The <input type=file> of a file chooser the page opened that no file
    // went into yet.
    let chooser: Arc<std::sync::Mutex<Option<i64>>> = Arc::default();
    // When the viewer last clicked or pressed a key in the page: only a
    // chooser that followed it opens the box's dialog (#414).
    let last_press: Arc<std::sync::Mutex<Option<std::time::Instant>>> = Arc::default();
    // The screencast session of the frame the dashboard has not drawn yet.
    // Chrome sends the next frame only after an ack, so acking on "drawn"
    // paces the frames to the viewer (no queue grows behind a slow one).
    let unacked: Arc<std::sync::Mutex<Option<i64>>> = Arc::default();
    // A file is being put into this tab right now.
    let uploading = Arc::new(std::sync::atomic::AtomicBool::new(false));

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
            let id = cdp::next_id(&seq);
            let _ = to_page.send(cdp::request(id, method, params));
        }
    };

    call("Page.enable", json!({}));
    // Copies in the page come back through a binding (Runtime.bindingCalled).
    call("Runtime.enable", json!({}));
    call("Runtime.addBinding", json!({ "name": CLIP_BINDING }));
    // The page scripts go into later documents AND the one already open; the
    // HLS one lets Studio's live preview play (#415).
    for hook in [copy_hook_source(), hls_cors_hook_source().to_string()] {
        call(
            "Page.addScriptToEvaluateOnNewDocument",
            json!({ "source": hook }),
        );
        call("Runtime.evaluate", json!({ "expression": hook }));
    }
    // The page's file choosers ("Upload thumbnail") come to the operator.
    call(
        "Page.setInterceptFileChooserDialog",
        json!({ "enabled": true }),
    );
    // #407: a room a viewer opens is never left frozen, even when an earlier
    // hide got its `frozen` through but not its `active` (idempotent).
    call("Page.setWebLifecycleState", json!({ "state": "active" }));
    call("Page.bringToFront", json!({}));
    // #407: the other rooms are hidden once Chrome activated this window
    // (activation re-shows the window it leaves); focus emulation keeps
    // this one visible while the relay is attached.
    let front_id = seq.load(Ordering::Relaxed);
    call(
        "Emulation.setFocusEmulationEnabled",
        json!({ "enabled": true }),
    );
    // A tab shown after the first one gets the viewer's size at once.
    let size = view.lock().unwrap_or_else(|p| p.into_inner()).clone();
    if let Some(size) = size {
        call(SET_VIEWPORT, size);
    }
    call("Page.startScreencast", screencast_params());
    if let Some(url) = navigate_to {
        call("Page.navigate", json!({ "url": url }));
    }

    let from_page = {
        let call = call.clone();
        let to_client = to_client.clone();
        let view = Arc::clone(view);
        let chooser = Arc::clone(&chooser);
        let unacked = Arc::clone(&unacked);
        let (dialog, last_press, seq) = (dialog.clone(), Arc::clone(&last_press), Arc::clone(&seq));
        let (page_weak, viewer_weak) = (to_page.downgrade(), to_client.downgrade());
        let main_frame = tab.to_string();
        let (hide_cdp, hide_shown) = (cdp.clone(), shown_tabs.clone());
        async move {
            let mut last = (0, 0);
            while let Some(Ok(msg)) = page_rx.next().await {
                let TgMessage::Text(text) = msg else { continue };
                let Ok(v) = serde_json::from_str::<Value>(&text) else {
                    continue;
                };
                if v.get("id").and_then(Value::as_u64) == Some(front_id) {
                    let (cdp, shown) = (hide_cdp.clone(), hide_shown.clone());
                    tokio::spawn(async move { hide_others(&cdp, &shown).await });
                    continue;
                }
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
                        // Acked when the dashboard reports it drawn.
                        *unacked.lock().unwrap_or_else(|p| p.into_inner()) = Some(session_id);
                    }
                    PageOut::Url(u) => {
                        // A new document: a file chooser of the old one is gone.
                        let had = chooser.lock().unwrap_or_else(|p| p.into_inner()).take();
                        if had.is_some() {
                            tracing::info!(
                                "control rooms: tab {main_frame} navigated; its file chooser is gone"
                            );
                            let m = json!({ "t": "file-chooser-closed" }).to_string();
                            if to_client.send(AxMessage::Text(m.into())).is_err() {
                                break;
                            }
                        }
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
                    PageOut::FileChooser { node, mode } => {
                        tracing::info!(
                            "control rooms: tab {main_frame} opened a file chooser ({mode}, input {node:?})"
                        );
                        *chooser.lock().unwrap_or_else(|p| p.into_inner()) = node;
                        // #414: a chooser this viewer's own click or key caused
                        // opens the box's dialog; while it is open (for any
                        // room) the bar shows instead.
                        let since = last_press
                            .lock()
                            .unwrap_or_else(|p| p.into_inner())
                            .map(|t| t.elapsed());
                        if let Some(dialog) = &dialog
                            && opens_box_dialog(node, since)
                            && !dialog.open.swap(true, Ordering::SeqCst)
                        {
                            let to = PickTarget {
                                chooser: Arc::clone(&chooser),
                                page: page_weak.clone(),
                                seq: Arc::clone(&seq),
                                viewer: viewer_weak.clone(),
                                tab: main_frame.clone(),
                            };
                            spawn_pick(dialog.clone(), mode, to);
                            continue;
                        }
                        let m = json!({ "t": "file-chooser", "mode": mode }).to_string();
                        if to_client.send(AxMessage::Text(m.into())).is_err() {
                            break;
                        }
                    }
                    PageOut::Ignore => {}
                }
            }
            // The tab is gone: a closed popup hands back to the tab under it.
            if is_popup {
                TabEnd::PopupDone(None)
            } else {
                TabEnd::Over
            }
        }
    };
    let follow_popups = {
        let cdp = cdp.clone();
        let shown = tab.to_string();
        let call = call.clone();
        async move {
            // Popups already moved into this tab, even if closing one failed.
            let mut moved: std::collections::HashSet<String> = std::collections::HashSet::new();
            loop {
                tokio::time::sleep(POPUP_POLL).await;
                let pages = match cdp.pages().await {
                    Ok(p) => p,
                    Err(e) => {
                        tracing::debug!("control rooms: tab list for popups failed: {e:#}");
                        continue;
                    }
                };
                if is_popup && let Some(end) = popup_end(&pages, &shown) {
                    return TabEnd::PopupDone(match end {
                        PopupEnd::Closed => None,
                        PopupEnd::Landed(url) => Some(url),
                    });
                }
                for m in popup_moves(&pages, &shown, skip) {
                    let p = match m {
                        PopupMove::ShowInPlace(p) => return TabEnd::Popup(p),
                        PopupMove::MoveIntoTab(p) => p,
                    };
                    if !moved.insert(p.target_id.clone()) {
                        continue;
                    }
                    tracing::info!(
                        "control rooms: tab {shown} opened {}; moving it into tab {shown}",
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
    let (chooser_left, uploading_left) = (Arc::clone(&chooser), Arc::clone(&uploading));
    let tab = tab.to_string();
    let to_viewer = to_client.clone();
    let from_client = async move {
        // A Close frame ends the stream, which ends this loop.
        while let Some(Ok(msg)) = client_rx.next().await {
            let AxMessage::Text(text) = msg else { continue };
            let Ok(v) = serde_json::from_str::<Value>(&text) else {
                continue;
            };
            if v["t"] == "drawn" {
                let pending = unacked.lock().unwrap_or_else(|p| p.into_inner()).take();
                if let Some(session_id) = pending {
                    call(
                        "Page.screencastFrameAck",
                        json!({ "sessionId": session_id }),
                    );
                }
                continue;
            }
            if v["t"] == "close-popup" {
                if is_popup {
                    return TabEnd::LeavePopup;
                }
                continue;
            }
            if v["t"] == "file" {
                // Decoding and writing up to 20 MB must not hold up the relay.
                let (uploads, chooser, call, tab) = (
                    uploads.to_path_buf(),
                    Arc::clone(&chooser),
                    call.clone(),
                    tab.clone(),
                );
                uploading.store(true, Ordering::Relaxed);
                let reply = tokio::task::spawn_blocking(move || {
                    put_file(&v, &uploads, &chooser, &call, &tab)
                })
                .await
                .unwrap_or_else(
                    |e| json!({ "t": "file-error", "error": format!("interná chyba ({e})") }),
                );
                uploading.store(false, Ordering::Relaxed);
                if to_viewer
                    .send(AxMessage::Text(reply.to_string().into()))
                    .is_err()
                {
                    break;
                }
                continue;
            }
            if is_own_press(&v) {
                *last_press.lock().unwrap_or_else(|p| p.into_inner()) =
                    Some(std::time::Instant::now());
            }
            if let Some((method, params)) = client_to_cdp(&v) {
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
        // The viewer closed the room.
        TabEnd::Over
    };
    let end = tokio::select! {
        e = from_page => e,
        e = from_client => e,
        e = follow_popups => e,
    };
    let chooser_pending = chooser_left
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .take()
        .is_some();
    for n in switch_notices(
        matches!(end, TabEnd::Over),
        chooser_pending,
        uploading_left.load(Ordering::Relaxed),
    ) {
        let _ = to_client.send(text_message(&n));
    }
    // #407: a room the viewer left stops rendering, unless another relay
    // still shows it. A switch to or from a popup needs nothing here: the
    // next session hides the tab it leaves.
    drop(shown_guard);
    if matches!(end, TabEnd::Over) {
        let (cdp, shown) = (cdp.clone(), shown_tabs.clone());
        tokio::spawn(async move {
            // Another viewer still watches it, or opened it again meanwhile.
            if shown.is_shown(&leaving) {
                return;
            }
            if let Err(e) = hide_if_visible(&cdp, &leaving).await {
                tracing::warn!("control rooms: hiding tab {leaving} failed: {e:#}");
            }
        });
    }
    // The writer sends what is still queued (a file's last commands), then
    // ends with the last sender.
    drop(to_page);
    drop(page_writer);
    end
}

#[cfg(test)]
#[path = "relay_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "relay_file_tests.rs"]
mod file_tests;
