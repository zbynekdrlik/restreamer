//! Control-room tabs (#388): one tab per YT/FB destination of the active
//! event (every destination when no event is active). A tab shows that
//! destination's YT Studio / FB Live Producer page from the box-hosted
//! Chrome: a screencast on a canvas, with mouse, keyboard, paste and files
//! (a page's file chooser, a drop, a pasted image: #399) sent back.

use leptos::prelude::*;
use leptos::task::spawn_local;
use wasm_bindgen::JsValue;
use wasm_bindgen::prelude::*;

use crate::api::{self, RoomAccount, RoomDestination, RoomsResponse};

#[wasm_bindgen(inline_js = r#"
// Put text on the operator's clipboard. navigator.clipboard needs a secure
// context (http://<box-ip> is not one); execCommand needs a recent user
// gesture. Resolves whether either worked.
export async function cr_copy(text) {
  try {
    if (navigator.clipboard && window.isSecureContext) { await navigator.clipboard.writeText(text); return true; }
  } catch (_) { /* refused: try the older way */ }
  const prev = document.activeElement;
  const ta = document.createElement('textarea');
  ta.value = text;
  ta.setAttribute('readonly', '');
  ta.style.position = 'fixed';
  ta.style.opacity = '0';
  document.body.appendChild(ta);
  ta.select();
  let ok = false;
  try { ok = document.execCommand('copy'); } catch (_) { ok = false; }
  ta.remove();
  if (prev && prev.focus) prev.focus();
  return ok;
}

export function cr_attach(canvas, url, onText) {
  const g = canvas.getContext('2d');
  let meta = { w: canvas.width, h: canvas.height };
  const ws = new WebSocket(url);
  ws.binaryType = 'blob';
  // The canvas fills the window below it; the box-hosted page gets exactly
  // that viewport, so the room is as big as the window and stays sharp.
  let sentSize = '';
  const fit = () => {
    const r = canvas.getBoundingClientRect();
    if (r.width < 1) return;
    canvas.style.height = Math.max(240, Math.floor(window.innerHeight - (r.top + window.scrollY) - 12)) + 'px';
    // The viewport is the drawing area: the box minus its border.
    const w = canvas.clientWidth, h = canvas.clientHeight;
    const s = w + 'x' + h;
    if (s !== sentSize && ws.readyState === 1) { sentSize = s; ws.send(JSON.stringify({ t: 'size', w, h })); }
  };
  let fitTimer = 0;
  const refit = () => { clearTimeout(fitTimer); fitTimer = setTimeout(fit, 150); };
  const ro = new ResizeObserver(refit);
  ro.observe(canvas.parentElement);
  window.addEventListener('resize', refit);
  ws.onopen = () => { onText(JSON.stringify({ t: 'status', s: 'open' })); fit(); canvas.focus(); };
  ws.onclose = () => onText(JSON.stringify({ t: 'status', s: 'closed' }));
  ws.onmessage = async (e) => {
    if (typeof e.data === 'string') {
      try {
        const m = JSON.parse(e.data);
        if (m.t === 'clip' && typeof m.text === 'string') {
          const ok = await cr_copy(m.text);
          onText(JSON.stringify({ t: 'clip', text: m.text, ok }));
          return;
        }
        if (m.t === 'meta' && m.w > 0 && m.h > 0) {
          meta = m;
          if (canvas.width !== m.w || canvas.height !== m.h) { canvas.width = m.w; canvas.height = m.h; }
        }
      } catch (_) { return; }
      onText(e.data);
      return;
    }
    try {
      const bmp = await createImageBitmap(e.data);
      g.drawImage(bmp, 0, 0, canvas.width, canvas.height);
      bmp.close();
      canvas.dataset.frames = String((Number(canvas.dataset.frames) || 0) + 1);
    } finally {
      // Chrome sends the next frame only now: frames come at the pace this
      // viewer draws them, never queued behind it.
      if (ws.readyState === 1) ws.send('{"t":"drawn"}');
    }
  };
  const send = (o) => { if (ws.readyState === 1) ws.send(JSON.stringify(o)); };
  const pos = (e) => { const r = canvas.getBoundingClientRect(); return { x: (e.clientX - r.left - canvas.clientLeft) * meta.w / canvas.clientWidth, y: (e.clientY - r.top - canvas.clientTop) * meta.h / canvas.clientHeight }; };
  const mods = (e) => (e.altKey ? 1 : 0) | (e.ctrlKey ? 2 : 0) | (e.metaKey ? 4 : 0) | (e.shiftKey ? 8 : 0);
  const btn = (b) => ['left', 'middle', 'right'][b] || 'none';
  // #419: a move names the button held down (`buttons` bit 1 left, 4 middle,
  // 2 right): Chrome drags a scrollbar thumb only then, a bare move is a hover.
  const held = (b) => (b & 1 ? 'left' : b & 4 ? 'middle' : b & 2 ? 'right' : 'none');
  const move = (e) => ({ t: 'm', type: 'mouseMoved', ...pos(e), button: held(e.buttons), buttons: e.buttons, modifiers: mods(e) });
  const release = (e) => ({ t: 'm', type: 'mouseReleased', ...pos(e), button: btn(e.button), buttons: e.buttons, clickCount: e.detail || 1, modifiers: mods(e) });
  let lastMove = 0;
  // A press in the room: its moves and its release go to the page even once
  // the pointer has left the canvas (a scrollbar drag runs past its edge).
  let dragging = false;
  const outside = {
    mousemove: (e) => { if (!dragging || e.target === canvas) return; const n = Date.now(); if (n - lastMove < 30) return; lastMove = n; send(move(e)); },
    mouseup: (e) => { if (!dragging || e.target === canvas) return; dragging = false; send(release(e)); },
  };
  // Keys the room handles itself (reload, copy): their keyup stays here too.
  const local = new Set();
  // Where the pointer last was over the room: a pasted file is dropped there.
  let lastPos = { x: 0, y: 0 };
  // #399: a file (thumbnail) goes to the page as base64; the relay puts it
  // into the page's file chooser or drops it at (x, y).
  const MAX_FILE = 20 * 1024 * 1024;
  const sendFile = (file, p) => {
    if (!file) return;
    if (file.size > MAX_FILE) { onText(JSON.stringify({ t: 'file-error', error: 'súbor je väčší ako 20 MB' })); return; }
    const r = new FileReader();
    r.onload = () => {
      const s = String(r.result);
      send({ t: 'file', name: file.name || 'obrazok.png', type: file.type, b64: s.slice(s.indexOf(',') + 1), x: p.x, y: p.y });
    };
    r.onerror = () => onText(JSON.stringify({ t: 'file-error', error: 'súbor sa nedal prečítať' }));
    r.readAsDataURL(file);
  };
  const h = {
    mousedown: (e) => { canvas.focus(); dragging = true; send({ t: 'm', type: 'mousePressed', ...pos(e), button: btn(e.button), buttons: e.buttons, clickCount: e.detail || 1, modifiers: mods(e) }); e.preventDefault(); },
    mouseup: (e) => { dragging = e.buttons !== 0; send(release(e)); },
    mousemove: (e) => { lastPos = pos(e); const n = Date.now(); if (n - lastMove < 30) return; lastMove = n; send(move(e)); },
    dragover: (e) => e.preventDefault(),
    drop: (e) => { e.preventDefault(); canvas.focus(); lastPos = pos(e); sendFile(e.dataTransfer && e.dataTransfer.files[0], lastPos); },
    wheel: (e) => { send({ t: 'm', type: 'mouseWheel', ...pos(e), deltaX: e.deltaX, deltaY: e.deltaY, modifiers: mods(e) }); e.preventDefault(); },
    contextmenu: (e) => e.preventDefault(),
    keydown: (e) => {
      if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'v') return;
      // Reload the room's page (F5, Ctrl+R; Shift skips the cache): as a key it
      // would never reach Chrome's reload.
      if (e.key === 'F5' || ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'r')) {
        local.add(e.code);
        send({ t: 'reload', hard: e.shiftKey });
        onText(JSON.stringify({ t: 'reloading' }));
        e.preventDefault();
        return;
      }
      // Copy: the page copies its selection, and the relay sends the text back.
      if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'c') { local.add(e.code); send({ t: 'copy' }); e.preventDefault(); return; }
      const printable = e.key.length === 1 && !e.ctrlKey && !e.metaKey;
      send({ t: 'k', type: printable ? 'keyDown' : 'rawKeyDown', key: e.key, code: e.code, text: printable ? e.key : undefined, windowsVirtualKeyCode: e.keyCode, modifiers: mods(e) });
      e.preventDefault();
    },
    keyup: (e) => { if (local.delete(e.code)) { e.preventDefault(); return; } send({ t: 'k', type: 'keyUp', key: e.key, code: e.code, windowsVirtualKeyCode: e.keyCode, modifiers: mods(e) }); e.preventDefault(); },
  };
  for (const [k, f] of Object.entries(h)) canvas.addEventListener(k, f, k === 'wheel' ? { passive: false } : undefined);
  for (const [k, f] of Object.entries(outside)) window.addEventListener(k, f);
  const paste = (e) => {
    if (document.activeElement !== canvas) return;
    const file = e.clipboardData.files && e.clipboardData.files[0];
    if (file) sendFile(file, lastPos);
    else send({ t: 'paste', text: e.clipboardData.getData('text') });
    e.preventDefault();
  };
  document.addEventListener('paste', paste);
  return {
    send: (s) => { if (ws.readyState === 1) ws.send(s); },
    sendInput: (input) => { sendFile(input.files && input.files[0], lastPos); input.value = ''; },
    close: () => {
      for (const [k, f] of Object.entries(h)) canvas.removeEventListener(k, f);
      for (const [k, f] of Object.entries(outside)) window.removeEventListener(k, f);
      document.removeEventListener('paste', paste);
      ro.disconnect();
      window.removeEventListener('resize', refit);
      clearTimeout(fitTimer);
      ws.onclose = null;
      ws.close();
    },
  };
}
"#)]
extern "C" {
    type CrHandle;
    fn cr_attach(canvas: &JsValue, url: &str, on_text: &Closure<dyn FnMut(String)>) -> CrHandle;
    fn cr_copy(text: &str) -> js_sys::Promise;
    #[wasm_bindgen(method)]
    fn send(this: &CrHandle, json: &str);
    #[wasm_bindgen(method)]
    fn close(this: &CrHandle);
    #[wasm_bindgen(method, js_name = sendInput)]
    fn send_input(this: &CrHandle, input: &JsValue);
}

/// A live viewer; closing it on drop releases the socket and the listeners
/// when its tab's owner is disposed.
struct Viewer {
    handle: CrHandle,
    _on_text: Closure<dyn FnMut(String)>,
}

impl Drop for Viewer {
    fn drop(&mut self) {
        self.handle.close();
    }
}

/// The text a destination's tab shows.
pub fn tab_label(d: &RoomDestination) -> String {
    format!("{} · {}", api::service_badge(&d.service_type), d.alias)
}

/// The account line under a destination.
pub fn account_line(d: &RoomDestination) -> String {
    match d.account.as_deref().filter(|a| !a.is_empty()) {
        Some(a) if d.sure => a.to_string(),
        Some(a) => format!("{a} (?)"),
        None => "účet nezistený".into(),
    }
}

/// The sign-in dot of a destination.
pub fn signed_in_text(d: &RoomDestination) -> &'static str {
    match d.signed_in {
        Some(true) => "prihlásený",
        Some(false) => "neprihlásený",
        None => "",
    }
}

/// The room status while its page reloads.
const RELOADING: &str = "obnovujem stránku…";

/// The room status after a copy reached the operator's clipboard, or not.
pub fn clip_status(ok: bool) -> &'static str {
    if ok {
        "skopírované do schránky"
    } else {
        "schránka odmietla kópiu: klikni Kopírovať"
    }
}

/// Tab strip + the selected room. `selected` = `None` shows the dashboard.
#[component]
pub fn ControlRoomTabs(selected: RwSignal<Option<i64>>) -> impl IntoView {
    let rooms: RwSignal<RoomsResponse> = RwSignal::new(RoomsResponse::default());
    let refresh = move || {
        spawn_local(async move {
            match api::list_control_rooms().await {
                Ok(r) => {
                    rooms.try_set(r);
                }
                Err(e) => leptos::logging::warn!("control rooms: list failed: {e}"),
            }
        });
    };
    refresh();
    crate::utils::interval_until_disposed(10_000, refresh);

    // A tab whose destination left the event falls back to the dashboard.
    Effect::new(move |_| {
        let r = rooms.get();
        if let Some(id) = selected.get_untracked()
            && !r.destinations.iter().any(|d| d.endpoint_id == id)
        {
            selected.set(None);
        }
    });

    view! {
        <Show when=move || !rooms.get().destinations.is_empty()>
            <nav class="cr-tabs" data-testid="control-room-tabs">
                <button
                    class="cr-tab"
                    class:cr-tab--active=move || selected.get().is_none()
                    data-testid="cr-tab-dashboard"
                    on:click=move |_| selected.set(None)
                >
                    "Dashboard"
                </button>
                <For
                    each=move || rooms.get().destinations
                    key=|d| (d.endpoint_id, d.alias.clone(), d.account.clone(), d.signed_in)
                    children=move |d: RoomDestination| {
                        let id = d.endpoint_id;
                        let svc = if d.service_type == "FB" { "cr-badge cr-badge--fb" } else { "cr-badge cr-badge--yt" };
                        let dot = match d.signed_in {
                            Some(true) => "cr-dot cr-dot--on",
                            Some(false) => "cr-dot cr-dot--off",
                            None => "cr-dot",
                        };
                        view! {
                            <button
                                class="cr-tab"
                                class:cr-tab--active=move || selected.get() == Some(id)
                                data-testid=format!("cr-tab-{id}")
                                title=account_line(&d)
                                aria-label=tab_label(&d)
                                on:click=move |_| selected.set(Some(id))
                            >
                                <span class=svc>{api::service_badge(&d.service_type)}</span>
                                <span class="cr-tab__alias">{d.alias.clone()}</span>
                                <span class=dot title=signed_in_text(&d)></span>
                            </button>
                        }
                    }
                />
                {move || rooms.get().event.map(|e| view! {
                    <span class="cr-tabs__event">{format!("Event: {}", e.name)}</span>
                })}
            </nav>
            {move || {
                let id = selected.get()?;
                let d = rooms.get_untracked().destinations.into_iter().find(|d| d.endpoint_id == id)?;
                Some(view! { <ControlRoomView dest=d /> })
            }}
        </Show>
    }
}

/// One destination's room: header, URL bar, and the screencast canvas.
#[component]
fn ControlRoomView(dest: RoomDestination) -> impl IntoView {
    let canvas = NodeRef::<leptos::html::Canvas>::new();
    let status: RwSignal<String> = RwSignal::new("otváram…".into());
    let url: RwSignal<String> = RwSignal::new(dest.url.clone());
    // The last text the page copied; shown with a button that copies it again.
    let clip: RwSignal<Option<String>> = RwSignal::new(None);
    // The page opened a file chooser ("Upload thumbnail", #399).
    let wants_file = RwSignal::new(false);
    // The room shows a popup of its page (a sign-in, "Overte svoju
    // totožnosť", #405) instead of the page itself.
    let in_popup = RwSignal::new(false);
    let viewer = StoredValue::new_local(None::<Viewer>);
    let editing = RwSignal::new(false);
    let acct_input = RwSignal::new(dest.account.clone().unwrap_or_default());
    let endpoint_id = dest.endpoint_id;

    let attach = move || {
        status.set("otváram…".into());
        // A new relay tells again whether a popup is shown.
        in_popup.set(false);
        spawn_local(async move {
            let target = match api::open_control_room(endpoint_id).await {
                Ok(t) => t,
                Err(e) => {
                    status.try_set(format!("nepodarilo sa otvoriť: {e}"));
                    return;
                }
            };
            let Some(Some(el)) = canvas.try_get_untracked() else {
                return;
            };
            let on_text = Closure::<dyn FnMut(String)>::new(move |text: String| {
                let Ok(m) = serde_json::from_str::<serde_json::Value>(&text) else {
                    return;
                };
                match m.get("t").and_then(|t| t.as_str()) {
                    Some("url") => {
                        if let Some(u) = m.get("url").and_then(|u| u.as_str()) {
                            url.try_set(u.to_string());
                        }
                        // The reloaded page has navigated: it is back.
                        if status.try_get_untracked().as_deref() == Some(RELOADING) {
                            status.try_set("pripojené".into());
                        }
                    }
                    Some("clip") => {
                        let text = m.get("text").and_then(|t| t.as_str()).unwrap_or_default();
                        let ok = m.get("ok").and_then(|o| o.as_bool()).unwrap_or(false);
                        clip.try_set(Some(text.to_string()));
                        status.try_set(clip_status(ok).into());
                    }
                    Some("reloading") => {
                        status.try_set(RELOADING.into());
                    }
                    Some("file-chooser") => {
                        wants_file.try_set(true);
                        status.try_set("stránka chce súbor".into());
                    }
                    Some("file-chooser-closed") => {
                        wants_file.try_set(false);
                    }
                    Some("popup") => {
                        let open = m.get("open").and_then(|o| o.as_bool()).unwrap_or(false);
                        in_popup.try_set(open);
                        status.try_set(
                            if open { "vyskakovacie okno stránky" } else { "pripojené" }.into(),
                        );
                    }
                    Some("file-done") => {
                        let name = m.get("name").and_then(|n| n.as_str()).unwrap_or_default();
                        wants_file.try_set(false);
                        status.try_set(format!("vložené: {name}"));
                    }
                    Some("file-error") => {
                        let e = m.get("error").and_then(|e| e.as_str()).unwrap_or_default();
                        status.try_set(format!("súbor sa nevložil: {e}"));
                    }
                    Some("status") => {
                        let s = if m.get("s").and_then(|s| s.as_str()) == Some("open") {
                            "pripojené"
                        } else {
                            "odpojené"
                        };
                        status.try_set(s.into());
                    }
                    _ => {}
                }
            });
            let handle = cr_attach(
                &JsValue::from(el),
                &api::control_room_ws_url(&target),
                &on_text,
            );
            let v = Viewer {
                handle,
                _on_text: on_text,
            };
            // Disposed meanwhile: dropping `v` closes it at once.
            if viewer.try_update_value(|slot| *slot = Some(v)).is_none() {
                leptos::logging::log!("control rooms: tab closed before the viewer attached");
            }
        });
    };
    attach();

    let send = move |json: String| {
        viewer.with_value(|v| {
            if let Some(v) = v {
                v.handle.send(&json);
            }
        });
    };
    // The editor changes only the email; the channel the room opens is kept.
    // A StoredValue keeps the closure Copy (it is used inside <Show>).
    let keep = StoredValue::new((dest.channel.clone(), dest.channel_name.clone()));
    let save_account = move |_| {
        let a = acct_input.get_untracked().trim().to_string();
        let account = RoomAccount {
            account: (!a.is_empty()).then_some(a),
            channel: keep.get_value().0,
            channel_name: keep.get_value().1,
            sure: true,
        };
        spawn_local(async move {
            if let Err(e) = api::set_control_room_account(endpoint_id, &account).await {
                status.try_set(format!("účet sa neuložil: {e}"));
            }
            editing.try_set(false);
        });
    };
    let copy_again = move |_| {
        let Some(text) = clip.get_untracked() else {
            return;
        };
        // Called inside the click, so the browser sees the user gesture.
        let promise = cr_copy(&text);
        spawn_local(async move {
            let ok = wasm_bindgen_futures::JsFuture::from(promise)
                .await
                .ok()
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            status.try_set(clip_status(ok).into());
        });
    };
    let svc_class = if dest.service_type == "FB" {
        "cr-badge cr-badge--fb"
    } else {
        "cr-badge cr-badge--yt"
    };

    view! {
        <section class="cr-room" data-testid="control-room">
            <header class="cr-room__head">
                <span class=svc_class>{api::service_badge(&dest.service_type)}</span>
                <b class="cr-room__alias" data-testid="cr-room-alias">{dest.alias.clone()}</b>
                {dest.channel_name.clone().map(|c| view! { <span class="cr-room__channel">{c}</span> })}
                <span class="cr-room__account" data-testid="cr-room-account" title="Prihlás sa týmto účtom">
                    {account_line(&dest)}
                </span>
                <button class="cr-btn" on:click=move |_| editing.update(|e| *e = !*e)>"Zmeniť účet"</button>
                <span class="cr-room__status" data-testid="cr-room-status">{move || status.get()}</span>
            </header>
            <Show when=move || editing.get()>
                <div class="cr-room__edit">
                    <input
                        class="cr-input"
                        data-testid="cr-account-input"
                        placeholder="e-mail účtu"
                        prop:value=move || acct_input.get()
                        on:input=move |ev| acct_input.set(event_target_value(&ev))
                    />
                    <button class="cr-btn" data-testid="cr-account-save" on:click=save_account>"Uložiť"</button>
                </div>
            </Show>
            <div class="cr-room__bar">
                <button class="cr-btn" title="Späť" on:click=move |_| send(r#"{"t":"back"}"#.into())>"←"</button>
                <input
                    class="cr-input cr-room__url"
                    data-testid="cr-url"
                    prop:value=move || url.get()
                    on:input=move |ev| url.set(event_target_value(&ev))
                />
                <button
                    class="cr-btn"
                    on:click=move |_| send(serde_json::json!({"t": "nav", "url": url.get_untracked()}).to_string())
                >
                    "Otvoriť URL"
                </button>
                <button
                    class="cr-btn"
                    data-testid="cr-reload"
                    title="Obnoviť stránku (F5)"
                    on:click=move |_| {
                        send(r#"{"t":"reload"}"#.into());
                        status.set(RELOADING.into());
                    }
                >
                    "⟳ Obnoviť"
                </button>
                <button class="cr-btn" title="Znova pripojiť obraz" on:click=move |_| {
                    viewer.set_value(None);
                    attach();
                }>"↻"</button>
            </div>
            <Show when=move || in_popup.get()>
                <div class="cr-room__clip" data-testid="cr-popup-bar">
                    <span>"Stránka otvorila vyskakovacie okno (napr. overenie totožnosti). Po dokončení sa zavrie samo."</span>
                    <button
                        class="cr-btn"
                        data-testid="cr-popup-close"
                        on:click=move |_| send(r#"{"t":"close-popup"}"#.into())
                    >
                        "✕ Zavrieť okno a späť na stránku"
                    </button>
                </div>
            </Show>
            <Show when=move || wants_file.get()>
                <div class="cr-room__clip" data-testid="cr-file-bar">
                    <span>"Stránka chce súbor:"</span>
                    <input
                        type="file"
                        data-testid="cr-file-input"
                        on:change=move |ev| {
                            let input = event_target::<web_sys::HtmlInputElement>(&ev);
                            viewer.with_value(|v| {
                                if let Some(v) = v {
                                    v.handle.send_input(&input);
                                }
                            });
                        }
                    />
                </div>
            </Show>
            <Show when=move || clip.get().is_some()>
                <div class="cr-room__clip">
                    <span>"Skopírované v miestnosti:"</span>
                    <input
                        class="cr-input cr-room__url"
                        data-testid="cr-clip"
                        readonly=true
                        prop:value=move || clip.get().unwrap_or_default()
                    />
                    <button class="cr-btn" data-testid="cr-clip-copy" on:click=copy_again>"Kopírovať"</button>
                </div>
            </Show>
            <canvas
                class="cr-room__screen"
                data-testid="cr-canvas"
                tabindex="0"
                width="1600"
                height="900"
                node_ref=canvas
            ></canvas>
        </section>
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dest(account: Option<&str>, sure: bool, signed_in: Option<bool>) -> RoomDestination {
        RoomDestination {
            endpoint_id: 1,
            alias: "YT NLCH 4K".into(),
            service_type: "YT_RTMP".into(),
            account: account.map(str::to_string),
            sure,
            channel: None,
            channel_name: None,
            url: "https://studio.youtube.com/".into(),
            target_id: None,
            signed_in,
        }
    }

    #[test]
    fn labels_and_account_lines() {
        assert_eq!(tab_label(&dest(None, false, None)), "YT · YT NLCH 4K");
        assert_eq!(account_line(&dest(Some("a@x.com"), true, None)), "a@x.com");
        assert_eq!(
            account_line(&dest(Some("a@x.com"), false, None)),
            "a@x.com (?)"
        );
        assert_eq!(account_line(&dest(None, false, None)), "účet nezistený");
        assert_eq!(signed_in_text(&dest(None, false, Some(true))), "prihlásený");
        assert_eq!(
            signed_in_text(&dest(None, false, Some(false))),
            "neprihlásený"
        );
        assert_eq!(signed_in_text(&dest(None, false, None)), "");
    }

    #[test]
    fn clip_status_says_whether_the_copy_landed() {
        assert_eq!(clip_status(true), "skopírované do schránky");
        assert!(clip_status(false).contains("Kopírovať"));
    }
}
