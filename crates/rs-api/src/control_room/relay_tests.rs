use super::*;

#[test]
fn mouse_and_key_forward_only_known_fields() {
    let m = json!({"t": "m", "type": "mousePressed", "x": 10.5, "y": 20, "button": "left",
                   "clickCount": 1, "modifiers": 0, "evil": "x"});
    let (method, p) = client_to_cdp(&m).unwrap();
    assert_eq!(method, "Input.dispatchMouseEvent");
    assert_eq!(p["x"], 10.5);
    assert_eq!(p["button"], "left");
    assert!(p.get("evil").is_none());
    assert!(p.get("t").is_none());

    let k = json!({"t": "k", "type": "keyDown", "key": "a", "code": "KeyA", "text": "a",
                   "windowsVirtualKeyCode": 65, "modifiers": 0, "text2": null});
    let (method, p) = client_to_cdp(&k).unwrap();
    assert_eq!(method, "Input.dispatchKeyEvent");
    assert_eq!(p["windowsVirtualKeyCode"], 65);
    assert!(p.get("text2").is_none());

    let raw = json!({"t": "k", "type": "rawKeyDown", "key": "Enter", "text": null});
    let (_, p) = client_to_cdp(&raw).unwrap();
    assert!(p.get("text").is_none(), "a null text must not be forwarded");
}

#[test]
fn paste_back_and_navigation() {
    let (m, p) = client_to_cdp(&json!({"t": "paste", "text": "hello"})).unwrap();
    assert_eq!((m, p["text"].as_str()), ("Input.insertText", Some("hello")));
    let (m, p) = client_to_cdp(&json!({"t": "back"})).unwrap();
    assert_eq!(m, "Runtime.evaluate");
    assert_eq!(p["expression"], "history.back()");
    let (m, p) = client_to_cdp(&json!({"t": "nav", "url": "studio.youtube.com"})).unwrap();
    assert_eq!(m, "Page.navigate");
    assert_eq!(p["url"], "https://studio.youtube.com");
    assert!(client_to_cdp(&json!({"t": "nav", "url": "javascript://alert(1)"})).is_none());
    assert!(client_to_cdp(&json!({"t": "nav", "url": "file:///etc/passwd"})).is_none());
}

#[test]
fn unknown_or_malformed_messages_are_dropped() {
    assert!(client_to_cdp(&json!({"t": "eval", "expression": "1"})).is_none());
    assert!(client_to_cdp(&json!({"type": "mousePressed"})).is_none());
    assert!(client_to_cdp(&json!({"t": "paste"})).is_none());
}

fn frame_msg(data: &str, w: f64, h: f64, session: i64) -> Value {
    json!({"method": "Page.screencastFrame", "params": {
        "data": data, "sessionId": session,
        "metadata": {"deviceWidth": w, "deviceHeight": h}}})
}

#[test]
fn page_frames_urls_and_noise() {
    let jpeg = base64::engine::general_purpose::STANDARD.encode([0xFF, 0xD8, 1, 2]);
    assert_eq!(
        page_to_client(&frame_msg(&jpeg, 1599.6, 900.2, 7)),
        PageOut::Frame {
            w: 1600,
            h: 900,
            jpeg: vec![0xFF, 0xD8, 1, 2],
            session_id: 7
        }
    );
    assert_eq!(
        page_to_client(&frame_msg("%%%", 10.0, 10.0, 1)),
        PageOut::Ignore
    );
    let nav = json!({"method": "Page.frameNavigated", "params": {"frame": {"url": "https://a/"}}});
    assert_eq!(page_to_client(&nav), PageOut::Url("https://a/".into()));
    let sub = json!({"method": "Page.frameNavigated",
                     "params": {"frame": {"url": "https://ad/", "parentId": "F1"}}});
    assert_eq!(page_to_client(&sub), PageOut::Ignore);
    assert_eq!(
        page_to_client(&json!({"id": 3, "result": {}})),
        PageOut::Ignore
    );
}

/// A fake Chrome page: records every request method, sends one screencast
/// frame each time the screencast starts. With `navigate`, the first frame is
/// followed by a cross-site main-frame navigation.
async fn fake_page(seen: mpsc::UnboundedSender<Value>, navigate: bool) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
        let mut navigated = !navigate;
        while let Some(Ok(TgMessage::Text(text))) = ws.next().await {
            let req: Value = serde_json::from_str(&text).unwrap();
            let _ = seen.send(req.clone());
            ws.send(TgMessage::Text(
                json!({"id": req["id"], "result": {}}).to_string().into(),
            ))
            .await
            .unwrap();
            if req["method"] == "Page.startScreencast" {
                let jpeg = base64::engine::general_purpose::STANDARD.encode([0xFF, 0xD8, 9]);
                let f = frame_msg(&jpeg, 1600.0, 900.0, 42);
                ws.send(TgMessage::Text(f.to_string().into()))
                    .await
                    .unwrap();
                if !navigated {
                    navigated = true;
                    let nav = json!({"method": "Page.frameNavigated", "params": {"frame": {
                        "id": "F1", "url": "https://www.youtube.com/account"}}});
                    ws.send(TgMessage::Text(nav.to_string().into()))
                        .await
                        .unwrap();
                }
            }
        }
    });
    addr
}

#[tokio::test]
async fn relay_streams_frames_and_forwards_input() {
    use axum::extract::WebSocketUpgrade;
    let (seen_tx, mut seen) = mpsc::unbounded_channel();
    let page_addr = fake_page(seen_tx, false).await;

    let app =
        axum::Router::new().route(
            "/ws",
            axum::routing::get(move |ws: WebSocketUpgrade| {
                let cdp = Cdp::new(page_addr.clone());
                async move {
                    ws.on_upgrade(move |s| relay(s, cdp, "T1".into(), "https://start/".into()))
                }
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let (mut client, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
        .await
        .unwrap();
    let run = async {
        let mut got_url = false;
        let mut got_meta = false;
        loop {
            match client.next().await.unwrap().unwrap() {
                TgMessage::Text(t) => {
                    let v: Value = serde_json::from_str(&t).unwrap();
                    if v["t"] == "url" {
                        assert_eq!(v["url"], "https://start/");
                        got_url = true;
                    }
                    if v["t"] == "meta" {
                        assert_eq!((v["w"].as_u64(), v["h"].as_u64()), (Some(1600), Some(900)));
                        got_meta = true;
                    }
                }
                TgMessage::Binary(b) => {
                    assert!(
                        got_url && got_meta,
                        "url and meta come before the first frame"
                    );
                    assert_eq!(b.as_ref(), &[0xFF, 0xD8, 9]);
                    break;
                }
                _ => {}
            }
        }
        // The dashboard drew the frame: only now may Chrome send the next.
        client
            .send(TgMessage::Text(json!({"t": "drawn"}).to_string().into()))
            .await
            .unwrap();
        let click = json!({"t": "m", "type": "mousePressed", "x": 5, "y": 6, "button": "left", "clickCount": 1});
        client
            .send(TgMessage::Text(click.to_string().into()))
            .await
            .unwrap();
        client
            .send(TgMessage::Text(
                json!({"t": "eval", "x": 1}).to_string().into(),
            ))
            .await
            .unwrap();
        let mut methods = Vec::new();
        let mut next_id = 1;
        while let Some(req) = seen.recv().await {
            assert_eq!(req["id"], next_id, "CDP request ids count up from 1");
            next_id += 1;
            let m = req["method"].as_str().unwrap().to_string();
            if m == "Input.dispatchMouseEvent" {
                assert_eq!(req["params"]["x"], 5);
                methods.push(m);
                break;
            }
            methods.push(m);
        }
        methods
    };
    let methods = tokio::time::timeout(std::time::Duration::from_secs(10), run)
        .await
        .expect("relay hung");
    assert_eq!(
        methods,
        vec![
            "Page.enable",
            "Runtime.enable",
            "Runtime.addBinding",
            "Page.addScriptToEvaluateOnNewDocument",
            "Runtime.evaluate",
            "Page.setInterceptFileChooserDialog",
            "Page.bringToFront",
            "Emulation.setFocusEmulationEnabled",
            "Page.startScreencast",
            "Page.screencastFrameAck",
            "Input.dispatchMouseEvent"
        ]
    );
}

/// Serve [`relay`] for `target` at `ws://<addr>/ws`.
async fn serve_relay(cdp: Cdp, target: &str) -> std::net::SocketAddr {
    use axum::extract::WebSocketUpgrade;
    let target = target.to_string();
    let app = axum::Router::new().route(
        "/ws",
        axum::routing::get(move |ws: WebSocketUpgrade| {
            let (cdp, target) = (cdp.clone(), target.clone());
            async move { ws.on_upgrade(move |s| relay(s, cdp, target, "https://start/".into())) }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    addr
}

#[tokio::test]
async fn screencast_restarts_after_a_main_frame_navigation() {
    let (seen_tx, mut seen) = mpsc::unbounded_channel();
    let page_addr = fake_page(seen_tx, true).await;
    let addr = serve_relay(Cdp::new(page_addr), "T1").await;
    let (mut client, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
        .await
        .unwrap();
    let run = async {
        let mut starts = 0;
        while let Some(req) = seen.recv().await {
            if req["method"] == "Page.startScreencast" {
                assert_eq!(req["params"]["format"], "jpeg");
                starts += 1;
                if starts == 2 {
                    break;
                }
            }
        }
        let mut url_seen = false;
        while let Some(Ok(m)) = client.next().await {
            if let TgMessage::Text(t) = m {
                let v: Value = serde_json::from_str(&t).unwrap();
                if v["t"] == "url" && v["url"] == "https://www.youtube.com/account" {
                    url_seen = true;
                    break;
                }
            }
        }
        url_seen
    };
    let url_seen = tokio::time::timeout(std::time::Duration::from_secs(10), run)
        .await
        .expect("the screencast was not restarted after the navigation");
    assert!(url_seen, "the dashboard hears about the new URL");
}

#[tokio::test]
async fn relay_moves_a_popup_into_the_room_tab() {
    use crate::control_room::fake_chrome::FakeChrome;
    let chrome = FakeChrome::start().await;
    chrome.set_targets(vec![
        json!({"targetId": "T1", "type": "page", "url": "https://studio.youtube.com/", "title": "r"}),
        json!({"targetId": "P1", "type": "page", "url": "https://www.youtube.com/account",
               "title": "p", "openerId": "T1"}),
    ]);
    let addr = serve_relay(Cdp::new(chrome.addr.clone()), "T1").await;
    let (_client, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
        .await
        .unwrap();
    let navigations = |c: &FakeChrome| -> Vec<Value> {
        c.state
            .lock()
            .unwrap()
            .requests
            .iter()
            .filter(|(path, r)| path == "page/T1" && r["method"] == "Page.navigate")
            .map(|(_, r)| r["params"].clone())
            .collect()
    };
    let closed = |c: &FakeChrome| -> Vec<Value> {
        c.state
            .lock()
            .unwrap()
            .requests
            .iter()
            .filter(|(path, r)| path.starts_with("browser/") && r["method"] == "Target.closeTarget")
            .map(|(_, r)| r["params"].clone())
            .collect()
    };
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    while closed(&chrome).is_empty() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the popup was never taken over; requests: {:?}",
            chrome.methods()
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert_eq!(closed(&chrome), vec![json!({"targetId": "P1"})]);
    assert_eq!(
        navigations(&chrome),
        vec![json!({"url": "https://www.youtube.com/account"})]
    );
    // The popup is gone now: later polls must not navigate again.
    tokio::time::sleep(std::time::Duration::from_millis(2500)).await;
    assert_eq!(navigations(&chrome).len(), 1);
    assert_eq!(closed(&chrome).len(), 1);
}

#[test]
fn size_sets_a_clamped_viewport() {
    let (m, p) = client_to_cdp(&json!({"t": "size", "w": 1890.4, "h": 812.6})).unwrap();
    assert_eq!(m, "Emulation.setDeviceMetricsOverride");
    assert_eq!(
        p,
        json!({"width": 1890, "height": 813, "deviceScaleFactor": 1, "mobile": false})
    );
    let (_, p) = client_to_cdp(&json!({"t": "size", "w": 10, "h": 10})).unwrap();
    assert_eq!(
        (p["width"].as_u64(), p["height"].as_u64()),
        (Some(320), Some(240))
    );
    // Too big: scaled down to fit, keeping the canvas's aspect ratio
    // (review of PR #398: a 4K canvas got a 2560x1600 page, drawn stretched).
    let (_, p) = client_to_cdp(&json!({"t": "size", "w": 99999, "h": 99999})).unwrap();
    assert_eq!(
        (p["width"].as_u64(), p["height"].as_u64()),
        (Some(1600), Some(1600))
    );
    let (_, p) = client_to_cdp(&json!({"t": "size", "w": 3840, "h": 1600})).unwrap();
    assert_eq!(
        (p["width"].as_u64(), p["height"].as_u64()),
        (Some(2560), Some(1067))
    );
    let (_, p) = client_to_cdp(&json!({"t": "size", "w": 2000, "h": 2000})).unwrap();
    assert_eq!(
        (p["width"].as_u64(), p["height"].as_u64()),
        (Some(1600), Some(1600))
    );
    let (_, p) = client_to_cdp(&json!({"t": "size", "w": 321, "h": 241})).unwrap();
    assert_eq!(
        (p["width"].as_u64(), p["height"].as_u64()),
        (Some(321), Some(241))
    );
    let (_, p) = client_to_cdp(&json!({"t": "size", "w": 2559, "h": 1599})).unwrap();
    assert_eq!(
        (p["width"].as_u64(), p["height"].as_u64()),
        (Some(2559), Some(1599))
    );
    let (_, p) = client_to_cdp(&json!({"t": "size", "w": -5, "h": 300})).unwrap();
    assert_eq!(p["width"].as_u64(), Some(320));
    assert!(client_to_cdp(&json!({"t": "size", "w": 800})).is_none());
    assert!(client_to_cdp(&json!({"t": "size", "w": "800", "h": 600})).is_none());
    assert_eq!(
        screencast_params()["maxWidth"].as_u64(),
        Some(u64::from(MAX_VIEW_W))
    );
    assert_eq!(
        screencast_params()["maxHeight"].as_u64(),
        Some(u64::from(MAX_VIEW_H))
    );
}

/// A fake page that answers every request and navigates its main frame once,
/// right after the first viewport change.
async fn fake_page_navigating_on_resize(seen: mpsc::UnboundedSender<Value>) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
        let mut navigated = false;
        while let Some(Ok(TgMessage::Text(text))) = ws.next().await {
            let req: Value = serde_json::from_str(&text).unwrap();
            let _ = seen.send(req.clone());
            ws.send(TgMessage::Text(
                json!({"id": req["id"], "result": {}}).to_string().into(),
            ))
            .await
            .unwrap();
            if req["method"] == "Emulation.setDeviceMetricsOverride" && !navigated {
                navigated = true;
                let nav = json!({"method": "Page.frameNavigated", "params": {"frame": {
                    "id": "F1", "url": "https://www.facebook.com/live/producer/"}}});
                ws.send(TgMessage::Text(nav.to_string().into()))
                    .await
                    .unwrap();
            }
        }
    });
    addr
}

#[tokio::test]
async fn the_viewport_follows_the_canvas_and_survives_a_navigation() {
    let (seen_tx, mut seen) = mpsc::unbounded_channel();
    let page_addr = fake_page_navigating_on_resize(seen_tx).await;
    let addr = serve_relay(Cdp::new(page_addr), "T1").await;
    let (mut client, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
        .await
        .unwrap();
    let size = json!({"t": "size", "w": 1890, "h": 820});
    client
        .send(TgMessage::Text(size.to_string().into()))
        .await
        .unwrap();
    let want = json!({"width": 1890, "height": 820, "deviceScaleFactor": 1, "mobile": false});
    let run = async {
        // Every request after the size, in order, until the screencast was
        // restarted after the navigation.
        let mut after = Vec::new();
        let mut starts_after_size = 0;
        while let Some(req) = seen.recv().await {
            let m = req["method"].as_str().unwrap().to_string();
            if m == "Emulation.setDeviceMetricsOverride" {
                assert_eq!(req["params"], want);
            }
            if !after.is_empty() || m == "Emulation.setDeviceMetricsOverride" {
                if m == "Page.startScreencast" {
                    starts_after_size += 1;
                }
                after.push(m);
                if starts_after_size == 2 {
                    break;
                }
            }
        }
        after
    };
    let after = tokio::time::timeout(std::time::Duration::from_secs(10), run)
        .await
        .expect("the viewport was not re-applied after the navigation");
    assert_eq!(
        after,
        vec![
            "Emulation.setDeviceMetricsOverride",
            "Page.startScreencast",
            "Emulation.setDeviceMetricsOverride",
            "Page.startScreencast",
        ]
    );
}

#[test]
fn same_document_url_only_for_the_main_frame() {
    let nav = |frame: &str, url: &str| {
        json!({"method": "Page.navigatedWithinDocument",
               "params": {"frameId": frame, "url": url, "navigationType": "historyApi"}})
    };
    // A page target's id is its main frame's id.
    assert_eq!(
        same_document_url(
            &nav("T1", "https://studio.youtube.com/channel/UC1/livestreaming"),
            "T1"
        ),
        Some("https://studio.youtube.com/channel/UC1/livestreaming".into())
    );
    assert_eq!(same_document_url(&nav("F9", "https://ad/"), "T1"), None);
    let other = json!({"method": "Page.frameNavigated", "params": {"frame": {"id": "T1", "url": "https://a/"}}});
    assert_eq!(same_document_url(&other, "T1"), None);
}

/// A fake page that answers every request and, once the screencast runs,
/// navigates (cross-document) and then pushes a same-document URL change,
/// the way YT Studio switches pages.
async fn fake_page_spa(seen: mpsc::UnboundedSender<Value>) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
        let mut done = false;
        while let Some(Ok(TgMessage::Text(text))) = ws.next().await {
            let req: Value = serde_json::from_str(&text).unwrap();
            let _ = seen.send(req.clone());
            ws.send(TgMessage::Text(
                json!({"id": req["id"], "result": {}}).to_string().into(),
            ))
            .await
            .unwrap();
            if req["method"] == "Page.startScreencast" && !done {
                done = true;
                for ev in [
                    json!({"method": "Page.frameNavigated", "params": {"frame": {
                        "id": "T1", "url": "https://studio.youtube.com/"}}}),
                    json!({"method": "Page.navigatedWithinDocument", "params": {
                        "frameId": "T1", "url": "https://studio.youtube.com/channel/UC7/livestreaming"}}),
                    json!({"method": "Page.navigatedWithinDocument", "params": {
                        "frameId": "AD", "url": "https://ads.example/"}}),
                ] {
                    ws.send(TgMessage::Text(ev.to_string().into()))
                        .await
                        .unwrap();
                }
            }
        }
    });
    addr
}

#[tokio::test]
async fn every_main_frame_url_reaches_the_dashboard_and_the_url_sink() {
    use axum::extract::WebSocketUpgrade;
    let (seen_tx, _seen) = mpsc::unbounded_channel();
    let page_addr = fake_page_spa(seen_tx).await;
    let (url_tx, mut urls) = mpsc::unbounded_channel::<String>();
    let sink: UrlSink = std::sync::Arc::new(move |u: String| {
        let _ = url_tx.send(u);
    });
    let app = axum::Router::new().route(
        "/ws",
        axum::routing::get(move |ws: WebSocketUpgrade| {
            let (cdp, sink) = (Cdp::new(page_addr.clone()), sink.clone());
            async move {
                ws.on_upgrade(move |s| {
                    relay_with(s, cdp, "T1".into(), "https://start/".into(), sink)
                })
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let (mut client, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
        .await
        .unwrap();
    let want = [
        "https://studio.youtube.com/",
        "https://studio.youtube.com/channel/UC7/livestreaming",
    ];
    let run = async {
        let mut got = Vec::new();
        while got.len() < want.len() {
            got.push(urls.recv().await.unwrap());
        }
        let mut shown = Vec::new();
        while shown.len() < want.len() + 1 {
            if let TgMessage::Text(t) = client.next().await.unwrap().unwrap() {
                let v: Value = serde_json::from_str(&t).unwrap();
                if v["t"] == "url" {
                    shown.push(v["url"].as_str().unwrap().to_string());
                }
            }
        }
        (got, shown)
    };
    let (got, shown) = tokio::time::timeout(std::time::Duration::from_secs(10), run)
        .await
        .expect("the URL changes never arrived");
    assert_eq!(got, want);
    assert_eq!(shown, ["https://start/", want[0], want[1]]);
    // The subframe's same-document change is neither shown nor kept.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert!(urls.try_recv().is_err());
}

#[test]
fn copy_runs_the_page_copy_command_as_a_user_gesture() {
    let (m, p) = client_to_cdp(&json!({"t": "copy"})).unwrap();
    assert_eq!(m, "Runtime.evaluate");
    assert_eq!(p["expression"], "document.execCommand('copy')");
    assert_eq!(p["userGesture"], true);
}

fn binding_called(name: &str, payload: &str) -> Value {
    json!({"method": "Runtime.bindingCalled", "params": {
        "name": name, "payload": payload, "executionContextId": 1}})
}

#[test]
fn page_copies_reach_the_dashboard() {
    let url = "rtmp://a.rtmp.youtube.com/live2";
    assert_eq!(
        page_to_client(&binding_called(CLIP_BINDING, url)),
        PageOut::Clip(url.into())
    );
    assert_eq!(
        page_to_client(&binding_called("other", url)),
        PageOut::Ignore
    );
    assert_eq!(
        page_to_client(&binding_called(CLIP_BINDING, "")),
        PageOut::Ignore
    );
    // The 64 KB limit as a literal, so the test pins CLIP_MAX itself.
    let most = "x".repeat(65_536);
    assert_eq!(
        page_to_client(&binding_called(CLIP_BINDING, &most)),
        PageOut::Clip(most.clone())
    );
    let over = "x".repeat(65_537);
    assert_eq!(
        page_to_client(&binding_called(CLIP_BINDING, &over)),
        PageOut::Ignore
    );
}

#[test]
fn the_copy_hook_reports_through_the_binding() {
    let src = copy_hook_source();
    assert!(src.contains(CLIP_BINDING), "the hook calls the binding");
    assert!(src.contains("'copy'"), "the hook listens to copy events");
    assert!(
        src.contains("writeText"),
        "the hook wraps navigator.clipboard.writeText"
    );
}

/// A fake page that answers every request and, once the relay added the
/// copy binding, reports one copy through it, as the page hook would.
async fn fake_page_copying(seen: mpsc::UnboundedSender<Value>) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
        while let Some(Ok(TgMessage::Text(text))) = ws.next().await {
            let req: Value = serde_json::from_str(&text).unwrap();
            let _ = seen.send(req.clone());
            ws.send(TgMessage::Text(
                json!({"id": req["id"], "result": {}}).to_string().into(),
            ))
            .await
            .unwrap();
            if req["method"] == "Runtime.addBinding" {
                let name = req["params"]["name"].as_str().unwrap();
                ws.send(TgMessage::Text(
                    binding_called(name, "rtmp://x/live2").to_string().into(),
                ))
                .await
                .unwrap();
            }
        }
    });
    addr
}

#[tokio::test]
async fn a_copy_in_the_page_reaches_the_dashboard() {
    let (seen_tx, mut seen) = mpsc::unbounded_channel();
    let page_addr = fake_page_copying(seen_tx).await;
    let addr = serve_relay(Cdp::new(page_addr), "T1").await;
    let (mut client, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
        .await
        .unwrap();
    let clip = async {
        loop {
            if let TgMessage::Text(t) = client.next().await.unwrap().unwrap() {
                let v: Value = serde_json::from_str(&t).unwrap();
                if v["t"] == "clip" {
                    return v;
                }
            }
        }
    };
    let clip = tokio::time::timeout(std::time::Duration::from_secs(10), clip)
        .await
        .expect("no clip message reached the dashboard");
    assert_eq!(clip, json!({"t": "clip", "text": "rtmp://x/live2"}));

    // The hook is installed for later documents AND the current one.
    let mut reqs = Vec::new();
    while let Ok(r) = seen.try_recv() {
        reqs.push(r);
    }
    let by = |m: &str| reqs.iter().find(|r| r["method"] == m).cloned();
    assert_eq!(
        by("Runtime.addBinding").unwrap()["params"]["name"],
        CLIP_BINDING
    );
    assert!(
        by("Runtime.enable").is_some(),
        "bindingCalled needs the Runtime domain"
    );
    let src = copy_hook_source();
    assert_eq!(
        by("Page.addScriptToEvaluateOnNewDocument").unwrap()["params"]["source"],
        src
    );
    assert!(
        reqs.iter()
            .any(|r| r["method"] == "Runtime.evaluate" && r["params"]["expression"] == src),
        "the hook also runs in the page already open"
    );
}

#[test]
fn reload_reloads_the_page_in_chrome() {
    // F5 sent as a key never reloads: Chrome handles it in the browser UI,
    // which a CDP key event does not reach.
    let (m, p) = client_to_cdp(&json!({"t": "reload"})).unwrap();
    assert_eq!(m, "Page.reload");
    assert_eq!(p, json!({"ignoreCache": false}));
    let (m, p) = client_to_cdp(&json!({"t": "reload", "hard": true})).unwrap();
    assert_eq!(m, "Page.reload");
    assert_eq!(p, json!({"ignoreCache": true}));
}

/// Chrome sends the next screencast frame only after the last one was
/// acked. The relay acks once the dashboard DREW the frame, so a slow viewer
/// (a seek or a playing video in the room: a flood of 2560x1600 frames) gets
/// frames at its own pace instead of an ever-growing queue (stream-pp,
/// 2026-10-14: the room froze on a YouTube seek and on opening a tab).
#[tokio::test]
async fn a_frame_is_acked_only_after_the_dashboard_drew_it() {
    let (seen_tx, mut seen) = mpsc::unbounded_channel();
    let page_addr = fake_page(seen_tx, false).await;
    let addr = serve_relay(Cdp::new(page_addr), "T1").await;
    let (mut client, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
        .await
        .unwrap();
    let run = async {
        loop {
            if let TgMessage::Binary(_) = client.next().await.unwrap().unwrap() {
                break;
            }
        }
        // Not drawn yet: no ack, however long the viewer takes.
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        let mut early = Vec::new();
        while let Ok(r) = seen.try_recv() {
            early.push(r["method"].as_str().unwrap_or_default().to_string());
        }
        assert!(
            !early.iter().any(|m| m == "Page.screencastFrameAck"),
            "acked before the dashboard drew the frame: {early:?}"
        );
        // A second "drawn" with no frame pending acks nothing more.
        for _ in 0..2 {
            client
                .send(TgMessage::Text(json!({"t": "drawn"}).to_string().into()))
                .await
                .unwrap();
        }
        let ack = loop {
            let r = seen.recv().await.unwrap();
            if r["method"] == "Page.screencastFrameAck" {
                break r;
            }
        };
        assert_eq!(ack["params"]["sessionId"], 42);
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        let mut later = Vec::new();
        while let Ok(r) = seen.try_recv() {
            later.push(r["method"].as_str().unwrap_or_default().to_string());
        }
        assert!(
            !later.iter().any(|m| m == "Page.screencastFrameAck"),
            "one frame, one ack: {later:?}"
        );
    };
    tokio::time::timeout(std::time::Duration::from_secs(10), run)
        .await
        .expect("relay hung");
}

#[path = "relay_popup_tests.rs"]
mod popup;

#[path = "relay_visibility_tests.rs"]
mod visibility;
