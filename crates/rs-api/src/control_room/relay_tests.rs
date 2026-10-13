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
/// frame once the screencast starts.
async fn fake_page(seen: mpsc::UnboundedSender<Value>) -> String {
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
            if req["method"] == "Page.startScreencast" {
                let jpeg = base64::engine::general_purpose::STANDARD.encode([0xFF, 0xD8, 9]);
                let f = frame_msg(&jpeg, 1600.0, 900.0, 42);
                ws.send(TgMessage::Text(f.to_string().into()))
                    .await
                    .unwrap();
            }
        }
    });
    addr
}

#[tokio::test]
async fn relay_streams_frames_and_forwards_input() {
    use axum::extract::WebSocketUpgrade;
    let (seen_tx, mut seen) = mpsc::unbounded_channel();
    let page_addr = fake_page(seen_tx).await;

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
            "Page.bringToFront",
            "Page.startScreencast",
            "Page.screencastFrameAck",
            "Input.dispatchMouseEvent"
        ]
    );
}
