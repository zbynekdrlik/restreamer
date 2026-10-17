use super::*;

fn target(id: &str, kind: &str, url: &str) -> TargetInfo {
    TargetInfo {
        target_id: id.into(),
        kind: kind.into(),
        url: url.into(),
        title: String::new(),
        browser_context_id: None,
        opener_id: None,
        can_access_opener: false,
    }
}

#[test]
fn page_targets_keeps_only_real_tabs() {
    let all = vec![
        target("a", "page", "https://studio.youtube.com/"),
        target("b", "service_worker", "https://www.youtube.com/sw.js"),
        target("c", "page", "chrome-extension://abc/background.html"),
        target("d", "page", "about:blank"),
    ];
    let ids: Vec<String> = page_targets(all).into_iter().map(|t| t.target_id).collect();
    assert_eq!(ids, vec!["a", "d"]);
}

#[test]
fn target_info_parses_chrome_field_names() {
    let v = serde_json::json!({
        "targetId": "T1", "type": "page", "url": "https://x/", "title": "X",
        "browserContextId": "C1", "attached": false
    });
    let t: TargetInfo = serde_json::from_value(v).unwrap();
    assert_eq!(t.target_id, "T1");
    assert_eq!(t.kind, "page");
    assert_eq!(t.browser_context_id.as_deref(), Some("C1"));
}

#[test]
fn request_is_cdp_json() {
    let v: Value = serde_json::from_str(&request(7, "Page.navigate", json!({"url": "u"}))).unwrap();
    assert_eq!(v["id"], 7);
    assert_eq!(v["method"], "Page.navigate");
    assert_eq!(v["params"]["url"], "u");
}

#[test]
fn response_for_matches_only_its_id() {
    let ok = json!({"id": 3, "result": {"x": 1}});
    let err = json!({"id": 3, "error": {"code": -32000, "message": "nope"}});
    let event = json!({"method": "Page.screencastFrame", "params": {}});
    let other = json!({"id": 4, "result": {}});
    assert_eq!(response_for(3, &ok).unwrap().unwrap()["x"], 1);
    assert!(response_for(3, &err).unwrap().is_err());
    assert!(response_for(3, &event).is_none());
    assert!(response_for(3, &other).is_none());
}

/// A fake CDP page endpoint: answers every request with `{"echo": method}`,
/// after first sending an unrelated event (which the client must skip).
async fn fake_cdp() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
                while let Some(Ok(Message::Text(text))) = ws.next().await {
                    let req: Value = serde_json::from_str(&text).unwrap();
                    let event = json!({"method": "Page.frameNavigated", "params": {}});
                    ws.send(Message::Text(event.to_string().into()))
                        .await
                        .unwrap();
                    let reply = json!({"id": req["id"], "result": {"echo": req["method"]}});
                    ws.send(Message::Text(reply.to_string().into()))
                        .await
                        .unwrap();
                }
            });
        }
    });
    addr
}

#[tokio::test]
async fn page_call_returns_the_matching_result_and_skips_events() {
    let addr = fake_cdp().await;
    let cdp = Cdp::new(addr);
    let r = cdp
        .page_call("T1", "Page.bringToFront", json!({}))
        .await
        .unwrap();
    assert_eq!(r["echo"], "Page.bringToFront");
}

#[tokio::test]
async fn page_call_fails_cleanly_when_chrome_is_not_there() {
    let addr = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().to_string()
    };
    let err = Cdp::new(addr)
        .page_call("T1", "Page.enable", json!({}))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("CDP connect"), "{err}");
}

#[tokio::test]
async fn browser_endpoint_and_pages_through_a_fake_chrome() {
    use crate::control_room::fake_chrome::{FakeChrome, page};
    let chrome = FakeChrome::start().await;
    chrome.set_targets(vec![
        page("T1", "C1", "https://studio.youtube.com/"),
        serde_json::json!({"targetId": "W1", "type": "service_worker", "url": "https://x/sw.js"}),
    ]);
    let cdp = Cdp::new(chrome.addr.clone());
    assert_eq!(
        cdp.browser_ws_url().await.unwrap(),
        format!("ws://{}/devtools/browser/b1", chrome.addr)
    );
    let pages = cdp.pages().await.unwrap();
    assert_eq!(pages.len(), 1);
    assert_eq!(pages[0].target_id, "T1");
    assert_eq!(pages[0].browser_context_id.as_deref(), Some("C1"));
    assert_eq!(chrome.methods(), vec!["Target.getTargets"]);
}

#[test]
fn target_info_reads_whether_a_popup_can_reach_its_opener() {
    let popup: TargetInfo = serde_json::from_value(json!({
        "targetId": "P", "type": "page", "url": "https://x/", "openerId": "R",
        "canAccessOpener": true
    }))
    .unwrap();
    assert!(popup.can_access_opener);
    let tab: TargetInfo =
        serde_json::from_value(json!({"targetId": "T", "type": "page", "url": "https://x/"}))
            .unwrap();
    assert!(!tab.can_access_opener);
}
