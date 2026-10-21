//! #399: files (thumbnails) the operator puts into a control room.

use super::*;
use base64::engine::general_purpose::STANDARD;
use rs_core::native_picker::NativeFilePicker;

#[test]
fn upload_names_are_one_safe_path_component() {
    assert_eq!(upload_name("thumb 1.png"), "thumb_1.png");
    assert_eq!(upload_name("../../etc/passwd"), "_.._etc_passwd");
    assert_eq!(upload_name("C:\\x\\obrázok.jpg"), "C__x_obr_zok.jpg");
    assert_eq!(upload_name(".."), "file");
    assert_eq!(upload_name(""), "file");
    let long = format!("{}.png", "a".repeat(100));
    let n = upload_name(&long);
    assert_eq!(n.len(), 80);
    assert!(n.ends_with("aaa.png"), "the end (extension) is kept: {n}");
}

#[test]
fn a_file_message_decodes_and_is_capped() {
    let b64 = STANDARD.encode(b"\x89PNG");
    assert_eq!(
        decode_file(&json!({"t": "file", "name": "t 1.png", "b64": b64, "x": 10, "y": 20.5})),
        Ok(Upload {
            name: "t_1.png".into(),
            bytes: b"\x89PNG".to_vec(),
            x: 10.0,
            y: 20.5
        })
    );
    // No position: the page's top-left corner.
    let f = decode_file(&json!({"t": "file", "name": "t.png", "b64": b64})).unwrap();
    assert_eq!((f.x, f.y), (0.0, 0.0));
    assert!(decode_file(&json!({"t": "file", "name": "t.png", "b64": "%%%"})).is_err());
    assert!(decode_file(&json!({"t": "file", "name": "t.png"})).is_err());
    assert!(decode_file(&json!({"t": "file", "name": "t.png", "b64": ""})).is_err());
    // 20 MB exactly passes, one byte more does not (literals: they pin FILE_MAX).
    let most = STANDARD.encode(vec![7u8; 20_971_520]);
    assert!(decode_file(&json!({"t": "file", "name": "a", "b64": most})).is_ok());
    let over = STANDARD.encode(vec![7u8; 20_971_521]);
    assert!(decode_file(&json!({"t": "file", "name": "a", "b64": over})).is_err());
}

#[test]
fn a_pending_chooser_gets_the_file_else_it_is_dropped_on_the_page() {
    assert_eq!(
        file_actions("/u/t.png", Some(42), 10.0, 20.0),
        vec![(
            "DOM.setFileInputFiles",
            json!({"files": ["/u/t.png"], "backendNodeId": 42})
        )]
    );
    let drop = file_actions("/u/t.png", None, 10.0, 20.0);
    let types: Vec<_> = drop
        .iter()
        .map(|(m, p)| (*m, p["type"].as_str().unwrap_or_default()))
        .collect();
    assert_eq!(
        types,
        [
            ("Input.dispatchDragEvent", "dragEnter"),
            ("Input.dispatchDragEvent", "dragOver"),
            ("Input.dispatchDragEvent", "drop"),
        ]
    );
    for (_, p) in &drop {
        assert_eq!((p["x"].as_f64(), p["y"].as_f64()), (Some(10.0), Some(20.0)));
        assert_eq!(
            p["data"],
            json!({"items": [], "files": ["/u/t.png"], "dragOperationsMask": 1})
        );
    }
}

#[test]
fn file_chooser_events_reach_the_dashboard() {
    let opened = |p: Value| json!({"method": "Page.fileChooserOpened", "params": p});
    assert_eq!(
        page_to_client(&opened(
            json!({"frameId": "F", "mode": "selectSingle", "backendNodeId": 42})
        )),
        PageOut::FileChooser {
            node: Some(42),
            mode: "selectSingle".into()
        }
    );
    // A chooser not opened by an <input type=file> has no node.
    assert_eq!(
        page_to_client(&opened(json!({"frameId": "F", "mode": "selectMultiple"}))),
        PageOut::FileChooser {
            node: None,
            mode: "selectMultiple".into()
        }
    );
}

#[test]
fn an_upload_expires_only_past_its_max_age() {
    let hour = std::time::Duration::from_secs(3600);
    assert!(!is_expired(hour, hour), "exactly the max age stays");
    assert!(is_expired(hour + std::time::Duration::from_millis(1), hour));
    assert!(!is_expired(std::time::Duration::ZERO, hour));
}

#[test]
fn old_uploads_are_pruned() {
    let dir = tempfile::tempdir().unwrap();
    let old = dir.path().join("old.png");
    let new = dir.path().join("new.png");
    std::fs::write(&old, b"o").unwrap();
    std::fs::write(&new, b"n").unwrap();
    let two_hours_ago = std::time::SystemTime::now() - std::time::Duration::from_secs(7200);
    std::fs::File::options()
        .write(true)
        .open(&old)
        .unwrap()
        .set_modified(two_hours_ago)
        .unwrap();
    prune_uploads(dir.path(), std::time::Duration::from_secs(3600));
    assert!(!old.exists(), "a file older than the limit is deleted");
    assert!(new.exists(), "a recent file stays");
    // A missing directory is not an error.
    prune_uploads(&dir.path().join("missing"), UPLOAD_MAX_AGE);
}

/// A fake page that answers every request; when the relay turns on file
/// chooser interception it reports a chooser for node 42, as Chrome does when
/// the operator clicks "Upload thumbnail".
async fn fake_page_with_chooser(seen: mpsc::UnboundedSender<Value>) -> String {
    let opened = json!({"method": "Page.fileChooserOpened", "params": {
        "frameId": "T1", "mode": "selectSingle", "backendNodeId": 42}});
    fake_page_events(seen, INTERCEPT, vec![opened]).await
}

/// The relay turned on file chooser interception (it attached).
const INTERCEPT: &str = "Page.setInterceptFileChooserDialog";
/// The viewer clicked in the page.
const CLICK: &str = "Input.dispatchMouseEvent";

/// A fake page that answers every request and sends `events` once it got a
/// `trigger` request. When the relay lets the page go, `seen` gets
/// `{"closed": true}`.
async fn fake_page_events(
    seen: mpsc::UnboundedSender<Value>,
    trigger: &'static str,
    events: Vec<Value>,
) -> String {
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
            if req["method"] == INTERCEPT {
                assert_eq!(req["params"]["enabled"], true);
            }
            if req["method"] == trigger {
                for ev in &events {
                    ws.send(TgMessage::Text(ev.to_string().into()))
                        .await
                        .unwrap();
                }
            }
        }
        let _ = seen.send(json!({"closed": true}));
    });
    addr
}

async fn next_text(
    client: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    t: &str,
) -> Value {
    loop {
        if let TgMessage::Text(m) = client.next().await.unwrap().unwrap() {
            let v: Value = serde_json::from_str(&m).unwrap();
            if v["t"] == t {
                return v;
            }
        }
    }
}

#[tokio::test]
async fn a_file_goes_into_the_chooser_then_a_second_one_is_dropped() {
    use axum::extract::WebSocketUpgrade;
    let uploads = tempfile::tempdir().unwrap();
    let dir = uploads.path().to_path_buf();
    let (seen_tx, mut seen) = mpsc::unbounded_channel();
    let page_addr = fake_page_with_chooser(seen_tx).await;
    let app = axum::Router::new().route(
        "/ws",
        axum::routing::get(move |ws: WebSocketUpgrade| {
            let (cdp, dir) = (Cdp::new(page_addr.clone()), dir.clone());
            async move {
                ws.max_message_size(32 << 20).on_upgrade(move |s| {
                    relay_room(
                        s,
                        cdp,
                        "T1".into(),
                        "https://start/".into(),
                        Arc::new(|_| {}),
                        dir,
                        super::ShownTabs::default(),
                        None,
                    )
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

    let run = async {
        let chooser = next_text(&mut client, "file-chooser").await;
        assert_eq!(
            chooser,
            json!({"t": "file-chooser", "mode": "selectSingle"})
        );

        let send_file = |name: &str, bytes: &[u8]| {
            json!({"t": "file", "name": name, "b64": STANDARD.encode(bytes), "x": 30, "y": 40})
                .to_string()
        };
        client
            .send(TgMessage::Text(send_file("thumb.png", b"PNG1").into()))
            .await
            .unwrap();
        let done = next_text(&mut client, "file-done").await;
        assert_eq!(
            done,
            json!({"t": "file-done", "name": "thumb.png", "how": "chooser"})
        );
        let set = loop {
            let r = seen.recv().await.unwrap();
            if r["method"] == "DOM.setFileInputFiles" {
                break r;
            }
        };
        assert_eq!(set["params"]["backendNodeId"], 42);
        let path = std::path::PathBuf::from(set["params"]["files"][0].as_str().unwrap());
        assert!(
            path.starts_with(uploads.path()),
            "written to the uploads dir: {path:?}"
        );
        assert!(path.to_string_lossy().ends_with("thumb.png"));
        assert_eq!(std::fs::read(&path).unwrap(), b"PNG1");

        // The chooser is used up: the next file is dropped where it was let go.
        client
            .send(TgMessage::Text(send_file("b.jpg", b"JPG2").into()))
            .await
            .unwrap();
        let done = next_text(&mut client, "file-done").await;
        assert_eq!(
            done,
            json!({"t": "file-done", "name": "b.jpg", "how": "drop"})
        );
        let mut drags = Vec::new();
        while drags.len() < 3 {
            let r = seen.recv().await.unwrap();
            if r["method"] == "Input.dispatchDragEvent" {
                drags.push(r);
            }
        }
        assert_eq!(drags[2]["params"]["type"], "drop");
        assert_eq!(
            (
                drags[2]["params"]["x"].as_f64(),
                drags[2]["params"]["y"].as_f64()
            ),
            (Some(30.0), Some(40.0))
        );
        let dropped = drags[2]["params"]["data"]["files"][0].as_str().unwrap();
        assert_eq!(std::fs::read(dropped).unwrap(), b"JPG2");

        // A broken file is refused, with a reason for the operator.
        client
            .send(TgMessage::Text(
                json!({"t": "file", "name": "x.png", "b64": "%%%"})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        let err = next_text(&mut client, "file-error").await;
        assert!(err["error"].as_str().is_some_and(|e| !e.is_empty()));
    };
    tokio::time::timeout(std::time::Duration::from_secs(10), run)
        .await
        .expect("the relay never handled the files");
}

/// A fake page that opens a file chooser and then navigates away (the
/// operator reloaded, or left the thumbnail dialog) before any file came.
async fn fake_page_chooser_then_navigation(seen: mpsc::UnboundedSender<Value>) -> String {
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
            if req["method"] == "Page.setInterceptFileChooserDialog" {
                for ev in [
                    json!({"method": "Page.fileChooserOpened", "params": {
                        "frameId": "T1", "mode": "selectSingle", "backendNodeId": 42}}),
                    json!({"method": "Page.frameNavigated", "params": {"frame": {
                        "id": "T1", "url": "https://studio.youtube.com/other"}}}),
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

/// #399 review: a navigation ends the page's file chooser (its input is gone
/// with the document). A later file is dropped, never set into the old input,
/// and the room is told to hide its file bar.
#[tokio::test]
async fn a_navigation_ends_a_pending_file_chooser() {
    let uploads = tempfile::tempdir().unwrap();
    let (seen_tx, mut seen) = mpsc::unbounded_channel();
    let page_addr = fake_page_chooser_then_navigation(seen_tx).await;
    let dir = uploads.path().to_path_buf();
    let app = axum::Router::new().route(
        "/ws",
        axum::routing::get(move |ws: axum::extract::WebSocketUpgrade| {
            let (cdp, dir) = (Cdp::new(page_addr.clone()), dir.clone());
            async move {
                ws.on_upgrade(move |s| {
                    relay_room(
                        s,
                        cdp,
                        "T1".into(),
                        "https://start/".into(),
                        Arc::new(|_| {}),
                        dir,
                        super::ShownTabs::default(),
                        None,
                    )
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
    let run = async {
        next_text(&mut client, "file-chooser").await;
        let closed = next_text(&mut client, "file-chooser-closed").await;
        assert_eq!(closed, json!({"t": "file-chooser-closed"}));
        let file =
            json!({"t": "file", "name": "t.png", "b64": STANDARD.encode(b"PNG"), "x": 5, "y": 6});
        client
            .send(TgMessage::Text(file.to_string().into()))
            .await
            .unwrap();
        let done = next_text(&mut client, "file-done").await;
        assert_eq!(
            done["how"], "drop",
            "the file went into the dead chooser: {done}"
        );
        while let Ok(r) = seen.try_recv() {
            assert_ne!(
                r["method"], "DOM.setFileInputFiles",
                "set into an input of the old page"
            );
        }
    };
    tokio::time::timeout(std::time::Duration::from_secs(10), run)
        .await
        .expect("the relay never handled the navigation");
}

/// #414: the box's own dialog, answering `file` (`None`: the operator
/// cancelled) once `gate` lets it, and counting how often it was opened.
struct FakeDialog {
    file: Option<std::path::PathBuf>,
    opened: Arc<std::sync::atomic::AtomicUsize>,
    gate: Option<Arc<tokio::sync::Notify>>,
}

impl FakeDialog {
    fn new(
        file: Option<std::path::PathBuf>,
        gate: Option<Arc<tokio::sync::Notify>>,
    ) -> (Arc<Self>, Arc<std::sync::atomic::AtomicUsize>) {
        let opened = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let dialog = FakeDialog {
            file,
            opened: Arc::clone(&opened),
            gate,
        };
        (Arc::new(dialog), opened)
    }
}

impl NativeFilePicker for FakeDialog {
    fn pick_file(&self) -> rs_core::native_picker::Pick {
        self.opened.fetch_add(1, Ordering::SeqCst);
        let (file, gate) = (self.file.clone(), self.gate.clone());
        Box::pin(async move {
            if let Some(gate) = gate {
                gate.notified().await;
            }
            file
        })
    }
}

/// The relay of tab T1 of `page_addr` for a viewer on the box, whose dialog
/// is `dialog`, open while `open` is set.
async fn serve_box_viewer(
    page_addr: String,
    dialog: Arc<FakeDialog>,
    open: Arc<std::sync::atomic::AtomicBool>,
) -> std::net::SocketAddr {
    use axum::extract::WebSocketUpgrade;
    let app = axum::Router::new().route(
        "/ws",
        axum::routing::get(move |ws: WebSocketUpgrade| {
            let cdp = Cdp::new(page_addr.clone());
            let dialog = BoxDialog {
                picker: dialog.clone(),
                open: Arc::clone(&open),
            };
            async move {
                ws.on_upgrade(move |s| {
                    relay_room(
                        s,
                        cdp,
                        "T1".into(),
                        "https://start/".into(),
                        Arc::new(|_| {}),
                        std::env::temp_dir().join("restreamer-room-uploads"),
                        super::ShownTabs::default(),
                        Some(dialog),
                    )
                })
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    addr
}

type Client =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn connect(addr: std::net::SocketAddr) -> Client {
    tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
        .await
        .unwrap()
        .0
}

/// The viewer clicks "Upload thumbnail" in the room.
async fn click(client: &mut Client) {
    let press = json!({"t": "m", "type": "mousePressed", "x": 5, "y": 5,
        "button": "left", "clickCount": 1, "modifiers": 0});
    client
        .send(TgMessage::Text(press.to_string().into()))
        .await
        .unwrap();
}

/// The dashboard's text messages up to (and with) the first one of type `t`.
async fn texts_until(client: &mut Client, t: &str) -> Vec<Value> {
    let mut texts = Vec::new();
    loop {
        if let TgMessage::Text(m) = client.next().await.unwrap().unwrap() {
            let v: Value = serde_json::from_str(&m).unwrap();
            let last = v["t"] == t;
            texts.push(v);
            if last {
                return texts;
            }
        }
    }
}

fn chooser_opened() -> Value {
    json!({"method": "Page.fileChooserOpened", "params": {
        "frameId": "T1", "mode": "selectSingle", "backendNodeId": 42}})
}

/// A copy the page reports: once it reached the dashboard, the relay has
/// handled every page event before it (it reads the page in order).
fn marker() -> Value {
    json!({"method": "Runtime.bindingCalled", "params": {
        "name": CLIP_BINDING, "payload": "marker", "executionContextId": 1}})
}

/// Waits (bounded) until the dialog was opened `n` times.
async fn opened_times(opened: &std::sync::atomic::AtomicUsize, n: usize) {
    while opened.load(Ordering::SeqCst) < n {
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

const LIVENESS: std::time::Duration = std::time::Duration::from_secs(10);

#[tokio::test]
async fn the_box_viewer_picks_the_file_with_the_box_dialog() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("náhľad 1.png");
    std::fs::write(&file, b"PNG1").unwrap();
    let (dialog, opened) = FakeDialog::new(Some(file.clone()), None);
    let (seen_tx, mut seen) = mpsc::unbounded_channel();
    let page = fake_page_events(seen_tx, CLICK, vec![chooser_opened()]).await;
    let mut client = connect(serve_box_viewer(page, dialog, Arc::default()).await).await;
    let run = async {
        click(&mut client).await;
        let texts = texts_until(&mut client, "file-done").await;
        assert_eq!(
            texts.last().unwrap(),
            &json!({"t": "file-done", "name": "náhľad 1.png", "how": "picker"})
        );
        assert!(
            !texts.iter().any(|v| v["t"] == "file-chooser"),
            "no bar for a viewer on the box: {texts:?}"
        );
        let set = loop {
            let r = seen.recv().await.unwrap();
            if r["method"] == "DOM.setFileInputFiles" {
                break r;
            }
        };
        assert_eq!(
            set["params"],
            json!({"files": [file.to_string_lossy()], "backendNodeId": 42})
        );
    };
    tokio::time::timeout(LIVENESS, run)
        .await
        .expect("the box dialog's file never reached the page");
    assert_eq!(opened.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_cancelled_box_dialog_shows_the_bar() {
    let (dialog, opened) = FakeDialog::new(None, None);
    let (seen_tx, mut seen) = mpsc::unbounded_channel();
    let page = fake_page_events(seen_tx, CLICK, vec![chooser_opened()]).await;
    let mut client = connect(serve_box_viewer(page, dialog, Arc::default()).await).await;
    click(&mut client).await;
    let bar = tokio::time::timeout(LIVENESS, next_text(&mut client, "file-chooser"))
        .await
        .expect("no bar after the cancel");
    assert_eq!(bar, json!({"t": "file-chooser", "mode": "selectSingle"}));
    assert_eq!(opened.load(Ordering::SeqCst), 1, "the dialog was opened");
    while let Ok(r) = seen.try_recv() {
        assert_ne!(r["method"], "DOM.setFileInputFiles", "nothing was chosen");
    }
}

/// A chooser the page opened by itself (no click or key of this viewer
/// before it), or one a second viewer of the same tab caused, never opens a
/// dialog on the box's screen: the bar shows.
#[tokio::test]
async fn a_chooser_without_the_viewers_own_input_shows_the_bar() {
    let (dialog, opened) = FakeDialog::new(Some("/x/a.png".into()), None);
    let (seen_tx, _seen) = mpsc::unbounded_channel();
    let page = fake_page_events(seen_tx, INTERCEPT, vec![chooser_opened()]).await;
    let mut client = connect(serve_box_viewer(page, dialog, Arc::default()).await).await;
    let bar = tokio::time::timeout(LIVENESS, next_text(&mut client, "file-chooser"))
        .await
        .expect("no bar for a chooser the viewer did not cause");
    assert_eq!(bar["mode"], "selectSingle");
    assert_eq!(opened.load(Ordering::SeqCst), 0, "no dialog on the box");
}

/// The operators clicked "Upload thumbnail" 7 times in 4 s (stream-pp,
/// 2026-10-21). A chooser while the dialog is open opens no second dialog,
/// and still shows the bar: a dialog the window manager put behind the
/// kiosk must never leave the operator with nothing.
#[tokio::test]
async fn a_second_chooser_while_the_dialog_is_open_shows_the_bar() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("t.png");
    std::fs::write(&file, b"PNG").unwrap();
    let gate = Arc::new(tokio::sync::Notify::new());
    let (dialog, opened) = FakeDialog::new(Some(file), Some(Arc::clone(&gate)));
    let (seen_tx, _seen) = mpsc::unbounded_channel();
    let events = vec![chooser_opened(), chooser_opened(), marker()];
    let page = fake_page_events(seen_tx, CLICK, events).await;
    let mut client = connect(serve_box_viewer(page, dialog, Arc::default()).await).await;
    let run = async {
        click(&mut client).await;
        let texts = texts_until(&mut client, "clip").await;
        assert!(
            texts.iter().any(|v| v["t"] == "file-chooser"),
            "the bar shows while the dialog is open: {texts:?}"
        );
        assert_eq!(opened.load(Ordering::SeqCst), 1, "one dialog only");
        gate.notify_one();
        let done = next_text(&mut client, "file-done").await;
        assert_eq!(done["how"], "picker");
    };
    tokio::time::timeout(LIVENESS, run)
        .await
        .expect("the relay did not handle the two choosers");
    assert_eq!(opened.load(Ordering::SeqCst), 1);
}

/// One dialog for the whole box: another room (another relay) asking while
/// it is open gets the bar.
#[tokio::test]
async fn one_box_dialog_at_a_time_across_rooms() {
    let gate = Arc::new(tokio::sync::Notify::new());
    let (dialog, opened) = FakeDialog::new(Some("/x/a.png".into()), Some(Arc::clone(&gate)));
    let open: Arc<std::sync::atomic::AtomicBool> = Arc::default();
    let (seen_tx, _seen) = mpsc::unbounded_channel();
    let first = fake_page_events(seen_tx.clone(), CLICK, vec![chooser_opened()]).await;
    let second = fake_page_events(seen_tx, CLICK, vec![chooser_opened()]).await;
    let mut a =
        connect(serve_box_viewer(first, Arc::clone(&dialog), Arc::clone(&open)).await).await;
    let mut b = connect(serve_box_viewer(second, dialog, open).await).await;
    let run = async {
        click(&mut a).await;
        opened_times(&opened, 1).await;
        click(&mut b).await;
        let bar = next_text(&mut b, "file-chooser").await;
        assert_eq!(bar["mode"], "selectSingle");
    };
    tokio::time::timeout(LIVENESS, run)
        .await
        .expect("the second room got no bar");
    assert_eq!(opened.load(Ordering::SeqCst), 1, "one dialog for the box");
    gate.notify_one();
}

/// A viewer that leaves while the dialog is open lets the page go: the
/// relay's CDP session (screencast, focus emulation, chooser interception)
/// must not stay attached until the operator closes the dialog.
#[tokio::test]
async fn a_relay_that_ends_with_the_dialog_open_lets_the_page_go() {
    let gate = Arc::new(tokio::sync::Notify::new());
    let (dialog, opened) = FakeDialog::new(Some("/x/a.png".into()), Some(Arc::clone(&gate)));
    let (seen_tx, mut seen) = mpsc::unbounded_channel();
    let page = fake_page_events(seen_tx, CLICK, vec![chooser_opened()]).await;
    let mut client = connect(serve_box_viewer(page, dialog, Arc::default()).await).await;
    let run = async {
        click(&mut client).await;
        opened_times(&opened, 1).await;
        client.close(None).await.unwrap();
        loop {
            let r = seen.recv().await.unwrap();
            if r["closed"] == true {
                break;
            }
        }
    };
    tokio::time::timeout(LIVENESS, run)
        .await
        .expect("the page stayed attached while the dialog was open");
    gate.notify_one();
}
