use std::path::PathBuf;

use super::*;

/// The CDP calls `picked_reply` made.
fn calls_of(picked: Option<&Path>, chooser: &Mutex<Option<i64>>) -> (Value, Vec<(String, Value)>) {
    let calls = Mutex::new(Vec::new());
    let reply = picked_reply(
        picked,
        "selectSingle",
        chooser,
        &|m: &str, p: Value| calls.lock().unwrap().push((m.to_string(), p)),
        "T1",
    );
    (reply, calls.into_inner().unwrap())
}

#[test]
fn a_chosen_file_goes_into_the_pending_chooser_by_its_path() {
    let path = PathBuf::from("/home/op/Obrázky/náhľad 1.png");
    let chooser = Mutex::new(Some(42));
    let (reply, calls) = calls_of(Some(&path), &chooser);
    assert_eq!(
        calls,
        vec![(
            "DOM.setFileInputFiles".to_string(),
            json!({"files": ["/home/op/Obrázky/náhľad 1.png"], "backendNodeId": 42})
        )]
    );
    assert_eq!(
        reply,
        json!({"t": "file-done", "name": "náhľad 1.png", "how": "picker"})
    );
    assert_eq!(*chooser.lock().unwrap(), None, "the chooser is used up");
}

#[test]
fn a_chooser_the_page_dropped_meanwhile_gets_nothing() {
    // The page navigated while the dialog was open: its input is gone.
    let chooser = Mutex::new(None);
    let (reply, calls) = calls_of(Some(Path::new("/x/a.png")), &chooser);
    assert!(
        calls.is_empty(),
        "nothing is sent to a gone input: {calls:?}"
    );
    assert_eq!(reply["t"], "file-error");
    assert!(reply["error"].as_str().is_some_and(|e| !e.is_empty()));
}

#[test]
fn a_cancel_shows_the_bar_and_keeps_the_chooser() {
    let chooser = Mutex::new(Some(42));
    let (reply, calls) = calls_of(None, &chooser);
    assert!(calls.is_empty());
    assert_eq!(reply, json!({"t": "file-chooser", "mode": "selectSingle"}));
    assert_eq!(
        *chooser.lock().unwrap(),
        Some(42),
        "the bar can still fill it"
    );
}

struct Cancels;

impl NativeFilePicker for Cancels {
    fn pick_file(&self) -> rs_core::native_picker::Pick {
        Box::pin(async { None })
    }
}

/// The next "Upload thumbnail" after a closed dialog opens it again.
#[tokio::test]
async fn a_closed_dialog_lets_the_next_chooser_open_one() {
    let picking = Arc::new(AtomicBool::new(true));
    let reply = pick_for_page(
        Arc::new(Cancels),
        "selectSingle".into(),
        Arc::new(Mutex::new(Some(42))),
        |_: &str, _: Value| {},
        Arc::clone(&picking),
        "T1".into(),
    )
    .await;
    assert_eq!(reply["t"], "file-chooser");
    assert!(!picking.load(Ordering::SeqCst), "the dialog is closed");
}

/// #414 review: only a chooser that followed this viewer's own click or key
/// opens the box dialog, and only for an `<input>`.
#[test]
fn the_box_dialog_opens_only_after_the_viewers_own_input() {
    let s = |ms: u64| Some(std::time::Duration::from_millis(ms));
    assert!(opens_box_dialog(Some(42), s(0)));
    assert!(opens_box_dialog(Some(42), s(3_000)), "the window's edge");
    assert!(!opens_box_dialog(Some(42), s(3_001)), "too long after");
    assert!(!opens_box_dialog(Some(42), None), "the page by itself");
    assert!(!opens_box_dialog(None, s(0)), "no <input> to fill");
}

#[test]
fn a_press_is_the_viewers_own_click_or_key() {
    assert!(is_own_press(&json!({"t": "m", "type": "mousePressed"})));
    assert!(is_own_press(&json!({"t": "k", "type": "keyDown"})));
    assert!(is_own_press(&json!({"t": "k", "type": "rawKeyDown"})));
    assert!(!is_own_press(&json!({"t": "m", "type": "mouseMoved"})));
    assert!(!is_own_press(&json!({"t": "m", "type": "mouseReleased"})));
    assert!(!is_own_press(&json!({"t": "k", "type": "keyUp"})));
    assert!(!is_own_press(&json!({"t": "paste", "type": "keyDown"})));
}
