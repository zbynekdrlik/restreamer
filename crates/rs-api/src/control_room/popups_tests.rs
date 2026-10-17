use std::collections::HashSet;

use super::*;

fn tinfo(id: &str, url: &str, opener: Option<&str>) -> TargetInfo {
    TargetInfo {
        target_id: id.into(),
        kind: "page".into(),
        url: url.into(),
        title: String::new(),
        browser_context_id: None,
        opener_id: opener.map(Into::into),
        can_access_opener: false,
    }
}

fn opener_popup(id: &str, url: &str, opener: &str) -> TargetInfo {
    TargetInfo {
        can_access_opener: true,
        ..tinfo(id, url, Some(opener))
    }
}

#[test]
fn popups_are_the_http_tabs_the_room_opened() {
    let pages = vec![
        tinfo("R", "https://studio.youtube.com/", None),
        tinfo("P1", "https://www.youtube.com/account", Some("R")),
        tinfo("P2", "about:blank", Some("R")),
        tinfo("P3", "https://x/", Some("OTHER")),
        tinfo("P4", "http://y/", Some("R")),
        tinfo("P5", "chrome://newtab/", Some("R")),
    ];
    let ids: Vec<String> = popups_of(&pages, "R")
        .into_iter()
        .map(|t| t.target_id)
        .collect();
    assert_eq!(ids, vec!["P1", "P4"]);
}

#[test]
fn a_popup_that_can_reach_its_opener_is_shown_in_place_and_first() {
    let auth = opener_popup(
        "P1",
        "https://accounts.google.com/v3/signin/challenge/pwd",
        "R",
    );
    let link = tinfo("P2", "https://www.youtube.com/account", Some("R"));
    // The link is listed first: moving it into the room tab before the
    // popup is shown would destroy the popup's opener.
    let pages = vec![
        tinfo("R", "https://studio.youtube.com/", None),
        link.clone(),
        auth.clone(),
        opener_popup("P3", "https://x/", "OTHER"),
    ];
    assert_eq!(
        popup_moves(&pages, "R", &HashSet::new()),
        vec![PopupMove::ShowInPlace(auth), PopupMove::MoveIntoTab(link)]
    );
}

#[test]
fn popups_the_room_is_done_with_are_left_alone() {
    let pages = vec![
        tinfo("R", "https://studio.youtube.com/", None),
        opener_popup("P1", "https://studio.youtube.com/channel/UC2", "R"),
        tinfo("P2", "https://www.youtube.com/account", Some("R")),
    ];
    let skip: HashSet<String> = ["P1".to_string(), "P2".to_string()].into();
    assert_eq!(popup_moves(&pages, "R", &skip), vec![]);
}

#[test]
fn a_shown_popup_ends_when_it_closes_or_lands_on_a_room_page() {
    let room = tinfo(
        "R",
        "https://studio.youtube.com/channel/UC1/livestreaming",
        None,
    );
    let reauth = opener_popup("P", "https://studio.youtube.com/reauth?rapt=x", "R");
    let challenge = opener_popup(
        "P",
        "https://accounts.google.com/v3/signin/challenge/dp",
        "R",
    );
    assert_eq!(popup_end(&[room.clone(), reauth], "P"), None);
    assert_eq!(popup_end(&[room.clone(), challenge], "P"), None);
    assert_eq!(
        popup_end(std::slice::from_ref(&room), "P"),
        Some(PopupEnd::Closed)
    );
    let landed = opener_popup(
        "P",
        "https://studio.youtube.com/channel/UC2/livestreaming",
        "R",
    );
    assert_eq!(
        popup_end(&[room, landed], "P"),
        Some(PopupEnd::Landed(
            "https://studio.youtube.com/channel/UC2/livestreaming".into()
        ))
    );
}

#[test]
fn a_tab_closes_with_everything_it_opened() {
    let pages = vec![
        tinfo("R", "https://studio.youtube.com/", None),
        opener_popup("P1", "https://accounts.google.com/", "R"),
        tinfo("P2", "about:blank", Some("R")),
        tinfo("P3", "https://x/", Some("OTHER")),
    ];
    assert_eq!(opened_by(&pages, "R"), vec!["P1", "P2"]);
    assert!(opened_by(&pages, "P3").is_empty());
}

#[test]
fn a_switch_tells_the_dashboard_what_the_old_tab_took_with_it() {
    assert!(switch_notices(false, false).is_empty());
    assert_eq!(
        switch_notices(true, false),
        vec![json!({ "t": "file-chooser-closed" })]
    );
    let both = switch_notices(true, true);
    assert_eq!(both.len(), 2);
    assert_eq!(both[0]["t"], "file-chooser-closed");
    assert_eq!(both[1]["t"], "file-error");
    assert_eq!(switch_notices(false, true)[0]["t"], "file-error");
}

#[test]
fn the_popup_message_says_whether_a_popup_is_shown() {
    assert_eq!(popup_message(true), json!({ "t": "popup", "open": true }));
    assert_eq!(popup_message(false), json!({ "t": "popup", "open": false }));
}
