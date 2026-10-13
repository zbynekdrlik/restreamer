//! Control rooms (#388): the YT/FB destination tabs of the dashboard.

use serde::{Deserialize, Serialize};

use super::{api_base, http_get, http_post_json, http_put_json};

/// One destination tab (`GET /control-rooms`). Never carries a stream key.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RoomDestination {
    pub endpoint_id: i64,
    pub alias: String,
    pub service_type: String,
    #[serde(default)]
    pub account: Option<String>,
    #[serde(default)]
    pub sure: bool,
    #[serde(default)]
    pub channel: Option<String>,
    #[serde(default)]
    pub channel_name: Option<String>,
    pub url: String,
    #[serde(default)]
    pub target_id: Option<String>,
    #[serde(default)]
    pub signed_in: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RoomEvent {
    pub id: i64,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct RoomsResponse {
    #[serde(default)]
    pub event: Option<RoomEvent>,
    #[serde(default)]
    pub destinations: Vec<RoomDestination>,
}

/// The sign-in account of one destination (`PUT .../account`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct RoomAccount {
    pub account: Option<String>,
    pub channel: Option<String>,
    pub channel_name: Option<String>,
    pub sure: bool,
}

pub async fn list_control_rooms() -> Result<RoomsResponse, String> {
    http_get("/control-rooms").await
}

/// Open (or reuse) a destination's room; returns its tab id.
pub async fn open_control_room(endpoint_id: i64) -> Result<String, String> {
    let v = http_post_json(
        &format!("/control-rooms/{endpoint_id}/open"),
        &serde_json::json!({}),
    )
    .await?;
    v.get("target_id")
        .and_then(|t| t.as_str())
        .map(str::to_string)
        .ok_or_else(|| "no target_id in the reply".to_string())
}

pub async fn set_control_room_account(
    endpoint_id: i64,
    account: &RoomAccount,
) -> Result<(), String> {
    http_put_json(&format!("/control-rooms/{endpoint_id}/account"), account).await
}

/// The screencast websocket of tab `target_id`.
pub fn control_room_ws_url(target_id: &str) -> String {
    ws_url_from_api_base(&api_base(), target_id)
}

/// `http(s)://host/api/v1` -> `ws(s)://host/api/v1/control-rooms/ws?target=..`.
pub fn ws_url_from_api_base(base: &str, target_id: &str) -> String {
    let ws_base = if let Some(rest) = base.strip_prefix("https://") {
        format!("wss://{rest}")
    } else if let Some(rest) = base.strip_prefix("http://") {
        format!("ws://{rest}")
    } else {
        base.to_string()
    };
    let target: String = target_id
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '_')
        .collect();
    format!("{ws_base}/control-rooms/ws?target={target}")
}

/// The tab label prefix of a service type.
pub fn service_badge(service_type: &str) -> &'static str {
    if service_type == "FB" { "FB" } else { "YT" }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ws_url_follows_the_page_scheme_and_sanitizes_the_target() {
        assert_eq!(
            ws_url_from_api_base("https://streampp.newlevel.media/api/v1", "AB12"),
            "wss://streampp.newlevel.media/api/v1/control-rooms/ws?target=AB12"
        );
        assert_eq!(
            ws_url_from_api_base("http://127.0.0.1:8910/api/v1", "x&y=1"),
            "ws://127.0.0.1:8910/api/v1/control-rooms/ws?target=xy1"
        );
    }

    #[test]
    fn badge_per_service() {
        assert_eq!(service_badge("FB"), "FB");
        assert_eq!(service_badge("YT_RTMP"), "YT");
    }

    #[test]
    fn rooms_response_parses_the_api_shape() {
        let r: RoomsResponse = serde_json::from_str(
            r#"{"event":{"id":3,"name":"Sunday"},"destinations":[{"endpoint_id":1,"alias":"YT A",
            "service_type":"YT_RTMP","account":"a@x.com","sure":true,"channel_name":null,
            "profile":"acct-a-x-com","url":"https://studio.youtube.com/","target_id":null,"signed_in":true}]}"#,
        )
        .unwrap();
        assert_eq!(r.event.unwrap().name, "Sunday");
        assert_eq!(r.destinations[0].signed_in, Some(true));
    }
}
