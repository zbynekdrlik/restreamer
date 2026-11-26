//! #437: set a fast endpoint's read-delay target live
//! (`POST /delivery/endpoints/fast-delay`, forwarded to the delivery VPS).

use serde::Deserialize;

use super::http_post_json;

/// What the VPS applied: the target before and after (clamped to 5..=120 s).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct FastDelaySet {
    pub alias: String,
    pub from_secs: u64,
    pub to_secs: u64,
}

pub async fn delivery_set_fast_delay(
    event_id: i64,
    alias: &str,
    target_secs: u64,
) -> Result<FastDelaySet, String> {
    let body = serde_json::json!({
        "event_id": event_id,
        "alias": alias,
        "target_secs": target_secs,
    });
    let value = http_post_json("/delivery/endpoints/fast-delay", &body).await?;
    serde_json::from_value(value).map_err(|e| format!("Parse error: {e}"))
}
