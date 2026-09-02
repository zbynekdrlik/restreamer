//! YouTube OAuth-grant endpoint helpers, split out of `api/mod.rs` to keep it
//! under the 1000-line cap (#199).

use super::{OAuthGrant, OauthSuggest, api_base, http_get};
use serde::Serialize;

/// List authorized YouTube OAuth grants (for the edit-endpoint OAuth dropdown).
pub async fn list_oauth_grants() -> Result<Vec<OAuthGrant>, String> {
    http_get("/youtube/oauths").await
}

/// Server-side auto-suggest verdict for one endpoint's stream key.
pub async fn oauth_suggest(id: i64) -> Result<OauthSuggest, String> {
    http_get(&format!("/endpoints/{id}/oauth-suggest")).await
}

/// Link (or, with `None`, unlink) an OAuth grant to an endpoint.
/// `POST /endpoints/{id}/link-oauth` returns 204 No Content, so this does
/// not parse a response body — any 2xx is success.
pub async fn link_endpoint_oauth(id: i64, oauth_id: Option<i64>) -> Result<(), String> {
    #[derive(Serialize)]
    struct Body {
        oauth_id: Option<i64>,
    }
    let url = format!("{}/endpoints/{id}/link-oauth", api_base());
    let resp = gloo_net::http::Request::post(&url)
        .header("Content-Type", "application/json")
        .body(serde_json::to_string(&Body { oauth_id }).map_err(|e| e.to_string())?)
        .map_err(|e| format!("Request error: {e}"))?
        .send()
        .await
        .map_err(|e| format!("HTTP error: {e}"))?;
    if !resp.ok() {
        return Err(format!("HTTP {}", resp.status()));
    }
    Ok(())
}
