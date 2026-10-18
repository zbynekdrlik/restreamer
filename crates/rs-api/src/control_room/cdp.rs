//! Minimal Chrome DevTools Protocol client for the control rooms (#388).
//!
//! Only the handful of calls the control rooms need: target listing, one-shot
//! page/browser calls, and a persistent page session for the screencast
//! relay. Everything talks to Chrome's debugging port on 127.0.0.1.

use anyhow::{Context, Result, anyhow, bail};
use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio_tungstenite::tungstenite::Message;

/// One target as `Target.getTargets` reports it.
#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TargetInfo {
    pub target_id: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub url: String,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub browser_context_id: Option<String>,
    /// The tab that opened this one (`window.open`, a `target=_blank` link).
    #[serde(default)]
    pub opener_id: Option<String>,
    /// Whether this tab can still reach its opener (`window.opener`): true for
    /// a `window.open` popup, false for a `noopener` link (#405).
    #[serde(default)]
    pub can_access_opener: bool,
}

/// The page targets an operator can see: real tabs, no extension pages.
pub fn page_targets(all: Vec<TargetInfo>) -> Vec<TargetInfo> {
    all.into_iter()
        .filter(|t| t.kind == "page" && !t.url.starts_with("chrome-extension://"))
        .collect()
}

/// Build the JSON text of one CDP request.
pub fn request(id: u64, method: &str, params: Value) -> String {
    json!({ "id": id, "method": method, "params": params }).to_string()
}

/// The result of the response to request `id`, if `msg` is that response.
/// `Some(Err)` for a CDP error, `None` for any other message (an event or a
/// response to another request).
pub fn response_for(id: u64, msg: &Value) -> Option<Result<Value>> {
    if msg.get("id").and_then(Value::as_u64) != Some(id) {
        return None;
    }
    if let Some(err) = msg.get("error") {
        return Some(Err(anyhow!("CDP error: {err}")));
    }
    Some(Ok(msg.get("result").cloned().unwrap_or(Value::Null)))
}

/// Chrome's debugging endpoint on 127.0.0.1.
#[derive(Debug, Clone)]
pub struct Cdp {
    /// `host:port` of the debugging port.
    pub addr: String,
}

impl Cdp {
    pub fn new(addr: impl Into<String>) -> Self {
        Self { addr: addr.into() }
    }

    /// The browser-level websocket URL from `/json/version`.
    pub async fn browser_ws_url(&self) -> Result<String> {
        let v: Value = reqwest::Client::new()
            .get(format!("http://{}/json/version", self.addr))
            .timeout(std::time::Duration::from_secs(5))
            .send()
            .await
            .context("Chrome debugging port did not answer")?
            .json()
            .await
            .context("bad /json/version body")?;
        v.get("webSocketDebuggerUrl")
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| anyhow!("/json/version has no webSocketDebuggerUrl"))
    }

    /// The websocket URL of one page target.
    pub fn page_ws_url(&self, target_id: &str) -> String {
        format!("ws://{}/devtools/page/{target_id}", self.addr)
    }

    /// One call on the browser endpoint.
    pub async fn browser_call(&self, method: &str, params: Value) -> Result<Value> {
        let url = self.browser_ws_url().await?;
        one_call(&url, method, params).await
    }

    /// One call on a page target.
    pub async fn page_call(&self, target_id: &str, method: &str, params: Value) -> Result<Value> {
        one_call(&self.page_ws_url(target_id), method, params).await
    }

    /// All page targets (tabs) of the browser.
    pub async fn pages(&self) -> Result<Vec<TargetInfo>> {
        let r = self.browser_call("Target.getTargets", json!({})).await?;
        let infos: Vec<TargetInfo> = serde_json::from_value(
            r.get("targetInfos")
                .cloned()
                .unwrap_or(Value::Array(vec![])),
        )
        .context("bad Target.getTargets result")?;
        Ok(page_targets(infos))
    }
}

/// Open a websocket, send one request, return its result.
async fn one_call(url: &str, method: &str, params: Value) -> Result<Value> {
    let (mut ws, _) = tokio_tungstenite::connect_async(url)
        .await
        .with_context(|| format!("CDP connect {url}"))?;
    ws.send(Message::Text(request(1, method, params).into()))
        .await?;
    let wait = async {
        while let Some(msg) = ws.next().await {
            if let Message::Text(text) = msg? {
                let v: Value = serde_json::from_str(&text)?;
                if let Some(r) = response_for(1, &v) {
                    return r;
                }
            }
        }
        bail!("CDP socket closed before the {method} reply")
    };
    let out = tokio::time::timeout(std::time::Duration::from_secs(10), wait)
        .await
        .map_err(|_| anyhow!("CDP {method} timed out"))?;
    let _ = ws.close(None).await;
    out
}

#[cfg(test)]
#[path = "cdp_tests.rs"]
mod tests;
