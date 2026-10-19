//! Rooms nobody views are hidden, so they stop rendering (#407).
//!
//! The control-room Chrome draws every window in software through one
//! compositor thread, and with no window manager nothing is ever occluded or
//! minimized: every open room kept decoding and compositing its live previews,
//! the thread saturated, and the room the operator looked at dropped more than
//! half its frames (stream-pp, 2026-10-18).
//!
//! `Page.setWebLifecycleState` `frozen` then `active` hides a page (Chrome's
//! `WasHidden`) and keeps its JS running; `Emulation.setFocusEmulationEnabled`
//! on the relay session keeps the shown room visible (measured on dev2, #407).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use serde_json::{Value, json};

use super::cdp::{Cdp, TargetInfo};

/// The tabs some relay shows right now, counted: two viewers may watch two
/// rooms (the PP kiosk and a notebook), and one must never hide the other's.
#[derive(Debug, Clone, Default)]
pub struct ShownTabs(Arc<Mutex<HashMap<String, u32>>>);

impl ShownTabs {
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, u32>> {
        self.0.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Count `tab` as shown until the guard drops.
    pub fn show(&self, tab: &str) -> ShownGuard {
        *self.lock().entry(tab.to_string()).or_default() += 1;
        ShownGuard {
            tabs: self.clone(),
            tab: tab.to_string(),
        }
    }

    pub fn is_shown(&self, tab: &str) -> bool {
        self.lock().contains_key(tab)
    }

    fn snapshot(&self) -> HashMap<String, u32> {
        self.lock().clone()
    }
}

/// One relay showing one tab.
#[derive(Debug)]
pub struct ShownGuard {
    tabs: ShownTabs,
    tab: String,
}

impl Drop for ShownGuard {
    fn drop(&mut self) {
        let mut m = self.tabs.lock();
        if let Some(n) = m.get_mut(&self.tab) {
            *n -= 1;
            if *n == 0 {
                m.remove(&self.tab);
            }
        }
    }
}

/// The pages to hide: every room tab (a web page no tab opened) that no
/// relay shows. A popup is the room's business: it is shown in place or
/// moved into its room, and the room is hidden with it.
pub fn to_hide(pages: &[TargetInfo], shown: &HashMap<String, u32>) -> Vec<String> {
    pages
        .iter()
        .filter(|t| t.url.starts_with("https://") || t.url.starts_with("http://"))
        .filter(|t| t.opener_id.is_none())
        .filter(|t| !shown.contains_key(&t.target_id))
        .map(|t| t.target_id.clone())
        .collect()
}

/// The `document.visibilityState` a `Runtime.evaluate` reply reports.
pub fn visibility(reply: &Value) -> Option<&str> {
    reply.pointer("/result/value").and_then(Value::as_str)
}

/// Hide `tab` if it is visible: `frozen` hides it, `active` lets its JS run
/// on. A hidden page is left alone (a freeze would close its WebSockets for
/// nothing). Returns whether it was hidden now.
pub async fn hide_if_visible(cdp: &Cdp, tab: &str) -> anyhow::Result<bool> {
    let reply = cdp
        .page_call(
            tab,
            "Runtime.evaluate",
            json!({ "expression": "document.visibilityState", "returnByValue": true }),
        )
        .await?;
    if visibility(&reply) != Some("visible") {
        return Ok(false);
    }
    for state in ["frozen", "active"] {
        cdp.page_call(tab, "Page.setWebLifecycleState", json!({ "state": state }))
            .await?;
    }
    Ok(true)
}

/// Hide every visible web page no relay shows. Failures are logged: a page
/// that cannot be hidden only costs CPU.
pub async fn hide_others(cdp: &Cdp, shown: &ShownTabs) {
    let pages = match cdp.pages().await {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!("control rooms: listing tabs to hide failed: {e:#}");
            return;
        }
    };
    for tab in to_hide(&pages, &shown.snapshot()) {
        // A viewer may have opened it since the snapshot.
        if shown.is_shown(&tab) {
            continue;
        }
        match hide_if_visible(cdp, &tab).await {
            Ok(true) => tracing::info!("control rooms: tab {tab} hidden (nobody views it)"),
            Ok(false) => {}
            Err(e) => tracing::warn!("control rooms: hiding tab {tab} failed: {e:#}"),
        }
    }
}

#[cfg(test)]
#[path = "visibility_tests.rs"]
mod tests;
