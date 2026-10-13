//! Control rooms (#388): the YT Studio / FB Live Producer page of every
//! streaming destination, opened in a box-hosted Chrome and shown inside the
//! dashboard as a tab (screencast + mouse/keyboard), locally and remotely.
//!
//! - One Chrome profile per ACCOUNT (`acct-<email>`): the YT channels are
//!   brand channels of one Google account, so all of an account's
//!   destinations share one sign-in. An endpoint with no known account gets
//!   its own profile (`dest-<id>`).
//! - The account of each endpoint lives in a box-local file,
//!   `<data_dir>/control-rooms.json` (emails never go into the repo).
//! - Which tab belongs to which endpoint is kept in
//!   `<data_dir>/control-room-tabs.json`, so a Restreamer restart reattaches
//!   to the tabs the (still running) Chrome has open.

pub mod browser;
pub mod cdp;
#[cfg(test)]
pub(crate) mod fake_chrome;
pub mod handlers;
pub mod relay;

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use rs_core::models::EndpointConfig;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::Mutex;

use browser::Browser;
use cdp::TargetInfo;

/// Endpoint service types that have a control room.
pub const ROOM_TYPES: &[&str] = &["YT_RTMP", "FB"];

/// What the operator should sign in with for one endpoint.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Account {
    /// The account's sign-in email.
    #[serde(default)]
    pub account: Option<String>,
    /// YouTube channel id (`UC...`): the room opens that channel's live page.
    #[serde(default)]
    pub channel: Option<String>,
    #[serde(default)]
    pub channel_name: Option<String>,
    /// False when the account was only inferred (shown as "(?)").
    #[serde(default)]
    pub sure: bool,
}

/// `<data_dir>/control-rooms.json`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AccountsFile {
    /// Keyed by endpoint id.
    #[serde(default)]
    pub endpoints: BTreeMap<String, Account>,
}

/// `<data_dir>/control-room-tabs.json`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TabMap {
    /// browserContextId -> profile directory.
    #[serde(default)]
    pub ctx: HashMap<String, String>,
    /// targetId -> endpoint id.
    #[serde(default)]
    pub tab: HashMap<String, i64>,
}

/// One destination as the dashboard shows it.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Destination {
    pub endpoint_id: i64,
    pub alias: String,
    pub service_type: String,
    pub account: Option<String>,
    pub sure: bool,
    /// YouTube channel id the room opens (kept so an account edit preserves it).
    pub channel: Option<String>,
    pub channel_name: Option<String>,
    pub profile: String,
    pub url: String,
    /// The open tab of this destination, if any.
    pub target_id: Option<String>,
    /// From the profile's login cookie; `None` when no tab of that profile
    /// is open (or Chrome is not running).
    pub signed_in: Option<bool>,
}

/// `acct-<email>`: the profile of a signed-in ACCOUNT.
pub fn account_profile(account: &str) -> String {
    format!(
        "acct-{}",
        account
            .trim()
            .to_lowercase()
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
            .collect::<String>()
    )
}

fn non_empty(account: Option<&str>) -> Option<&str> {
    account.map(str::trim).filter(|a| !a.is_empty())
}

/// The Chrome profile directory of an endpoint.
///
/// YouTube: one profile per DESTINATION. A Google profile has one active
/// channel identity at a time, so two brand channels of one account cannot
/// share a profile (#388 finding: the second one asks "Select a channel" and
/// switching breaks the first). Facebook picks the page inside Live
/// Producer, so FB shares one profile per account.
pub fn profile_for(service_type: &str, account: Option<&str>, endpoint_id: i64) -> String {
    match non_empty(account) {
        Some(a) if service_type == "FB" => account_profile(a),
        _ => format!("dest-{endpoint_id}"),
    }
}

/// Signed-in profiles a new YouTube destination profile can be cloned from
/// (same account), best first: the account's own profile, then the other
/// YouTube destinations of that account. Empty for FB or an unknown account.
pub fn clone_sources(
    dest: &Destination,
    endpoints: &[EndpointConfig],
    accounts: &AccountsFile,
) -> Vec<String> {
    let Some(account) = non_empty(dest.account.as_deref()) else {
        return vec![];
    };
    if dest.service_type == "FB" {
        return vec![];
    }
    let mut out = vec![account_profile(account)];
    for e in endpoints {
        let same = accounts
            .endpoints
            .get(&e.id.to_string())
            .and_then(|a| non_empty(a.account.as_deref()))
            .is_some_and(|a| a.eq_ignore_ascii_case(account));
        if same && e.id != dest.endpoint_id && e.service_type == "YT_RTMP" {
            out.push(format!("dest-{}", e.id));
        }
    }
    out
}

/// Profile parts never worth copying (caches, locks).
const CLONE_SKIP: &[&str] = &[
    "Cache",
    "Code Cache",
    "GPUCache",
    "DawnGraphiteCache",
    "DawnWebGPUCache",
    "GrShaderCache",
    "ShaderCache",
    "Service Worker",
    "Shared Dictionary",
    "blob_storage",
    "LOCK",
];

fn copy_dir(from: &Path, to: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(to)?;
    for entry in std::fs::read_dir(from)? {
        let entry = entry?;
        let name = entry.file_name();
        if CLONE_SKIP.iter().any(|s| name == *s) {
            continue;
        }
        let ty = entry.file_type()?;
        if ty.is_dir() {
            copy_dir(&entry.path(), &to.join(&name))?;
        } else if ty.is_file() {
            std::fs::copy(entry.path(), to.join(&name))?;
        }
    }
    Ok(())
}

/// Create `profile` as a copy of the first existing `sources` profile, unless
/// `profile` already exists. Returns the source used.
pub fn clone_profile(
    user_data: &Path,
    profile: &str,
    sources: &[String],
) -> Result<Option<String>> {
    if user_data.join(profile).exists() {
        return Ok(None);
    }
    let Some(src) = sources
        .iter()
        .find(|s| s.as_str() != profile && user_data.join(s.as_str()).is_dir())
    else {
        return Ok(None);
    };
    copy_dir(&user_data.join(src), &user_data.join(profile))
        .with_context(|| format!("clone profile {src} -> {profile}"))?;
    Ok(Some(src.clone()))
}

/// The control-room page of a destination.
pub fn room_url(service_type: &str, channel: Option<&str>) -> Option<String> {
    match (
        service_type,
        channel.map(str::trim).filter(|c| !c.is_empty()),
    ) {
        ("YT_RTMP", Some(ch)) => Some(format!(
            "https://studio.youtube.com/channel/{ch}/livestreaming"
        )),
        ("YT_RTMP", None) => Some("https://studio.youtube.com/".into()),
        ("FB", _) => Some("https://www.facebook.com/live/producer/".into()),
        _ => None,
    }
}

/// The cookie a signed-in session leaves: (url to read it for, name).
pub fn login_cookie(service_type: &str) -> (&'static str, &'static str) {
    if service_type == "FB" {
        ("https://www.facebook.com/", "c_user")
    } else {
        ("https://www.youtube.com/", "SID")
    }
}

/// The destinations of `endpoints` (control-room types only), joined with
/// the account mapping. Pure: no Chrome involved.
pub fn destinations(endpoints: &[EndpointConfig], accounts: &AccountsFile) -> Vec<Destination> {
    endpoints
        .iter()
        .filter_map(|e| {
            let url_default = Account::default();
            let a = accounts
                .endpoints
                .get(&e.id.to_string())
                .unwrap_or(&url_default);
            let url = room_url(&e.service_type, a.channel.as_deref())?;
            Some(Destination {
                endpoint_id: e.id,
                alias: e.alias.clone(),
                service_type: e.service_type.clone(),
                account: a.account.clone().filter(|s| !s.trim().is_empty()),
                sure: a.sure,
                channel: a.channel.clone().filter(|c| !c.trim().is_empty()),
                channel_name: a.channel_name.clone(),
                profile: profile_for(&e.service_type, a.account.as_deref(), e.id),
                url,
                target_id: None,
                signed_in: None,
            })
        })
        .collect()
}

/// Fill `target_id` from the live tabs: a tab counts only while Chrome still
/// has it open. Returns, per profile, one live page to read cookies through.
pub fn attach_tabs(
    dests: &mut [Destination],
    map: &TabMap,
    pages: &[TargetInfo],
) -> HashMap<String, String> {
    let mut page_of_profile = HashMap::new();
    for p in pages {
        if let Some(prof) = p.browser_context_id.as_ref().and_then(|c| map.ctx.get(c)) {
            page_of_profile
                .entry(prof.clone())
                .or_insert_with(|| p.target_id.clone());
        }
    }
    for d in dests.iter_mut() {
        d.target_id = pages
            .iter()
            .find(|p| map.tab.get(&p.target_id) == Some(&d.endpoint_id))
            .map(|p| p.target_id.clone());
    }
    page_of_profile
}

fn read_json<T: Default + for<'de> Deserialize<'de>>(path: &Path) -> T {
    match std::fs::read_to_string(path) {
        Ok(s) => serde_json::from_str(&s).unwrap_or_else(|e| {
            tracing::warn!("control rooms: ignoring unreadable {}: {e}", path.display());
            T::default()
        }),
        Err(_) => T::default(),
    }
}

fn write_json<T: Serialize>(path: &Path, v: &T) -> Result<()> {
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, serde_json::to_vec_pretty(v)?)
        .with_context(|| format!("write {}", tmp.display()))?;
    std::fs::rename(&tmp, path).with_context(|| format!("replace {}", path.display()))?;
    Ok(())
}

/// The control rooms of this box.
pub struct ControlRooms {
    pub browser: Browser,
    accounts_path: PathBuf,
    tabs_path: PathBuf,
    tabs: Mutex<TabMap>,
    /// One open at a time: two concurrent opens would each see the other's new
    /// tab as "theirs" (duplicate tabs, a tab mapped to the wrong endpoint).
    open_lock: Mutex<()>,
}

impl ControlRooms {
    pub fn new(data_dir: &Path) -> Self {
        Self::with_browser(data_dir, Browser::new(data_dir))
    }

    pub fn with_browser(data_dir: &Path, browser: Browser) -> Self {
        let tabs_path = data_dir.join("control-room-tabs.json");
        Self {
            browser,
            accounts_path: data_dir.join("control-rooms.json"),
            tabs: Mutex::new(read_json(&tabs_path)),
            open_lock: Mutex::new(()),
            tabs_path,
        }
    }

    pub fn accounts(&self) -> AccountsFile {
        read_json(&self.accounts_path)
    }

    /// Set (or clear, with `Account::default()`) one endpoint's account.
    pub fn set_account(&self, endpoint_id: i64, account: Account) -> Result<()> {
        let mut f = self.accounts();
        if account == Account::default() {
            f.endpoints.remove(&endpoint_id.to_string());
        } else {
            f.endpoints.insert(endpoint_id.to_string(), account);
        }
        write_json(&self.accounts_path, &f)
    }

    /// The destinations of `endpoints` with their live tab and sign-in state.
    /// Never starts Chrome: without it, nothing is open or signed in.
    pub async fn status(&self, endpoints: &[EndpointConfig]) -> Vec<Destination> {
        let mut dests = destinations(endpoints, &self.accounts());
        let Ok(pages) = self.browser.cdp.pages().await else {
            return dests;
        };
        let map = self.tabs.lock().await.clone();
        let page_of_profile = attach_tabs(&mut dests, &map, &pages);
        let mut signed: HashMap<String, Option<bool>> = HashMap::new();
        for d in dests.iter_mut() {
            let key = format!("{}|{}", d.profile, d.service_type);
            if !signed.contains_key(&key) {
                let v = match page_of_profile.get(&d.profile) {
                    Some(page) => self.signed_in(page, &d.service_type).await,
                    None => None,
                };
                signed.insert(key.clone(), v);
            }
            d.signed_in = signed[&key];
        }
        dests
    }

    async fn signed_in(&self, page: &str, service_type: &str) -> Option<bool> {
        let (url, name) = login_cookie(service_type);
        match self
            .browser
            .cdp
            .page_call(page, "Network.getCookies", json!({ "urls": [url] }))
            .await
        {
            Ok(r) => Some(
                r.get("cookies")
                    .and_then(|c| c.as_array())
                    .is_some_and(|cs| {
                        cs.iter()
                            .any(|c| c.get("name").and_then(|n| n.as_str()) == Some(name))
                    }),
            ),
            Err(e) => {
                tracing::warn!("control rooms: cookie check on {page} failed: {e:#}");
                None
            }
        }
    }

    /// Open (or reuse) the room of `dest`; returns its tab's target id.
    /// `sources`: signed-in profiles a missing YouTube profile is cloned from
    /// (see [`clone_sources`]).
    pub async fn open(&self, dest: &Destination, sources: &[String]) -> Result<String> {
        let _one_at_a_time = self.open_lock.lock().await;
        self.browser.ensure_running().await?;
        let before = self.browser.cdp.pages().await?;
        {
            let map = self.tabs.lock().await;
            if let Some(t) = before
                .iter()
                .find(|t| map.tab.get(&t.target_id) == Some(&dest.endpoint_id))
            {
                return Ok(t.target_id.clone());
            }
        }
        match clone_profile(&self.browser.user_data, &dest.profile, sources) {
            Ok(Some(src)) => tracing::info!(
                "control rooms: profile {} cloned from signed-in {src} (pick the channel once in the tab)",
                dest.profile
            ),
            Ok(None) => {}
            Err(e) => tracing::warn!("control rooms: {e:#}; {} starts signed out", dest.profile),
        }
        self.browser.open(&dest.profile, &dest.url).await?;
        for _ in 0..60 {
            tokio::time::sleep(Duration::from_millis(250)).await;
            let now = self.browser.cdp.pages().await?;
            if let Some(t) = new_target(&before, &now) {
                let mut map = self.tabs.lock().await;
                if let Some(ctx) = &t.browser_context_id {
                    map.ctx.insert(ctx.clone(), dest.profile.clone());
                }
                let alive: Vec<String> = now.iter().map(|p| p.target_id.clone()).collect();
                map.tab.retain(|k, _| alive.contains(k));
                map.tab.insert(t.target_id.clone(), dest.endpoint_id);
                if let Err(e) = write_json(&self.tabs_path, &*map) {
                    tracing::warn!("control rooms: could not persist the tab map: {e:#}");
                }
                tracing::info!(
                    "control rooms: endpoint {} ({}) -> profile {} tab {}",
                    dest.endpoint_id,
                    dest.alias,
                    dest.profile,
                    t.target_id
                );
                return Ok(t.target_id.clone());
            }
        }
        Err(anyhow!("no tab appeared for {} within 15 s", dest.alias))
    }
}

/// The new tab that carries the room: the first page in `now` that was not
/// in `before`, skipping the blank / new-tab pages a profile's own startup
/// window may add next to it (seen on stream-pp).
pub fn new_target<'a>(before: &[TargetInfo], now: &'a [TargetInfo]) -> Option<&'a TargetInfo> {
    now.iter().find(|t| {
        !before.iter().any(|b| b.target_id == t.target_id)
            && t.url != "about:blank"
            && !t.url.starts_with("chrome://")
    })
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
