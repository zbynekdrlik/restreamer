//! Manage-scope YouTube Data API client for the A/V gate (#357).
//!
//! Everything else in this crate reads with `youtube.readonly`. The A/V gate
//! needs to CREATE a broadcast, bind it, and transition it live and complete,
//! which needs the `youtube` scope. That grant lives only on stream.lan, in a
//! JSON file written by the owner's device-flow consent
//! (`{refresh_token, scope, ...}`); this client reads it from there and
//! refreshes it with the `youtube.device_flow` client credentials.
//!
//! Secret handling: the refresh token, the client secret and every access
//! token stay inside this module. `Debug` is redacted, errors never quote the
//! oauth file or a token response, and nothing here logs a value.
//!
//! Every call charges its YouTube quota cost ([`units`]) to a counter BEFORE
//! the request (Google bills failed calls too), so the session can enforce its
//! daily budget from real spend. With [`ManageClient::with_quota_tracker`] it
//! also draws from the process-wide project bucket the health polling uses, so
//! the two together can never plan past Google's per-project quota.
//!
//! The token refresh does not reuse `oauth::refresh_access_token`: that one
//! copies Google's response body into its error, and a body can echo request
//! data. Here only Google's error CODE leaves the refresh.

use std::path::Path;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use reqwest::{Client, Method};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::Mutex;

use crate::quota::QuotaTracker;
use crate::{Result, YouTubeError};

/// The scope that allows inserting and transitioning broadcasts.
pub const MANAGE_SCOPE: &str = "https://www.googleapis.com/auth/youtube";
pub const DEFAULT_API_BASE: &str = "https://www.googleapis.com/youtube/v3";
pub const DEFAULT_TOKEN_URI: &str = "https://oauth2.googleapis.com/token";

/// YouTube Data API v3 quota cost per call.
pub mod units {
    pub const LIST: u32 = 1;
    pub const INSERT: u32 = 50;
    pub const BIND: u32 = 50;
    pub const TRANSITION: u32 = 50;
}

/// What a call costs, and whether the shared project bucket may refuse it.
/// Completing a live broadcast (and reading its life cycle for that) is
/// `forced`: the bucket goes into debt rather than leave a broadcast on air.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Cost {
    units: u32,
    forced: bool,
}

const fn admit(units: u32) -> Cost {
    Cost {
        units,
        forced: false,
    }
}

const fn forced(units: u32) -> Cost {
    Cost {
        units,
        forced: true,
    }
}

/// A cached access token is refreshed this long before Google says it expires.
const EXPIRY_MARGIN: Duration = Duration::from_secs(60);
/// Pagination cap for the stream lookup (same bound as `streams.rs`, #200).
const MAX_PAGES: usize = 10;

/// The manage-scope grant plus the client credentials that refresh it.
#[derive(Clone)]
pub struct ManageCredentials {
    refresh_token: String,
    client_id: String,
    client_secret: String,
}

impl std::fmt::Debug for ManageCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManageCredentials")
            .field("refresh_token", &"***")
            .field("client_id", &self.client_id)
            .field("client_secret", &"***")
            .finish()
    }
}

#[derive(Deserialize)]
struct OAuthFile {
    #[serde(default)]
    refresh_token: String,
    #[serde(default)]
    scope: String,
}

/// True when the space-separated `scope` list grants [`MANAGE_SCOPE`] itself
/// (not only `youtube.readonly`, which merely starts with the same text).
pub fn grants_manage_scope(scope: &str) -> bool {
    scope.split_whitespace().any(|s| s == MANAGE_SCOPE)
}

impl ManageCredentials {
    /// Read the grant from `path` and pair it with the device-flow client.
    /// Fails closed: a missing file, unparsable JSON, an empty refresh token,
    /// a grant without [`MANAGE_SCOPE`] or empty client credentials are all
    /// errors. No error message quotes the file.
    pub fn from_oauth_file(path: &Path, client_id: &str, client_secret: &str) -> Result<Self> {
        let raw = std::fs::read_to_string(path).map_err(|e| {
            YouTubeError::OAuth(format!(
                "cannot read the av-gate oauth file {}: {}",
                path.display(),
                e.kind()
            ))
        })?;
        // PowerShell `Set-Content -Encoding UTF8` writes a BOM.
        let raw = raw.strip_prefix('\u{FEFF}').unwrap_or(&raw);
        let file: OAuthFile = serde_json::from_str(raw).map_err(|_| {
            YouTubeError::OAuth(format!(
                "the av-gate oauth file {} is not valid JSON",
                path.display()
            ))
        })?;
        if file.refresh_token.trim().is_empty() {
            return Err(YouTubeError::OAuth(
                "the av-gate oauth file has no refresh_token".to_string(),
            ));
        }
        if !grants_manage_scope(&file.scope) {
            return Err(YouTubeError::OAuth(format!(
                "the av-gate grant lacks the {MANAGE_SCOPE} scope"
            )));
        }
        if client_id.is_empty() || client_secret.is_empty() {
            return Err(YouTubeError::OAuth(
                "youtube.device_flow client_id/client_secret are not configured".to_string(),
            ));
        }
        Ok(Self {
            refresh_token: file.refresh_token.trim().to_string(),
            client_id: client_id.to_string(),
            client_secret: client_secret.to_string(),
        })
    }
}

/// `liveBroadcasts/transition` target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BroadcastTransition {
    Live,
    Complete,
}

impl BroadcastTransition {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Live => "live",
            Self::Complete => "complete",
        }
    }
}

/// The reusable stream a session binds to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedStream {
    pub id: String,
    pub is_reusable: bool,
    pub stream_status: String,
}

struct CachedToken {
    value: String,
    refresh_after: Instant,
}

/// The client. One per session; cheap to build.
pub struct ManageClient {
    http: Client,
    api_base: String,
    token_uri: String,
    creds: ManageCredentials,
    cached: Mutex<Option<CachedToken>>,
    units: AtomicU32,
    /// The project-wide bucket shared with the health polling, if attached.
    quota: Option<&'static QuotaTracker>,
}

/// Google's `{"error": {"message", "errors": [{"reason"}]}}` as
/// `"<reason>: <message>"`. Never contains a credential.
fn api_error_message(body: &str) -> String {
    let v: Value = serde_json::from_str(body).unwrap_or(Value::Null);
    let message = v["error"]["message"].as_str().unwrap_or("");
    let reason = v["error"]["errors"][0]["reason"].as_str().unwrap_or("");
    match (reason.is_empty(), message.is_empty()) {
        (false, _) => format!("{reason}: {message}"),
        (true, false) => message.to_string(),
        (true, true) => "unparsable error body".to_string(),
    }
}

/// A cached token is reused strictly before its refresh point.
fn token_is_fresh(now: Instant, refresh_after: Instant) -> bool {
    now < refresh_after
}

/// True for the error YouTube returns when a broadcast is ALREADY in the
/// requested state. Transitions are retried by the session, so this is success.
pub fn is_redundant_transition(err: &YouTubeError) -> bool {
    matches!(err, YouTubeError::Api { message, .. } if message.starts_with("redundantTransition"))
}

impl ManageClient {
    /// A client against the real Google endpoints.
    pub fn new(creds: ManageCredentials) -> Self {
        Self::with_endpoints(creds, DEFAULT_API_BASE, DEFAULT_TOKEN_URI)
    }

    /// A client against other endpoints (wiremock in tests).
    pub fn with_endpoints(creds: ManageCredentials, api_base: &str, token_uri: &str) -> Self {
        let http = Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(30))
            .build()
            .unwrap_or_else(|_| Client::new());
        Self {
            http,
            api_base: api_base.trim_end_matches('/').to_string(),
            token_uri: token_uri.to_string(),
            creds,
            cached: Mutex::new(None),
            units: AtomicU32::new(0),
            quota: None,
        }
    }

    /// Draw every call's cost from `tracker` too; a call the bucket cannot
    /// pay is refused before it is sent.
    pub fn with_quota_tracker(mut self, tracker: &'static QuotaTracker) -> Self {
        self.quota = Some(tracker);
        self
    }

    /// Quota units charged so far by this client.
    pub fn units_used(&self) -> u32 {
        self.units.load(Ordering::Relaxed)
    }

    /// A valid access token, refreshing when the cached one is due.
    async fn access_token(&self) -> Result<String> {
        let mut cached = self.cached.lock().await;
        if let Some(t) = cached.as_ref() {
            if token_is_fresh(Instant::now(), t.refresh_after) {
                return Ok(t.value.clone());
            }
        }
        let resp = self
            .http
            .post(&self.token_uri)
            .form(&[
                ("client_id", self.creds.client_id.as_str()),
                ("client_secret", self.creds.client_secret.as_str()),
                ("refresh_token", self.creds.refresh_token.as_str()),
                ("grant_type", "refresh_token"),
            ])
            .send()
            .await?;
        let status = resp.status();
        let body: Value = resp.json().await.unwrap_or(Value::Null);
        if !status.is_success() {
            // Only Google's error CODE: the description can echo request data.
            let code = body["error"].as_str().unwrap_or("unknown_error");
            return Err(YouTubeError::TokenExpired(format!(
                "av-gate token refresh failed: HTTP {} {code}",
                status.as_u16()
            )));
        }
        let value = body["access_token"]
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                YouTubeError::OAuth("token refresh returned no access_token".to_string())
            })?
            .to_string();
        let lifetime = Duration::from_secs(body["expires_in"].as_u64().unwrap_or(3600));
        *cached = Some(CachedToken {
            value: value.clone(),
            refresh_after: Instant::now() + lifetime.saturating_sub(EXPIRY_MARGIN),
        });
        Ok(value)
    }

    /// One HTTP attempt: charge `cost`, send with the current token, return
    /// the status and body text.
    async fn send_once(
        &self,
        method: &Method,
        path: &str,
        query: &[(&str, &str)],
        body: Option<&Value>,
        cost: Cost,
    ) -> Result<(u16, String)> {
        match self.quota {
            Some(q) if cost.forced => q.charge(cost.units),
            Some(q) => q
                .acquire(cost.units)
                .map_err(|e| YouTubeError::Other(e.to_string()))?,
            None => {}
        }
        self.units.fetch_add(cost.units, Ordering::Relaxed);
        let token = self.access_token().await?;
        let mut req = self
            .http
            .request(method.clone(), format!("{}/{path}", self.api_base))
            .bearer_auth(token)
            .query(query);
        if let Some(b) = body {
            req = req.json(b);
        }
        let resp = req.send().await?;
        let status = resp.status().as_u16();
        Ok((status, resp.text().await.unwrap_or_default()))
    }

    /// One Data API call. A 401 drops the cached token and retries ONCE with
    /// a fresh one (a revoked or rotated access token).
    async fn call(
        &self,
        method: Method,
        path: &str,
        query: &[(&str, &str)],
        body: Option<&Value>,
        cost: Cost,
    ) -> Result<Value> {
        let mut sent = self.send_once(&method, path, query, body, cost).await?;
        if sent.0 == 401 {
            *self.cached.lock().await = None;
            sent = self.send_once(&method, path, query, body, cost).await?;
        }
        let (status, text) = sent;
        if !(200..300).contains(&status) {
            return Err(YouTubeError::Api {
                status,
                message: api_error_message(&text),
            });
        }
        Ok(serde_json::from_str(&text).unwrap_or(Value::Null))
    }

    /// The stream titled exactly `title` among the channel's own streams.
    pub async fn find_stream_by_title(&self, title: &str) -> Result<Option<ManagedStream>> {
        let mut page_token = String::new();
        for _ in 0..MAX_PAGES {
            let mut query = vec![
                ("part", "id,snippet,contentDetails,status"),
                ("mine", "true"),
                ("maxResults", "50"),
            ];
            if !page_token.is_empty() {
                query.push(("pageToken", page_token.as_str()));
            }
            let page = self
                .call(Method::GET, "liveStreams", &query, None, admit(units::LIST))
                .await?;
            let items = page["items"].as_array().cloned().unwrap_or_default();
            if let Some(s) = items.iter().find(|s| s["snippet"]["title"] == title) {
                return Ok(Some(ManagedStream {
                    id: s["id"].as_str().unwrap_or_default().to_string(),
                    is_reusable: s["contentDetails"]["isReusable"].as_bool() == Some(true),
                    stream_status: s["status"]["streamStatus"]
                        .as_str()
                        .unwrap_or_default()
                        .to_string(),
                }));
            }
            match page["nextPageToken"].as_str() {
                Some(t) if !t.is_empty() => page_token = t.to_string(),
                _ => return Ok(None),
            }
        }
        Ok(None)
    }

    /// `status.streamStatus` of one stream (`active` once YouTube receives
    /// data), or `None` when the stream no longer exists.
    pub async fn stream_status(&self, stream_id: &str) -> Result<Option<String>> {
        let v = self
            .call(
                Method::GET,
                "liveStreams",
                &[("part", "status"), ("id", stream_id)],
                None,
                admit(units::LIST),
            )
            .await?;
        Ok(v["items"][0]["status"]["streamStatus"]
            .as_str()
            .map(str::to_string))
    }

    /// Create an unlisted broadcast with auto-start, auto-stop and the monitor
    /// stream off (owner rules, 2026-10-05). Returns its id, which is also the
    /// id of the VOD YouTube makes from it.
    pub async fn insert_broadcast(&self, title: &str, scheduled_start: &str) -> Result<String> {
        let body = json!({
            "snippet": { "title": title, "scheduledStartTime": scheduled_start },
            "status": { "privacyStatus": "unlisted", "selfDeclaredMadeForKids": false },
            "contentDetails": {
                "enableAutoStart": false,
                "enableAutoStop": false,
                "monitorStream": { "enableMonitorStream": false }
            }
        });
        let v = self
            .call(
                Method::POST,
                "liveBroadcasts",
                &[("part", "id,snippet,status,contentDetails")],
                Some(&body),
                admit(units::INSERT),
            )
            .await?;
        v["id"]
            .as_str()
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .ok_or_else(|| YouTubeError::Other("liveBroadcasts.insert returned no id".to_string()))
    }

    /// Bind the broadcast to a stream.
    pub async fn bind_broadcast(&self, broadcast_id: &str, stream_id: &str) -> Result<()> {
        self.call(
            Method::POST,
            "liveBroadcasts/bind",
            &[
                ("part", "id,contentDetails"),
                ("id", broadcast_id),
                ("streamId", stream_id),
            ],
            None,
            admit(units::BIND),
        )
        .await
        .map(|_| ())
    }

    /// Transition the broadcast. A transition to the state it is already in
    /// counts as success. `Complete` is never refused by the project bucket.
    pub async fn transition_broadcast(
        &self,
        broadcast_id: &str,
        to: BroadcastTransition,
    ) -> Result<()> {
        let cost = match to {
            BroadcastTransition::Live => admit(units::TRANSITION),
            BroadcastTransition::Complete => forced(units::TRANSITION),
        };
        match self
            .call(
                Method::POST,
                "liveBroadcasts/transition",
                &[
                    ("part", "id,status"),
                    ("id", broadcast_id),
                    ("broadcastStatus", to.as_str()),
                ],
                None,
                cost,
            )
            .await
        {
            Err(e) if !is_redundant_transition(&e) => Err(e),
            _ => Ok(()),
        }
    }

    /// `status.lifeCycleStatus` of one broadcast, or `None` if it is gone.
    /// Never refused by the project bucket: the teardown needs it to complete.
    pub async fn broadcast_life_cycle(&self, broadcast_id: &str) -> Result<Option<String>> {
        let v = self
            .call(
                Method::GET,
                "liveBroadcasts",
                &[("part", "status"), ("id", broadcast_id)],
                None,
                forced(units::LIST),
            )
            .await?;
        Ok(v["items"][0]["status"]["lifeCycleStatus"]
            .as_str()
            .map(str::to_string))
    }

    /// Where YouTube is with the VOD of `video_id`.
    pub async fn vod_status(&self, video_id: &str) -> Result<VodStatus> {
        let v = self
            .call(
                Method::GET,
                "videos",
                &[("part", "processingDetails,status"), ("id", video_id)],
                None,
                admit(units::LIST),
            )
            .await?;
        let item = &v["items"][0];
        Ok(VodStatus {
            processing: item["processingDetails"]["processingStatus"]
                .as_str()
                .map(str::to_string),
            upload: item["status"]["uploadStatus"].as_str().map(str::to_string),
        })
    }
}

/// `videos.list` facts about a VOD. Both fields are read because Google does
/// not document which one a live archive reports while it is processed:
/// `processingDetails.processingStatus` (`processing|succeeded|failed|
/// terminated`) and `status.uploadStatus` (`uploaded|processed|failed|
/// rejected|deleted`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VodStatus {
    pub processing: Option<String>,
    pub upload: Option<String>,
}

#[cfg(test)]
#[path = "manage_tests.rs"]
mod tests;
