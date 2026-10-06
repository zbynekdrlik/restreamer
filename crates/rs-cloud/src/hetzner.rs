/// Hetzner Cloud REST API client implementation.
///
/// Wraps the Hetzner API v1 for server, snapshot, and SSH key management.
use crate::{CloudError, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::time::Duration;

const API_BASE: &str = "https://api.hetzner.cloud/v1";

/// Total `create_server` attempts (1 initial + 3 retries), and the base of
/// the exponential backoff (1s, 3s, 9s). Bounded so a genuinely-down Hetzner
/// API fails delivery in tens of seconds (each request also capped by the
/// client's own 30s timeout — see [`REQUEST_TIMEOUT`]) rather
/// than hanging forever (#223).
const DEFAULT_MAX_ATTEMPTS: u32 = 4;
const DEFAULT_BASE_BACKOFF: Duration = Duration::from_secs(1);

/// Low-level Hetzner API client.
pub struct HetznerClient {
    client: Client,
    api_token: String,
    base_url: String,
    /// `create_server` retry policy (#223). Defaults from the consts above;
    /// override with [`HetznerClient::with_retry`] (tests use a ~1ms backoff).
    max_attempts: u32,
    base_backoff: Duration,
}

/// Maximum single backoff sleep — caps the exponential growth so an
/// operator-supplied huge `max_attempts` cannot overflow or sleep for hours.
const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// HTTP timeouts of every Hetzner call (#223 W3): 10 s to connect, 30 s for
/// the whole request including the body. See [`HetznerClient::build_client`].
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// The sleep before retry number `attempt` (1-based: the first failed attempt
/// is 1): `base * 3^(attempt-1)`, i.e. 1 s, 3 s, 9 s with the 1 s default,
/// capped at [`MAX_BACKOFF`]. `saturating_*` so a large operator-supplied
/// `max_attempts` can never overflow (#223 S1), and attempt 0 cannot
/// underflow.
fn retry_backoff(base: Duration, attempt: u32) -> Duration {
    base.saturating_mul(3u32.saturating_pow(attempt.saturating_sub(1)))
        .min(MAX_BACKOFF)
}

/// A `create_server` error worth retrying (#223):
/// - a transport-level `reqwest` failure: `timeout` / `connect` / `request`
///   (the observed CI error — a send/await-response failure) / `decode`
///   (a truncated or unreadable response body — reqwest 0.12 maps *response*
///   body-read failures to `Kind::Decode`, NOT `Kind::Body`, which is only for
///   *request*-body streaming and unreachable for our in-memory JSON POST);
/// - a server-side `429` / `5xx`;
/// - a `409` name-conflict, which for our deterministic unique name means a
///   prior attempt already created the VPS (adopted by name — see
///   [`HetznerClient::create_server`]) OR the just-deleted old VPS of this
///   event has not finished deleting yet, which clears on a later attempt.
///
/// Any other `4xx` (bad token, malformed request) is a permanent rejection,
/// surfaced immediately.
fn is_transient(err: &CloudError) -> bool {
    match err {
        CloudError::Http(e) => TransportClass::of(e).is_transient(),
        CloudError::Api { status, .. } => *status == 429 || *status == 409 || *status >= 500,
        _ => false,
    }
}

/// The reqwest error classes the transport half of [`is_transient`] looks
/// at. reqwest sets several at once (a refused connection is both `connect`
/// and `request`), so the retry rule is tested class by class on this value
/// rather than through real errors (#367).
#[derive(Debug, Clone, Copy, Default)]
struct TransportClass {
    timeout: bool,
    connect: bool,
    request: bool,
    decode: bool,
}

impl TransportClass {
    fn of(e: &reqwest::Error) -> Self {
        Self {
            timeout: e.is_timeout(),
            connect: e.is_connect(),
            request: e.is_request(),
            decode: e.is_decode(),
        }
    }

    /// ANY of the four classes is a transient transport failure.
    fn is_transient(self) -> bool {
        self.timeout || self.connect || self.request || self.decode
    }
}

/// `true` when `err` is the Hetzner `409` name-conflict — the definitive
/// "the server already exists under this name" signal that drives adoption
/// on retry instead of creating a second VPS (#223).
fn is_name_conflict(err: &CloudError) -> bool {
    matches!(err, CloudError::Api { status: 409, .. })
}

/// Whether a server found by name is safe to ADOPT as the one this
/// `create_server` call was creating: it must NOT be the old same-named VPS
/// mid-deletion (`start_delivery` deletes the previous `rs-delivery-evt{id}`
/// right before creating the new one — #244/#352), and it must carry every
/// label we are creating with (`app` / `event_id` / `client_uuid`), so a
/// server from another install or another event is never adopted (#223 W4).
fn is_adoptable(found: &Server, want_labels: &std::collections::HashMap<String, String>) -> bool {
    if found.status == "deleting" {
        return false;
    }
    want_labels
        .iter()
        .all(|(k, v)| found.labels.get(k) == Some(v))
}

// --- API response types ---

#[derive(Debug, Deserialize)]
pub struct ServerResponse {
    pub server: Server,
}

#[derive(Debug, Deserialize)]
pub struct ServersResponse {
    pub servers: Vec<Server>,
}

#[derive(Debug, Deserialize)]
pub struct Server {
    pub id: i64,
    pub name: String,
    pub status: String,
    pub public_net: PublicNet,
    pub server_type: ServerType,
    pub created: String,
    /// Hetzner labels attached at create time (`app`, `event_id`,
    /// `client_uuid`). Defaults empty when absent so older/partial API
    /// responses deserialize. Used by the orphan reaper (#352) to re-verify a
    /// server's `client_uuid` locally before ever deleting it (defense in
    /// depth over the server-side `label_selector`, the #137 guard).
    #[serde(default)]
    pub labels: std::collections::HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
pub struct PublicNet {
    pub ipv4: Ipv4,
}

#[derive(Debug, Deserialize)]
pub struct Ipv4 {
    pub ip: String,
}

#[derive(Debug, Deserialize)]
pub struct ServerType {
    pub name: String,
}

#[derive(Debug, Deserialize)]
pub struct ImageResponse {
    pub image: Image,
}

#[derive(Debug, Deserialize)]
pub struct ImagesResponse {
    pub images: Vec<Image>,
}

#[derive(Debug, Deserialize)]
pub struct Image {
    pub id: i64,
    pub description: String,
    pub status: String,
    pub created: String,
    #[serde(default)]
    pub labels: std::collections::HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
pub struct SshKeysResponse {
    pub ssh_keys: Vec<SshKey>,
}

#[derive(Debug, Deserialize)]
pub struct SshKeyResponse {
    pub ssh_key: SshKey,
}

#[derive(Debug, Deserialize)]
pub struct SshKey {
    pub id: i64,
    pub name: String,
    pub fingerprint: String,
}

#[derive(Debug, Deserialize)]
pub struct ActionResponse {
    pub action: Action,
}

#[derive(Debug, Deserialize)]
pub struct Action {
    pub id: i64,
    pub status: String,
}

#[derive(Debug, Deserialize)]
struct ErrorResponse {
    error: ApiError,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct ApiError {
    code: String,
    message: String,
}

// --- Request types ---

#[derive(Debug, Serialize)]
struct CreateServerRequest {
    name: String,
    server_type: String,
    location: String,
    image: String,
    ssh_keys: Vec<String>,
    user_data: String,
    labels: std::collections::HashMap<String, String>,
}

#[derive(Debug, Serialize)]
struct CreateSshKeyRequest {
    name: String,
    public_key: String,
}

#[derive(Debug, Serialize)]
struct CreateImageRequest {
    description: String,
    #[serde(rename = "type")]
    image_type: String,
    labels: std::collections::HashMap<String, String>,
}

impl HetznerClient {
    /// Build the shared reqwest client with bounded timeouts (#223 W3).
    /// reqwest's default is NO timeout, so without these a hung `POST /servers`
    /// (or any Hetzner call) would block `delivery_start` forever and the
    /// `is_timeout()` retry branch could never fire. Production uses
    /// [`CONNECT_TIMEOUT`] / [`REQUEST_TIMEOUT`] (10 s / 30 s) — a VPS-create
    /// POST returns in a few seconds (the server boots asynchronously), so
    /// 30 s is generous headroom, not a normal wait.
    fn build_client(connect_timeout: Duration, request_timeout: Duration) -> Client {
        Client::builder()
            .connect_timeout(connect_timeout)
            .timeout(request_timeout)
            .build()
            // Only fails if the TLS backend can't initialize — a fatal
            // deploy-time condition, not a runtime one.
            .expect("reqwest client with static timeout config must build")
    }

    pub fn new(api_token: &str) -> Self {
        Self {
            client: Self::build_client(CONNECT_TIMEOUT, REQUEST_TIMEOUT),
            api_token: api_token.to_string(),
            base_url: API_BASE.to_string(),
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            base_backoff: DEFAULT_BASE_BACKOFF,
        }
    }

    /// Create with a custom base URL (for testing).
    pub fn with_base_url(api_token: &str, base_url: &str) -> Self {
        Self {
            client: Self::build_client(CONNECT_TIMEOUT, REQUEST_TIMEOUT),
            api_token: api_token.to_string(),
            base_url: base_url.to_string(),
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            base_backoff: DEFAULT_BASE_BACKOFF,
        }
    }

    /// Override the `create_server` retry policy (#223). `max_attempts` is
    /// clamped to at least 1 (a single try, no retries). Tests use a tiny
    /// `base_backoff` so retries don't sleep whole seconds.
    pub fn with_retry(mut self, max_attempts: u32, base_backoff: Duration) -> Self {
        self.max_attempts = max_attempts.max(1);
        self.base_backoff = base_backoff;
        self
    }

    /// Test-only: override the HTTP timeouts (production always uses
    /// [`CONNECT_TIMEOUT`] / [`REQUEST_TIMEOUT`]), so a stalled Hetzner
    /// response surfaces as a timeout in a fraction of a second.
    #[cfg(test)]
    fn with_timeouts(mut self, connect_timeout: Duration, request_timeout: Duration) -> Self {
        self.client = Self::build_client(connect_timeout, request_timeout);
        self
    }

    async fn check_error(&self, response: reqwest::Response) -> Result<reqwest::Response> {
        if response.status().is_success() {
            return Ok(response);
        }
        let status = response.status().as_u16();
        let body = response.text().await.unwrap_or_default();
        if let Ok(err) = serde_json::from_str::<ErrorResponse>(&body) {
            Err(CloudError::Api {
                status,
                message: err.error.message,
            })
        } else {
            Err(CloudError::Api {
                status,
                message: body,
            })
        }
    }

    // --- Servers ---

    /// Create a delivery VPS, retrying transient Hetzner API failures
    /// server-side (#223).
    ///
    /// A transient error ([`is_transient`] — a network timeout/connect/send/
    /// decode failure, `429`, `5xx`, or a `409` name-conflict) is retried up
    /// to `self.max_attempts` times with capped exponential backoff
    /// (`base_backoff * 3^n`: 1s, 3s, 9s by default, each capped at
    /// [`MAX_BACKOFF`]). A permanent rejection (any other `4xx` — bad token,
    /// malformed request) is surfaced immediately.
    ///
    /// **Idempotency (no double-create).** A transport error may have created
    /// the VPS before surfacing (e.g. the connection dropped while awaiting
    /// the response), so a blind retry could create a SECOND VPS. Rather than
    /// speculatively guessing, we lean on Hetzner's per-project name
    /// uniqueness: a retry of an already-created server returns `409`, and on
    /// that signal we look the server up by its unique `name` and ADOPT it
    /// ([`is_adoptable`] — never the old same-named VPS mid-deletion, never a
    /// server from another install/event) instead of surfacing an error. A
    /// `409` we cannot adopt (the previous VPS of this event is still
    /// deleting) is itself transient — a later attempt succeeds once the name
    /// frees.
    #[allow(clippy::too_many_arguments)]
    pub async fn create_server(
        &self,
        name: &str,
        server_type: &str,
        location: &str,
        image: &str,
        ssh_keys: &[String],
        user_data: &str,
        labels: std::collections::HashMap<String, String>,
    ) -> Result<Server> {
        // A bounded range, not a hand-advanced counter: the loop cannot run
        // away (#367). `max_attempts >= 1` (`with_retry` clamps it), and the
        // last attempt always returns, so the end of the range is unreachable.
        for attempt in 1..=self.max_attempts {
            let err = match self
                .create_server_inner(
                    name,
                    server_type,
                    location,
                    image,
                    ssh_keys,
                    user_data,
                    labels.clone(),
                )
                .await
            {
                Ok(server) => return Ok(server),
                Err(e) => e,
            };

            // A `409` name-conflict is the definitive "already created" signal:
            // a prior attempt (or a live orphan of this event) holds the name.
            // Adopt OUR server rather than create a second one.
            if is_name_conflict(&err) {
                match self.get_server_by_name(name).await {
                    Ok(Some(found)) if is_adoptable(&found, &labels) => {
                        tracing::warn!(
                            attempt,
                            name,
                            hetzner_id = found.id,
                            "create_server: name conflict — adopting the already-created \
                             server (no double-create)"
                        );
                        return Ok(found);
                    }
                    Ok(_) => {
                        // Name taken by the old VPS still deleting (or nothing
                        // matched) — treat as transient and let backoff give
                        // the delete time to finish.
                    }
                    Err(lookup_err) => {
                        tracing::warn!(
                            attempt,
                            error = %lookup_err,
                            "create_server: name-conflict lookup failed; retrying"
                        );
                    }
                }
            }

            if !is_transient(&err) {
                // Permanent rejection — bad token, malformed request. Fail fast.
                return Err(err);
            }
            if attempt >= self.max_attempts {
                tracing::warn!(
                    attempt,
                    max_attempts = self.max_attempts,
                    error = %err,
                    "create_server: transient error, retries exhausted"
                );
                return Err(err);
            }

            let backoff = retry_backoff(self.base_backoff, attempt);
            tracing::warn!(
                attempt,
                max_attempts = self.max_attempts,
                backoff_ms = backoff.as_millis() as u64,
                error = %err,
                "create_server: transient Hetzner error, retrying after backoff"
            );
            tokio::time::sleep(backoff).await;
        }
        unreachable!("create_server: the last of max_attempts (>= 1) attempts always returns")
    }

    /// One `POST /servers` attempt (no retry). See [`create_server`].
    #[allow(clippy::too_many_arguments)]
    async fn create_server_inner(
        &self,
        name: &str,
        server_type: &str,
        location: &str,
        image: &str,
        ssh_keys: &[String],
        user_data: &str,
        labels: std::collections::HashMap<String, String>,
    ) -> Result<Server> {
        let req = CreateServerRequest {
            name: name.to_string(),
            server_type: server_type.to_string(),
            location: location.to_string(),
            image: image.to_string(),
            ssh_keys: ssh_keys.to_vec(),
            user_data: user_data.to_string(),
            labels,
        };
        let resp = self
            .client
            .post(format!("{}/servers", self.base_url))
            .bearer_auth(&self.api_token)
            .json(&req)
            .send()
            .await?;
        let resp = self.check_error(resp).await?;
        let body: ServerResponse = resp.json().await?;
        Ok(body.server)
    }

    /// Look up a server by its exact `name` (`GET /servers?name=`). Returns
    /// `None` if no server has that name. Used by [`create_server`]'s
    /// idempotency guard to avoid double-creating a VPS on retry (#223).
    pub async fn get_server_by_name(&self, name: &str) -> Result<Option<Server>> {
        let resp = self
            .client
            .get(format!("{}/servers", self.base_url))
            .bearer_auth(&self.api_token)
            .query(&[("name", name)])
            .send()
            .await?;
        let resp = self.check_error(resp).await?;
        let body: ServersResponse = resp.json().await?;
        Ok(body.servers.into_iter().next())
    }

    pub async fn get_server(&self, id: i64) -> Result<Server> {
        let resp = self
            .client
            .get(format!("{}/servers/{id}", self.base_url))
            .bearer_auth(&self.api_token)
            .send()
            .await?;
        let resp = self.check_error(resp).await?;
        let body: ServerResponse = resp.json().await?;
        Ok(body.server)
    }

    pub async fn list_servers(&self, label_selector: Option<&str>) -> Result<Vec<Server>> {
        let mut all_servers = Vec::new();
        let mut page = 1u32;
        loop {
            let url = format!("{}/servers", self.base_url);
            let page_str = page.to_string();
            let mut params: Vec<(&str, &str)> = vec![("page", &page_str), ("per_page", "50")];
            if let Some(selector) = label_selector {
                params.push(("label_selector", selector));
            }
            let resp = self
                .client
                .get(&url)
                .bearer_auth(&self.api_token)
                .query(&params)
                .send()
                .await?;
            let resp = self.check_error(resp).await?;
            let body: ServersResponse = resp.json().await?;
            if body.servers.is_empty() {
                break;
            }
            all_servers.extend(body.servers);
            page += 1;
        }
        Ok(all_servers)
    }

    pub async fn delete_server(&self, id: i64) -> Result<()> {
        let resp = self
            .client
            .delete(format!("{}/servers/{id}", self.base_url))
            .bearer_auth(&self.api_token)
            .send()
            .await?;
        self.check_error(resp).await?;
        Ok(())
    }

    // --- Snapshots (Images) ---

    pub async fn create_snapshot(&self, server_id: i64, description: &str) -> Result<Image> {
        let req = CreateImageRequest {
            description: description.to_string(),
            image_type: "snapshot".to_string(),
            labels: std::collections::HashMap::new(),
        };
        let resp = self
            .client
            .post(format!(
                "{}/servers/{server_id}/actions/create_image",
                self.base_url
            ))
            .bearer_auth(&self.api_token)
            .json(&req)
            .send()
            .await?;
        let resp = self.check_error(resp).await?;
        let body: ImageResponse = resp.json().await?;
        Ok(body.image)
    }

    pub async fn list_snapshots(&self, label_selector: Option<&str>) -> Result<Vec<Image>> {
        let mut all_images = Vec::new();
        let mut page = 1u32;
        loop {
            let url = format!("{}/images", self.base_url);
            let page_str = page.to_string();
            let mut params: Vec<(&str, &str)> = vec![
                ("type", "snapshot"),
                ("page", &page_str),
                ("per_page", "50"),
            ];
            if let Some(selector) = label_selector {
                params.push(("label_selector", selector));
            }
            let resp = self
                .client
                .get(&url)
                .bearer_auth(&self.api_token)
                .query(&params)
                .send()
                .await?;
            let resp = self.check_error(resp).await?;
            let body: ImagesResponse = resp.json().await?;
            if body.images.is_empty() {
                break;
            }
            all_images.extend(body.images);
            page += 1;
        }
        Ok(all_images)
    }

    pub async fn delete_image(&self, id: i64) -> Result<()> {
        let resp = self
            .client
            .delete(format!("{}/images/{id}", self.base_url))
            .bearer_auth(&self.api_token)
            .send()
            .await?;
        self.check_error(resp).await?;
        Ok(())
    }

    // --- SSH Keys ---

    pub async fn list_ssh_keys(&self) -> Result<Vec<SshKey>> {
        let resp = self
            .client
            .get(format!("{}/ssh_keys", self.base_url))
            .bearer_auth(&self.api_token)
            .send()
            .await?;
        let resp = self.check_error(resp).await?;
        let body: SshKeysResponse = resp.json().await?;
        Ok(body.ssh_keys)
    }

    pub async fn create_ssh_key(&self, name: &str, public_key: &str) -> Result<SshKey> {
        let req = CreateSshKeyRequest {
            name: name.to_string(),
            public_key: public_key.to_string(),
        };
        let resp = self
            .client
            .post(format!("{}/ssh_keys", self.base_url))
            .bearer_auth(&self.api_token)
            .json(&req)
            .send()
            .await?;
        let resp = self.check_error(resp).await?;
        let body: SshKeyResponse = resp.json().await?;
        Ok(body.ssh_key)
    }
}

#[cfg(test)]
#[path = "hetzner_tests.rs"]
mod tests;
