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
/// client's own 30s timeout — see [`HetznerClient::build_client`]) rather
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
        CloudError::Http(e) => e.is_timeout() || e.is_connect() || e.is_request() || e.is_decode(),
        CloudError::Api { status, .. } => *status == 429 || *status == 409 || *status >= 500,
        _ => false,
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
    /// `is_timeout()` retry branch could never fire. 10s to connect, 30s total
    /// per request — a VPS-create POST returns in a few seconds (the server
    /// boots asynchronously), so 30s is generous headroom, not a normal wait.
    fn build_client() -> Client {
        Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(30))
            .build()
            // Only fails if the TLS backend can't initialize — a fatal
            // deploy-time condition, not a runtime one.
            .expect("reqwest client with static timeout config must build")
    }

    pub fn new(api_token: &str) -> Self {
        Self {
            client: Self::build_client(),
            api_token: api_token.to_string(),
            base_url: API_BASE.to_string(),
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            base_backoff: DEFAULT_BASE_BACKOFF,
        }
    }

    /// Create with a custom base URL (for testing).
    pub fn with_base_url(api_token: &str, base_url: &str) -> Self {
        Self {
            client: Self::build_client(),
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
        let mut attempt = 0u32;
        loop {
            attempt += 1;
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

            // Capped exponential backoff — `saturating_*` so a large
            // operator-supplied `max_attempts` can never overflow (#223 S1).
            let backoff = self
                .base_backoff
                .saturating_mul(3u32.saturating_pow(attempt - 1))
                .min(MAX_BACKOFF);
            tracing::warn!(
                attempt,
                max_attempts = self.max_attempts,
                backoff_ms = backoff.as_millis() as u64,
                error = %err,
                "create_server: transient Hetzner error, retrying after backoff"
            );
            tokio::time::sleep(backoff).await;
        }
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
mod tests {
    use super::*;

    #[test]
    fn hetzner_client_new() {
        let client = HetznerClient::new("test-token");
        assert_eq!(client.api_token, "test-token");
        assert_eq!(client.base_url, API_BASE);
    }

    #[test]
    fn hetzner_client_custom_base_url() {
        let client = HetznerClient::with_base_url("token", "http://localhost:8080");
        assert_eq!(client.base_url, "http://localhost:8080");
    }

    #[tokio::test]
    async fn create_server_request_format() {
        // Test that the request body is properly constructed
        let req = CreateServerRequest {
            name: "test-server".to_string(),
            server_type: "cpx22".to_string(),
            location: "nbg1".to_string(),
            image: "ubuntu-22.04".to_string(),
            ssh_keys: vec!["restreamer".to_string()],
            user_data: "#cloud-config\n".to_string(),
            labels: [("app".to_string(), "restreamer".to_string())]
                .into_iter()
                .collect(),
        };
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["name"], "test-server");
        assert_eq!(json["server_type"], "cpx22");
        assert_eq!(json["location"], "nbg1");
        assert_eq!(json["ssh_keys"][0], "restreamer");
    }

    #[test]
    fn server_response_deserialize() {
        let json = r#"{
            "server": {
                "id": 123,
                "name": "rs-delivery-1",
                "status": "running",
                "public_net": {"ipv4": {"ip": "1.2.3.4"}, "ipv6": {"ip": "::1"}},
                "server_type": {"name": "cx23", "description": "CX23"},
                "created": "2026-01-01T00:00:00+00:00"
            }
        }"#;
        let resp: ServerResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.server.id, 123);
        assert_eq!(resp.server.name, "rs-delivery-1");
        assert_eq!(resp.server.public_net.ipv4.ip, "1.2.3.4");
    }

    #[test]
    fn image_response_deserialize() {
        let json = r#"{
            "image": {
                "id": 456,
                "description": "rs-delivery snapshot",
                "status": "available",
                "created": "2026-01-01T00:00:00+00:00",
                "labels": {"app": "restreamer"}
            }
        }"#;
        let resp: ImageResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.image.id, 456);
        assert_eq!(resp.image.description, "rs-delivery snapshot");
    }

    #[test]
    fn ssh_key_response_deserialize() {
        let json = r#"{
            "ssh_keys": [
                {"id": 1, "name": "restreamer", "fingerprint": "aa:bb:cc"}
            ]
        }"#;
        let resp: SshKeysResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.ssh_keys.len(), 1);
        assert_eq!(resp.ssh_keys[0].name, "restreamer");
    }

    #[test]
    fn error_response_deserialize() {
        let json = r#"{"error": {"code": "not_found", "message": "Server not found"}}"#;
        let resp: ErrorResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.error.code, "not_found");
    }

    // ----- create_server transient-error retry (#223) -----

    /// A bare Hetzner server object (the shape inside `{"server": …}` and each
    /// element of `{"servers": […]}`), with an explicit status and labels.
    fn server_obj(id: i64, name: &str, status: &str, labels: &[(&str, &str)]) -> serde_json::Value {
        let lbls: serde_json::Map<String, serde_json::Value> = labels
            .iter()
            .map(|(k, v)| (k.to_string(), serde_json::json!(v)))
            .collect();
        serde_json::json!({
            "id": id,
            "name": name,
            "status": status,
            "public_net": {"ipv4": {"ip": "1.2.3.4"}},
            "server_type": {"name": "cpx22"},
            "created": "2026-01-01T00:00:00+00:00",
            "labels": lbls
        })
    }

    fn ok_server_body(id: i64, name: &str) -> serde_json::Value {
        serde_json::json!({ "server": server_obj(id, name, "initializing", &[]) })
    }

    /// The labels `start_delivery` attaches to a delivery VPS — used both when
    /// creating and (subset-matched) when deciding a found server is adoptable.
    fn evt_labels(event_id: &str) -> std::collections::HashMap<String, String> {
        [
            ("app", "restreamer"),
            ("event_id", event_id),
            ("client_uuid", "inst-1"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
    }

    /// #223 RED: a transient 5xx from `POST /servers` must be retried
    /// server-side, and the eventual 201 returns the created server. Before
    /// the fix, `create_server` POSTs once and surfaces the 503 immediately.
    #[tokio::test]
    async fn create_server_retries_transient_5xx_then_succeeds() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;

        // First POST -> transient 503. Mounted FIRST and capped at one hit:
        // wiremock serves the first-registered mock that still has capacity,
        // so this answers POST #1, then `up_to_n_times(1)` exhausts it.
        Mock::given(method("POST"))
            .and(path("/servers"))
            .respond_with(ResponseTemplate::new(503).set_body_json(serde_json::json!({
                "error": {"code": "unavailable", "message": "service temporarily unavailable"}
            })))
            .up_to_n_times(1)
            .expect(1)
            .mount(&server)
            .await;

        // Retry POST -> 201 success. Mounted SECOND, so once the 503 mock is
        // exhausted this one answers the retried request.
        Mock::given(method("POST"))
            .and(path("/servers"))
            .respond_with(
                ResponseTemplate::new(201).set_body_json(ok_server_body(999, "rs-delivery-evt7")),
            )
            .expect(1)
            .mount(&server)
            .await;

        let client = HetznerClient::with_base_url("tok", &server.uri())
            .with_retry(4, std::time::Duration::from_millis(1));
        let got = client
            .create_server(
                "rs-delivery-evt7",
                "cpx22",
                "fsn1",
                "ubuntu-24.04",
                &["restreamer".to_string()],
                "#cloud-config\n",
                std::collections::HashMap::new(),
            )
            .await
            .expect("create_server should retry the transient 503 and return the 201 server");
        assert_eq!(got.id, 999);
        assert_eq!(got.name, "rs-delivery-evt7");
    }

    /// #223: a permanent 4xx (e.g. malformed request) is NOT retried — it is
    /// surfaced immediately after a single POST.
    #[tokio::test]
    async fn create_server_permanent_4xx_not_retried() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/servers"))
            .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
                "error": {"code": "invalid_input", "message": "bad server_type"}
            })))
            .expect(1) // exactly one attempt, no retry
            .mount(&server)
            .await;

        let client = HetznerClient::with_base_url("tok", &server.uri())
            .with_retry(4, std::time::Duration::from_millis(1));
        let err = client
            .create_server(
                "rs-delivery-evt8",
                "cpx22",
                "fsn1",
                "ubuntu-24.04",
                &["restreamer".to_string()],
                "#cloud-config\n",
                std::collections::HashMap::new(),
            )
            .await
            .expect_err("permanent 4xx must not be retried");
        match err {
            CloudError::Api { status, .. } => assert_eq!(status, 400),
            other => panic!("expected Api 400, got {other:?}"),
        }
    }

    /// #223 idempotency: a `409` name-conflict means a prior attempt already
    /// created the VPS, so create_server looks it up by name and ADOPTS the
    /// existing (label-matching, non-deleting) server instead of erroring or
    /// POSTing a second VPS.
    #[tokio::test]
    async fn create_server_adopts_on_name_conflict_409() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;

        // The name lookup finds OUR server (matching labels, not deleting).
        Mock::given(method("GET"))
            .and(path("/servers"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "servers": [server_obj(
                    555, "rs-delivery-evt9", "initializing",
                    &[("app", "restreamer"), ("event_id", "9"), ("client_uuid", "inst-1")]
                )]
            })))
            .expect(1)
            .mount(&server)
            .await;

        // Exactly ONE POST — it 409s (name taken); the code must adopt via the
        // lookup, never POST a second server.
        Mock::given(method("POST"))
            .and(path("/servers"))
            .respond_with(ResponseTemplate::new(409).set_body_json(serde_json::json!({
                "error": {"code": "uniqueness_error", "message": "name already used"}
            })))
            .expect(1)
            .mount(&server)
            .await;

        let client = HetznerClient::with_base_url("tok", &server.uri())
            .with_retry(4, std::time::Duration::from_millis(1));
        let got = client
            .create_server(
                "rs-delivery-evt9",
                "cpx22",
                "fsn1",
                "ubuntu-24.04",
                &["restreamer".to_string()],
                "#cloud-config\n",
                evt_labels("9"),
            )
            .await
            .expect("must adopt the already-created server");
        assert_eq!(got.id, 555, "adopted the existing VPS, not a new one");
    }

    /// #223 W4: a `409` whose only same-named server is the PREVIOUS VPS of
    /// this event still `deleting` must NOT be adopted (it would point the DB
    /// row at a VPS about to vanish, with a stale auth token). It is treated
    /// as transient and eventually surfaced after the retry bound.
    #[tokio::test]
    async fn create_server_does_not_adopt_deleting_server() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;

        // Every name lookup returns the OLD server, mid-deletion.
        Mock::given(method("GET"))
            .and(path("/servers"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "servers": [server_obj(
                    111, "rs-delivery-evt9", "deleting",
                    &[("app", "restreamer"), ("event_id", "9"), ("client_uuid", "inst-1")]
                )]
            })))
            .mount(&server)
            .await;

        // POST always 409 — name still held by the deleting VPS.
        Mock::given(method("POST"))
            .and(path("/servers"))
            .respond_with(ResponseTemplate::new(409).set_body_json(serde_json::json!({
                "error": {"code": "uniqueness_error", "message": "name already used"}
            })))
            .expect(2) // with_retry(2, ..) => two attempts, neither adopts
            .mount(&server)
            .await;

        let client = HetznerClient::with_base_url("tok", &server.uri())
            .with_retry(2, std::time::Duration::from_millis(1));
        let err = client
            .create_server(
                "rs-delivery-evt9",
                "cpx22",
                "fsn1",
                "ubuntu-24.04",
                &["restreamer".to_string()],
                "#cloud-config\n",
                evt_labels("9"),
            )
            .await
            .expect_err("a deleting same-named server must not be adopted");
        match err {
            CloudError::Api { status, .. } => assert_eq!(status, 409),
            other => panic!("expected Api 409 after exhaustion, got {other:?}"),
        }
    }

    /// #223 S2: the actual observed failure class — a transport-level error
    /// (connection refused) — is transient and retried, then surfaced as
    /// `CloudError::Http` once the bound is reached. Points at a closed port.
    #[tokio::test]
    async fn create_server_retries_transport_error_then_surfaces_http() {
        // 127.0.0.1:1 refuses connections — a connect-level reqwest error,
        // the send-level class the ticket's CI failure belongs to.
        let client = HetznerClient::with_base_url("tok", "http://127.0.0.1:1")
            .with_retry(2, std::time::Duration::from_millis(1));
        let err = client
            .create_server(
                "rs-delivery-evt11",
                "cpx22",
                "fsn1",
                "ubuntu-24.04",
                &["restreamer".to_string()],
                "#cloud-config\n",
                std::collections::HashMap::new(),
            )
            .await
            .expect_err("transport error must surface after retries");
        assert!(
            matches!(err, CloudError::Http(_)),
            "expected CloudError::Http, got {err:?}"
        );
    }

    /// #223: a persistently-down API exhausts the retry bound and surfaces
    /// the last transient error (no infinite loop).
    #[tokio::test]
    async fn create_server_exhausts_retries_then_errors() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        // POST always 503 (a 5xx does not trigger a name lookup — only a 409
        // does — so no GET mock is needed here).
        Mock::given(method("POST"))
            .and(path("/servers"))
            .respond_with(ResponseTemplate::new(503).set_body_json(serde_json::json!({
                "error": {"code": "unavailable", "message": "still down"}
            })))
            .expect(2) // with_retry(2, ..) => exactly 2 attempts
            .mount(&server)
            .await;

        let client = HetznerClient::with_base_url("tok", &server.uri())
            .with_retry(2, std::time::Duration::from_millis(1));
        let err = client
            .create_server(
                "rs-delivery-evt10",
                "cpx22",
                "fsn1",
                "ubuntu-24.04",
                &["restreamer".to_string()],
                "#cloud-config\n",
                std::collections::HashMap::new(),
            )
            .await
            .expect_err("exhausted retries must surface the transient error");
        match err {
            CloudError::Api { status, .. } => assert_eq!(status, 503),
            other => panic!("expected Api 503, got {other:?}"),
        }
    }
}
