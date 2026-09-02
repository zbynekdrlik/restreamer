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
/// API fails delivery in ~13s of backoff rather than hanging (#223).
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

/// A `create_server` error worth retrying: a transport-level failure
/// (timeout / connect / send / body), or a server-side `429`/`5xx`. A
/// permanent rejection (any other `4xx` — bad token, malformed request) is
/// surfaced immediately (#223).
fn is_transient(err: &CloudError) -> bool {
    match err {
        CloudError::Http(e) => e.is_timeout() || e.is_connect() || e.is_request() || e.is_body(),
        CloudError::Api { status, .. } => *status == 429 || *status >= 500,
        _ => false,
    }
}

/// Whether a transient failure could have created the server server-side
/// despite the error, so a blind POST retry risks a SECOND VPS: a `5xx`
/// response (request reached Hetzner) or a post-send timeout. A connect/send
/// error never reached Hetzner, so no server was created and no name lookup
/// is needed before retrying (#223).
fn may_have_created(err: &CloudError) -> bool {
    match err {
        CloudError::Api { status, .. } => *status >= 500,
        CloudError::Http(e) => e.is_timeout(),
        _ => false,
    }
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
    pub fn new(api_token: &str) -> Self {
        Self {
            client: Client::new(),
            api_token: api_token.to_string(),
            base_url: API_BASE.to_string(),
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            base_backoff: DEFAULT_BASE_BACKOFF,
        }
    }

    /// Create with a custom base URL (for testing).
    pub fn with_base_url(api_token: &str, base_url: &str) -> Self {
        Self {
            client: Client::new(),
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
    /// A transient error (network timeout/connect/send failure, `429`, or
    /// `5xx` — see [`is_transient`]) is retried up to `self.max_attempts`
    /// times with exponential backoff (`base_backoff * 3^n`: 1s, 3s, 9s by
    /// default). A permanent rejection (other `4xx`) is surfaced at once.
    ///
    /// **Idempotency:** before re-POSTing after an error that could have
    /// created the server server-side ([`may_have_created`] — a `5xx` or a
    /// post-send timeout), it looks the server up by its unique `name`
    /// (`GET /servers?name=`) and ADOPTS an existing one instead of creating
    /// a second VPS. A pure connect/send error never reached Hetzner, so no
    /// server exists and the lookup is skipped.
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

            let last = attempt >= self.max_attempts;
            if !is_transient(&err) {
                // Permanent rejection — bad token, malformed request. Fail fast.
                return Err(err);
            }
            if last {
                tracing::warn!(
                    attempt,
                    max_attempts = self.max_attempts,
                    error = %err,
                    "create_server: transient error, retries exhausted"
                );
                return Err(err);
            }

            // Idempotency guard: an error that could have reached Hetzner may
            // have already created the VPS. Adopt it by name rather than
            // POSTing a second one.
            if may_have_created(&err) {
                match self.get_server_by_name(name).await {
                    Ok(Some(existing)) => {
                        tracing::warn!(
                            attempt,
                            name,
                            hetzner_id = existing.id,
                            error = %err,
                            "create_server: transient error but server already exists by \
                             name; adopting it (no double-create)"
                        );
                        return Ok(existing);
                    }
                    Ok(None) => {}
                    Err(lookup_err) => {
                        // Can't confirm — log and fall through to backoff+retry.
                        tracing::warn!(
                            attempt,
                            error = %lookup_err,
                            "create_server: post-error name lookup failed; retrying create"
                        );
                    }
                }
            }

            let backoff = self.base_backoff * 3u32.pow(attempt - 1);
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

    fn ok_server_body(id: i64, name: &str) -> serde_json::Value {
        serde_json::json!({
            "server": {
                "id": id,
                "name": name,
                "status": "initializing",
                "public_net": {"ipv4": {"ip": "1.2.3.4"}},
                "server_type": {"name": "cpx22"},
                "created": "2026-01-01T00:00:00+00:00"
            }
        })
    }

    /// #223 RED: a transient 5xx from `POST /servers` must be retried
    /// server-side (after an idempotency name lookup that finds nothing),
    /// and the eventual 201 returns the created server. Before the fix,
    /// `create_server` POSTs once and surfaces the 503 immediately.
    #[tokio::test]
    async fn create_server_retries_transient_5xx_then_succeeds() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;

        // Idempotency name lookup: no pre-existing server with this name.
        Mock::given(method("GET"))
            .and(path("/servers"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"servers": []})),
            )
            .mount(&server)
            .await;

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

        let client = HetznerClient::with_base_url("tok", &server.uri());
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

    /// #223 idempotency: when a transient 5xx MIGHT have created the server,
    /// create_server looks it up by name and ADOPTS the existing one instead
    /// of POSTing a second VPS.
    #[tokio::test]
    async fn create_server_adopts_existing_on_transient_after_create() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;

        // The name lookup finds a server that was created despite the 5xx.
        Mock::given(method("GET"))
            .and(path("/servers"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "servers": [ok_server_body(555, "rs-delivery-evt9")["server"].clone()]
            })))
            .expect(1)
            .mount(&server)
            .await;

        // Exactly ONE POST — it 503s; the retry must adopt via lookup, not POST again.
        Mock::given(method("POST"))
            .and(path("/servers"))
            .respond_with(ResponseTemplate::new(503).set_body_json(serde_json::json!({
                "error": {"code": "unavailable", "message": "boom"}
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
                std::collections::HashMap::new(),
            )
            .await
            .expect("must adopt the already-created server");
        assert_eq!(got.id, 555, "adopted the existing VPS, not a new one");
    }

    /// #223: a persistently-down API exhausts the retry bound and surfaces
    /// the last transient error (no infinite loop).
    #[tokio::test]
    async fn create_server_exhausts_retries_then_errors() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        // Name lookup always empty — nothing to adopt.
        Mock::given(method("GET"))
            .and(path("/servers"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"servers": []})),
            )
            .mount(&server)
            .await;
        // POST always 503.
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
