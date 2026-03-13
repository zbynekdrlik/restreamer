/// Hetzner Cloud REST API client implementation.
///
/// Wraps the Hetzner API v1 for server, snapshot, and SSH key management.
use crate::{CloudError, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};

const API_BASE: &str = "https://api.hetzner.cloud/v1";

/// Low-level Hetzner API client.
pub struct HetznerClient {
    client: Client,
    api_token: String,
    base_url: String,
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
        }
    }

    /// Create with a custom base URL (for testing).
    pub fn with_base_url(api_token: &str, base_url: &str) -> Self {
        Self {
            client: Client::new(),
            api_token: api_token.to_string(),
            base_url: base_url.to_string(),
        }
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
            let mut params: Vec<(&str, &str)> =
                vec![("type", "snapshot"), ("page", &page_str), ("per_page", "50")];
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
            server_type: "cx23".to_string(),
            location: "fsn1".to_string(),
            image: "ubuntu-22.04".to_string(),
            ssh_keys: vec!["restreamer".to_string()],
            user_data: "#cloud-config\n".to_string(),
            labels: [("app".to_string(), "restreamer".to_string())]
                .into_iter()
                .collect(),
        };
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["name"], "test-server");
        assert_eq!(json["server_type"], "cx23");
        assert_eq!(json["location"], "fsn1");
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
}
