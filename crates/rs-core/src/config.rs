use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

use crate::error::Result;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub client_uuid: String,
    pub s3: S3Config,
    #[serde(default)]
    pub hetzner: HetznerConfig,
    #[serde(default)]
    pub youtube: YouTubeOAuthConfig,
    #[serde(default)]
    pub inpoint: InpointConfig,
    #[serde(default)]
    pub api: ApiConfig,
    #[serde(default)]
    pub delivery: DeliveryConfig,
    #[serde(default)]
    pub obs: ObsConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HetznerConfig {
    #[serde(default)]
    pub api_token: String,
    #[serde(default = "default_hetzner_location")]
    pub location: String,
    #[serde(default = "default_hetzner_server_type")]
    pub default_server_type: String,
    #[serde(default = "default_hetzner_snapshot_label")]
    pub snapshot_label: String,
    #[serde(default = "default_hetzner_ssh_key_name")]
    pub ssh_key_name: String,
}

fn default_hetzner_location() -> String {
    "nbg1".to_string()
}
fn default_hetzner_server_type() -> String {
    "cpx22".to_string()
}
fn default_hetzner_snapshot_label() -> String {
    "rs-delivery".to_string()
}
fn default_hetzner_ssh_key_name() -> String {
    "restreamer".to_string()
}

impl Default for HetznerConfig {
    fn default() -> Self {
        Self {
            api_token: String::new(),
            location: default_hetzner_location(),
            default_server_type: default_hetzner_server_type(),
            snapshot_label: default_hetzner_snapshot_label(),
            ssh_key_name: default_hetzner_ssh_key_name(),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct YouTubeOAuthConfig {
    #[serde(default)]
    pub client_id: String,
    #[serde(default)]
    pub client_secret: String,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct S3Config {
    pub bucket: String,
    pub region: String,
    pub endpoint: String,
    pub access_key_id: String,
    pub secret_access_key: String,
}

impl std::fmt::Debug for S3Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Config")
            .field("bucket", &self.bucket)
            .field("region", &self.region)
            .field("endpoint", &self.endpoint)
            .field("access_key_id", &"***")
            .field("secret_access_key", &"***")
            .finish()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InpointConfig {
    #[serde(default = "default_rtmp_port")]
    pub rtmp_port: u16,
    #[serde(default = "default_rtmp_bind")]
    pub rtmp_bind: String,
    #[serde(default = "default_chunk_duration_ms")]
    pub chunk_duration_ms: u64,
    #[serde(default = "default_read_buffer_bytes")]
    pub read_buffer_bytes: usize,
    /// Chunk storage format: "flv" (direct FLV, zero overhead) or "ts" (MPEG-TS legacy).
    #[serde(default = "default_chunk_format")]
    pub chunk_format: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiConfig {
    #[serde(default = "default_api_port")]
    pub port: u16,
    #[serde(default = "default_api_bind")]
    pub bind: String,
    #[serde(default)]
    pub tls: bool,
    #[serde(default = "default_https_port")]
    pub https_port: u16,
    #[serde(default = "default_tls_cert")]
    pub tls_cert: String,
    #[serde(default = "default_tls_key")]
    pub tls_key: String,
    #[serde(default)]
    pub https_domain: Option<String>,
}

fn default_rtmp_port() -> u16 {
    1234
}
fn default_rtmp_bind() -> String {
    "127.0.0.1".to_string()
}
fn default_chunk_duration_ms() -> u64 {
    1000
}
fn default_read_buffer_bytes() -> usize {
    102_400
}
fn default_chunk_format() -> String {
    "flv".to_string()
}
fn default_api_port() -> u16 {
    8910
}
fn default_api_bind() -> String {
    "127.0.0.1".to_string()
}
fn default_https_port() -> u16 {
    443
}
fn default_tls_cert() -> String {
    "cert.pem".to_string()
}
fn default_tls_key() -> String {
    "key.pem".to_string()
}

impl Default for InpointConfig {
    fn default() -> Self {
        Self {
            rtmp_port: default_rtmp_port(),
            rtmp_bind: default_rtmp_bind(),
            chunk_duration_ms: default_chunk_duration_ms(),
            read_buffer_bytes: default_read_buffer_bytes(),
            chunk_format: default_chunk_format(),
        }
    }
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            port: default_api_port(),
            bind: default_api_bind(),
            tls: false,
            https_port: default_https_port(),
            tls_cert: default_tls_cert(),
            tls_key: default_tls_key(),
            https_domain: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeliveryConfig {
    #[serde(default = "default_delivery_delay_secs")]
    pub delivery_delay_secs: u64,
}

fn default_delivery_delay_secs() -> u64 {
    120
}

impl Default for DeliveryConfig {
    fn default() -> Self {
        Self {
            delivery_delay_secs: default_delivery_delay_secs(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObsConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_obs_ws_url")]
    pub ws_url: String,
    #[serde(default)]
    pub ws_password: String,
}

fn default_obs_ws_url() -> String {
    "ws://127.0.0.1:4455".to_string()
}

impl Default for ObsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            ws_url: default_obs_ws_url(),
            ws_password: String::new(),
        }
    }
}

impl Config {
    /// Load config from file, with env var overrides.
    pub fn load(path: &Path) -> Result<Self> {
        let content = std::fs::read_to_string(path)?;
        // Strip UTF-8 BOM if present (PowerShell writes BOM with -Encoding UTF8)
        let content = content.strip_prefix('\u{FEFF}').unwrap_or(&content);
        let mut config: Config = serde_json::from_str(content)?;
        config.apply_env_overrides();
        Ok(config)
    }

    /// Save config to file atomically (write to temp + rename).
    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let content = serde_json::to_string_pretty(self)?;
        let tmp_path = path.with_extension("json.tmp");
        std::fs::write(&tmp_path, &content)?;
        std::fs::rename(&tmp_path, path)?;
        Ok(())
    }

    /// Default config file path.
    pub fn default_path() -> PathBuf {
        if cfg!(windows) {
            PathBuf::from(r"C:\ProgramData\Restreamer\config.json")
        } else {
            PathBuf::from("/etc/restreamer/config.json")
        }
    }

    /// Directory where delivery VPS logs are persisted to disk as a backup
    /// to the `delivery_logs` DB table. Survives DB truncation and can be
    /// inspected with a plain text editor (no sqlite tooling needed).
    pub fn delivery_log_dir() -> PathBuf {
        let base = Self::default_path()
            .parent()
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("."));
        base.join("delivery-logs")
    }

    fn apply_env_overrides(&mut self) {
        if let Ok(v) = std::env::var("RESTREAMER_CLIENT_UUID") {
            self.client_uuid = v;
        }
        if let Ok(v) = std::env::var("RESTREAMER_S3_BUCKET") {
            self.s3.bucket = v;
        }
        if let Ok(v) = std::env::var("RESTREAMER_S3_REGION") {
            self.s3.region = v;
        }
        if let Ok(v) = std::env::var("RESTREAMER_S3_ENDPOINT") {
            self.s3.endpoint = v;
        }
        if let Ok(v) = std::env::var("RESTREAMER_S3_ACCESS_KEY_ID") {
            self.s3.access_key_id = v;
        }
        if let Ok(v) = std::env::var("RESTREAMER_S3_SECRET_ACCESS_KEY") {
            self.s3.secret_access_key = v;
        }
        if let Ok(v) = std::env::var("RESTREAMER_HETZNER_API_TOKEN") {
            self.hetzner.api_token = v;
        }
        if let Ok(v) = std::env::var("RESTREAMER_RTMP_PORT") {
            match v.parse() {
                Ok(port) => self.inpoint.rtmp_port = port,
                Err(e) => tracing::warn!("Invalid RESTREAMER_RTMP_PORT '{v}': {e}"),
            }
        }
        if let Ok(v) = std::env::var("RESTREAMER_RTMP_BIND") {
            self.inpoint.rtmp_bind = v;
        }
        if let Ok(v) = std::env::var("RESTREAMER_API_PORT") {
            match v.parse() {
                Ok(port) => self.api.port = port,
                Err(e) => tracing::warn!("Invalid RESTREAMER_API_PORT '{v}': {e}"),
            }
        }
        if let Ok(v) = std::env::var("RESTREAMER_API_BIND") {
            self.api.bind = v;
        }
        if let Ok(v) = std::env::var("RESTREAMER_DELIVERY_DELAY_SECS") {
            match v.parse() {
                Ok(secs) => self.delivery.delivery_delay_secs = secs,
                Err(e) => tracing::warn!("Invalid RESTREAMER_DELIVERY_DELAY_SECS '{v}': {e}"),
            }
        }
        if let Ok(v) = std::env::var("RESTREAMER_OBS_ENABLED") {
            self.obs.enabled = v == "1" || v.eq_ignore_ascii_case("true");
        }
        if let Ok(v) = std::env::var("RESTREAMER_OBS_WS_URL") {
            self.obs.ws_url = v;
        }
        if let Ok(v) = std::env::var("RESTREAMER_OBS_WS_PASSWORD") {
            self.obs.ws_password = v;
        }
    }

    /// Validate that required configuration fields are present.
    pub fn validate(&self) -> std::result::Result<(), String> {
        if self.client_uuid.is_empty() {
            return Err("client_uuid is required".to_string());
        }
        if self.s3.bucket.is_empty() {
            return Err("s3.bucket is required".to_string());
        }
        if self.s3.access_key_id.is_empty() {
            return Err("s3.access_key_id is required".to_string());
        }
        if self.s3.secret_access_key.is_empty() {
            return Err("s3.secret_access_key is required".to_string());
        }
        if self.inpoint.chunk_format == "ts" {
            return Err("chunk_format \"ts\" is no longer supported, use \"flv\"".to_string());
        }
        Ok(())
    }

    /// Create a minimal config for testing.
    pub fn for_testing() -> Self {
        Self {
            client_uuid: "test-uuid-00000000".to_string(),
            s3: S3Config {
                bucket: "test-bucket".to_string(),
                region: "us-east-1".to_string(),
                endpoint: "http://localhost:9000".to_string(),
                access_key_id: "test-key".to_string(),
                secret_access_key: "test-secret".to_string(),
            },
            hetzner: HetznerConfig::default(),
            youtube: YouTubeOAuthConfig::default(),
            inpoint: InpointConfig::default(),
            api: ApiConfig {
                port: 0, // random port for tests
                bind: "127.0.0.1".to_string(),
                ..ApiConfig::default()
            },
            delivery: DeliveryConfig::default(),
            obs: ObsConfig {
                enabled: false, // Disable in tests to avoid background connection attempts
                ..ObsConfig::default()
            },
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            client_uuid: String::new(),
            s3: S3Config {
                bucket: "restreamer-chunks".to_string(),
                region: "nbg1".to_string(),
                endpoint: "https://nbg1.your-objectstorage.com".to_string(),
                access_key_id: String::new(),
                secret_access_key: String::new(),
            },
            hetzner: HetznerConfig::default(),
            youtube: YouTubeOAuthConfig::default(),
            inpoint: InpointConfig::default(),
            api: ApiConfig::default(),
            delivery: DeliveryConfig::default(),
            obs: ObsConfig::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    #[test]
    fn config_serde_roundtrip() {
        let config = Config::for_testing();
        let json = serde_json::to_string_pretty(&config).unwrap();
        let parsed: Config = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.client_uuid, config.client_uuid);
        assert_eq!(parsed.s3.bucket, config.s3.bucket);
        assert_eq!(parsed.inpoint.rtmp_port, config.inpoint.rtmp_port);
        assert_eq!(parsed.api.port, config.api.port);
        assert_eq!(parsed.hetzner.location, "nbg1");
        assert_eq!(parsed.delivery.delivery_delay_secs, 120);
    }

    #[test]
    fn config_defaults() {
        let json = r#"{
            "client_uuid": "abc",
            "s3": {
                "bucket": "b",
                "region": "r",
                "endpoint": "e",
                "access_key_id": "k",
                "secret_access_key": "s"
            }
        }"#;
        let config: Config = serde_json::from_str(json).unwrap();
        assert_eq!(config.inpoint.rtmp_port, 1234);
        assert_eq!(config.inpoint.chunk_duration_ms, 1000);
        assert_eq!(config.api.port, 8910);
        assert_eq!(config.api.bind, "127.0.0.1");
        assert_eq!(config.hetzner.default_server_type, "cpx22");
        assert_eq!(config.delivery.delivery_delay_secs, 120);
        assert_eq!(config.inpoint.chunk_format, "flv");
        assert!(config.obs.enabled);
        assert_eq!(config.obs.ws_url, "ws://127.0.0.1:4455");
        assert!(config.obs.ws_password.is_empty());
    }

    #[test]
    fn config_save_and_load() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.json");
        let config = Config::for_testing();
        config.save(&path).unwrap();
        let loaded = Config::load(&path).unwrap();
        assert_eq!(loaded.s3.bucket, config.s3.bucket);
        assert_eq!(loaded.hetzner.location, config.hetzner.location);
        assert_eq!(
            loaded.inpoint.chunk_duration_ms,
            config.inpoint.chunk_duration_ms
        );
    }

    #[serial]
    #[test]
    fn env_overrides() {
        // SAFETY: This test runs in isolation; env var mutation is acceptable.
        unsafe {
            std::env::set_var("RESTREAMER_CLIENT_UUID", "env-uuid");
            std::env::set_var("RESTREAMER_RTMP_PORT", "5678");
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.json");
        let config = Config::for_testing();
        config.save(&path).unwrap();
        let loaded = Config::load(&path).unwrap();
        assert_eq!(loaded.client_uuid, "env-uuid");
        assert_eq!(loaded.inpoint.rtmp_port, 5678);
        // SAFETY: Cleaning up env vars set by this test.
        unsafe {
            std::env::remove_var("RESTREAMER_CLIENT_UUID");
            std::env::remove_var("RESTREAMER_RTMP_PORT");
        }
    }

    #[test]
    fn validate_rejects_empty_client_uuid() {
        let config = Config::default();
        assert!(config.validate().is_err());
        assert!(config.validate().unwrap_err().contains("client_uuid"));
    }

    #[test]
    fn validate_rejects_empty_s3_credentials() {
        let mut config = Config::for_testing();
        config.s3.access_key_id = String::new();
        assert!(config.validate().is_err());
        assert!(config.validate().unwrap_err().contains("access_key_id"));
    }

    #[test]
    fn validate_accepts_valid_config() {
        let config = Config::for_testing();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn validate_rejects_ts_chunk_format() {
        let mut config = Config::for_testing();
        config.inpoint.chunk_format = "ts".to_string();
        let err = config.validate().unwrap_err();
        assert!(err.contains("ts"), "Error should mention ts: {err}");
    }

    #[test]
    fn tls_config_defaults() {
        let json = r#"{
            "client_uuid": "test",
            "s3": { "bucket": "b", "region": "r", "endpoint": "e", "access_key_id": "a", "secret_access_key": "s" },
            "delivery": { "snapshot_label": "test" },
            "api": {}
        }"#;
        let config: Config = serde_json::from_str(json).unwrap();
        assert!(!config.api.tls);
        assert_eq!(config.api.https_port, 443);
        assert_eq!(config.api.tls_cert, "cert.pem");
        assert_eq!(config.api.tls_key, "key.pem");
        assert!(config.api.https_domain.is_none());
    }

    #[test]
    fn tls_config_explicit() {
        let json = r#"{
            "client_uuid": "test",
            "s3": { "bucket": "b", "region": "r", "endpoint": "e", "access_key_id": "a", "secret_access_key": "s" },
            "delivery": { "snapshot_label": "test" },
            "api": {
                "tls": true,
                "https_port": 8443,
                "tls_cert": "my-cert.pem",
                "tls_key": "my-key.pem",
                "https_domain": "streamsnv.newlevel.media"
            }
        }"#;
        let config: Config = serde_json::from_str(json).unwrap();
        assert!(config.api.tls);
        assert_eq!(config.api.https_port, 8443);
        assert_eq!(config.api.tls_cert, "my-cert.pem");
        assert_eq!(config.api.tls_key, "my-key.pem");
        assert_eq!(
            config.api.https_domain.as_deref(),
            Some("streamsnv.newlevel.media")
        );
    }

    #[test]
    fn s3_config_debug_redacts_credentials() {
        let config = Config::for_testing();
        let debug_str = format!("{:?}", config.s3);
        assert!(debug_str.contains("***"));
        assert!(!debug_str.contains("test-key"));
        assert!(!debug_str.contains("test-secret"));
    }
}
