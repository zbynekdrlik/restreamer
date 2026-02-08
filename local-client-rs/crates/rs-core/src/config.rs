use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

use crate::error::Result;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub client_uuid: String,
    pub manager_url: String,
    pub s3: S3Config,
    #[serde(default)]
    pub inpoint: InpointConfig,
    #[serde(default)]
    pub api: ApiConfig,
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
    #[serde(default = "default_chunk_duration_ms")]
    pub chunk_duration_ms: u64,
    #[serde(default = "default_read_buffer_bytes")]
    pub read_buffer_bytes: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiConfig {
    #[serde(default = "default_api_port")]
    pub port: u16,
    #[serde(default = "default_api_bind")]
    pub bind: String,
}

fn default_rtmp_port() -> u16 {
    1234
}
fn default_chunk_duration_ms() -> u64 {
    1000
}
fn default_read_buffer_bytes() -> usize {
    102_400
}
fn default_api_port() -> u16 {
    8910
}
fn default_api_bind() -> String {
    "127.0.0.1".to_string()
}

impl Default for InpointConfig {
    fn default() -> Self {
        Self {
            rtmp_port: default_rtmp_port(),
            chunk_duration_ms: default_chunk_duration_ms(),
            read_buffer_bytes: default_read_buffer_bytes(),
        }
    }
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            port: default_api_port(),
            bind: default_api_bind(),
        }
    }
}

impl Config {
    /// Load config from file, with env var overrides.
    pub fn load(path: &Path) -> Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let mut config: Config = serde_json::from_str(&content)?;
        config.apply_env_overrides();
        Ok(config)
    }

    /// Save config to file.
    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let content = serde_json::to_string_pretty(self)?;
        std::fs::write(path, content)?;
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

    fn apply_env_overrides(&mut self) {
        if let Ok(v) = std::env::var("RESTREAMER_CLIENT_UUID") {
            self.client_uuid = v;
        }
        if let Ok(v) = std::env::var("RESTREAMER_MANAGER_URL") {
            self.manager_url = v;
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
        if let Ok(v) = std::env::var("RESTREAMER_RTMP_PORT") {
            match v.parse() {
                Ok(port) => self.inpoint.rtmp_port = port,
                Err(e) => tracing::warn!("Invalid RESTREAMER_RTMP_PORT '{v}': {e}"),
            }
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
    }

    /// Validate that required configuration fields are present.
    pub fn validate(&self) -> std::result::Result<(), String> {
        if self.client_uuid.is_empty() {
            return Err("client_uuid is required".to_string());
        }
        if self.manager_url.is_empty() {
            return Err("manager_url is required".to_string());
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
        Ok(())
    }

    /// Create a minimal config for testing.
    pub fn for_testing() -> Self {
        Self {
            client_uuid: "test-uuid-00000000".to_string(),
            manager_url: "http://localhost:9999".to_string(),
            s3: S3Config {
                bucket: "test-bucket".to_string(),
                region: "us-east-1".to_string(),
                endpoint: "http://localhost:9000".to_string(),
                access_key_id: "test-key".to_string(),
                secret_access_key: "test-secret".to_string(),
            },
            inpoint: InpointConfig::default(),
            api: ApiConfig {
                port: 0, // random port for tests
                bind: "127.0.0.1".to_string(),
            },
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            client_uuid: String::new(),
            manager_url: "https://restreamer.newlevel.media".to_string(),
            s3: S3Config {
                bucket: "restreamer-chunks".to_string(),
                region: "eu-central-1".to_string(),
                endpoint: "https://eu-central-1.linodeobjects.com".to_string(),
                access_key_id: String::new(),
                secret_access_key: String::new(),
            },
            inpoint: InpointConfig::default(),
            api: ApiConfig::default(),
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
        assert_eq!(parsed.manager_url, config.manager_url);
        assert_eq!(parsed.s3.bucket, config.s3.bucket);
        assert_eq!(parsed.inpoint.rtmp_port, config.inpoint.rtmp_port);
        assert_eq!(parsed.api.port, config.api.port);
    }

    #[test]
    fn config_defaults() {
        let json = r#"{
            "client_uuid": "abc",
            "manager_url": "http://test",
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
    }

    #[test]
    fn config_save_and_load() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.json");
        let config = Config::for_testing();
        config.save(&path).unwrap();
        let loaded = Config::load(&path).unwrap();
        // Check fields that aren't affected by env var overrides
        assert_eq!(loaded.s3.bucket, config.s3.bucket);
        assert_eq!(loaded.manager_url, config.manager_url);
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
    fn s3_config_debug_redacts_credentials() {
        let config = Config::for_testing();
        let debug_str = format!("{:?}", config.s3);
        assert!(debug_str.contains("***"));
        assert!(!debug_str.contains("test-key"));
        assert!(!debug_str.contains("test-secret"));
    }
}
