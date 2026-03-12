/// Hetzner Cloud API client for managing delivery VPS instances.
///
/// Handles full lifecycle: create from image/snapshot, health monitoring,
/// binary version checks, snapshot management, and teardown.
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub mod hetzner;

#[derive(Debug, Error)]
pub enum CloudError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("API error: {status} - {message}")]
    Api { status: u16, message: String },
    #[error("server not found: {0}")]
    ServerNotFound(String),
    #[error("snapshot not found: {0}")]
    SnapshotNotFound(String),
    #[error("timeout waiting for server: {0}")]
    Timeout(String),
    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, CloudError>;

/// Configuration for Hetzner Cloud operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HetznerConfig {
    pub api_token: String,
    pub location: String,
    pub default_server_type: String,
    pub snapshot_label: String,
    pub ssh_key_name: String,
}

impl Default for HetznerConfig {
    fn default() -> Self {
        Self {
            api_token: String::new(),
            location: "fsn1".to_string(),
            default_server_type: "cx23".to_string(),
            snapshot_label: "rs-delivery".to_string(),
            ssh_key_name: "restreamer".to_string(),
        }
    }
}

/// Server status in our tracking database.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ServerStatus {
    Creating,
    Running,
    Stopping,
    Deleted,
    Failed,
}

impl std::fmt::Display for ServerStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Creating => write!(f, "creating"),
            Self::Running => write!(f, "running"),
            Self::Stopping => write!(f, "stopping"),
            Self::Deleted => write!(f, "deleted"),
            Self::Failed => write!(f, "failed"),
        }
    }
}

impl std::str::FromStr for ServerStatus {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "creating" => Ok(Self::Creating),
            "running" => Ok(Self::Running),
            "stopping" => Ok(Self::Stopping),
            "deleted" => Ok(Self::Deleted),
            "failed" => Ok(Self::Failed),
            other => Err(format!("unknown server status: {other}")),
        }
    }
}

/// Represents a tracked delivery instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeliveryInstance {
    pub id: i64,
    pub hetzner_id: i64,
    pub name: String,
    pub ipv4: String,
    pub status: ServerStatus,
    pub server_type: String,
    pub event_id: Option<i64>,
    pub created_at: String,
    pub last_health_at: Option<String>,
}

/// Select server type based on endpoint count.
pub fn select_server_type(endpoint_count: usize) -> &'static str {
    match endpoint_count {
        0..=2 => "cx23",
        3..=7 => "cx33",
        _ => "cx43",
    }
}

/// Cloud-init script for bootstrapping a delivery VPS from scratch.
pub fn bootstrap_cloud_init(delivery_binary_url: &str) -> String {
    format!(
        r#"#cloud-config
packages:
  - ffmpeg
  - curl

write_files:
  - path: /opt/restreamer/setup.sh
    permissions: '0755'
    content: |
      #!/bin/bash
      set -e
      mkdir -p /opt/restreamer
      curl -fsSL -o /opt/restreamer/rs-delivery "{delivery_binary_url}"
      chmod +x /opt/restreamer/rs-delivery
      nohup /opt/restreamer/rs-delivery > /opt/restreamer/rs-delivery.log 2>&1 &

runcmd:
  - /opt/restreamer/setup.sh
"#
    )
}

/// Cloud-init script for starting delivery from an existing snapshot.
pub fn snapshot_cloud_init() -> &'static str {
    r#"#cloud-config
runcmd:
  - nohup /opt/restreamer/rs-delivery > /opt/restreamer/rs-delivery.log 2>&1 &
"#
}
