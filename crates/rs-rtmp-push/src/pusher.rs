//! `RtmpPusher` — public API. Filled in Tasks 4, 6, 8, 10.

use crate::{PushError, PusherConfig, PusherState};

pub struct RtmpPusher {
    url: String,
    #[allow(dead_code)]
    config: PusherConfig,
    state: PusherState,
}

impl RtmpPusher {
    pub fn new(url: String, config: PusherConfig) -> Self {
        Self {
            url,
            config,
            state: PusherState::default(),
        }
    }

    pub fn last_output_ts_ms(&self) -> u64 {
        self.state.last_output_ts_ms
    }

    pub fn reconnect_count(&self) -> u32 {
        self.state.reconnect_count
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    /// Lazy-connect + write FLV bytes. Filled in Tasks 4 + 6 + 8 + 10.
    /// Stub returns `LocalCancel` so the type signature is exercisable but no
    /// behavior is implemented.
    pub async fn push_flv_bytes(&mut self, _bytes: &[u8]) -> Result<(), PushError> {
        Err(PushError::LocalCancel)
    }

    pub async fn close(&mut self) {
        self.state.connected = false;
    }
}
