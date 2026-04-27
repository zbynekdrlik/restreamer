//! `RtmpPusher` - public API.

use crate::session::Session;
use crate::{PushError, PusherConfig, PusherState};

pub struct RtmpPusher {
    url: String,
    config: PusherConfig,
    state: PusherState,
    session: Option<Session>,
}

impl RtmpPusher {
    pub fn new(url: String, config: PusherConfig) -> Self {
        Self {
            url,
            config,
            state: PusherState::default(),
            session: None,
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

    /// Lazy-connect + write FLV bytes.
    ///
    /// On the first call (or after a reconnect) the pusher dials the server,
    /// performs the full RTMP handshake + connect + publish sequence, and
    /// stores the resulting `Session`.
    ///
    /// For empty `bytes` slices the method returns `Ok(())` after connecting -
    /// this is the Task 3 test contract: "handshake completes, no tags sent".
    ///
    /// Tag-write loop (parsing FLV tags and calling `session.send_*_tag`) is
    /// filled in Task 6.
    pub async fn push_flv_bytes(&mut self, bytes: &[u8]) -> Result<(), PushError> {
        // Lazy connect.
        if self.session.is_none() {
            let s = Session::connect(&self.url, self.config.timeout_ms).await?;
            self.session = Some(s);
            self.state.connected = true;
        }

        // Empty slice -> handshake verified, nothing to send.
        if bytes.is_empty() {
            return Ok(());
        }

        // Tag-write loop is filled in Task 6.
        Err(PushError::LocalCancel)
    }

    pub async fn close(&mut self) {
        if let Some(s) = self.session.take() {
            s.close().await;
        }
        self.state.connected = false;
    }
}
