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
    /// For non-empty `bytes`, each audio/video tag body is written via
    /// `ChunkPacketizer` with monotonically rewritten timestamps.
    pub async fn push_flv_bytes(&mut self, bytes: &[u8]) -> Result<(), PushError> {
        // Lazy connect.
        if self.session.is_none() {
            // A reconnect is any connect that happens after media has been sent
            // (last_output_ts_ms > 0 means at least one tag was written in a
            // previous session).  Count the attempt before the connect so the
            // dashboard always sees an accurate reconnect count, even when the
            // reconnect itself fails.
            let is_reconnect = self.state.last_output_ts_ms > 0;
            let connect_result = Session::connect(&self.url, self.config.timeout_ms).await;
            if is_reconnect {
                self.state.reconnect_count = self.state.reconnect_count.saturating_add(1);
            }
            let s = connect_result?;
            self.session = Some(s);
            self.state.connected = true;
        }

        // Empty slice -> handshake verified, nothing to send.
        if bytes.is_empty() {
            return Ok(());
        }

        // Parse FLV tags with monotonic timestamp rewriting.
        let iter = crate::flv::FlvTagIter::new(bytes)?;
        let mut chunk_first_ts: Option<u32> = None;
        let monotonic_offset = self.state.last_output_ts_ms;

        // Collect tag metadata (type, output timestamp) and body slices.
        // We hold the body references into `bytes` (lifetime tied to the `iter`
        // borrow of `bytes`), so we send each tag immediately inside the loop.
        for tag in iter {
            // Compute the per-chunk delta and add to the monotonic output base.
            let first = *chunk_first_ts.get_or_insert(tag.timestamp_ms);
            let delta = tag.timestamp_ms.saturating_sub(first);
            let output_ts_u64 = monotonic_offset + delta as u64;
            let output_ts = output_ts_u64 as u32;
            self.state.last_output_ts_ms = output_ts_u64;

            let session = self.session.as_mut().expect("session was just set");
            let send_result = match tag.tag_type {
                crate::flv::FLV_TAG_AUDIO => session.send_audio_tag(output_ts, tag.body).await,
                crate::flv::FLV_TAG_VIDEO => session.send_video_tag(output_ts, tag.body).await,
                crate::flv::FLV_TAG_SCRIPT => Ok(()), // skip metadata
                other => {
                    tracing::warn!(tag_type = other, "unknown FLV tag type, skipping");
                    Ok(())
                }
            };

            if let Err(e) = send_result {
                // Drop session so next call lazy-reconnects.
                self.state.connected = false;
                self.session = None;
                return Err(e);
            }
        }

        Ok(())
    }

    pub async fn close(&mut self) {
        if let Some(s) = self.session.take() {
            s.close().await;
        }
        self.state.connected = false;
    }
}
