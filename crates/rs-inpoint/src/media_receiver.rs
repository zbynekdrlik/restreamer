//! Hub -> chunker media receiver (event-driven since #367).
//!
//! ONE `tokio::select!` loop multiplexes the hub's broadcast events, the
//! current subscription's frames / reply, the frame-stall timer and the
//! re-subscribe timer. No event can queue up behind a retry anymore.
//!
//! Before #367 the receiver read `event_rx` only between streams. A Publish
//! that arrived while it was re-subscribing after a stall stayed queued, and
//! the receiver consumed it later as a STALE event. streamhub broadcasts only
//! `Publish`, never UnPublish, so the receiver then stayed one event behind
//! for good: every real republish was joined 0-8 s late by the retry ladder,
//! and xiu's GOP-cache replay to the late subscriber baked a constant A/V
//! offset into the stream (2026-10-01, +1430 ms on YouTube). Any `RecvError`,
//! even `Lagged`, also ended `run()`, which the RTMP server read as a clean
//! stop.
//!
//! The rules now:
//! - A Publish at ANY point (streaming, stalled, subscribing, retrying)
//!   supersedes the current subscription, re-anchors the chunker session
//!   (`start_new_session`) and subscribes immediately.
//! - Every successful (re)subscribe after frames have flowed re-anchors too.
//!   xiu gives no session id, so a new publisher can never be ruled out, and
//!   re-anchoring an unchanged session costs one benign discontinuity.
//! - `Lagged` is logged and survived; a `Closed` hub channel is an error, so
//!   the orchestrator restarts the RTMP server.
//! - A dropped live subscription is explicitly unsubscribed from the hub.
//!   xiu never prunes dead frame senders on its own; it would log a send
//!   error on every frame.

use std::sync::Arc;
use std::time::Duration;

use streamhub::define::{
    BroadcastEvent, BroadcastEventReceiver, DataReceiver, FrameData, FrameDataReceiver,
    StatisticDataSender, StreamHubEvent, StreamHubEventSender, SubDataType, SubscribeType,
    SubscriberInfo,
};
use streamhub::errors::StreamHubError;
use streamhub::stream::StreamIdentifier;
use streamhub::utils::{RandomDigitCount, Uuid};
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::oneshot;
use tokio::time::{Instant, sleep_until};
use tracing::{debug, error, info, warn};

use rs_core::models::InpointState;

use crate::InpointError;
use crate::flv_chunker::FlvChunkSink;

/// If no frames arrive for this long, assume the stream stalled and re-subscribe.
/// 30s is well above the ~33ms frame interval at 30fps, so this won't trigger
/// during normal streaming.
const FRAME_TIMEOUT: Duration = Duration::from_secs(30);

/// Timeout for hub subscription response.
const SUBSCRIPTION_TIMEOUT: Duration = Duration::from_secs(10);

/// Interval for frame processing heartbeat log.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(60);

/// Maximum consecutive failed/stalled subscriptions before giving up on a
/// published stream (a new Publish always starts over).
const MAX_RESUBSCRIBE_RETRIES: u32 = 30;

/// Delay before the `retry`-th consecutive re-subscribe: 2 s, 4 s, ... 10 s.
fn retry_delay(retry: u32) -> Duration {
    Duration::from_secs((u64::from(retry) * 2).min(10))
}

/// The hub's answer to a Subscribe request.
type SubscribeReply = Result<(DataReceiver, Option<StatisticDataSender>), StreamHubError>;

/// What the receiver is doing for the current publisher.
enum Phase {
    /// No publisher, or gave up on it. Only a hub event moves us on.
    Idle,
    /// A Subscribe request is in flight.
    Subscribing {
        reply: oneshot::Receiver<SubscribeReply>,
        info: SubscriberInfo,
        deadline: Instant,
    },
    /// Subscribed: frames are flowing, or stalled until FRAME_TIMEOUT.
    Streaming {
        frames: FrameDataReceiver,
        info: SubscriberInfo,
        last_frame: Instant,
    },
    /// Waiting to re-subscribe after a stall or a failed subscription.
    RetryWait { at: Instant },
}

impl Phase {
    fn name(&self) -> &'static str {
        match self {
            Phase::Idle => "idle",
            Phase::Subscribing { .. } => "subscribing",
            Phase::Streaming { .. } => "streaming",
            Phase::RetryWait { .. } => "retry_wait",
        }
    }
}

/// Why the receiver woke up.
enum Wake {
    Hub(Result<BroadcastEvent, RecvError>),
    SubscribeReply(Result<SubscribeReply, oneshot::error::RecvError>),
    SubscribeTimedOut,
    Frame(Option<FrameData>),
    Stalled,
    RetryDue,
}

/// Wait for the current phase's next wakeup. Never resolves while `Idle`.
async fn next_phase_wake(phase: &mut Phase) -> Wake {
    match phase {
        Phase::Idle => std::future::pending().await,
        Phase::Subscribing {
            reply, deadline, ..
        } => {
            tokio::select! {
                r = reply => Wake::SubscribeReply(r),
                _ = sleep_until(*deadline) => Wake::SubscribeTimedOut,
            }
        }
        Phase::Streaming {
            frames, last_frame, ..
        } => {
            tokio::select! {
                f = frames.recv() => Wake::Frame(f),
                _ = sleep_until(*last_frame + FRAME_TIMEOUT) => Wake::Stalled,
            }
        }
        Phase::RetryWait { at } => {
            sleep_until(*at).await;
            Wake::RetryDue
        }
    }
}

/// The published stream the receiver is currently attached to.
struct Session {
    identifier: StreamIdentifier,
    /// Consecutive failed/stalled subscriptions; reset by the first frame.
    retries: u32,
    /// Frames were written since the last `start_new_session` -- the next
    /// successful subscribe must re-anchor.
    dirty: bool,
    total_frames: u64,
    frames_since_heartbeat: u64,
    last_heartbeat: Instant,
}

/// Receives media data from the xiu StreamsHub and processes it into FLV chunks.
pub struct MediaReceiver {
    event_rx: BroadcastEventReceiver,
    hub_event_tx: StreamHubEventSender,
    flv_chunk_sink: Arc<FlvChunkSink>,
    inpoint_state: InpointState,
    session: Option<Session>,
    phase: Phase,
}

impl MediaReceiver {
    pub fn new(
        event_rx: BroadcastEventReceiver,
        hub_event_tx: StreamHubEventSender,
        flv_chunk_sink: Arc<FlvChunkSink>,
        inpoint_state: InpointState,
    ) -> Self {
        Self {
            event_rx,
            hub_event_tx,
            flv_chunk_sink,
            inpoint_state,
            session: None,
            phase: Phase::Idle,
        }
    }

    /// Run the media receiver loop until the hub's broadcast channel closes,
    /// which is returned as an error so the RTMP server is restarted.
    pub async fn run(mut self) -> Result<(), InpointError> {
        info!("Media receiver started, waiting for RTMP publishers");

        loop {
            // Hub events first (`biased`): a Publish must never wait behind
            // a frame burst or a retry timer.
            let wake = tokio::select! {
                biased;
                ev = self.event_rx.recv() => Wake::Hub(ev),
                w = next_phase_wake(&mut self.phase) => w,
            };
            match wake {
                Wake::Hub(Ok(event)) => self.on_hub_event(event).await,
                Wake::Hub(Err(RecvError::Lagged(skipped))) => {
                    warn!(
                        skipped,
                        phase = self.phase.name(),
                        "Media receiver lagged behind the hub broadcast channel -- continuing (#367)"
                    );
                }
                Wake::Hub(Err(RecvError::Closed)) => {
                    error!("Hub broadcast event channel closed -- media receiver cannot continue");
                    self.drop_subscription();
                    self.end_session("hub_closed").await;
                    return Err(InpointError::Protocol(
                        "hub broadcast event channel closed".to_string(),
                    ));
                }
                Wake::SubscribeReply(reply) => self.on_subscribe_reply(reply).await,
                Wake::SubscribeTimedOut => {
                    warn!(
                        "Subscription timeout after {}s",
                        SUBSCRIPTION_TIMEOUT.as_secs()
                    );
                    self.drop_subscription();
                    self.schedule_retry("subscription_timeout").await;
                }
                Wake::Frame(Some(frame)) => self.on_frame(frame).await,
                Wake::Frame(None) => {
                    // Channel closed -- publisher disconnected normally.
                    let total = self.session.as_ref().map_or(0, |s| s.total_frames);
                    info!(
                        total_frames = total,
                        "Frame channel closed, flushing remaining data"
                    );
                    self.phase = Phase::Idle;
                    self.flv_chunk_sink.flush().await;
                    self.end_session("publisher_closed").await;
                }
                Wake::Stalled => {
                    let total = self.session.as_ref().map_or(0, |s| s.total_frames);
                    error!(
                        total_frames = total,
                        timeout_secs = FRAME_TIMEOUT.as_secs(),
                        "No frames received -- stream stalled, will re-subscribe"
                    );
                    self.drop_subscription();
                    self.flv_chunk_sink.flush().await;
                    self.schedule_retry("stalled").await;
                }
                Wake::RetryDue => self.subscribe().await,
            }
        }
    }

    async fn on_hub_event(&mut self, event: BroadcastEvent) {
        match event {
            BroadcastEvent::Publish { identifier } => self.on_publish(identifier).await,
            BroadcastEvent::UnPublish { identifier } => {
                // streamhub 0.2.4 never broadcasts this; handled for safety.
                info!("Stream unpublished: {identifier}");
                self.drop_subscription();
                self.flv_chunk_sink.flush().await;
                self.end_session("unpublish").await;
            }
            BroadcastEvent::Subscribe { identifier, .. } => {
                debug!("New subscriber for stream: {identifier}");
            }
            BroadcastEvent::UnSubscribe { .. } => {
                debug!("Subscriber disconnected");
            }
        }
    }

    /// A publisher (re)connected. Supersede whatever we were doing, re-anchor
    /// the chunker session and subscribe right now.
    async fn on_publish(&mut self, identifier: StreamIdentifier) {
        info!("Stream published: {identifier}");
        if self.session.is_some() {
            warn!(
                phase = self.phase.name(),
                "Publish arrived while a session is active -- superseding the current \
                 subscription and subscribing immediately (#367)"
            );
            self.drop_subscription();
            self.end_session("superseded_by_publish").await;
        }

        self.inpoint_state.mark_connected().await;
        self.audit_rtmp(
            rs_core::audit::Action::RtmpConnected,
            serde_json::json!({ "stream_identifier": format!("{identifier}") }),
        );
        // Re-anchor audio+video onto a fresh shared session origin on every
        // (re)publish (#255, #367): the new publisher's source ts restart.
        self.flv_chunk_sink.start_new_session().await;
        self.session = Some(Session {
            identifier,
            retries: 0,
            dirty: false,
            total_frames: 0,
            frames_since_heartbeat: 0,
            last_heartbeat: Instant::now(),
        });
        self.subscribe().await;
    }

    /// Send a Subscribe request for the current session's stream.
    async fn subscribe(&mut self) {
        let Some(identifier) = self.session.as_ref().map(|s| s.identifier.clone()) else {
            self.phase = Phase::Idle;
            return;
        };
        let info = SubscriberInfo {
            id: Uuid::new(RandomDigitCount::Six),
            sub_type: SubscribeType::RtmpPull,
            notify_info: streamhub::define::NotifyInfo {
                request_url: String::new(),
                remote_addr: String::from("local-chunker"),
            },
            sub_data_type: SubDataType::Frame,
        };
        let (result_tx, result_rx) = oneshot::channel();
        if self
            .hub_event_tx
            .send(StreamHubEvent::Subscribe {
                identifier,
                info: info.clone(),
                result_sender: result_tx,
            })
            .is_err()
        {
            warn!("Failed to send subscribe request to hub");
            self.phase = Phase::Idle;
            self.schedule_retry("hub_unreachable").await;
            return;
        }
        debug!(subscriber_id = %info.id, "Subscribe request sent to hub");
        self.phase = Phase::Subscribing {
            reply: result_rx,
            info,
            deadline: Instant::now() + SUBSCRIPTION_TIMEOUT,
        };
    }

    async fn on_subscribe_reply(
        &mut self,
        reply: Result<SubscribeReply, oneshot::error::RecvError>,
    ) {
        let info = match std::mem::replace(&mut self.phase, Phase::Idle) {
            Phase::Subscribing { info, .. } => info,
            other => {
                // Cannot happen: a reply only wakes us in Subscribing.
                self.phase = other;
                return;
            }
        };
        let frames = match reply {
            Ok(Ok((data, _statistics))) => data.frame_receiver,
            Ok(Err(e)) => {
                warn!("Hub rejected subscription: {e}");
                None
            }
            Err(_) => {
                warn!("Hub subscription channel dropped");
                None
            }
        };
        let Some(frames) = frames else {
            self.schedule_retry("subscription_failed").await;
            return;
        };

        // #367: xiu gives no session id, so ANY successful re-subscribe after
        // frames have flowed may be a new publisher (and will start with a
        // GOP-cache replay) -- re-anchor the chunker session for both tracks.
        if self.session.as_ref().is_some_and(|s| s.dirty) {
            info!(
                "Re-subscribed after frames had flowed -- re-anchoring the chunker session (#367)"
            );
            self.flv_chunk_sink.start_new_session().await;
            if let Some(s) = self.session.as_mut() {
                s.dirty = false;
            }
        }
        info!("Subscribed to stream, processing frames");
        self.phase = Phase::Streaming {
            frames,
            info,
            last_frame: Instant::now(),
        };
    }

    async fn on_frame(&mut self, frame: FrameData) {
        if let Phase::Streaming { last_frame, .. } = &mut self.phase {
            *last_frame = Instant::now();
        }
        if let Some(s) = self.session.as_mut() {
            if s.retries > 0 {
                info!(after_retries = s.retries, "Frames flowing again");
                s.retries = 0;
            }
            s.total_frames += 1;
            s.frames_since_heartbeat += 1;
            if s.last_heartbeat.elapsed() >= HEARTBEAT_INTERVAL {
                info!(
                    frames_last_60s = s.frames_since_heartbeat,
                    total_frames = s.total_frames,
                    "Frame processing heartbeat"
                );
                s.frames_since_heartbeat = 0;
                s.last_heartbeat = Instant::now();
            }
        }
        match frame {
            FrameData::Video { timestamp, data } => {
                self.mark_dirty();
                self.flv_chunk_sink.write_video(timestamp, &data).await;
            }
            FrameData::Audio { timestamp, data } => {
                self.mark_dirty();
                self.flv_chunk_sink.write_audio(timestamp, &data).await;
            }
            FrameData::MediaInfo { .. } => {
                debug!("Received media info");
            }
            FrameData::MetaData { .. } => {
                debug!("Received metadata");
            }
        }
    }

    fn mark_dirty(&mut self) {
        if let Some(s) = self.session.as_mut() {
            s.dirty = true;
        }
    }

    /// Count a failed/stalled subscription and either wait to retry or give up.
    async fn schedule_retry(&mut self, reason: &'static str) {
        let Some(s) = self.session.as_mut() else {
            self.phase = Phase::Idle;
            return;
        };
        s.retries += 1;
        let retry = s.retries;
        let identifier = s.identifier.clone();
        if retry >= MAX_RESUBSCRIBE_RETRIES {
            error!(
                retry,
                reason, "Max re-subscribe retries reached, giving up: {identifier}"
            );
            self.phase = Phase::Idle;
            self.end_session("gave_up").await;
            return;
        }
        let delay = retry_delay(retry);
        warn!(
            retry,
            reason,
            "Re-subscribing to stream in {}s: {identifier}",
            delay.as_secs()
        );
        self.phase = Phase::RetryWait {
            at: Instant::now() + delay,
        };
    }

    /// Leave the current subscription (or pending subscribe): tell the hub to
    /// drop our frame sender so xiu stops sending to a dead channel.
    fn drop_subscription(&mut self) {
        let info = match std::mem::replace(&mut self.phase, Phase::Idle) {
            Phase::Streaming { info, .. } | Phase::Subscribing { info, .. } => info,
            Phase::Idle | Phase::RetryWait { .. } => return,
        };
        let Some(identifier) = self.session.as_ref().map(|s| s.identifier.clone()) else {
            return;
        };
        debug!(subscriber_id = %info.id, "Unsubscribing from hub: {identifier}");
        if self
            .hub_event_tx
            .send(StreamHubEvent::UnSubscribe { identifier, info })
            .is_err()
        {
            warn!("Failed to send unsubscribe request to hub");
        }
    }

    /// End the current published-stream session: mark the inpoint
    /// disconnected and audit it. No-op without a session.
    async fn end_session(&mut self, reason: &'static str) {
        let Some(s) = self.session.take() else {
            return;
        };
        info!(reason, "Stream ended: {}", s.identifier);
        let duration_secs = self.inpoint_state.mark_disconnected().await;
        self.audit_rtmp(
            rs_core::audit::Action::RtmpDisconnected,
            serde_json::json!({
                "stream_identifier": format!("{}", s.identifier),
                "duration_secs": duration_secs,
                "reason": reason,
            }),
        );
    }

    fn audit_rtmp(&self, action: rs_core::audit::Action, detail: serde_json::Value) {
        if let Some(tx) = self.inpoint_state.audit_tx() {
            rs_core::audit::record(
                tx,
                rs_core::audit::AuditRow {
                    severity: rs_core::audit::Severity::Info,
                    source: rs_core::audit::Source::Inpoint,
                    event_id: None,
                    instance_id: None,
                    endpoint: None,
                    action,
                    detail,
                    ts_override: None,
                },
            );
        }
    }
}

#[cfg(test)]
#[path = "media_receiver_tests.rs"]
mod tests;
