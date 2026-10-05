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
//! - A Publish of the SAME stream at ANY point (streaming, stalled,
//!   subscribing, retrying) supersedes the current subscription, re-anchors
//!   the chunker session (`start_new_session`) and subscribes immediately. A
//!   Publish of a DIFFERENT stream supersedes only a session that is not
//!   streaming. While one is streaming, the other Publish is deferred and
//!   taken over the moment the live stream stops streaming (it stalls, ends
//!   or is given up); it never orphans the live publisher.
//! - Taking over a deferred Publish, and looking for a Publish a broadcast
//!   lag may have swallowed, are PROBES: a Subscribe the hub accepts starts
//!   the session; a failed one (rejected or timed out) never marks the
//!   inpoint "connected" and never runs a retry ladder against a publisher
//!   that is gone.
//! - A stream the receiver LEAVES without seeing it end is remembered
//!   (`remembered`): a non-streaming session (stalled, retrying, or with
//!   its first Subscribe in flight) a takeover or a Publish of another
//!   stream supersedes, an in-flight probe such a Publish abandons, a
//!   deferred Publish a newer one overwrites, and the previous last stream
//!   with a pending lag when another stream's session starts. Every time
//!   the receiver is Idle with no session it probes ONE remembered stream
//!   (most recent first): a stalled publisher stays registered at the hub
//!   and can resume without a new Publish. Two keys do reach stream.lan (OBS
//!   and the CI ffmpeg). Probing a stream or starting its session forgets
//!   it, so each entry costs at most one probe. Seeing a stream end
//!   (publisher closed, given up, UnPublish) never remembers it by itself;
//!   a pending lag still can. Known limit: nothing remembered is probed
//!   while a session runs its retry ladder (up to ~4.5 min rejected, longer
//!   while a registered stream sends no frames).
//! - Every successful (re)subscribe after frames have flowed re-anchors too.
//!   xiu gives no session id, so a new publisher can never be ruled out, and
//!   re-anchoring an unchanged session costs one benign discontinuity.
//! - `Lagged` is logged and survived. The lag is kept until the receiver is
//!   next Idle with no session and nothing remembered is left; then the
//!   last known stream is probed: the lag may have swallowed its Publish,
//!   even the live stream's own reconnect. `begin_session`, the one place
//!   the last stream changes, settles every pending lag: another previous
//!   stream is remembered; for the same stream a Publish read after the lag
//!   is newer than anything it lost. Sending ANY probe of the last stream
//!   clears it too (`send_subscribe`), and an accepted Subscribe sets it to
//!   whether a lag arrived while it was in flight. A lag costs at most one
//!   probe per stream it can belong to (the previous last stream, and the
//!   attached one if it arrived during that Subscribe). A `Closed` hub
//!   channel is an error, so the orchestrator restarts the RTMP server.
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
use crate::frame_stats::FrameStats;

/// If no frames arrive for this long, assume the stream stalled and re-subscribe.
/// 30s is well above the ~33ms frame interval at 30fps, so this won't trigger
/// during normal streaming.
const FRAME_TIMEOUT: Duration = Duration::from_secs(30);

/// Timeout for hub subscription response.
const SUBSCRIPTION_TIMEOUT: Duration = Duration::from_secs(10);

/// Maximum consecutive failed/stalled subscriptions before giving up on a
/// published stream (a new Publish always starts over).
const MAX_RESUBSCRIBE_RETRIES: u32 = 30;

/// Delay before the `retry`-th consecutive re-subscribe: 2 s, 4 s, ... 10 s.
fn retry_delay(retry: u32) -> Duration {
    Duration::from_secs((u64::from(retry) * 2).min(10))
}

/// The hub's answer to a Subscribe request.
type SubscribeReply = Result<(DataReceiver, Option<StatisticDataSender>), StreamHubError>;

/// A Subscribe sent with no session behind it yet (#367): a deferred
/// Publish taken over, a remembered stream, or a stream a broadcast lag may
/// have hidden. Acceptance starts the session (audited with `trigger`); a
/// failure only returns to Idle (`settle` decides what comes next).
struct Probe {
    identifier: StreamIdentifier,
    trigger: &'static str,
}

/// What the receiver is doing for the current publisher.
enum Phase {
    /// No publisher, or gave up on it. Only a hub event moves us on.
    Idle,
    /// A Subscribe request is in flight; `probe` is set when no session
    /// stands behind it yet. `lagged`: a broadcast lag arrived while it was
    /// in flight, which its acceptance does NOT cover (that lag can hide a
    /// Publish newer than the attachment).
    Subscribing {
        reply: oneshot::Receiver<SubscribeReply>,
        info: SubscriberInfo,
        deadline: Instant,
        probe: Option<Probe>,
        lagged: bool,
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
                biased;
                r = reply => Wake::SubscribeReply(r),
                _ = sleep_until(*deadline) => Wake::SubscribeTimedOut,
            }
        }
        Phase::Streaming {
            frames, last_frame, ..
        } => {
            // `biased`: after a process freeze a waiting frame and an already
            // expired stall timer are ready together. The frame wins, so a
            // live stream is never dropped as stalled.
            tokio::select! {
                biased;
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
    stats: FrameStats,
}

impl Session {
    fn new(identifier: StreamIdentifier) -> Self {
        Self {
            identifier,
            retries: 0,
            dirty: false,
            stats: FrameStats::new(Instant::now()),
        }
    }

    /// A frame arrived, so the re-subscribe ladder starts over. Returns how
    /// many retries it took to get frames flowing again, if any.
    fn frames_resumed(&mut self) -> Option<u32> {
        let retries = std::mem::take(&mut self.retries);
        (retries > 0).then_some(retries)
    }
}

/// Receives media data from the xiu StreamsHub and processes it into FLV chunks.
pub struct MediaReceiver {
    event_rx: BroadcastEventReceiver,
    hub_event_tx: StreamHubEventSender,
    flv_chunk_sink: Arc<FlvChunkSink>,
    inpoint_state: InpointState,
    session: Option<Session>,
    phase: Phase,
    /// A Publish of a different stream that arrived while the current one
    /// was live; taken over the moment the current one stops streaming.
    pending_publish: Option<StreamIdentifier>,
    /// The last stream that published (target of a lost-Publish probe).
    last_identifier: Option<StreamIdentifier>,
    /// A broadcast lag happened that no probe has covered yet. It belongs to
    /// `last_identifier`: probed once the receiver is Idle with no session
    /// and nothing remembered is left; turned into a remembered stream when
    /// another stream's session starts (`begin_session`).
    lag_unprobed: bool,
    /// Streams the receiver left without seeing them end, most recent last:
    /// each is probed ONCE when the receiver is Idle with no session, and
    /// forgotten when any probe of it is sent or it gets its own session.
    remembered: Vec<StreamIdentifier>,
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
            pending_publish: None,
            last_identifier: None,
            lag_unprobed: false,
            remembered: Vec::new(),
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
                        "Media receiver lagged behind the hub broadcast channel -- continuing; \
                         the last stream is probed once the receiver is idle (#367)"
                    );
                    self.lag_unprobed = true;
                    if let Phase::Subscribing { lagged, .. } = &mut self.phase {
                        *lagged = true;
                    }
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
                    match self.drop_subscription() {
                        Some(p) => info!(
                            trigger = p.trigger,
                            "Probe of {} timed out -- nothing publishing (#367)", p.identifier
                        ),
                        None => self.schedule_retry("subscription_timeout").await,
                    }
                }
                Wake::Frame(Some(frame)) => self.on_frame(frame).await,
                Wake::Frame(None) => {
                    // Channel closed -- publisher disconnected normally.
                    let total = self.session.as_ref().map_or(0, |s| s.stats.total());
                    info!(
                        total_frames = total,
                        "Frame channel closed, flushing remaining data"
                    );
                    self.phase = Phase::Idle;
                    self.flv_chunk_sink.flush().await;
                    self.end_session("publisher_closed").await;
                }
                Wake::Stalled => {
                    let total = self.session.as_ref().map_or(0, |s| s.stats.total());
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
            self.settle().await;
        }
    }

    /// Run after every wakeup (#367). A Publish deferred behind a live
    /// stream is taken over the moment that stream is no longer live
    /// (stalled, retrying, given up or ended). Once the receiver is Idle
    /// with no session, one remembered stream is probed, else an unprobed
    /// broadcast lag is covered. All three are probes.
    async fn settle(&mut self) {
        if matches!(self.phase, Phase::Idle | Phase::RetryWait { .. }) {
            if let Some(next) = self.pending_publish.take() {
                info!(
                    phase = self.phase.name(),
                    "The live stream stopped streaming -- taking over the Publish deferred \
                     behind it, as a probe: {next} (#367)"
                );
                // A stalled / retrying session is left without being seen
                // to end; a session that already ended (Idle) is not.
                if let Some(left) = self.session.as_ref().map(|s| s.identifier.clone()) {
                    self.remember(left, &next);
                }
                self.end_session("superseded_by_deferred_publish").await;
                self.send_subscribe(next, Some("deferred_publish")).await;
                return;
            }
        }
        if self.session.is_some() || !matches!(self.phase, Phase::Idle) {
            return;
        }
        if let Some(left) = self.remembered.pop() {
            warn!(
                "Idle -- probing once a stream the receiver left without seeing it end: {left} \
                 (a stalled publisher can resume without a new Publish, #367)"
            );
            self.send_subscribe(left, Some("remembered_probe")).await;
            return;
        }
        if std::mem::take(&mut self.lag_unprobed) {
            if let Some(identifier) = self.last_identifier.clone() {
                warn!(
                    "Idle after a lagged broadcast -- probing the last stream {identifier} (#367)"
                );
                self.send_subscribe(identifier, Some("lagged_probe")).await;
            }
        }
    }

    async fn on_hub_event(&mut self, event: BroadcastEvent) {
        match event {
            BroadcastEvent::Publish { identifier } => {
                let busy_with_other = self.session.as_ref().is_some_and(|s| {
                    s.identifier != identifier && matches!(self.phase, Phase::Streaming { .. })
                });
                if busy_with_other {
                    warn!(
                        "Publish of a different stream ({identifier}) while another one is \
                         streaming -- deferred until the live stream stops streaming (#367)"
                    );
                    // An older deferred Publish it replaces is remembered.
                    if let Some(older) = self.pending_publish.replace(identifier.clone()) {
                        self.remember(older, &identifier);
                    }
                } else {
                    self.on_publish(identifier).await;
                }
            }
            BroadcastEvent::UnPublish { identifier } => {
                // streamhub 0.2.4 never broadcasts this (any 0.2.x might);
                // handled for safety, and only for the stream it names.
                info!("Stream unpublished: {identifier}");
                if self.pending_publish.as_ref() == Some(&identifier) {
                    self.pending_publish = None;
                }
                if self
                    .session
                    .as_ref()
                    .is_some_and(|s| s.identifier == identifier)
                {
                    self.drop_subscription();
                    self.flv_chunk_sink.flush().await;
                    self.end_session("unpublish").await;
                }
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
        }
        // Also abandons a probe in flight (no session behind it). The stream
        // we leave this way, if it is another one, is remembered.
        let abandoned = self.drop_subscription();
        let left = match (&self.session, abandoned) {
            (Some(s), _) => Some(s.identifier.clone()),
            (None, Some(p)) => Some(p.identifier),
            (None, None) => None,
        };
        if let Some(left) = left {
            self.remember(left, &identifier);
        }
        self.end_session("superseded_by_publish").await;

        if self.pending_publish.as_ref() == Some(&identifier) {
            self.pending_publish = None;
        }
        self.begin_session(identifier, "publish").await;
        self.subscribe().await;
    }

    /// Remember a stream the receiver leaves for `next` without seeing it
    /// end (a stalled publisher stays registered at the hub and can resume
    /// without a new Publish): it is probed once when the receiver is next
    /// Idle with no session. Leaving a stream for itself remembers nothing.
    fn remember(&mut self, left: StreamIdentifier, next: &StreamIdentifier) {
        if left == *next {
            return;
        }
        if self.remembered.contains(&left) {
            return;
        }
        debug!("Remembering {left} to probe once the receiver is idle (#367)");
        self.remembered.push(left);
    }

    /// Start a published-stream session: mark the inpoint connected, audit
    /// it and re-anchor audio+video onto a fresh shared session origin
    /// (#255, #367): a new publisher's source ts restart.
    ///
    /// The ONE place `last_identifier` changes, so every pending lag is
    /// settled here: it belongs to the previous last stream, which is
    /// remembered if it is another one (its reconnect may hide behind the
    /// lag). For the same stream nothing is lost: the Publish or probe that
    /// leads here was read after the lag, so it is newer than anything the
    /// lag swallowed. The caller sets the flag again (`lagged`) for a lag
    /// that arrived while its Subscribe was in flight. A remembered entry
    /// for this stream is moot now.
    async fn begin_session(&mut self, identifier: StreamIdentifier, trigger: &'static str) {
        if std::mem::take(&mut self.lag_unprobed) {
            if let Some(last) = self.last_identifier.clone() {
                self.remember(last, &identifier);
            }
        }
        self.remembered.retain(|r| *r != identifier);
        self.last_identifier = Some(identifier.clone());
        self.inpoint_state.mark_connected().await;
        self.audit_rtmp(
            rs_core::audit::Action::RtmpConnected,
            serde_json::json!({
                "stream_identifier": format!("{identifier}"),
                "trigger": trigger,
            }),
        );
        self.flv_chunk_sink.start_new_session().await;
        self.session = Some(Session::new(identifier));
    }

    /// Send a Subscribe request for the current session's stream.
    async fn subscribe(&mut self) {
        let Some(identifier) = self.session.as_ref().map(|s| s.identifier.clone()) else {
            self.phase = Phase::Idle;
            return;
        };
        self.send_subscribe(identifier, None).await;
    }

    /// Send a Subscribe for `identifier`. `probe`: the trigger of a probe
    /// with no session behind it yet (see [`Probe`]); `None` subscribes for
    /// the current session.
    async fn send_subscribe(&mut self, identifier: StreamIdentifier, probe: Option<&'static str>) {
        let probe = probe.map(|trigger| {
            // ANY probe of the last stream is what a lag probe would send:
            // it covers every lag seen so far (a lag during its flight sets
            // the flag again).
            if self.last_identifier.as_ref() == Some(&identifier) {
                self.lag_unprobed = false;
            }
            // A probe of a remembered stream covers its entry.
            self.remembered.retain(|r| *r != identifier);
            Probe {
                identifier: identifier.clone(),
                trigger,
            }
        });
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
            if probe.is_none() {
                self.schedule_retry("hub_unreachable").await;
            }
            return;
        }
        debug!(
            subscriber_id = %info.id,
            probe = probe.as_ref().map(|p| p.trigger),
            "Subscribe request sent to hub"
        );
        self.phase = Phase::Subscribing {
            reply: result_rx,
            info,
            deadline: Instant::now() + SUBSCRIPTION_TIMEOUT,
            probe,
            lagged: false,
        };
    }

    async fn on_subscribe_reply(
        &mut self,
        reply: Result<SubscribeReply, oneshot::error::RecvError>,
    ) {
        let (info, probe, lagged) = match std::mem::replace(&mut self.phase, Phase::Idle) {
            Phase::Subscribing {
                info,
                probe,
                lagged,
                ..
            } => (info, probe, lagged),
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
            match probe {
                Some(p) => info!(
                    trigger = p.trigger,
                    "Probe found nothing publishing on {} (#367)", p.identifier
                ),
                None => self.schedule_retry("subscription_failed").await,
            }
            return;
        };
        if let Some(p) = probe {
            info!(
                trigger = p.trigger,
                "Probe found {} publishing -- starting its session (#367)", p.identifier
            );
            // begin_session settles every pending lag; `lagged` (below) sets
            // it again for this stream.
            self.begin_session(p.identifier, p.trigger).await;
        }
        // Attached: a Publish a lag hid BEFORE this Subscribe went out is moot
        // (or kept as a remembered stream by begin_session). A lag while it
        // was in flight is not covered.
        self.lag_unprobed = lagged;

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
            if let Some(retries) = s.frames_resumed() {
                info!(after_retries = retries, "Frames flowing again");
            }
            if let Some(beat) = s.stats.count(Instant::now()) {
                info!(
                    frames_last_60s = beat.frames_since_last,
                    total_frames = beat.total_frames,
                    "Frame processing heartbeat"
                );
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
    /// drop our frame sender so xiu stops sending to a dead channel. Returns
    /// the probe that was in flight, if the abandoned Subscribe was one.
    fn drop_subscription(&mut self) -> Option<Probe> {
        let (info, probe) = match std::mem::replace(&mut self.phase, Phase::Idle) {
            Phase::Streaming { info, .. } => (info, None),
            Phase::Subscribing { info, probe, .. } => (info, probe),
            Phase::Idle | Phase::RetryWait { .. } => return None,
        };
        let identifier = match (&self.session, &probe) {
            (Some(s), _) => s.identifier.clone(),
            (None, Some(p)) => p.identifier.clone(),
            (None, None) => return probe,
        };
        debug!(subscriber_id = %info.id, "Unsubscribing from hub: {identifier}");
        if self
            .hub_event_tx
            .send(StreamHubEvent::UnSubscribe { identifier, info })
            .is_err()
        {
            warn!("Failed to send unsubscribe request to hub");
        }
        probe
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
