//! A/V-gate session state machine (#357): every transition and a failure at
//! every step, each followed by its cleanup. YouTube is a wiremock server
//! driven by a small shared state; the rig is scripted (`FakeRig`), because the
//! real one boots a Hetzner VPS (the production rig has its own tests in
//! `av_gate_rig_tests.rs`). All waits are milliseconds, real time.

use super::*;
use std::collections::{HashSet, VecDeque};
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

use crate::av_gate::RigEvent;
use rs_core::db::{create_memory_pool, run_migrations};
use rs_youtube::manage::ManageCredentials;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

pub(crate) const EVENT: i64 = 9278;

// ---- scripted rig -------------------------------------------------------------

pub(crate) struct FakeRig {
    pub calls: StdMutex<Vec<String>>,
    pub resolve: StdMutex<Result<RigEvent, String>>,
    pub start: StdMutex<Result<(), StartEventError>>,
    /// Popped per call; the last entry repeats.
    pub deliveries: StdMutex<VecDeque<Result<RigDelivery, String>>>,
    pub stop: StdMutex<Result<(), String>>,
    pub servers: StdMutex<VecDeque<Result<usize, String>>>,
    pub panic_on_delivery: AtomicBool,
    pub panic_on_start: AtomicBool,
    /// What `event_active` answers.
    pub active: AtomicBool,
    /// When set, `server_count` panics once (then clears itself).
    pub panic_on_servers: AtomicBool,
    /// A FAILING stop still deactivates the event (the real `stop_stream`
    /// deactivates before the delivery stop that can fail).
    pub stop_deactivates: AtomicBool,
    /// When set, `resolve_event` waits for a notification first.
    pub resolve_gate: StdMutex<Option<Arc<tokio::sync::Notify>>>,
    /// When set, `start_event` waits for a notification first.
    pub start_gate: StdMutex<Option<Arc<tokio::sync::Notify>>>,
}

impl Default for FakeRig {
    fn default() -> Self {
        Self {
            calls: StdMutex::new(Vec::new()),
            resolve: StdMutex::new(Ok(RigEvent {
                id: EVENT,
                drain: Duration::ZERO,
            })),
            start: StdMutex::new(Ok(())),
            deliveries: StdMutex::new(VecDeque::from([Ok(RigDelivery::Delivering)])),
            stop: StdMutex::new(Ok(())),
            servers: StdMutex::new(VecDeque::from([Ok(0)])),
            panic_on_delivery: AtomicBool::new(false),
            panic_on_start: AtomicBool::new(false),
            active: AtomicBool::new(false),
            panic_on_servers: AtomicBool::new(false),
            stop_deactivates: AtomicBool::new(false),
            resolve_gate: StdMutex::new(None),
            start_gate: StdMutex::new(None),
        }
    }
}

fn next<T: Clone>(q: &StdMutex<VecDeque<T>>) -> T {
    let mut q = q.lock().unwrap();
    if q.len() > 1 {
        q.pop_front().unwrap()
    } else {
        q.front().cloned().expect("script must not be empty")
    }
}

impl FakeRig {
    pub fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }
    pub fn called(&self, what: &str) -> bool {
        self.calls().iter().any(|c| c == what)
    }
    fn log(&self, what: String) {
        self.calls.lock().unwrap().push(what);
    }
}

#[async_trait::async_trait]
impl AvGateRig for FakeRig {
    async fn resolve_event(&self, name: &str) -> Result<RigEvent, String> {
        self.log(format!("resolve:{name}"));
        let gate = self.resolve_gate.lock().unwrap().clone();
        if let Some(gate) = gate {
            gate.notified().await;
        }
        self.resolve.lock().unwrap().clone()
    }
    async fn start_event(&self, event_id: i64) -> Result<(), StartEventError> {
        self.log(format!("start:{event_id}"));
        let gate = self.start_gate.lock().unwrap().clone();
        if let Some(gate) = gate {
            gate.notified().await;
        }
        assert!(
            !self.panic_on_start.load(Ordering::SeqCst),
            "scripted start panic"
        );
        let result = self.start.lock().unwrap().clone();
        // Like `start_stream`: anything but a refusal leaves the event active.
        if !matches!(result, Err(StartEventError::Refused(_))) {
            self.active.store(true, Ordering::SeqCst);
        }
        result
    }
    async fn delivery(&self, event_id: i64) -> Result<RigDelivery, String> {
        self.log(format!("delivery:{event_id}"));
        assert!(
            !self.panic_on_delivery.load(Ordering::SeqCst),
            "scripted driver panic"
        );
        next(&self.deliveries)
    }
    async fn stop_event(&self, event_id: i64) -> Result<(), String> {
        self.log(format!("stop:{event_id}"));
        let result = self.stop.lock().unwrap().clone();
        if result.is_ok() || self.stop_deactivates.load(Ordering::SeqCst) {
            self.active.store(false, Ordering::SeqCst);
        }
        result
    }
    async fn event_active(&self, event_id: i64) -> Result<bool, String> {
        self.log(format!("active:{event_id}"));
        Ok(self.active.load(Ordering::SeqCst))
    }
    async fn server_count(&self, event_id: i64) -> Result<usize, String> {
        self.log(format!("servers:{event_id}"));
        assert!(
            !self.panic_on_servers.swap(false, Ordering::SeqCst),
            "scripted maintenance panic"
        );
        next(&self.servers)
    }
}

// ---- fake YouTube -----------------------------------------------------------

/// What the fake YouTube currently answers. Transitions update `life`.
pub(crate) struct YtState {
    pub stream_title: StdMutex<String>,
    pub reusable: AtomicBool,
    /// Popped per `liveStreams?id=` call; the last entry repeats.
    pub stream_status: StdMutex<VecDeque<String>>,
    pub life: StdMutex<String>,
    /// The life cycle a successful `transition live` lands in.
    pub live_lands_as: StdMutex<String>,
    pub video: StdMutex<String>,
    /// `status.uploadStatus` of the VOD.
    pub upload: StdMutex<String>,
    /// Calls that answer an error: lookup, insert, bind, stream, live,
    /// complete, life, video.
    pub fail: StdMutex<HashSet<&'static str>>,
    pub transitions: StdMutex<Vec<String>>,
    pub inserts: AtomicU32,
    pub video_polls: AtomicU32,
}

impl Default for YtState {
    fn default() -> Self {
        Self {
            stream_title: StdMutex::new("e2e rtmp".to_string()),
            reusable: AtomicBool::new(true),
            stream_status: StdMutex::new(VecDeque::from(["active".to_string()])),
            life: StdMutex::new("ready".to_string()),
            live_lands_as: StdMutex::new("live".to_string()),
            video: StdMutex::new("succeeded".to_string()),
            upload: StdMutex::new("uploaded".to_string()),
            fail: StdMutex::new(HashSet::new()),
            transitions: StdMutex::new(Vec::new()),
            inserts: AtomicU32::new(0),
            video_polls: AtomicU32::new(0),
        }
    }
}

impl YtState {
    pub fn failing(&self, what: &'static str) {
        self.fail.lock().unwrap().insert(what);
    }
    pub fn healed(&self, what: &'static str) {
        self.fail.lock().unwrap().remove(what);
    }
    fn fails(&self, what: &str) -> bool {
        self.fail.lock().unwrap().contains(what)
    }
    pub fn transitions(&self) -> Vec<String> {
        self.transitions.lock().unwrap().clone()
    }
}

fn api_error(reason: &str) -> ResponseTemplate {
    ResponseTemplate::new(500).set_body_json(json!({
        "error": {"message": "scripted", "errors": [{"reason": reason}]}
    }))
}

fn query(req: &Request, key: &str) -> String {
    req.url
        .query_pairs()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.into_owned())
        .unwrap_or_default()
}

pub(crate) async fn fake_youtube(state: Arc<YtState>) -> MockServer {
    let s = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"access_token": "AT", "expires_in": 3600})),
        )
        .mount(&s)
        .await;
    let st = Arc::clone(&state);
    Mock::given(method("GET"))
        .and(path("/yt/liveStreams"))
        .and(query_param("mine", "true"))
        .respond_with(move |_: &Request| {
            if st.fails("lookup") {
                return api_error("backendError");
            }
            ResponseTemplate::new(200).set_body_json(json!({"items": [{
                "id": "st-e2e",
                "snippet": {"title": st.stream_title.lock().unwrap().clone()},
                "contentDetails": {"isReusable": st.reusable.load(Ordering::SeqCst)},
                "status": {"streamStatus": "inactive"}
            }]}))
        })
        .mount(&s)
        .await;
    let st = Arc::clone(&state);
    Mock::given(method("GET"))
        .and(path("/yt/liveStreams"))
        .respond_with(move |_: &Request| {
            if st.fails("stream") {
                return api_error("backendError");
            }
            let status = next(&st.stream_status);
            ResponseTemplate::new(200)
                .set_body_json(json!({"items": [{"status": {"streamStatus": status}}]}))
        })
        .mount(&s)
        .await;
    let st = Arc::clone(&state);
    Mock::given(method("POST"))
        .and(path("/yt/liveBroadcasts"))
        .respond_with(move |_: &Request| {
            st.inserts.fetch_add(1, Ordering::SeqCst);
            if st.fails("insert") {
                return api_error("backendError");
            }
            ResponseTemplate::new(200).set_body_json(json!({"id": "bc-1"}))
        })
        .mount(&s)
        .await;
    let st = Arc::clone(&state);
    Mock::given(method("POST"))
        .and(path("/yt/liveBroadcasts/bind"))
        .respond_with(move |_: &Request| {
            if st.fails("bind") {
                return api_error("backendError");
            }
            ResponseTemplate::new(200).set_body_json(json!({"id": "bc-1"}))
        })
        .mount(&s)
        .await;
    let st = Arc::clone(&state);
    Mock::given(method("POST"))
        .and(path("/yt/liveBroadcasts/transition"))
        .respond_with(move |req: &Request| {
            let to = query(req, "broadcastStatus");
            st.transitions.lock().unwrap().push(to.clone());
            let key = if to == "live" { "live" } else { "complete" };
            if st.fails(key) {
                return api_error("invalidTransition");
            }
            let lands = if to == "live" {
                st.live_lands_as.lock().unwrap().clone()
            } else {
                to
            };
            *st.life.lock().unwrap() = lands;
            ResponseTemplate::new(200).set_body_json(json!({}))
        })
        .mount(&s)
        .await;
    let st = Arc::clone(&state);
    Mock::given(method("GET"))
        .and(path("/yt/liveBroadcasts"))
        .respond_with(move |_: &Request| {
            if st.fails("life") {
                return api_error("backendError");
            }
            let life = st.life.lock().unwrap().clone();
            ResponseTemplate::new(200)
                .set_body_json(json!({"items": [{"status": {"lifeCycleStatus": life}}]}))
        })
        .mount(&s)
        .await;
    let st = Arc::clone(&state);
    Mock::given(method("GET"))
        .and(path("/yt/videos"))
        .respond_with(move |_: &Request| {
            if st.fails("video") {
                return api_error("backendError");
            }
            st.video_polls.fetch_add(1, Ordering::SeqCst);
            let video = st.video.lock().unwrap().clone();
            let upload = st.upload.lock().unwrap().clone();
            ResponseTemplate::new(200).set_body_json(
                json!({"items": [{"processingDetails": {"processingStatus": video},
                                  "status": {"uploadStatus": upload}}]}),
            )
        })
        .mount(&s)
        .await;
    s
}

pub(crate) fn oauth_file(dir: &tempfile::TempDir) -> std::path::PathBuf {
    let p = dir.path().join("oauth.json");
    std::fs::write(
        &p,
        r#"{"refresh_token":"rt-fake","scope":"https://www.googleapis.com/auth/youtube"}"#,
    )
    .unwrap();
    p
}

pub(crate) fn yt_client(server: &MockServer, dir: &tempfile::TempDir) -> Arc<ManageClient> {
    let creds = ManageCredentials::from_oauth_file(&oauth_file(dir), "cid", "cs-fake").unwrap();
    Arc::new(ManageClient::with_endpoints(
        creds,
        &format!("{}/yt", server.uri()),
        &format!("{}/token", server.uri()),
    ))
}

pub(crate) fn timings() -> AvGateTimings {
    AvGateTimings {
        poll: Duration::from_millis(5),
        drain_extra: Duration::ZERO,
        idle_timeout: Duration::from_secs(10),
        servers_gone_timeout: Duration::from_millis(150),
        processing_poll: Duration::from_millis(5),
        processing_timeout: Duration::from_secs(3),
        cleanup_retry: Duration::from_millis(20),
    }
}

/// Everything one test needs.
pub(crate) struct Harness {
    pub ctx: Arc<SessionCtx>,
    pub rig: Arc<FakeRig>,
    pub yt_state: Arc<YtState>,
    pub server: MockServer,
    pub dir: tempfile::TempDir,
    pub audit_rx: mpsc::Receiver<AuditRow>,
}

impl Harness {
    pub async fn new() -> Self {
        Self::with(FakeRig::default(), timings()).await
    }

    pub async fn with(rig: FakeRig, timings: AvGateTimings) -> Self {
        let pool = create_memory_pool().await.unwrap();
        run_migrations(&pool).await.unwrap();
        let (audit_tx, audit_rx) = mpsc::channel(1024);
        let rig = Arc::new(rig);
        let yt_state = Arc::new(YtState::default());
        let server = fake_youtube(Arc::clone(&yt_state)).await;
        let ctx = Arc::new(SessionCtx {
            pool,
            audit_tx,
            registry: {
                let r = AvGateRegistry::default();
                r.mark_reconciled();
                Arc::new(r)
            },
            rig: rig.clone(),
            timings,
            event_name: "E2E-Test".to_string(),
            stream_title: "e2e rtmp".to_string(),
            daily_quota_budget: 4_000,
            quota_bucket: None,
        });
        Self {
            ctx,
            rig,
            yt_state,
            server,
            dir: tempfile::tempdir().unwrap(),
            audit_rx,
        }
    }

    /// Another context over the same DB, rig and audit channel, with its own
    /// registry (not reconciled) and these timings.
    pub fn fresh_ctx(&self, timings: AvGateTimings) -> Arc<SessionCtx> {
        Arc::new(SessionCtx {
            pool: self.ctx.pool.clone(),
            audit_tx: self.ctx.audit_tx.clone(),
            registry: Arc::new(AvGateRegistry::default()),
            rig: self.rig.clone(),
            timings,
            event_name: "E2E-Test".to_string(),
            stream_title: "e2e rtmp".to_string(),
            daily_quota_budget: 4_000,
            quota_bucket: None,
        })
    }

    pub fn yt(&self) -> Arc<ManageClient> {
        yt_client(&self.server, &self.dir)
    }

    /// A factory that builds a fresh client per call (as the boot reconcile
    /// and the cleanup retries do).
    pub fn clients(&self) -> ClientFactory {
        let api = format!("{}/yt", self.server.uri());
        let token = format!("{}/token", self.server.uri());
        let oauth = oauth_file(&self.dir);
        Arc::new(move || {
            let creds = ManageCredentials::from_oauth_file(&oauth, "cid", "cs-fake")
                .map_err(|e| e.to_string())?;
            Ok(ManageClient::with_endpoints(creds, &api, &token))
        })
    }

    pub async fn create(&self, id: &str) -> CreateOutcome {
        create_session(
            Arc::clone(&self.ctx),
            self.yt(),
            id.to_string(),
            "camera-box".to_string(),
            "gate title".to_string(),
        )
        .await
    }

    pub async fn row(&self, id: &str) -> AvGateSessionRow {
        store::get(&self.ctx.pool, id).await.unwrap().expect("row")
    }

    /// Wait (bounded) until the session reaches `state`.
    pub async fn wait_state(&self, id: &str, state: SessionState) -> AvGateSessionRow {
        let wait = async {
            loop {
                let row = self.row(id).await;
                if row.state == state.as_str() {
                    return row;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        };
        match tokio::time::timeout(Duration::from_secs(8), wait).await {
            Ok(row) => row,
            Err(_) => panic!(
                "session {id} never reached {}: {:?}",
                state.as_str(),
                self.row(id).await
            ),
        }
    }

    /// The audit actions emitted so far, in order.
    pub fn actions(&mut self) -> Vec<Action> {
        let mut out = Vec::new();
        while let Ok(row) = self.audit_rx.try_recv() {
            assert!(row.detail["session_id"].is_string(), "{:?}", row.detail);
            out.push(row.action);
        }
        out
    }

    pub async fn ready_session(&self, id: &str) {
        assert!(matches!(
            self.create(id).await,
            CreateOutcome::Created { .. }
        ));
        self.wait_state(id, SessionState::Ready).await;
    }
}

pub(crate) fn reason(row: &AvGateSessionRow) -> String {
    row.reason.clone().unwrap_or_default()
}

// ---- pure helpers -------------------------------------------------------------

#[test]
fn exhausted_fires_at_the_limit() {
    assert!(!exhausted(4, 5));
    assert!(exhausted(5, 5));
    assert!(exhausted(6, 5));
}

#[test]
fn life_action_classifies_every_life_cycle() {
    assert_eq!(life_action("live"), LifeAction::Complete);
    assert_eq!(life_action("testing"), LifeAction::Complete);
    assert_eq!(life_action("liveStarting"), LifeAction::Wait);
    assert_eq!(life_action("testStarting"), LifeAction::Wait);
    for s in ["created", "ready", "complete", "revoked", ""] {
        assert_eq!(life_action(s), LifeAction::Nothing, "{s}");
    }
}

fn vod(processing: Option<&str>, upload: Option<&str>) -> VodStatus {
    VodStatus {
        processing: processing.map(str::to_string),
        upload: upload.map(str::to_string),
    }
}

#[test]
fn vod_step_reads_both_processing_and_upload_status() {
    assert_eq!(vod_step(&vod(Some("succeeded"), None)), VodStep::Done);
    assert_eq!(
        vod_step(&vod(Some("processing"), Some("processed"))),
        VodStep::Done
    );
    for (p, u, word) in [
        (Some("failed"), None, "(failed)"),
        (Some("terminated"), Some("uploaded"), "(terminated)"),
        (None, Some("failed"), "(failed)"),
        (Some("processing"), Some("rejected"), "(rejected)"),
        (None, Some("deleted"), "(deleted)"),
    ] {
        assert!(
            matches!(vod_step(&vod(p, u)), VodStep::Failed(r) if r.contains(word)),
            "{p:?} {u:?}"
        );
    }
    assert_eq!(
        vod_step(&vod(Some("processing"), Some("uploaded"))),
        VodStep::Pending
    );
    assert_eq!(vod_step(&vod(None, None)), VodStep::Pending);
}

#[test]
fn timestamps_are_fixed_width_utc_millis() {
    let ts = now_ts();
    assert_eq!(ts.len(), 24, "{ts}");
    assert!(ts.ends_with('Z'), "{ts}");
    let start = chrono::DateTime::parse_from_rfc3339(&quota_window_start()).unwrap();
    let age = Utc::now().signed_duration_since(start);
    assert!(
        (age - chrono::Duration::hours(24)).num_seconds().abs() <= 5,
        "{age}"
    );
}

// ---- the happy path -------------------------------------------------------------

#[tokio::test]
async fn a_session_goes_starting_ready_processing_done_and_cleans_up() {
    let mut h = Harness::new().await;
    let created = h.create("s-happy").await;
    assert_eq!(
        created,
        CreateOutcome::Created {
            session_id: "s-happy".to_string(),
            broadcast_id: "bc-1".to_string(),
        }
    );
    let starting = h.row("s-happy").await;
    assert_eq!(starting.stream_id.as_deref(), Some("st-e2e"));
    assert_eq!(starting.event_id, Some(EVENT));
    assert_eq!(starting.title, "gate title");
    assert_eq!(starting.requester, "camera-box");

    let ready = h.wait_state("s-happy", SessionState::Ready).await;
    assert!(ready.went_live);
    assert!(ready.ready_at.is_some());
    assert_eq!(
        h.ctx.registry.holder().map(|h| h.session_id).as_deref(),
        Some("s-happy")
    );

    assert!(h.ctx.registry.request_stop("s-happy"));
    let done = h.wait_state("s-happy", SessionState::Done).await;
    assert_eq!(done.vod_id.as_deref(), Some("bc-1"));
    assert!(done.stop_requested_at.is_some());
    assert!(done.processing_at.is_some());
    assert!(done.finished_at.is_some());
    assert_eq!(done.reason, None);
    // lookup 1 + insert 50 + bind 50; poll 1: life 1 + stream 1 + live 50;
    // poll 2: life 1; the teardown's life 1 + complete 50; one VOD poll 1.
    assert_eq!(done.quota_units, 206);
    assert_eq!(h.yt_state.transitions(), vec!["live", "complete"]);
    assert_eq!(h.ctx.registry.holder(), None);
    let calls = h.rig.calls();
    assert_eq!(calls[0], "resolve:E2E-Test");
    assert_eq!(calls[1], format!("start:{EVENT}"));
    assert!(h.rig.called(&format!("stop:{EVENT}")));
    assert_eq!(calls.last().unwrap(), &format!("servers:{EVENT}"));
    assert_eq!(
        h.actions(),
        vec![
            Action::AvGateSessionStarted,
            Action::AvGateSessionReady,
            Action::AvGateSessionStopRequested,
            Action::AvGateSessionProcessing,
            Action::AvGateSessionDone,
        ]
    );
}

#[tokio::test]
async fn readiness_waits_through_booting_and_an_inactive_stream() {
    let rig = FakeRig::default();
    *rig.deliveries.lock().unwrap() = VecDeque::from([
        Ok(RigDelivery::Booting),
        Ok(RigDelivery::Booting),
        Ok(RigDelivery::Delivering),
    ]);
    let h = Harness::with(rig, timings()).await;
    *h.yt_state.stream_status.lock().unwrap() =
        VecDeque::from(["inactive".to_string(), "active".to_string()]);
    h.ready_session("s-slow").await;
    let deliveries = h
        .rig
        .calls()
        .iter()
        .filter(|c| c.starts_with("delivery:"))
        .count();
    assert!(deliveries >= 4, "{deliveries}");
    assert_eq!(h.yt_state.transitions(), vec!["live"]);
}

#[tokio::test]
async fn a_transition_still_starting_is_not_ready_yet() {
    let h = Harness::new().await;
    *h.yt_state.live_lands_as.lock().unwrap() = "liveStarting".to_string();
    assert!(matches!(
        h.create("s-ls").await,
        CreateOutcome::Created { .. }
    ));
    let wait_went_live = async {
        while !h.row("s-ls").await.went_live {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(5), wait_went_live)
        .await
        .unwrap();
    // Several more polls see `liveStarting`; the session must stay starting.
    tokio::time::sleep(Duration::from_millis(40)).await;
    assert_eq!(h.row("s-ls").await.state, "starting");
    *h.yt_state.life.lock().unwrap() = "live".to_string();
    h.wait_state("s-ls", SessionState::Ready).await;
    assert_eq!(h.yt_state.transitions(), vec!["live"], "live is sent once");
}

// ---- readiness failures --------------------------------------------------------

#[tokio::test]
async fn a_delivery_that_is_not_running_fails_the_session() {
    let rig = FakeRig::default();
    *rig.deliveries.lock().unwrap() = VecDeque::from([Ok(RigDelivery::NotRunning)]);
    let h = Harness::with(rig, timings()).await;
    assert!(matches!(
        h.create("s1").await,
        CreateOutcome::Created { .. }
    ));
    let row = h.wait_state("s1", SessionState::Failed).await;
    assert!(reason(&row).contains("not running"), "{row:?}");
    assert!(h.rig.called(&format!("stop:{EVENT}")));
    assert!(h.rig.called(&format!("servers:{EVENT}")));
    assert_eq!(h.ctx.registry.holder(), None);
}

#[tokio::test]
async fn transition_live_is_retried_then_fails_the_session() {
    let h = Harness::new().await;
    h.yt_state.failing("live");
    assert!(matches!(
        h.create("s1").await,
        CreateOutcome::Created { .. }
    ));
    let row = h.wait_state("s1", SessionState::Failed).await;
    assert!(
        reason(&row).contains("transition to live failed"),
        "{row:?}"
    );
    assert!(row.went_live, "an attempted live transition counts");
    assert_eq!(h.yt_state.transitions(), vec!["live", "live", "live"]);
    assert!(h.rig.called(&format!("stop:{EVENT}")));
}

#[tokio::test]
async fn a_live_transition_that_recovers_makes_the_session_ready() {
    let h = Harness::with(
        FakeRig::default(),
        AvGateTimings {
            poll: Duration::from_millis(40),
            ..timings()
        },
    )
    .await;
    h.yt_state.failing("live");
    assert!(matches!(
        h.create("s1").await,
        CreateOutcome::Created { .. }
    ));
    let first = async {
        while h.yt_state.transitions().is_empty() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(5), first)
        .await
        .unwrap();
    h.yt_state.healed("live");
    h.wait_state("s1", SessionState::Ready).await;
}

#[tokio::test]
async fn repeated_poll_errors_fail_the_session() {
    let rig = FakeRig::default();
    *rig.deliveries.lock().unwrap() = VecDeque::from([Err("db locked".to_string())]);
    let h = Harness::with(rig, timings()).await;
    assert!(matches!(
        h.create("s1").await,
        CreateOutcome::Created { .. }
    ));
    let row = h.wait_state("s1", SessionState::Failed).await;
    assert!(
        reason(&row).contains("readiness polling failed 5x"),
        "{row:?}"
    );
    let polls = h
        .rig
        .calls()
        .iter()
        .filter(|c| c.starts_with("delivery:"))
        .count();
    assert_eq!(polls, 5);
}

#[tokio::test]
async fn a_single_poll_error_is_forgiven() {
    let rig = FakeRig::default();
    *rig.deliveries.lock().unwrap() = VecDeque::from([
        Err("blip".to_string()),
        Err("blip".to_string()),
        Err("blip".to_string()),
        Err("blip".to_string()),
        Ok(RigDelivery::Booting),
        Err("blip".to_string()),
        Err("blip".to_string()),
        Err("blip".to_string()),
        Err("blip".to_string()),
        Ok(RigDelivery::Delivering),
    ]);
    let h = Harness::with(rig, timings()).await;
    h.ready_session("s1").await;
}

#[tokio::test]
async fn a_stop_before_ready_fails_and_cleans_up() {
    let rig = FakeRig::default();
    *rig.deliveries.lock().unwrap() = VecDeque::from([Ok(RigDelivery::Booting)]);
    let mut h = Harness::with(rig, timings()).await;
    assert!(matches!(
        h.create("s1").await,
        CreateOutcome::Created { .. }
    ));
    assert!(h.ctx.registry.request_stop("s1"));
    let row = h.wait_state("s1", SessionState::Failed).await;
    assert_eq!(reason(&row), "stopped before the session was ready");
    assert!(h.rig.called(&format!("stop:{EVENT}")));
    assert_eq!(
        h.actions(),
        vec![Action::AvGateSessionStarted, Action::AvGateSessionFailed]
    );
}

#[path = "av_gate_driver_cleanup_tests.rs"]
mod cleanup_tests;
#[tokio::test]
async fn live_is_never_sent_before_went_live_is_durable() {
    let rig = FakeRig::default();
    *rig.deliveries.lock().unwrap() = VecDeque::from([Ok(RigDelivery::Booting)]);
    let h = Harness::with(rig, timings()).await;
    assert!(matches!(
        h.create("s1").await,
        CreateOutcome::Created { .. }
    ));
    sqlx::query("ALTER TABLE av_gate_sessions RENAME TO av_gate_sessions_away")
        .execute(&h.ctx.pool)
        .await
        .unwrap();
    *h.rig.deliveries.lock().unwrap() = VecDeque::from([Ok(RigDelivery::Delivering)]);
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert!(
        h.yt_state.transitions().is_empty(),
        "the went_live save failed, so live must not be sent"
    );
    sqlx::query("ALTER TABLE av_gate_sessions_away RENAME TO av_gate_sessions")
        .execute(&h.ctx.pool)
        .await
        .unwrap();
    let row = h.wait_state("s1", SessionState::Ready).await;
    assert!(row.went_live);
    assert_eq!(h.yt_state.transitions(), vec!["live"]);
}

#[path = "av_gate_driver_create_tests.rs"]
mod create_tests;
#[path = "av_gate_driver_stop_tests.rs"]
mod stop_tests;
