//! One submitted step job: admission through the capacity gate, the coordinator's start authorization, the run itself,
//! cancellation and the result. The states and reasons match the Python service, so coordinators behave the same.
use crate::artifacts;
use crate::capabilities;
use crate::gate::Acquire;
use crate::procutil::Tree;
use crate::program::{run_program, Emit, Emitter, ProgramRun};
use crate::service::App;
use crate::util::{atomic_json, eq_const, mono, random_hex, read_json};
use serde_json::{json, Value};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{oneshot, watch, Notify};

const AUTHORIZATION_TIMEOUT: Duration = Duration::from_secs(10);
const COORDINATOR_STALE: f64 = 10.0;
const MAX_EVENTS: usize = 5000;

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum JobState {
    Queued,
    Running,
    Done,
    Failed,
    Cancelled,
    Rejected,
}

impl JobState {
    pub fn as_str(&self) -> &'static str {
        match self {
            JobState::Queued => "queued",
            JobState::Running => "running",
            JobState::Done => "done",
            JobState::Failed => "failed",
            JobState::Cancelled => "cancelled",
            JobState::Rejected => "rejected",
        }
    }
}

/// Why `execute` ended before the job completed.
enum Stop {
    /// The state, error and reason were already set (a rejection).
    Ended,
    Failed(String),
}

struct Inner {
    state: JobState,
    error: Option<String>,
    reason: Option<String>,
    waiting_reason: Option<String>,
    events: Vec<Value>,
    dropped: usize,
    output: Option<Value>,
    nonce: Option<String>,
    confirm: Option<oneshot::Sender<()>>,
    started: bool,
    result_path: Option<std::path::PathBuf>,
    lease_loss: Option<String>,
    finished: bool,
}

pub struct Job {
    app: Arc<App>,
    pub id: String,
    authorize: bool,
    config: Mutex<Value>,
    inner: Mutex<Inner>,
    last_seen: Mutex<f64>,
    cancel: watch::Sender<bool>,
    done: Notify,
    tree: Arc<Mutex<Option<Tree>>>,
}

struct ActiveGuard(Arc<App>);
impl Drop for ActiveGuard {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Job {
    pub fn new(app: Arc<App>, id: String, config: Value) -> Arc<Job> {
        let authorize = config.get("authorize").and_then(|a| a.as_bool()).unwrap_or(false);
        Arc::new(Job {
            app,
            id,
            authorize,
            config: Mutex::new(config),
            inner: Mutex::new(Inner {
                state: JobState::Queued, error: None, reason: None, waiting_reason: None, events: vec![], dropped: 0, output: None, nonce: None, confirm: None,
                started: false, result_path: None, lease_loss: None, finished: false,
            }),
            last_seen: Mutex::new(mono()),
            cancel: watch::channel(false).0,
            done: Notify::new(),
            tree: Arc::new(Mutex::new(None)),
        })
    }

    pub fn spawn(self: &Arc<Self>) {
        let job = self.clone();
        tokio::spawn(async move { job.run().await });
    }

    pub fn touch(&self) {
        *self.last_seen.lock().unwrap() = mono();
    }

    pub fn last_seen(&self) -> f64 {
        *self.last_seen.lock().unwrap()
    }

    pub fn state(&self) -> JobState {
        self.inner.lock().unwrap().state
    }

    pub fn finished(&self) -> bool {
        self.inner.lock().unwrap().finished
    }

    pub fn input(&self, _item: Value) {}

    pub fn set_state_error(&self, state: JobState, error: &str) {
        let mut i = self.inner.lock().unwrap();
        i.state = state;
        i.error = Some(error.to_string());
    }

    pub fn mark_cancelled_if_active(&self) {
        let mut i = self.inner.lock().unwrap();
        if matches!(i.state, JobState::Queued | JobState::Running) {
            i.state = JobState::Cancelled;
            i.error = Some("Execution cancelled".into());
        }
    }

    /// Confirm the pending start, if `nonce` is the one this admission is waiting for. One nonce per admission:
    /// a late confirmation can never authorize a later one.
    pub fn authorize_with(&self, nonce: Option<&str>) -> bool {
        let mut i = self.inner.lock().unwrap();
        match (nonce, i.nonce.as_deref()) {
            (Some(given), Some(expected)) if eq_const(given, expected) => {
                if let Some(confirm) = i.confirm.take() {
                    let _ = confirm.send(());
                }
                true
            }
            _ => false,
        }
    }

    pub fn push_event(&self, event: Value) {
        let mut i = self.inner.lock().unwrap();
        i.events.push(event);
        if i.events.len() > MAX_EVENTS {
            i.events.remove(0);
            i.dropped += 1;
        }
    }

    pub fn snapshot(&self, after: usize) -> Value {
        let i = self.inner.lock().unwrap();
        let from = after.saturating_sub(i.dropped).min(i.events.len());
        let mut out = json!({
            "state": i.state.as_str(), "events": i.events[from..], "cursor": i.dropped + i.events.len(), "error": i.error,
            "worker": if i.started { self.app.identity.clone() } else { Value::Null }, "waiting_reason": i.waiting_reason,
            "authorization": i.nonce, "reason": i.reason,
        });
        if i.state == JobState::Done {
            out["output"] = i.output.clone().unwrap_or(Value::Null);
            out["files"] = i.result_path.as_ref().and_then(|p| read_json(p)).unwrap_or(json!({}));
        }
        out
    }

    pub fn request_cancel(&self) {
        let _ = self.cancel.send(true);
    }

    /// Cancel and wait until the job's task and processes are gone. False if that did not happen in time.
    pub async fn cancel_and_wait(&self, timeout: Duration) -> bool {
        self.request_cancel();
        let waited = tokio::time::timeout(timeout, async {
            loop {
                let notified = self.done.notified();
                if self.finished() {
                    return;
                }
                notified.await;
            }
        })
        .await;
        waited.is_ok()
    }

    async fn cancelled(&self) {
        let mut rx = self.cancel.subscribe();
        while !*rx.borrow() {
            if rx.changed().await.is_err() {
                std::future::pending::<()>().await;
            }
        }
    }

    fn set_waiting(&self, reason: Option<&str>) {
        self.inner.lock().unwrap().waiting_reason = reason.map(String::from);
    }

    fn reject(&self, state: JobState, message: &str, reason: &str) {
        let mut i = self.inner.lock().unwrap();
        i.state = state;
        i.error = Some(message.to_string());
        i.reason = Some(reason.to_string());
    }

    fn end_state(&self) -> JobState {
        if self.authorize { JobState::Rejected } else { JobState::Failed }
    }

    async fn run(self: Arc<Self>) {
        let lease: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let outcome = tokio::select! {
            r = self.clone().execute(lease.clone()) => Some(r),
            _ = self.cancelled() => None,
        };
        // Cleanup runs however the job ended: stop its processes, then release and revoke its capacity.
        if let Some(tree) = self.tree.lock().unwrap().take() {
            tree.kill();
        }
        if lease.lock().unwrap().take().is_some() {
            if let Err(e) = self.app.gate.finish(&self.id, Some(&self.app.gate_id)) {
                crate::log!("service", "job {}: {e}", self.id);
            }
        }
        let cancelled = outcome.is_none();
        match outcome {
            Some(Ok(())) => {
                let mut i = self.inner.lock().unwrap();
                i.state = JobState::Done;
            }
            Some(Err(Stop::Failed(message))) => {
                let mut i = self.inner.lock().unwrap();
                i.state = JobState::Failed;
                i.error = Some(message.chars().rev().take(8000).collect::<Vec<_>>().into_iter().rev().collect());
            }
            Some(Err(Stop::Ended)) => {}
            None => {
                let mut i = self.inner.lock().unwrap();
                i.error = Some(i.lease_loss.clone().unwrap_or_else(|| "Execution cancelled".into()));
                i.state = JobState::Cancelled;
            }
        }
        if let Err(e) = self.app.record_termination(&self.id) {
            if !cancelled {
                self.set_state_error(JobState::Failed, &e);
            }
        }
        self.touch();
        self.inner.lock().unwrap().finished = true;
        self.done.notify_waiters();
    }

    async fn missing_software(&self, requirement: &Value) -> Option<String> {
        for kind in ["agent", "command"] {
            let Some(name) = requirement.get(kind).and_then(|n| n.as_str()) else { continue };
            let (k, n) = (kind.to_string(), name.to_string());
            let result = tokio::task::spawn_blocking(move || capabilities::probe(&k, &n)).await.ok()?;
            let status = result["status"].as_str().unwrap_or("unknown").to_string();
            if status != "installed" {
                if kind == "command" {
                    self.app.refresh_inventory(&[name.to_string()]).await;
                }
                return Some(format!("Required software '{name}' is {status} on this worker"));
            }
        }
        None
    }

    async fn execute(self: Arc<Self>, lease_slot: Arc<Mutex<Option<String>>>) -> Result<(), Stop> {
        let (files, assets, start, requirement, gate_id, revision) = {
            let c = self.config.lock().unwrap();
            (c["files"].clone(), c["assets"].clone(), c["start"].clone(), c["requirement"].clone(), c.get("gate_id").and_then(|g| g.as_str()).map(String::from), c.get("settings_revision").and_then(|r| r.as_i64()))
        };
        let run_id = start["runId"].as_str().unwrap_or("").to_string();
        let directory = self.app.root.join(&self.id);
        let (workspaces, assets_dir) = (directory.join("workspaces"), directory.join("assets"));
        let workspace = workspaces.join(&run_id);
        {
            let (w, a, f, af) = (workspace.clone(), assets_dir.clone(), files.clone(), assets.clone());
            tokio::task::spawn_blocking(move || {
                artifacts::restore(&f, &w)?;
                artifacts::restore(&af, &a)
            })
            .await
            .map_err(|e| Stop::Failed(e.to_string()))?
            .map_err(Stop::Failed)?;
        }
        let mut fence;
        loop {
            if self.app.draining.load(Ordering::SeqCst) {
                // An update is waiting for running work to finish: never admit more, so the coordinator places it elsewhere.
                if lease_slot.lock().unwrap().take().is_some() {
                    self.app.gate.release(&self.id);
                }
                self.reject(self.end_state(), "This PC is updating", "updating");
                return Err(Stop::Ended);
            }
            if self.authorize && mono() - self.last_seen() > COORDINATOR_STALE {
                // Never take capacity for a coordinator that may have cancelled while unreachable.
                self.set_waiting(Some("coordinator_unreachable"));
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue;
            }
            match self.app.gate.try_acquire(&self.id, gate_id.as_deref(), revision) {
                Acquire::Refused(reason) => {
                    if ["revoked", "gate_changed", "settings_changed"].contains(&reason) {
                        self.reject(JobState::Rejected, "Job was revoked or worker settings changed before execution", reason);
                        return Err(Stop::Ended);
                    }
                    self.set_waiting(Some(reason));
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
                Acquire::Granted { fence: f, .. } => {
                    *lease_slot.lock().unwrap() = Some(f.clone());
                    fence = f;
                }
            }
            if self.authorize {
                // The coordinator must confirm, after admission, that this attempt is still wanted.
                let (tx, rx) = oneshot::channel();
                {
                    let mut i = self.inner.lock().unwrap();
                    i.nonce = Some(random_hex(16));
                    i.confirm = Some(tx);
                    i.waiting_reason = Some("awaiting_authorization".into());
                }
                let confirmed = tokio::time::timeout(AUTHORIZATION_TIMEOUT, rx).await;
                {
                    let mut i = self.inner.lock().unwrap(); // before any await, so no confirmation can still land
                    i.nonce = None;
                    i.confirm = None;
                }
                if !matches!(confirmed, Ok(Ok(()))) {
                    self.app.gate.release(&self.id);
                    *lease_slot.lock().unwrap() = None;
                    continue;
                }
            }
            if let Some(missing) = self.missing_software(&requirement).await {
                self.app.gate.release(&self.id);
                *lease_slot.lock().unwrap() = None;
                self.reject(self.end_state(), &missing, "software_missing");
                return Err(Stop::Ended);
            }
            break;
        }
        self.set_waiting(None);
        // Keep the lease alive; if it is lost the job is cancelled rather than running without capacity.
        let heartbeat = {
            let job = self.clone();
            let fence = fence.clone();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    if !job.app.gate.renew(&job.id, &fence) {
                        job.inner.lock().unwrap().lease_loss = Some("Capacity lease expired".into());
                        job.request_cancel();
                        return;
                    }
                }
            })
        };
        struct Abort(tokio::task::JoinHandle<()>);
        impl Drop for Abort {
            fn drop(&mut self) {
                self.0.abort();
            }
        }
        let _heartbeat = Abort(heartbeat);
        self.app.active.fetch_add(1, Ordering::SeqCst);
        let _active = ActiveGuard(self.app.clone());
        self.set_waiting(Some("starting"));
        self.app.gate.confirm_started(&self.id, &fence).map_err(Stop::Failed)?;
        {
            let mut i = self.inner.lock().unwrap();
            i.waiting_reason = None;
            i.state = JobState::Running;
            i.started = true;
        }
        self.push_event(json!({"type": "worker", "worker": self.app.identity.clone()}));

        let step_id = start["step"].as_str().unwrap_or("").to_string();
        let step = start["flow"]["steps"][&step_id].clone();
        let context = start["context"].clone();
        let job = self.clone();
        let sink: Emit = Arc::new(move |event| job.push_event(event));
        let emitter = Emitter::new(sink);
        let output = match step["kind"].as_str() {
            Some("program") => run_program(
                ProgramRun { step_id: &step_id, step: &step, context: &context, workspace: &workspace, assets: &assets_dir, workspaces: &workspaces, tree: self.tree.clone() },
                &emitter,
            )
            .await
            .map_err(Stop::Failed)?,
            Some("agent") => return Err(Stop::Failed("This worker does not run agent steps yet".into())),
            other => return Err(Stop::Failed(format!("Unknown step kind {other:?}"))),
        };
        self.inner.lock().unwrap().output = Some(output);
        let snapshot = {
            let (w, original) = (workspace.clone(), files.clone());
            tokio::task::spawn_blocking(move || artifacts::snapshot(&w, Some(&original))).await.map_err(|e| Stop::Failed(e.to_string()))?.map_err(Stop::Failed)?
        };
        let path = directory.join("result-files.json");
        let target = path.clone();
        let published = self.app.gate.publish(&self.id, &fence, move || atomic_json(&target, &snapshot));
        match published {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(Stop::Failed(e.to_string())),
            Err(e) => return Err(Stop::Failed(e)),
        }
        self.inner.lock().unwrap().result_path = Some(path);
        *self.config.lock().unwrap() = json!({}); // release the transferred file payloads
        Ok(())
    }
}
