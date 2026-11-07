//! A conversation with a coding agent, separate from the lifetime of one turn. The same state machine and events as
//! agents/agentdriver/session.py: starting, working, retrying, approval, question, awaiting_review, awaiting_input, stuck, done.
use crate::acp::AcpEngine;
use crate::procutil::Tree;
use crate::program::Emitter;
use crate::util::random_hex;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, watch, Notify};
use tokio::task::AbortHandle;

const WAITING: [&str; 3] = ["awaiting_review", "awaiting_input", "stuck"];
const MAX_ATTEMPTS: u64 = 3;
const CONTEXT_LIMIT: usize = 48000;

/// What the human sends to a running session. Shared so one job can run several sessions one after another.
pub type Inbox = Arc<tokio::sync::Mutex<mpsc::UnboundedReceiver<Value>>>;
/// Process trees the job must end when it is cancelled, whatever the session is doing at that moment.
pub type Trees = Arc<Mutex<Vec<Arc<Tree>>>>;

pub enum TurnError {
    /// The agent cannot accept this message; retrying or reconnecting would not help.
    Rejected(String),
    Failed(String),
}

struct Pending {
    kind: &'static str,
    options: Value,
    schema: Value,
    answer: oneshot::Sender<Option<Value>>,
}

pub struct Shared {
    pub config: Mutex<Value>,
    pub trees: Trees,
    emitter: Arc<Emitter>,
    state: Mutex<String>,
    history: Mutex<Vec<Value>>,
    pending: Mutex<HashMap<String, Pending>>,
    interrupted: AtomicBool,
    finishing: AtomicBool,
    messages: mpsc::UnboundedSender<Option<Value>>,
    engine: tokio::sync::Mutex<Option<Arc<Engine>>>,
    active: Mutex<Option<AbortHandle>>,
    idle: watch::Sender<bool>,
    interrupt_done: watch::Sender<bool>,
    cancel: Notify,
    connections: AtomicU32,
    failures: AtomicI64,
}

pub enum Engine {
    Acp(Arc<AcpEngine>),
    Demo(DemoEngine),
}

impl Engine {
    async fn cancel(&self) {
        match self {
            Engine::Acp(engine) => engine.cancel().await,
            Engine::Demo(engine) => engine.cancelled.store(true, Ordering::SeqCst),
        }
    }

    async fn close(&self) {
        match self {
            Engine::Acp(engine) => engine.close().await,
            Engine::Demo(engine) => engine.cancelled.store(true, Ordering::SeqCst),
        }
    }

    fn abort(&self) {
        if let Engine::Acp(engine) = self {
            engine.abort();
        }
    }

    fn resumed(&self) -> bool {
        matches!(self, Engine::Acp(engine) if engine.resumed.load(Ordering::SeqCst))
    }
}

fn message_text(message: &Value) -> String {
    message
        .as_str()
        .map(String::from)
        .unwrap_or_else(|| message["text"].as_str().unwrap_or("").to_string())
}

impl Shared {
    pub fn emit(&self, kind: &str, fields: Value) {
        if kind == "agent" {
            let text = fields["text"].as_str().unwrap_or("");
            let id = fields.get("message_id").cloned().unwrap_or(Value::Null);
            let mut history = self.history.lock().unwrap();
            let same = !id.is_null()
                && history
                    .last()
                    .map(|h| h["message_id"] == id)
                    .unwrap_or(false);
            if same {
                let last = history.last_mut().unwrap();
                last["text"] = json!(format!("{}{text}", last["text"].as_str().unwrap_or("")));
            } else {
                history.push(json!({"role": "agent", "text": text, "message_id": id}));
            }
        }
        self.emitter.emit(kind, fields);
    }

    pub fn status(&self, state: &str, mut fields: Value) {
        *self.state.lock().unwrap() = state.to_string();
        fields["state"] = json!(state);
        self.emit("status", fields);
    }

    fn state_is_waiting(&self) -> bool {
        WAITING.contains(&self.state.lock().unwrap().as_str())
    }

    pub fn config_str(&self, key: &str) -> String {
        self.config
            .lock()
            .unwrap()
            .get(key)
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string()
    }

    fn stopped(&self) -> bool {
        self.interrupted.load(Ordering::SeqCst) || self.finishing.load(Ordering::SeqCst)
    }

    /// Ask the human to allow something. The option they chose, or None when the turn was interrupted.
    pub async fn permission(
        &self,
        tool: &str,
        summary: &str,
        detail: &str,
        options: Value,
    ) -> Option<String> {
        if self.stopped() {
            return None;
        }
        let id = random_hex(16);
        let (answer, wait) = oneshot::channel();
        self.pending.lock().unwrap().insert(
            id.clone(),
            Pending {
                kind: "approval",
                options: options.clone(),
                schema: Value::Null,
                answer,
            },
        );
        self.emit("approval", json!({"id": id, "tool": tool, "summary": summary, "detail": detail, "options": options}));
        self.status("approval", json!({"reason": "Permission needed"}));
        let chosen = wait.await.ok().flatten();
        self.pending.lock().unwrap().remove(&id);
        chosen.and_then(|v| v.as_str().map(String::from))
    }

    /// Ask the human a question with a form. Their answers, or None when the turn was interrupted.
    pub async fn question(&self, message: &str, schema: Value) -> Option<Value> {
        if self.stopped() {
            return None;
        }
        let id = random_hex(16);
        let (answer, wait) = oneshot::channel();
        self.pending.lock().unwrap().insert(
            id.clone(),
            Pending {
                kind: "answer",
                options: Value::Null,
                schema: schema.clone(),
                answer,
            },
        );
        self.emit(
            "question",
            json!({"id": id, "message": message, "schema": schema}),
        );
        self.status("question", json!({"reason": "Your answer is needed"}));
        let answers = wait.await.ok().flatten();
        self.pending.lock().unwrap().remove(&id);
        answers
    }

    fn resolve(&self, message: &Value) {
        let id = message["id"].as_str().unwrap_or("").to_string();
        let kind = message["type"].as_str().unwrap_or("");
        let mut pending = self.pending.lock().unwrap();
        let live = pending
            .get(&id)
            .filter(|p| p.kind == kind && !p.answer.is_closed());
        let Some(request) = live else {
            drop(pending);
            self.emit(
                "error",
                json!({"message": "This request is no longer pending.", "will_retry": false}),
            );
            return;
        };
        let value = if kind == "approval" {
            let chosen = message.get("optionId").cloned().unwrap_or(Value::Null);
            let option = request
                .options
                .as_array()
                .and_then(|o| o.iter().find(|o| o["id"] == chosen))
                .cloned();
            let Some(option) = option else {
                drop(pending);
                self.emit("error", json!({"message": "Choose one of the permission options shown.", "will_retry": false}));
                return;
            };
            let decision = match option["kind"].as_str() {
                Some("allow_once") => "allow",
                Some("allow_always") => "allow_session",
                _ => "deny",
            };
            self.emit(
                "approval_resolved",
                json!({"id": id, "decision": decision, "optionId": option["id"]}),
            );
            option["id"].clone()
        } else {
            let answers = message.get("answers").cloned().unwrap_or(Value::Null);
            if let Err(problem) = crate::schema::validate(&answers, &request.schema) {
                let text: String = problem.chars().take(400).collect();
                drop(pending);
                self.emit("error", json!({"message": format!("Please check your answer: {text}"), "will_retry": false}));
                return;
            }
            self.emit("answer", json!({"id": id, "answers": answers}));
            answers
        };
        let request = pending.remove(&id).unwrap();
        let _ = request.answer.send(Some(value));
        let any_approval = pending.values().any(|p| p.kind == "approval");
        let any = !pending.is_empty();
        drop(pending);
        self.status(
            if any_approval {
                "approval"
            } else if any {
                "question"
            } else {
                "working"
            },
            json!({}),
        );
    }

    fn cancel_pending(&self) {
        let drained: Vec<(String, Pending)> = self.pending.lock().unwrap().drain().collect();
        for (id, request) in drained {
            if request.kind == "approval" {
                self.emit(
                    "approval_resolved",
                    json!({"id": id, "decision": "cancelled"}),
                );
            } else {
                self.emit(
                    "answer",
                    json!({"id": id, "answers": {}, "cancelled": true}),
                );
            }
            let _ = request.answer.send(None);
        }
    }

    async fn close_engine(&self) {
        let engine = self.engine.lock().await.take();
        if let Some(engine) = engine {
            engine.close().await;
        }
    }
}

async fn inputs(shared: Arc<Shared>, inbox: Inbox) {
    let mut inbox = inbox.lock().await;
    while let Some(message) = inbox.recv().await {
        let kind = message["type"].as_str().unwrap_or("").to_string();
        match kind.as_str() {
            "message" => {
                let text = message["text"].as_str().unwrap_or("").trim().to_string();
                let attachments = message
                    .get("attachments")
                    .and_then(|a| a.as_array())
                    .cloned()
                    .unwrap_or_default();
                if text.is_empty() && attachments.is_empty() {
                    continue;
                }
                let names: Vec<Value> = attachments
                    .iter()
                    .map(|a| json!({"name": a["name"], "mimeType": a["mimeType"]}))
                    .collect();
                shared.emit("user", json!({"text": text, "attachments": names, "queued": !shared.state_is_waiting()}));
                let queued = if attachments.is_empty() {
                    json!(text)
                } else {
                    json!({"text": text, "attachments": attachments})
                };
                let _ = shared.messages.send(Some(queued));
            }
            "approval" | "answer" => shared.resolve(&message),
            "config" | "mode" => {
                let engine = shared.engine.lock().await.clone();
                let result = match engine.as_deref() {
                    Some(Engine::Acp(engine)) => {
                        if kind == "config" {
                            engine
                                .set_option(
                                    &shared,
                                    message["id"].as_str().unwrap_or(""),
                                    message["value"].clone(),
                                )
                                .await
                        } else {
                            engine
                                .set_mode(&shared, message["id"].as_str().unwrap_or(""))
                                .await
                        }
                    }
                    Some(Engine::Demo(_)) => Err("The demo agent has no settings".to_string()),
                    None => Err("Agent session is not ready yet".to_string()),
                };
                if let Err(problem) = result {
                    shared.emit("error", json!({"message": format!("Could not change agent setting: {problem}"), "will_retry": false}));
                }
            }
            "interrupt" => {
                shared.interrupted.store(true, Ordering::SeqCst);
                shared.cancel.notify_waiters();
                shared.interrupt_done.send_replace(false);
                shared.cancel_pending();
                if !*shared.idle.borrow() {
                    let engine = shared.engine.lock().await.clone();
                    if let Some(engine) = engine {
                        let _ = tokio::time::timeout(Duration::from_secs(3), engine.cancel()).await;
                    }
                    let mut idle = shared.idle.subscribe();
                    if tokio::time::timeout(Duration::from_secs(5), idle.wait_for(|i| *i))
                        .await
                        .is_err()
                    {
                        if let Some(active) = shared.active.lock().unwrap().as_ref() {
                            active.abort();
                        }
                    }
                } else {
                    shared.status(
                        "awaiting_input",
                        json!({"reason": "Interrupted. Send a new instruction to continue."}),
                    );
                }
                shared.interrupt_done.send_replace(true);
            }
            "done" => {
                if !shared.state_is_waiting() {
                    shared.emit("error", json!({"message": "Interrupt or wait for the current turn before marking done.", "will_retry": false}));
                } else {
                    shared.finishing.store(true, Ordering::SeqCst);
                    // Done takes precedence over any messages queued just before the click.
                    let _ = shared.messages.send(None);
                }
            }
            _ => {}
        }
    }
}

async fn turn(shared: Arc<Shared>, message: Value) -> Result<String, TurnError> {
    let existing = shared.engine.lock().await.clone();
    let engine = match existing {
        Some(engine) => engine,
        None => {
            let agent = shared.config_str("agent");
            let engine = if agent == "demo" {
                let quiet = shared.config.lock().unwrap()["quietDemo"]
                    .as_bool()
                    .unwrap_or(false);
                Arc::new(Engine::Demo(DemoEngine::new(quiet)))
            } else {
                Arc::new(Engine::Acp(
                    AcpEngine::spawn(shared.clone())
                        .await
                        .map_err(TurnError::Failed)?,
                ))
            };
            *shared.engine.lock().await = Some(engine.clone());
            if let Engine::Acp(acp) = &*engine {
                acp.start(&shared).await.map_err(TurnError::Failed)?;
            }
            engine
        }
    };
    let mut message = message;
    let fresh = shared.connections.load(Ordering::SeqCst);
    if fresh > 0 && !engine.resumed() && engine_is_new(&shared, &engine).await {
        // A failed transport loses the provider's in-memory conversation. Restore the user's intent.
        let mut context =
            serde_json::to_string(&*shared.history.lock().unwrap()).unwrap_or_default();
        if context.chars().count() > CONTEXT_LIMIT {
            let tail: String = context
                .chars()
                .rev()
                .take(CONTEXT_LIMIT)
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect();
            context = format!("[Earlier conversation omitted]\n{tail}");
        }
        let restored = format!("The agent connection restarted. Continue the existing task; files may already contain earlier work.\nOriginal task: {}\nConversation so far:\n{context}\nLatest instruction: {}",
            shared.config_str("goal"), message_text(&message));
        if message.is_object() {
            message["text"] = json!(restored);
        } else {
            message = json!(restored);
        }
    }
    shared.connections.fetch_add(
        u32::from(engine_is_new(&shared, &engine).await),
        Ordering::SeqCst,
    );
    mark_used(&shared, &engine).await;
    if shared.failures.load(Ordering::SeqCst) > 0 {
        shared.failures.fetch_sub(1, Ordering::SeqCst);
        return Err(TurnError::Failed("Simulated failure (demo switch)".into()));
    }
    match &*engine {
        Engine::Acp(acp) => acp.prompt(&shared, &message).await,
        Engine::Demo(demo) => demo
            .prompt(&shared, &message_text(&message))
            .await
            .map_err(TurnError::Failed),
    }
}

// An engine counts as a new connection for its first turn only.
async fn engine_is_new(_shared: &Shared, engine: &Engine) -> bool {
    match engine {
        Engine::Acp(acp) => !acp.used.load(Ordering::SeqCst),
        Engine::Demo(demo) => !demo.used.load(Ordering::SeqCst),
    }
}

async fn mark_used(_shared: &Shared, engine: &Engine) {
    match engine {
        Engine::Acp(acp) => acp.used.store(true, Ordering::SeqCst),
        Engine::Demo(demo) => demo.used.store(true, Ordering::SeqCst),
    }
}

/// Whatever ends this session, a cancelled job included: no task or process stays behind.
struct Cleanup {
    shared: Arc<Shared>,
    inputs: AbortHandle,
}

impl Drop for Cleanup {
    fn drop(&mut self) {
        self.inputs.abort();
        if let Some(active) = self.shared.active.lock().unwrap().take() {
            active.abort();
        }
        if let Ok(mut engine) = self.shared.engine.try_lock() {
            if let Some(engine) = engine.take() {
                engine.abort();
            }
        }
    }
}

/// Run a session until it is marked done (or, with `autoDone`, until its first finished turn).
pub async fn run_session(
    config: Value,
    inbox: Inbox,
    emitter: Arc<Emitter>,
    trees: Trees,
) -> Result<Value, String> {
    let (messages, mut queue) = mpsc::unbounded_channel();
    let failures = config
        .get("simulateFailures")
        .and_then(|f| f.as_i64())
        .unwrap_or(0);
    let goal = config["goal"].as_str().unwrap_or("").to_string();
    let shared = Arc::new(Shared {
        config: Mutex::new(config),
        trees,
        emitter,
        state: Mutex::new("starting".into()),
        history: Mutex::new(vec![]),
        pending: Mutex::new(HashMap::new()),
        interrupted: AtomicBool::new(false),
        finishing: AtomicBool::new(false),
        messages,
        engine: tokio::sync::Mutex::new(None),
        active: Mutex::new(None),
        idle: watch::channel(true).0,
        interrupt_done: watch::channel(true).0,
        cancel: Notify::new(),
        connections: AtomicU32::new(0),
        failures: AtomicI64::new(failures),
    });
    shared.status(
        "starting",
        json!({"workspace": shared.config_str("workspace"), "agent": shared.config_str("agent")}),
    );
    shared.emit("user", json!({"text": goal}));
    let _ = shared.messages.send(Some(json!(goal)));
    let pump = tokio::spawn(inputs(shared.clone(), inbox));
    let _cleanup = Cleanup {
        shared: shared.clone(),
        inputs: pump.abort_handle(),
    };

    while !shared.finishing.load(Ordering::SeqCst) {
        let Some(Some(message)) = queue.recv().await else {
            break;
        };
        if shared.finishing.load(Ordering::SeqCst) {
            break;
        }
        shared.interrupted.store(false, Ordering::SeqCst);
        shared
            .history
            .lock()
            .unwrap()
            .push(json!({"role": "user", "text": message_text(&message)}));
        for attempt in 1..=MAX_ATTEMPTS {
            if shared.stopped() {
                break;
            }
            shared.status(
                "working",
                json!({"attempt": attempt, "max_attempts": MAX_ATTEMPTS}),
            );
            shared.idle.send_replace(false);
            let task = tokio::spawn(turn(shared.clone(), message.clone()));
            *shared.active.lock().unwrap() = Some(task.abort_handle());
            let result = task.await;
            *shared.active.lock().unwrap() = None;
            shared.idle.send_replace(true);
            match result {
                Ok(Ok(reason)) => {
                    let mut done = shared.interrupt_done.subscribe();
                    let _ = done.wait_for(|d| *d).await;
                    if shared.interrupted.load(Ordering::SeqCst) || reason == "cancelled" {
                        shared.status(
                            "awaiting_input",
                            json!({"reason": "Interrupted. Send an instruction to continue."}),
                        );
                    } else if reason == "end_turn"
                        && shared.config.lock().unwrap()["autoDone"]
                            .as_bool()
                            .unwrap_or(false)
                        && queue.is_empty()
                    {
                        // Flow steps without review: a finished turn finishes the step.
                        shared.finishing.store(true, Ordering::SeqCst);
                    } else if reason == "end_turn" {
                        shared.status("awaiting_review", json!({"reason": "Turn finished. Review the result, continue, or mark done."}));
                    } else {
                        shared.status("awaiting_input", json!({"reason": format!("Agent stopped: {reason}. Send a message to continue.")}));
                    }
                    break;
                }
                Err(_) => {
                    // Aborted by an interrupt that the agent did not answer in time.
                    shared.close_engine().await;
                    shared.status("awaiting_input", json!({"reason": "Interrupted. The agent connection will restart on your next message."}));
                    break;
                }
                Ok(Err(TurnError::Rejected(problem))) => {
                    shared.emit("error", json!({"message": problem, "will_retry": false}));
                    shared.status(
                        "awaiting_input",
                        json!({"reason": "Message not sent. Change it and try again."}),
                    );
                    break;
                }
                Ok(Err(TurnError::Failed(problem))) => {
                    shared.cancel_pending();
                    shared.close_engine().await;
                    if shared.interrupted.load(Ordering::SeqCst) {
                        shared.status(
                            "awaiting_input",
                            json!({"reason": "Interrupted. Send an instruction to continue."}),
                        );
                        break;
                    }
                    let retry = attempt < MAX_ATTEMPTS;
                    shared.emit("error", json!({"message": problem, "will_retry": retry}));
                    if retry {
                        shared.status("retrying", json!({"attempt": attempt, "max_attempts": MAX_ATTEMPTS, "reason": "Retrying failed turn"}));
                        let delay = shared.config.lock().unwrap()["retryDelay"]
                            .as_f64()
                            .unwrap_or(1.0);
                        let _ = tokio::time::timeout(
                            Duration::from_secs_f64(delay),
                            shared.cancel.notified(),
                        )
                        .await;
                    } else {
                        shared.status("stuck", json!({"attempt": attempt, "max_attempts": MAX_ATTEMPTS, "reason": "Retries exhausted. Send Continue to retry."}));
                    }
                }
            }
        }
    }
    let auto = shared.config.lock().unwrap()["autoDone"]
        .as_bool()
        .unwrap_or(false);
    shared.status(
        "done",
        json!({"reason": if auto { "Finished" } else { "Marked done by you" }}),
    );
    let last = shared
        .history
        .lock()
        .unwrap()
        .iter()
        .rev()
        .find(|h| h["role"] == "agent")
        .map(|h| h["text"].clone())
        .unwrap_or(json!(""));
    shared.cancel_pending();
    shared.close_engine().await;
    Ok(
        json!({"state": "done", "agent": shared.config_str("agent"), "workspace": shared.config_str("workspace"), "text": last}),
    )
}

/// Explicitly simulated interactions, useful offline and without CLI logins. `quiet` (flow steps): no permission or
/// question; it "reads" its prompt and answers from it, so a flow can run unattended with a live transcript.
pub struct DemoEngine {
    cancelled: AtomicBool,
    turn: AtomicU32,
    quiet: bool,
    used: AtomicBool,
}

impl DemoEngine {
    fn new(quiet: bool) -> DemoEngine {
        DemoEngine {
            cancelled: AtomicBool::new(false),
            turn: AtomicU32::new(0),
            quiet,
            used: AtomicBool::new(false),
        }
    }

    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    async fn quiet_prompt(&self, shared: &Shared, text: &str, turn: u32) -> String {
        let lines: Vec<&str> = text
            .lines()
            .map(str::trim)
            .filter(|l| !l.is_empty())
            .collect();
        let tool = random_hex(16);
        shared.emit("tool", json!({"id": tool, "name": "read", "summary": format!("Read the step input ({} chars, simulated)", text.chars().count()), "status": "running"}));
        tokio::time::sleep(Duration::from_millis(500)).await;
        if self.is_cancelled() {
            return "cancelled".into();
        }
        shared.emit("tool", json!({"id": tool, "status": "ok", "output": lines.iter().take(12).cloned().collect::<Vec<_>>().join("\n")}));
        let mut reply = vec![];
        if turn == 1 {
            reply.push(format!(
                "Simulated agent, no model is called. The task: {}",
                lines.first().copied().unwrap_or("(empty)")
            ));
            reply.push("Key points from the input:".to_string());
            reply.extend(
                lines
                    .iter()
                    .skip(1)
                    .take(5)
                    .map(|l| format!("- {}", l.chars().take(160).collect::<String>())),
            );
        } else {
            reply.push(format!(
                "Noted: {}",
                text.trim().chars().take(300).collect::<String>()
            ));
            reply.push("I would apply that instruction here (simulated).".to_string());
        }
        let id = random_hex(16);
        for line in reply {
            tokio::time::sleep(Duration::from_millis(300)).await;
            if self.is_cancelled() {
                return "cancelled".into();
            }
            shared.emit(
                "agent",
                json!({"message_id": id, "text": format!("{line}\n")}),
            );
        }
        "end_turn".into()
    }

    async fn prompt(&self, shared: &Shared, text: &str) -> Result<String, String> {
        self.cancelled.store(false, Ordering::SeqCst);
        let turn = self.turn.fetch_add(1, Ordering::SeqCst) + 1;
        if self.quiet {
            return Ok(self.quiet_prompt(shared, text, turn).await);
        }
        shared.emit("agent", json!({"message_id": random_hex(16), "text": "This is the simulated UI demo. No model is being called. I'll show a permission request and a question."}));
        let option = shared
            .permission(
                "execute",
                "Run the demo test suite",
                "python -m unittest -v",
                json!([
            {"id": "allow", "label": "Allow once", "kind": "allow_once"},
            {"id": "session", "label": "Allow for session", "kind": "allow_always"},
            {"id": "deny", "label": "Deny", "kind": "reject_once"}]),
            )
            .await;
        let Some(option) = option.filter(|_| !self.is_cancelled()) else {
            return Ok("cancelled".into());
        };
        if option == "deny" {
            shared.emit("agent", json!({"text": "Permission denied. I won't run that tool.", "message_id": random_hex(16)}));
            return Ok("end_turn".into());
        }
        let answer = shared.question("How should I describe the result?", json!({"type": "object", "properties": {
            "style": {"type": "string", "title": "Summary style", "oneOf": [{"const": "brief", "title": "Brief"}, {"const": "detailed", "title": "Detailed"}]},
            "checks": {"type": "array", "title": "Checks to mention", "items": {"anyOf": [{"const": "tests", "title": "Tests"}, {"const": "changes", "title": "Changes"}]}},
            "other": {"type": "string", "title": "Other instructions"}}, "required": ["style"]})).await;
        let Some(answer) = answer.filter(|_| !self.is_cancelled()) else {
            return Ok("cancelled".into());
        };
        let tool = random_hex(16);
        shared.emit("tool", json!({"id": tool, "name": "execute", "summary": "python -m unittest -v (simulated)", "status": "running"}));
        for _ in 0..8 {
            tokio::time::sleep(Duration::from_millis(250)).await;
            if self.is_cancelled() {
                shared.emit(
                    "tool",
                    json!({"id": tool, "status": "error", "output": "Interrupted"}),
                );
                return Ok("cancelled".into());
            }
        }
        shared.emit("tool", json!({"id": tool, "status": "ok", "output": "Simulated: 2 tests passed. No workspace files were changed."}));
        shared.emit("agent", json!({"message_id": random_hex(16), "text": format!("Demo turn {turn} finished ({} summary). Review this result. Continue starts another turn; Mark done ends the session.", answer["style"].as_str().unwrap_or(""))}));
        Ok("end_turn".into())
    }
}
