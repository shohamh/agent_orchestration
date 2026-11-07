//! The Agent Client Protocol: JSON-RPC over a coding agent's stdin and stdout. The same behaviour as
//! agents/agentdriver/acp_engine.py. Agents keep their own tools and logins.
use crate::procutil::{prepare_child, Tree};
use crate::session::{Shared, TurnError};
use crate::template::which;
use crate::util::random_hex;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::{oneshot, watch};
use tokio::task::AbortHandle;

const START_TIMEOUT: Duration = Duration::from_secs(60);
const PACKAGE_JSON: &str = include_str!("../../agents/package.json");
const PACKAGE_LOCK: &str = include_str!("../../agents/package-lock.json");

/// Where `agent-worker agents install` puts the npm adapters for this PC.
static ADAPTER_HOME: OnceLock<PathBuf> = OnceLock::new();

pub fn set_adapter_home(path: PathBuf) {
    let _ = ADAPTER_HOME.set(path);
}

fn adapter_homes() -> Vec<PathBuf> {
    let mut homes = vec![];
    if let Some(path) = std::env::var_os("AGENT_WORKER_ADAPTERS") {
        homes.push(PathBuf::from(path));
    }
    homes.extend(ADAPTER_HOME.get().cloned());
    homes
}

fn installed(relative: &[&str]) -> Option<PathBuf> {
    adapter_homes()
        .into_iter()
        .map(|home| {
            relative
                .iter()
                .fold(home.join("node_modules"), |path, part| path.join(part))
        })
        .find(|path| path.is_file())
}

/// npm's Windows launchers (.cmd) can't be started directly: find the package's own program next to the launcher.
fn behind_npm_shim(shim: &Path, package: &[&str], name: &str) -> Option<PathBuf> {
    let root = package
        .iter()
        .fold(shim.parent()?.join("node_modules"), |path, part| {
            path.join(part)
        });
    let manifest: Value =
        serde_json::from_slice(&std::fs::read(root.join("package.json")).ok()?).ok()?;
    let relative = match &manifest["bin"] {
        Value::String(single) => single.clone(),
        Value::Object(bins) => bins.get(name)?.as_str()?.to_string(),
        _ => return None,
    };
    Some(root.join(relative)).filter(|path| path.is_file())
}

fn is_shim(path: &Path) -> bool {
    cfg!(windows)
        && path
            .extension()
            .map(|e| e.eq_ignore_ascii_case("cmd") || e.eq_ignore_ascii_case("bat"))
            .unwrap_or(false)
}

fn with_node(script: PathBuf, args: &[&str]) -> Result<Vec<String>, String> {
    let node = which("node")
        .ok_or("Node.js is not installed or not on PATH; the coding-agent adapters need it")?;
    let mut command = vec![node.display().to_string(), script.display().to_string()];
    command.extend(args.iter().map(|a| a.to_string()));
    Ok(command)
}

/// The command that speaks ACP for an agent. `AGENT_WORKER_ACP_<AGENT>` (a JSON list) replaces it, for an agent
/// installed somewhere unusual.
pub fn agent_command(agent: &str) -> Result<Vec<String>, String> {
    if let Ok(custom) = std::env::var(format!("AGENT_WORKER_ACP_{}", agent.to_uppercase())) {
        return serde_json::from_str::<Vec<String>>(&custom)
            .ok()
            .filter(|c| !c.is_empty())
            .ok_or_else(|| {
                format!(
                    "AGENT_WORKER_ACP_{} must be a JSON list",
                    agent.to_uppercase()
                )
            });
    }
    let missing = || format!("The {agent} adapter is missing. Run: agent-worker agents install");
    match agent {
        "opencode2" => {
            let binary = if cfg!(windows) {
                "opencode.exe"
            } else {
                "opencode"
            };
            let path = installed(&["@opencode", "cli", "bin", binary]).ok_or_else(missing)?;
            Ok(vec![path.display().to_string(), "acp".into()])
        }
        "claude" | "codex" | "pi" => {
            let (package, command): (&[&str], &str) = match agent {
                "claude" => (
                    &["@agentclientprotocol", "claude-agent-acp"],
                    "claude-agent-acp",
                ),
                "codex" => (&["@agentclientprotocol", "codex-acp"], "codex-acp"),
                _ => (&["pi-acp"], "pi-acp"),
            };
            let mut relative = package.to_vec();
            relative.extend(["dist", "index.js"]);
            if let Some(adapter) = installed(&relative) {
                return with_node(adapter, &[]);
            }
            // Installed globally with npm.
            let global = which(command).ok_or_else(missing)?;
            if is_shim(&global) {
                return with_node(
                    behind_npm_shim(&global, package, command).ok_or_else(missing)?,
                    &[],
                );
            }
            Ok(vec![global.display().to_string()])
        }
        _ => {
            let executable =
                which(agent).ok_or_else(|| format!("{agent} is not installed or not on PATH."))?;
            if !is_shim(&executable) {
                return Ok(vec![executable.display().to_string(), "acp".into()]);
            }
            let package = if agent == "opencode" {
                "opencode-ai"
            } else {
                agent
            };
            let binary = behind_npm_shim(&executable, &[package], agent).ok_or_else(|| format!("Cannot resolve {agent}'s npm executable. Install a native ACP executable on PATH."))?;
            if binary
                .extension()
                .map(|e| e.eq_ignore_ascii_case("exe"))
                .unwrap_or(false)
            {
                return Ok(vec![binary.display().to_string(), "acp".into()]);
            }
            with_node(binary, &["acp"])
        }
    }
}

/// Is this agent ready to run on this PC? pi's adapter also needs the `pi` program it drives.
pub fn agent_available(agent: &str) -> bool {
    agent == "demo"
        || (agent_command(agent).is_ok()
            && (agent != "pi"
                || which("pi").is_some()
                || std::env::var_os("AGENT_WORKER_ACP_PI").is_some()))
}

/// `agent-worker agents install`: the pinned npm adapters, next to the worker. Needs Node.js and npm.
pub fn install_adapters(home: &Path, registry: Option<&str>) -> Result<(), String> {
    std::fs::create_dir_all(home).map_err(|e| e.to_string())?;
    std::fs::write(home.join("package.json"), PACKAGE_JSON).map_err(|e| e.to_string())?;
    std::fs::write(home.join("package-lock.json"), PACKAGE_LOCK).map_err(|e| e.to_string())?;
    let mut command = if cfg!(windows) {
        let mut c = std::process::Command::new(
            std::env::var("COMSPEC").unwrap_or_else(|_| "cmd.exe".into()),
        );
        c.args(["/c", "npm.cmd"]);
        c
    } else {
        std::process::Command::new("npm")
    };
    command
        .args(["ci", "--no-audit", "--no-fund"])
        .current_dir(home);
    if let Some(registry) = registry {
        command.args(["--registry", registry]);
    }
    let status = command
        .status()
        .map_err(|e| format!("Could not run npm (is Node.js installed?): {e}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("npm ci failed with {status}"))
    }
}

fn text_content(content: &Value) -> String {
    match content {
        Value::String(text) => text.clone(),
        Value::Object(map) => {
            if map.get("type").and_then(|t| t.as_str()) == Some("text") {
                map.get("text")
                    .and_then(|t| t.as_str())
                    .unwrap_or("")
                    .to_string()
            } else if let Some(inner) = map.get("content") {
                text_content(inner)
            } else {
                content.to_string()
            }
        }
        Value::Array(items) => items
            .iter()
            .map(text_content)
            .collect::<Vec<_>>()
            .join("\n"),
        _ => String::new(),
    }
}

struct Terminal {
    tree: Option<Arc<Tree>>,
    output: Mutex<Vec<u8>>,
    truncated: AtomicBool,
    exit: watch::Sender<Option<(Option<i64>, Option<String>)>>,
}

impl Terminal {
    fn exit_status(&self) -> Value {
        match &*self.exit.borrow() {
            Some((code, signal)) => json!({"exitCode": code, "signal": signal}),
            None => Value::Null,
        }
    }

    fn text(&self) -> String {
        String::from_utf8_lossy(&self.output.lock().unwrap()).to_string()
    }
}

pub struct AcpEngine {
    stdin: tokio::sync::Mutex<ChildStdin>,
    child: tokio::sync::Mutex<Option<Child>>,
    tree: Option<Arc<Tree>>,
    pending: Mutex<HashMap<i64, oneshot::Sender<Result<Value, String>>>>,
    next_id: AtomicI64,
    session_id: Mutex<Option<String>>,
    message_id: Mutex<Option<String>>,
    tools: Mutex<HashMap<String, (String, String)>>,
    state: Mutex<Value>,
    terminals: Mutex<HashMap<String, Arc<Terminal>>>,
    stderr_tail: Mutex<String>,
    tasks: Mutex<Vec<AbortHandle>>,
    replaying: AtomicBool,
    pub resumed: AtomicBool,
    pub used: AtomicBool,
    closing: AtomicBool,
}

impl AcpEngine {
    /// Start the agent's program in the session's workspace and begin reading from it.
    pub async fn spawn(shared: Arc<Shared>) -> Result<Arc<AcpEngine>, String> {
        let agent = shared.config_str("agent");
        let line = agent_command(&agent)?;
        let mut command = Command::new(&line[0]);
        command
            .args(&line[1..])
            .current_dir(shared.config_str("workspace"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        // Inherited nesting markers belong to this worker's host, not the coding session.
        command.env_remove("CLAUDECODE");
        if agent == "codex" {
            if std::env::var_os("INITIAL_AGENT_MODE").is_none() {
                command.env("INITIAL_AGENT_MODE", "read-only");
            }
            if std::env::var_os("NO_BROWSER").is_none() {
                command.env("NO_BROWSER", "1");
            }
        }
        if (agent == "opencode" || agent == "opencode2")
            && std::env::var_os("OPENCODE_CONFIG_CONTENT").is_none()
        {
            // opencode allows every tool by default, so a session would never see a permission prompt.
            command.env(
                "OPENCODE_CONFIG_CONTENT",
                r#"{"permission": {"edit": "ask", "bash": "ask", "webfetch": "ask"}}"#,
            );
        }
        prepare_child(&mut command);
        let mut child = command
            .spawn()
            .map_err(|e| format!("Could not start {agent}: {e}"))?;
        let tree = Tree::adopt(&child).map(Arc::new);
        if let Some(tree) = &tree {
            shared.trees.lock().unwrap().push(tree.clone());
        }
        let (stdin, stdout, stderr) = (
            child.stdin.take().unwrap(),
            child.stdout.take().unwrap(),
            child.stderr.take().unwrap(),
        );
        let engine = Arc::new(AcpEngine {
            stdin: tokio::sync::Mutex::new(stdin),
            child: tokio::sync::Mutex::new(Some(child)),
            tree,
            pending: Mutex::new(HashMap::new()),
            next_id: AtomicI64::new(1),
            session_id: Mutex::new(None),
            message_id: Mutex::new(None),
            tools: Mutex::new(HashMap::new()),
            state: Mutex::new(
                json!({"configOptions": [], "modes": null, "usage": null, "capabilities": {}}),
            ),
            terminals: Mutex::new(HashMap::new()),
            stderr_tail: Mutex::new(String::new()),
            tasks: Mutex::new(vec![]),
            replaying: AtomicBool::new(false),
            resumed: AtomicBool::new(false),
            used: AtomicBool::new(false),
            closing: AtomicBool::new(false),
        });
        let reader = tokio::spawn(engine.clone().read(shared.clone(), stdout));
        let drain = tokio::spawn(engine.clone().drain_stderr(stderr));
        engine
            .tasks
            .lock()
            .unwrap()
            .extend([reader.abort_handle(), drain.abort_handle()]);
        Ok(engine)
    }

    async fn drain_stderr(self: Arc<Self>, mut stderr: tokio::process::ChildStderr) {
        let mut buffer = [0u8; 4096];
        while let Ok(count) = stderr.read(&mut buffer).await {
            if count == 0 {
                break;
            }
            let mut tail = self.stderr_tail.lock().unwrap();
            tail.push_str(&String::from_utf8_lossy(&buffer[..count]));
            if tail.len() > 8000 {
                *tail = tail
                    .chars()
                    .rev()
                    .take(4000)
                    .collect::<Vec<_>>()
                    .into_iter()
                    .rev()
                    .collect();
            }
        }
    }

    async fn read(self: Arc<Self>, shared: Arc<Shared>, stdout: tokio::process::ChildStdout) {
        let mut lines = BufReader::new(stdout).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            let Ok(message) = serde_json::from_str::<Value>(&line) else {
                continue;
            };
            match (
                message.get("method").and_then(|m| m.as_str()),
                message.get("id"),
            ) {
                (Some(method), Some(id)) => {
                    let (engine, shared, method, id, params) = (
                        self.clone(),
                        shared.clone(),
                        method.to_string(),
                        id.clone(),
                        message["params"].clone(),
                    );
                    // A request may wait for a human, so it must not hold up the rest of the conversation.
                    let task = tokio::spawn(async move {
                        let outcome = engine.answer_request(&shared, &method, params).await;
                        let reply = match outcome {
                            Ok(result) => json!({"jsonrpc": "2.0", "id": id, "result": result}),
                            Err((code, problem)) => {
                                json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": problem}})
                            }
                        };
                        let _ = engine.write(&reply).await;
                    });
                    let mut tasks = self.tasks.lock().unwrap();
                    tasks.retain(|t| !t.is_finished());
                    tasks.push(task.abort_handle());
                }
                (Some(method), None) => {
                    if method == "session/update" {
                        self.session_update(&shared, &message["params"]["update"]);
                    }
                }
                (None, Some(id)) => {
                    let waiting = id
                        .as_i64()
                        .and_then(|id| self.pending.lock().unwrap().remove(&id));
                    if let Some(waiting) = waiting {
                        let result = match message.get("error") {
                            Some(error) if !error.is_null() => {
                                let data = error
                                    .get("data")
                                    .filter(|d| !d.is_null())
                                    .map(|d| format!(" ({d})"))
                                    .unwrap_or_default();
                                Err(format!(
                                    "{}{data}",
                                    error["message"]
                                        .as_str()
                                        .unwrap_or("The agent returned an error")
                                ))
                            }
                            _ => Ok(message.get("result").cloned().unwrap_or(Value::Null)),
                        };
                        let _ = waiting.send(result);
                    }
                }
                _ => {}
            }
        }
        // The agent is gone: nothing it was asked will be answered.
        let tail = self.stderr_tail.lock().unwrap().trim().to_string();
        let tail: String = tail
            .chars()
            .rev()
            .take(2000)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect();
        let reason = if tail.is_empty() {
            "The agent process ended".to_string()
        } else {
            format!("The agent process ended: {tail}")
        };
        for (_, waiting) in self.pending.lock().unwrap().drain() {
            let _ = waiting.send(Err(reason.clone()));
        }
    }

    async fn write(&self, message: &Value) -> Result<(), String> {
        let mut line = serde_json::to_vec(message).map_err(|e| e.to_string())?;
        line.push(b'\n');
        let mut stdin = self.stdin.lock().await;
        stdin
            .write_all(&line)
            .await
            .map_err(|e| format!("The agent is not accepting input: {e}"))?;
        stdin.flush().await.map_err(|e| e.to_string())
    }

    async fn call(&self, method: &str, params: Value) -> Result<Value, String> {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let (send, wait) = oneshot::channel();
        self.pending.lock().unwrap().insert(id, send);
        self.write(&json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}))
            .await?;
        wait.await
            .map_err(|_| "The agent connection closed".to_string())?
    }

    fn session(&self) -> String {
        self.session_id.lock().unwrap().clone().unwrap_or_default()
    }

    fn publish(&self, shared: &Shared, fields: Value) {
        let state = {
            let mut state = self.state.lock().unwrap();
            if let (Some(state), Some(fields)) = (state.as_object_mut(), fields.as_object()) {
                state.extend(fields.clone());
            }
            state.clone()
        };
        shared.emit("acp_state", state);
    }

    /// Initialize, then load the earlier session if there is one, or start a new one.
    pub async fn start(&self, shared: &Shared) -> Result<(), String> {
        // A reconnect creates a new ACP session; old commands no longer apply.
        shared.emit("commands", json!({"commands": []}));
        self.publish(shared, json!({}));
        tokio::time::timeout(START_TIMEOUT, self.handshake(shared))
            .await
            .map_err(|_| {
                let tail = self.stderr_tail.lock().unwrap().trim().to_string();
                format!(
                    "The agent did not start within 60 seconds{}",
                    if tail.is_empty() {
                        String::new()
                    } else {
                        format!(": {tail}")
                    }
                )
            })?
    }

    async fn handshake(&self, shared: &Shared) -> Result<(), String> {
        let init = self.call("initialize", json!({"protocolVersion": 1, "clientCapabilities": {
            "fs": {"readTextFile": false, "writeTextFile": false}, "terminal": true, "elicitation": {"form": {}}}})).await?;
        let caps = init.get("agentCapabilities").cloned().unwrap_or(json!({}));
        let can_load = caps["loadSession"].as_bool().unwrap_or(false);
        let workspace = shared.config_str("workspace");
        let requested = shared.config_str("resumeSessionId");
        let reconnecting = shared.config_str("_acp_session_id");
        let prior = if reconnecting.is_empty() {
            requested.clone()
        } else {
            reconnecting.clone()
        };
        if !prior.is_empty() && !can_load && !requested.is_empty() {
            return Err("This agent does not support loading previous sessions".into());
        }
        let response = if !prior.is_empty() && can_load {
            self.replaying
                .store(!reconnecting.is_empty(), Ordering::SeqCst);
            let loaded = self
                .call(
                    "session/load",
                    json!({"sessionId": prior, "cwd": workspace, "mcpServers": []}),
                )
                .await;
            self.replaying.store(false, Ordering::SeqCst);
            let loaded = loaded?;
            *self.session_id.lock().unwrap() = Some(prior.clone());
            self.resumed.store(true, Ordering::SeqCst);
            loaded
        } else {
            let created = self
                .call("session/new", json!({"cwd": workspace, "mcpServers": []}))
                .await?;
            let id = created["sessionId"]
                .as_str()
                .ok_or("The agent did not name its session")?
                .to_string();
            *self.session_id.lock().unwrap() = Some(id);
            created
        };
        let session = self.session();
        shared.config.lock().unwrap()["_acp_session_id"] = json!(session);
        let current = self.state.lock().unwrap().clone();
        self.publish(shared, json!({"capabilities": caps,
            "configOptions": response.get("configOptions").cloned().unwrap_or(current["configOptions"].clone()),
            "modes": response.get("modes").cloned().unwrap_or(current["modes"].clone()), "sessionId": session}));
        Ok(())
    }

    pub async fn set_option(
        &self,
        shared: &Shared,
        option: &str,
        value: Value,
    ) -> Result<(), String> {
        let result = self
            .call(
                "session/set_config_option",
                json!({"sessionId": self.session(), "configId": option, "value": value}),
            )
            .await?;
        self.publish(
            shared,
            json!({"configOptions": result.get("configOptions").cloned().unwrap_or(json!([]))}),
        );
        Ok(())
    }

    pub async fn set_mode(&self, shared: &Shared, mode: &str) -> Result<(), String> {
        self.call(
            "session/set_mode",
            json!({"sessionId": self.session(), "modeId": mode}),
        )
        .await?;
        let mut modes = self.state.lock().unwrap()["modes"].clone();
        if !modes.is_object() {
            modes = json!({});
        }
        modes["currentModeId"] = json!(mode);
        self.publish(shared, json!({"modes": modes}));
        Ok(())
    }

    pub async fn prompt(&self, _shared: &Shared, message: &Value) -> Result<String, TurnError> {
        *self.message_id.lock().unwrap() = None;
        let text = message
            .as_str()
            .map(String::from)
            .unwrap_or_else(|| message["text"].as_str().unwrap_or("").to_string());
        let mut blocks = vec![json!({"type": "text", "text": text})];
        let caps = self.state.lock().unwrap()["capabilities"]["promptCapabilities"].clone();
        for item in message
            .get("attachments")
            .and_then(|a| a.as_array())
            .into_iter()
            .flatten()
        {
            let mime = item["mimeType"].as_str().unwrap_or("");
            if mime.starts_with("image/") {
                if !caps["image"].as_bool().unwrap_or(false) {
                    return Err(TurnError::Rejected(
                        "This agent does not support image attachments".into(),
                    ));
                }
                blocks.push(json!({"type": "image", "data": item["data"], "mimeType": mime}));
            } else {
                if !caps["embeddedContext"].as_bool().unwrap_or(false) {
                    return Err(TurnError::Rejected(
                        "This agent does not support file attachments".into(),
                    ));
                }
                let name: String = item["name"]
                    .as_str()
                    .unwrap_or("")
                    .bytes()
                    .map(|b| {
                        if b.is_ascii_alphanumeric() || b"-_.~".contains(&b) {
                            (b as char).to_string()
                        } else {
                            format!("%{b:02X}")
                        }
                    })
                    .collect();
                blocks.push(json!({"type": "resource", "resource": {"uri": format!("attachment:///{name}"), "mimeType": mime, "blob": item["data"]}}));
            }
        }
        let result = self
            .call(
                "session/prompt",
                json!({"sessionId": self.session(), "prompt": blocks}),
            )
            .await
            .map_err(TurnError::Failed)?;
        Ok(result["stopReason"]
            .as_str()
            .unwrap_or("end_turn")
            .to_string())
    }

    pub async fn cancel(&self) {
        if self.session_id.lock().unwrap().is_some() {
            let _ = self.write(&json!({"jsonrpc": "2.0", "method": "session/cancel", "params": {"sessionId": self.session()}})).await;
        }
    }

    pub async fn close(&self) {
        self.closing.store(true, Ordering::SeqCst);
        let terminals: Vec<Arc<Terminal>> = self
            .terminals
            .lock()
            .unwrap()
            .drain()
            .map(|(_, t)| t)
            .collect();
        for terminal in terminals {
            if let Some(tree) = &terminal.tree {
                tree.kill();
            }
        }
        // Kill descendants too: adapters start an app server or a CLI of their own.
        if let Some(tree) = &self.tree {
            tree.kill();
        }
        if let Some(mut child) = self.child.lock().await.take() {
            let _ = child.start_kill();
            let _ = tokio::time::timeout(Duration::from_secs(5), child.wait()).await;
        }
        self.abort();
    }

    /// Stop reading and answering. Used where nothing can be awaited; the job ends the process trees itself.
    pub fn abort(&self) {
        for task in self.tasks.lock().unwrap().drain(..) {
            task.abort();
        }
    }

    fn session_update(&self, shared: &Shared, update: &Value) {
        let kind = update["sessionUpdate"].as_str().unwrap_or("");
        if self.replaying.load(Ordering::SeqCst)
            && [
                "agent_message_chunk",
                "user_message_chunk",
                "tool_call",
                "tool_call_update",
                "plan",
            ]
            .contains(&kind)
        {
            return;
        }
        match kind {
            "agent_message_chunk" => {
                let text = text_content(&update["content"]);
                if !text.is_empty() {
                    let id = self
                        .message_id
                        .lock()
                        .unwrap()
                        .get_or_insert_with(|| random_hex(16))
                        .clone();
                    shared.emit("agent", json!({"text": text, "message_id": id}));
                }
            }
            "user_message_chunk" => {
                let text = text_content(&update["content"]);
                if !text.is_empty() {
                    *self.message_id.lock().unwrap() = None;
                    shared.emit("user", json!({"text": text, "replayed": true}));
                }
            }
            "tool_call" | "tool_call_update" => {
                *self.message_id.lock().unwrap() = None;
                let status = match update["status"].as_str() {
                    Some("completed") => "ok",
                    Some("failed") => "error",
                    _ => "running",
                };
                let id = update["toolCallId"].as_str().unwrap_or("").to_string();
                // Updates often omit kind and title: repeat the last known ones, since a rolling transcript may keep
                // only a tool's latest event, which must still say what it was.
                let (name, summary) = {
                    let mut tools = self.tools.lock().unwrap();
                    let known = tools
                        .entry(id.clone())
                        .or_insert_with(|| ("tool".to_string(), String::new()));
                    if let Some(tool_kind) = update["kind"]
                        .as_str()
                        .filter(|k| *k != "other" && !k.is_empty())
                    {
                        known.0 = tool_kind.to_string();
                    }
                    if let Some(title) = update["title"].as_str().filter(|t| !t.is_empty()) {
                        known.1 = title.to_string();
                    }
                    known.clone()
                };
                let mut fields = json!({"id": id, "status": status, "name": name});
                if !summary.is_empty() {
                    fields["summary"] = json!(summary);
                }
                let content = &update["content"];
                if !content.is_null() && content.as_array().map(|c| !c.is_empty()).unwrap_or(true) {
                    fields["content"] = content.clone();
                } else if !update["rawOutput"].is_null() {
                    fields["output"] = update["rawOutput"]
                        .as_str()
                        .map(|s| json!(s))
                        .unwrap_or_else(|| json!(update["rawOutput"].to_string()));
                }
                if !update["rawInput"].is_null() && update["rawInput"] != json!({}) {
                    fields["detail"] = json!(update["rawInput"].to_string());
                }
                shared.emit("tool", fields);
            }
            "plan" => shared.emit(
                "plan",
                json!({"entries": update.get("entries").cloned().unwrap_or(json!([]))}),
            ),
            "available_commands_update" => shared.emit(
                "commands",
                json!({"commands": update.get("availableCommands").cloned().unwrap_or(json!([]))}),
            ),
            "usage_update" => {
                let mut usage = json!({});
                for key in ["used", "size", "cost"] {
                    if let Some(value) = update.get(key) {
                        usage[key] = value.clone();
                    }
                }
                self.publish(shared, json!({"usage": usage}));
            }
            "config_option_update" => self.publish(
                shared,
                json!({"configOptions": update.get("configOptions").cloned().unwrap_or(json!([]))}),
            ),
            "current_mode_update" => {
                let mut modes = self.state.lock().unwrap()["modes"].clone();
                if !modes.is_object() {
                    modes = json!({});
                }
                modes["currentModeId"] = update["currentModeId"].clone();
                self.publish(shared, json!({"modes": modes}));
            }
            _ => {}
        }
    }

    async fn answer_request(
        self: &Arc<Self>,
        shared: &Arc<Shared>,
        method: &str,
        params: Value,
    ) -> Result<Value, (i64, String)> {
        let invalid = |problem: String| (-32602, problem);
        match method {
            "session/request_permission" => {
                let tool = &params["toolCall"];
                let choices: Vec<Value> = params["options"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .map(|o| json!({"id": o["optionId"], "label": o["name"], "kind": o["kind"]}))
                    .collect();
                let content = text_content(&tool["content"]);
                let detail = if content.is_empty() {
                    tool.get("rawInput")
                        .cloned()
                        .unwrap_or(json!({}))
                        .to_string()
                } else {
                    content
                };
                let chosen = shared
                    .permission(
                        tool["kind"].as_str().unwrap_or("tool"),
                        tool["title"].as_str().unwrap_or("Permission requested"),
                        &detail,
                        json!(choices),
                    )
                    .await;
                Ok(match chosen {
                    Some(option) => json!({"outcome": {"outcome": "selected", "optionId": option}}),
                    None => json!({"outcome": {"outcome": "cancelled"}}),
                })
            }
            "elicitation/create" => {
                let schema = params
                    .get("requestedSchema")
                    .or_else(|| params["mode"].get("requestedSchema"))
                    .cloned()
                    .unwrap_or(json!({}));
                let answer = shared
                    .question(params["message"].as_str().unwrap_or(""), schema)
                    .await;
                Ok(match answer {
                    Some(content) => json!({"action": "accept", "content": content}),
                    None => json!({"action": "cancel"}),
                })
            }
            "elicitation/complete" => Ok(json!({})),
            "terminal/create" => self.create_terminal(shared, &params).await.map_err(invalid),
            "terminal/output" => {
                let terminal = self.terminal(&params).map_err(invalid)?;
                Ok(
                    json!({"output": terminal.text(), "truncated": terminal.truncated.load(Ordering::SeqCst), "exitStatus": terminal.exit_status()}),
                )
            }
            "terminal/wait_for_exit" => {
                let terminal = self.terminal(&params).map_err(invalid)?;
                let mut exit = terminal.exit.subscribe();
                let _ = exit.wait_for(|e| e.is_some()).await;
                Ok(terminal.exit_status())
            }
            "terminal/kill" => {
                let terminal = self.terminal(&params).map_err(invalid)?;
                if let Some(tree) = &terminal.tree {
                    tree.kill();
                }
                let mut exit = terminal.exit.subscribe();
                let _ =
                    tokio::time::timeout(Duration::from_secs(10), exit.wait_for(|e| e.is_some()))
                        .await;
                Ok(json!({}))
            }
            "terminal/release" => {
                let terminal = self.terminal(&params).map_err(invalid)?;
                if let Some(tree) = &terminal.tree {
                    tree.kill();
                }
                let mut exit = terminal.exit.subscribe();
                let _ =
                    tokio::time::timeout(Duration::from_secs(10), exit.wait_for(|e| e.is_some()))
                        .await;
                self.terminals
                    .lock()
                    .unwrap()
                    .remove(params["terminalId"].as_str().unwrap_or(""));
                if let Some(tree) = &terminal.tree {
                    shared
                        .trees
                        .lock()
                        .unwrap()
                        .retain(|t| !Arc::ptr_eq(t, tree));
                }
                Ok(json!({}))
            }
            _ => Err((-32601, format!("Method not found: {method}"))),
        }
    }

    fn terminal(&self, params: &Value) -> Result<Arc<Terminal>, String> {
        if params["sessionId"].as_str() != Some(self.session().as_str()) {
            return Err("Unknown session".into());
        }
        self.terminals
            .lock()
            .unwrap()
            .get(params["terminalId"].as_str().unwrap_or(""))
            .cloned()
            .ok_or_else(|| "Unknown terminal".to_string())
    }

    async fn create_terminal(
        self: &Arc<Self>,
        shared: &Arc<Shared>,
        params: &Value,
    ) -> Result<Value, String> {
        if params["sessionId"].as_str() != Some(self.session().as_str()) {
            return Err("Unknown session".into());
        }
        let program = params["command"]
            .as_str()
            .ok_or("A terminal needs a command")?
            .to_string();
        let args: Vec<String> = params["args"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|a| a.as_str().map(String::from))
            .collect();
        let workspace =
            std::fs::canonicalize(shared.config_str("workspace")).map_err(|e| e.to_string())?;
        let directory = match params["cwd"].as_str() {
            Some(cwd) => std::fs::canonicalize(cwd).map_err(|e| e.to_string())?,
            None => workspace.clone(),
        };
        if !directory.starts_with(&workspace) {
            return Err("Terminal directory must be inside the session workspace".into());
        }
        let env: Vec<(String, String)> = params["env"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|e| {
                Some((
                    e["name"].as_str()?.to_string(),
                    e["value"].as_str()?.to_string(),
                ))
            })
            .collect();
        let names: Vec<&String> = env.iter().map(|(name, _)| name).collect();
        let detail = json!({"command": program, "args": args, "cwd": directory.display().to_string(), "environmentNames": names}).to_string();
        let choice = shared.permission("execute", "Run terminal command", &detail,
            json!([{"id": "allow", "label": "Allow once", "kind": "allow_once"}, {"id": "deny", "label": "Deny", "kind": "reject_once"}])).await;
        if choice.as_deref() != Some("allow") {
            return Err("Terminal command was not approved".into());
        }
        let mut command = Command::new(&program);
        command
            .args(&args)
            .current_dir(&directory)
            .envs(env)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        prepare_child(&mut command);
        let mut child = command
            .spawn()
            .map_err(|e| format!("Could not start {program}: {e}"))?;
        let tree = Tree::adopt(&child).map(Arc::new);
        if let Some(tree) = &tree {
            shared.trees.lock().unwrap().push(tree.clone());
        }
        let limit = params["outputByteLimit"]
            .as_u64()
            .unwrap_or(65536)
            .clamp(1, 1024 * 1024) as usize;
        let id = random_hex(16);
        let terminal = Arc::new(Terminal {
            tree,
            output: Mutex::new(vec![]),
            truncated: AtomicBool::new(false),
            exit: watch::channel(None).0,
        });
        self.terminals
            .lock()
            .unwrap()
            .insert(id.clone(), terminal.clone());
        let tool = format!("terminal:{id}");
        shared.emit(
            "tool",
            json!({"id": tool, "name": "terminal", "summary": program, "status": "running"}),
        );
        let (stdout, stderr) = (child.stdout.take().unwrap(), child.stderr.take().unwrap());
        let (running, summary, owner) = (terminal.clone(), program, shared.clone());
        let task = tokio::spawn(async move {
            let shown = |t: &Terminal| {
                let o = t.output.lock().unwrap();
                let from = o.len().saturating_sub(8000);
                String::from_utf8_lossy(&o[from..]).to_string()
            };
            let pump = |mut stream: Box<dyn tokio::io::AsyncRead + Unpin + Send>| {
                let (terminal, owner, tool, summary) = (
                    running.clone(),
                    owner.clone(),
                    tool.clone(),
                    summary.clone(),
                );
                async move {
                    let mut buffer = [0u8; 4096];
                    while let Ok(count) = stream.read(&mut buffer).await {
                        if count == 0 {
                            break;
                        }
                        {
                            let mut output = terminal.output.lock().unwrap();
                            output.extend_from_slice(&buffer[..count]);
                            if output.len() > limit {
                                let mut cut = output.len() - limit;
                                while cut < output.len() && output[cut] & 0xc0 == 0x80 {
                                    cut += 1;
                                }
                                output.drain(..cut);
                                terminal.truncated.store(true, Ordering::SeqCst);
                            }
                        }
                        let text = {
                            let o = terminal.output.lock().unwrap();
                            let from = o.len().saturating_sub(8000);
                            String::from_utf8_lossy(&o[from..]).to_string()
                        };
                        owner.emit("tool", json!({"id": tool, "name": "terminal", "summary": summary, "status": "running", "output": text}));
                    }
                }
            };
            tokio::join!(pump(Box::new(stdout)), pump(Box::new(stderr)));
            let code = child.wait().await.ok().and_then(|s| s.code());
            owner.emit("tool", json!({"id": tool, "name": "terminal", "summary": summary, "status": if code == Some(0) { "ok" } else { "error" }, "output": shown(&running)}));
            // send_replace: nobody may be subscribed yet, and the status is read later.
            running.exit.send_replace(Some((
                code.map(i64::from),
                if code.is_none() {
                    Some("killed".to_string())
                } else {
                    None
                },
            )));
        });
        self.tasks.lock().unwrap().push(task.abort_handle());
        Ok(json!({"terminalId": id}))
    }
}
