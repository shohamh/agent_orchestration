//! Extra processes the runner keeps alive next to the execution service: any engine worker, generically.
//!
//! A component is a name and a command in `<home>/components.json`:
//!
//!     {"components": [{"name": "hatchet", "command": ["python", "worker.py"], "cwd": "C:/work", "env": {"KEY": "value"}}]}
//!
//! The runner starts every component, restarts it when it exits (1, 2, 5, 10, then 30 seconds apart; the delay resets
//! after a minute of uptime) and reloads the file while running. `agent-worker stop NAME` and `start NAME` work through
//! files in `<home>/control/` (`NAME.stop` keeps it stopped, `NAME.restart` restarts it once), so they need no network
//! and behave the same on every OS. What each component is doing is written to `<home>/runtime/components.json`.
use crate::config::Home;
use crate::procutil::{kill_tree, pid_alive, prepare_child};
use crate::util::{atomic_json, mono, now_unix, read_json};
use serde_json::{json, Map, Value};
use std::collections::HashMap;
use std::fs;
use std::process::Stdio;
use std::time::Duration;
use tokio::process::{Child, Command};

const BACKOFF: [u64; 5] = [1, 2, 5, 10, 30];
const STABLE_AFTER: f64 = 60.0;
const RESERVED: [&str; 2] = ["execution", "connector"];

pub fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name
            .chars()
            .next()
            .map(|c| c.is_ascii_alphanumeric())
            .unwrap_or(false)
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

pub fn validate(entry: &Value) -> Result<Value, String> {
    let object = entry.as_object().ok_or("A component is an object")?;
    let name = object
        .get("name")
        .and_then(|n| n.as_str())
        .filter(|n| valid_name(n))
        .ok_or("A component name is 1 to 64 letters, digits, dashes or underscores")?;
    if RESERVED.contains(&name) {
        return Err(format!("{name} is reserved for the worker itself"));
    }
    let command = object
        .get("command")
        .and_then(|c| c.as_array())
        .filter(|c| {
            !c.is_empty()
                && c.iter()
                    .all(|x| x.as_str().map(|s| !s.is_empty()).unwrap_or(false))
        })
        .ok_or("A component needs a command: a non-empty list of strings")?;
    let mut out = json!({"name": name, "command": command});
    if let Some(env) = object.get("env") {
        let env = env
            .as_object()
            .filter(|e| e.values().all(|v| v.is_string()))
            .ok_or("env must map names to strings")?;
        if !env.is_empty() {
            out["env"] = Value::Object(env.clone());
        }
    }
    if let Some(cwd) = object.get("cwd") {
        out["cwd"] = json!(cwd.as_str().ok_or("cwd must be a path")?);
    }
    Ok(out)
}

pub fn load(home: &Home) -> Result<Vec<Value>, String> {
    let path = home.file("components.json");
    if !path.exists() {
        return Ok(vec![]);
    }
    let data = read_json(&path).ok_or("components.json is unreadable")?;
    let entries = data
        .get("components")
        .and_then(|c| c.as_array())
        .ok_or("components.json must hold {\"components\": [...]}")?;
    let mut seen = std::collections::HashSet::new();
    let mut out = vec![];
    for entry in entries {
        let entry = validate(entry)?;
        if !seen.insert(entry["name"].as_str().unwrap().to_string()) {
            return Err(format!("Duplicate component {}", entry["name"]));
        }
        out.push(entry);
    }
    Ok(out)
}

fn save(home: &Home, entries: &[Value]) -> Result<(), String> {
    atomic_json(
        &home.file("components.json"),
        &json!({"components": entries}),
    )
    .map_err(|e| e.to_string())
}

pub fn add(home: &Home, entry: &Value) -> Result<Value, String> {
    let entry = validate(entry)?;
    let mut entries = load(home)?;
    if entries.iter().any(|e| e["name"] == entry["name"]) {
        return Err(format!(
            "{} already exists; remove it first",
            entry["name"].as_str().unwrap()
        ));
    }
    entries.push(entry.clone());
    save(home, &entries)?;
    Ok(entry)
}

pub fn remove(home: &Home, name: &str) -> Result<(), String> {
    let entries = load(home)?;
    if !entries.iter().any(|e| e["name"] == name) {
        return Err(format!("No component named {name}"));
    }
    save(
        home,
        &entries
            .into_iter()
            .filter(|e| e["name"] != name)
            .collect::<Vec<_>>(),
    )?;
    for suffix in ["stop", "restart"] {
        let _ = fs::remove_file(home.file("control").join(format!("{name}.{suffix}")));
    }
    Ok(())
}

/// stop: keep it stopped. start: lift a stop. restart: restart once.
pub fn control(home: &Home, name: &str, action: &str) -> Result<(), String> {
    if !load(home)?.iter().any(|e| e["name"] == name) {
        return Err(format!("No component named {name}"));
    }
    let dir = home.file("control");
    fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
    let (stop, restart) = (
        dir.join(format!("{name}.stop")),
        dir.join(format!("{name}.restart")),
    );
    match action {
        "stop" => fs::write(stop, "1").map_err(|e| e.to_string()),
        "start" => {
            let _ = fs::remove_file(stop);
            // a component that exited with auto restart off is only started again by an explicit command
            if read_status(home)[name]["state"] == "exited" {
                fs::write(restart, "1").map_err(|e| e.to_string())?;
            }
            Ok(())
        }
        "restart" => {
            let _ = fs::remove_file(stop);
            fs::write(restart, "1").map_err(|e| e.to_string())
        }
        other => Err(format!("Unknown action {other}")),
    }
}

pub fn read_status(home: &Home) -> Value {
    read_json(&home.file("runtime").join("components.json")).unwrap_or(json!({}))
}

struct Running {
    child: Child,
    since: f64,
    entry: Value,
}

pub struct Supervisor {
    home: Home,
    running: HashMap<String, Running>,
    state: Map<String, Value>,
    backoff: HashMap<String, (u32, f64)>,
    /// exited while auto restart is off: stays down until `agent-worker restart NAME`
    halted: std::collections::HashSet<String>,
}

impl Supervisor {
    pub fn new(home: Home) -> Supervisor {
        Supervisor {
            home,
            running: HashMap::new(),
            state: Map::new(),
            backoff: HashMap::new(),
            halted: std::collections::HashSet::new(),
        }
    }

    fn write_status(&self) {
        let _ = atomic_json(
            &self.home.file("runtime").join("components.json"),
            &Value::Object(self.state.clone()),
        );
    }

    /// A previous runner that was killed leaves its components behind: stop them before starting new ones.
    fn reap_stale(&self) {
        if let Some(status) = read_status(&self.home).as_object() {
            for (name, info) in status {
                let pid = info.get("pid").and_then(|p| p.as_u64()).unwrap_or(0) as u32;
                if pid != 0 && info["state"] == "running" && pid_alive(pid) {
                    crate::log!(
                        "components",
                        "stopping {name} (pid {pid}) left behind by an earlier runner"
                    );
                    kill_tree(pid, Duration::from_secs(10));
                }
            }
        }
    }

    async fn start_one(&mut self, entry: &Value) -> Result<(), String> {
        let name = entry["name"].as_str().unwrap().to_string();
        let argv: Vec<String> = entry["command"]
            .as_array()
            .unwrap()
            .iter()
            .map(|a| a.as_str().unwrap().to_string())
            .collect();
        let log = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.home.file(&format!("component-{name}.log")))
            .map_err(|e| e.to_string())?;
        let mut command = Command::new(&argv[0]);
        command
            .args(&argv[1..])
            .stdin(Stdio::null())
            .stdout(log.try_clone().map_err(|e| e.to_string())?)
            .stderr(log);
        command.current_dir(
            entry
                .get("cwd")
                .and_then(|c| c.as_str())
                .map(std::path::PathBuf::from)
                .unwrap_or_else(|| self.home.0.clone()),
        );
        if let Some(env) = entry.get("env").and_then(|e| e.as_object()) {
            for (k, v) in env {
                command.env(k, v.as_str().unwrap_or(""));
            }
        }
        prepare_child(&mut command);
        let child = command.spawn().map_err(|e| e.to_string())?;
        let pid = child.id().unwrap_or(0);
        let old = self.state.get(&name).cloned().unwrap_or(json!({}));
        self.state.insert(name.clone(), json!({"state": "running", "pid": pid, "restarts": old.get("restarts").cloned().unwrap_or(json!(0)), "since": now_unix(), "last_exit": old.get("last_exit")}));
        self.running.insert(
            name.clone(),
            Running {
                child,
                since: mono(),
                entry: entry.clone(),
            },
        );
        crate::log!("components", "started component {name} (pid {pid})");
        Ok(())
    }

    async fn stop_one(&mut self, name: &str, reason: &str) {
        let Some(mut running) = self.running.remove(name) else {
            return;
        };
        if let Some(pid) = running.child.id() {
            tokio::task::spawn_blocking(move || kill_tree(pid, Duration::from_secs(10)))
                .await
                .ok();
        }
        let _ = tokio::time::timeout(Duration::from_secs(5), running.child.wait()).await;
        let old = self.state.get(name).cloned().unwrap_or(json!({}));
        let mut next = old.as_object().cloned().unwrap_or_default();
        next.insert("state".into(), json!("stopped"));
        next.insert("pid".into(), Value::Null);
        next.insert("reason".into(), json!(reason));
        self.state.insert(name.to_string(), Value::Object(next));
        crate::log!("components", "stopped component {name} ({reason})");
    }

    async fn tick(&mut self) {
        let entries: HashMap<String, Value> = match load(&self.home) {
            Ok(list) => list
                .into_iter()
                .map(|e| (e["name"].as_str().unwrap().to_string(), e))
                .collect(),
            Err(error) => {
                crate::log!("components", "{error}");
                self.running
                    .iter()
                    .map(|(k, v)| (k.clone(), v.entry.clone()))
                    .collect() // keep what runs when the file is broken
            }
        };
        for name in self
            .running
            .keys()
            .filter(|n| !entries.contains_key(*n))
            .cloned()
            .collect::<Vec<_>>()
        {
            self.stop_one(&name, "removed").await;
        }
        self.state
            .retain(|name, _| entries.contains_key(name) || self.running.contains_key(name));
        let control = self.home.file("control");
        for (name, entry) in &entries {
            let (stop, restart) = (
                control.join(format!("{name}.stop")),
                control.join(format!("{name}.restart")),
            );
            if stop.exists() {
                self.stop_one(name, "stopped by command").await;
                let mut next = self
                    .state
                    .get(name)
                    .and_then(|s| s.as_object().cloned())
                    .unwrap_or_default();
                next.insert("state".into(), json!("stopped"));
                next.insert("pid".into(), Value::Null);
                next.insert("reason".into(), json!("stopped by command"));
                self.state.insert(name.clone(), Value::Object(next));
                continue;
            }
            if restart.exists() {
                let _ = fs::remove_file(&restart);
                self.stop_one(name, "restart requested").await;
                self.backoff.remove(name);
                self.halted.remove(name);
            }
            if self
                .running
                .get(name)
                .map(|r| &r.entry != entry)
                .unwrap_or(false)
            {
                self.stop_one(name, "configuration changed").await; // edited while running
            }
            let exited = match self.running.get_mut(name) {
                Some(r) => r
                    .child
                    .try_wait()
                    .ok()
                    .flatten()
                    .map(|status| (status.code().unwrap_or(-1), r.since)),
                None => None,
            };
            if let Some((code, _)) = exited.filter(|_| !crate::config::auto_restart(&self.home)) {
                self.running.remove(name);
                self.halted.insert(name.clone());
                let old = self
                    .state
                    .get(name)
                    .and_then(|s| s.as_object().cloned())
                    .unwrap_or_default();
                self.state.insert(name.clone(), json!({"state": "exited", "pid": null, "restarts": old.get("restarts").cloned().unwrap_or(json!(0)), "last_exit": code}));
                crate::log!("components", "component {name} exited with {code}; auto restart is off, so it stays down until: agent-worker restart {name}");
                continue;
            }
            if let Some((code, since)) = exited {
                self.running.remove(name);
                let (failures, _) = self.backoff.get(name).copied().unwrap_or((0, 0.0));
                let failures = if mono() - since > STABLE_AFTER {
                    0
                } else {
                    failures
                };
                let delay = BACKOFF[(failures as usize).min(BACKOFF.len() - 1)];
                self.backoff
                    .insert(name.clone(), (failures + 1, mono() + delay as f64));
                let old = self
                    .state
                    .get(name)
                    .and_then(|s| s.as_object().cloned())
                    .unwrap_or_default();
                let restarts = old.get("restarts").and_then(|r| r.as_u64()).unwrap_or(0) + 1;
                self.state.insert(name.clone(), json!({"state": "crashed", "pid": null, "restarts": restarts, "last_exit": code, "retry_in": delay}));
                crate::log!(
                    "components",
                    "component {name} exited with {code}; restarting in {delay} s"
                );
            }
            if !self.running.contains_key(name) {
                if self.halted.contains(name) {
                    continue;
                }
                if mono() < self.backoff.get(name).map(|b| b.1).unwrap_or(0.0) {
                    continue;
                }
                if let Err(error) = self.start_one(entry).await {
                    let failures = self.backoff.get(name).map(|b| b.0).unwrap_or(0);
                    let delay = BACKOFF[(failures as usize).min(BACKOFF.len() - 1)];
                    self.backoff
                        .insert(name.clone(), (failures + 1, mono() + delay as f64));
                    self.state.insert(name.clone(), json!({"state": "crashed", "pid": null, "error": error.chars().take(200).collect::<String>(), "retry_in": delay}));
                    crate::log!("components", "could not start component {name}: {error}");
                }
            }
        }
        self.write_status();
    }

    pub fn reap(&self) {
        self.reap_stale();
    }

    /// Stop every component (the worker is stopping).
    pub async fn stop_all(&mut self) {
        for name in self.running.keys().cloned().collect::<Vec<_>>() {
            self.stop_one(&name, "worker stopping").await;
        }
        self.write_status();
    }
}

/// Keep the configured components running until the task is dropped.
pub async fn supervise(supervisor: std::sync::Arc<tokio::sync::Mutex<Supervisor>>) {
    supervisor.lock().await.reap();
    loop {
        supervisor.lock().await.tick().await;
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
