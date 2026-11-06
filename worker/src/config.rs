//! The install folder: worker.json, the token, the binary's own version and the files the commands share.
use crate::util::{atomic_json, read_json};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

pub struct Home(pub PathBuf);

impl Home {
    /// `--home`, or the folder the binary lives in (the installer puts the binary at the top of the install folder).
    pub fn resolve(explicit: Option<PathBuf>) -> Home {
        if let Some(p) = explicit.or_else(|| std::env::var_os("AGENT_WORKER_HOME").map(PathBuf::from)) {
            return Home(p);
        }
        let exe = std::env::current_exe().and_then(|p| p.canonicalize()).unwrap_or_else(|_| PathBuf::from("."));
        Home(exe.parent().map(Path::to_path_buf).unwrap_or_else(|| PathBuf::from(".")))
    }

    pub fn file(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }

    pub fn config(&self) -> Result<Value, String> {
        let path = self.file("worker.json");
        if !path.exists() {
            return Err(format!("No worker is installed in {}", self.0.display()));
        }
        read_json(&path).filter(|v| v.is_object()).ok_or_else(|| "worker.json is unreadable".to_string())
    }

    pub fn save_config(&self, config: &Value) -> std::io::Result<()> {
        atomic_json(&self.file("worker.json"), config)
    }

    pub fn token(&self) -> String {
        std::fs::read_to_string(self.file("token")).map(|t| t.trim().to_string()).unwrap_or_default()
    }

    pub fn pid(&self) -> u32 {
        std::fs::read_to_string(self.file("worker.pid")).ok().and_then(|t| t.trim().parse().ok()).unwrap_or(0)
    }

    pub fn exe_name() -> &'static str {
        if cfg!(windows) { "agent-worker.exe" } else { "agent-worker" }
    }

    pub fn exe(&self) -> PathBuf {
        self.file(Self::exe_name())
    }
}

pub fn port_of(config: &Value) -> u16 {
    config.get("port").and_then(|p| p.as_u64()).unwrap_or(8096) as u16
}

pub fn os_name() -> &'static str {
    if cfg!(windows) { "windows" } else if cfg!(target_os = "macos") { "macos" } else { "linux" }
}

/// The worker version: the first 16 hex digits of the SHA-256 of this very binary. The server computes the same hash
/// of the file it serves, so "which version is installed" never depends on a separate version file.
pub fn self_version() -> String {
    static VERSION: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    VERSION
        .get_or_init(|| std::env::current_exe().ok().and_then(|p| std::fs::read(p).ok()).map(|bytes| file_version(&bytes)).unwrap_or_default())
        .clone()
}

pub fn file_version(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))[..16].to_string()
}

/// worker.json: `name` is what the owner chose (unique in the fleet); `uuid` is generated on this PC at install and
/// identifies it to the relay and the registry.
pub fn default_config(name: &str, uuid: &str, slots: u64, server: &str, port: u16) -> Value {
    json!({"name": name, "uuid": uuid, "slots": slots, "server": server, "port": port, "service": "process", "unit": format!("agent-worker-{name}")})
}

pub fn name_of(config: &Value) -> String {
    config["name"].as_str().or_else(|| config["id"].as_str()).unwrap_or("worker").to_string()
}

/// The key the relay and the registry know this PC by: its uuid (older installs used the name).
pub fn relay_id(config: &Value) -> String {
    config["uuid"].as_str().map(String::from).unwrap_or_else(|| name_of(config))
}
