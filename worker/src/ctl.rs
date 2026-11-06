//! The `agent-worker` management commands: status, start, stop, restart, update, uninstall, logs, version, component.
use crate::components;
use crate::config::{self, port_of, Home};
use crate::svc::Svc;
use crate::util::{atomic_json, now_unix, read_json};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::time::{Duration, Instant};

pub const DRAIN_TIMEOUT: u64 = 600;
const HEALTH_TIMEOUT: u64 = 90;

fn client() -> reqwest::Client {
    reqwest::Client::builder().connect_timeout(Duration::from_secs(10)).build().unwrap()
}

/// The version of the worker installed in this folder (not of the program that happens to be running this command).
fn installed_version(home: &Home) -> String {
    std::fs::read(home.exe()).map(|bytes| config::file_version(&bytes)).unwrap_or_else(|_| config::self_version())
}

pub async fn local_health(home: &Home, config: &Value) -> Option<Value> {
    let response = client().get(format!("http://127.0.0.1:{}/health", port_of(config))).bearer_auth(home.token()).timeout(Duration::from_secs(5)).send().await.ok()?;
    if response.status().is_success() { response.json().await.ok() } else { None }
}

pub async fn status(home: &Home, json_output: bool) -> Result<i32, String> {
    let config = home.config()?;
    let svc = Svc::from_config(Home(home.0.clone()), &config);
    let running = svc.running();
    let health = if running { local_health(home, &config).await } else { None };
    let connector = read_json(&home.file("runtime").join("connector.json")).unwrap_or(json!({}));
    let fresh = now_unix() - connector["at"].as_f64().unwrap_or(0.0) < 30.0;
    let connected = running && fresh && connector["connected"] == true;
    let components = components::read_status(home);
    let update = read_json(&home.file("update-status.json"));
    let active = health.as_ref().and_then(|h| h["active"].as_u64());
    let draining = health.as_ref().map(|h| h["draining"] == true).unwrap_or(false);
    if json_output {
        println!("{}", serde_json::to_string_pretty(&json!({"name": config::name_of(&config), "uuid": config::relay_id(&config), "version": installed_version(home), "home": home.0, "server": config["server"], "service": svc.describe(),
            "service_exists": svc.exists(), "starts_at_boot": svc.starts_at_boot(), "running": running, "pid": home.pid(), "serving": health.is_some(), "active_jobs": active, "draining": draining, "connected": connected,
            "components": components, "update": update})).unwrap());
    } else {
        println!("Worker      {}  ({})  version {}", config::name_of(&config), config::relay_id(&config), installed_version(home));
        println!("Service     {}", svc.describe());
        let mut state = format!("State       {}", if running { "running" } else { "stopped" });
        if running && home.pid() != 0 && svc.kind != crate::svc::Kind::Systemd {
            state += &format!(" (pid {})", home.pid());
        }
        if let Some(n) = active.filter(|n| *n > 0) {
            state += &format!(", {n} job(s) running");
        }
        if draining {
            state += ", draining for an update";
        }
        println!("{state}");
        println!("Server      {}  {}", config["server"].as_str().unwrap_or("?"), if connected { "connected" } else { "not connected" });
        if let Some(map) = components.as_object() {
            let mut names: Vec<_> = map.keys().collect();
            names.sort();
            for name in names {
                let info = &map[name];
                println!("Component   {name}: {}{}", info["state"].as_str().unwrap_or("?"), info["pid"].as_u64().map(|p| format!(" (pid {p})")).unwrap_or_default());
            }
        }
        if let Some(u) = update {
            println!("Last update {}{}", u["state"].as_str().unwrap_or("?"), u["error"].as_str().map(|e| format!(": {e}")).unwrap_or_default());
        }
    }
    Ok(if running { 0 } else { 3 })
}

pub fn start(home: &Home, name: Option<&str>) -> Result<(), String> {
    if let Some(name) = name {
        components::control(home, name, "start")?;
        println!("Component {name} will start within a few seconds");
        return Ok(());
    }
    let config = home.config()?;
    let svc = Svc::from_config(Home(home.0.clone()), &config);
    if !svc.exists() {
        return Err(format!("The startup entry is missing ({}); reinstall with a new install command", svc.describe()));
    }
    svc.start()?;
    println!("Worker started");
    Ok(())
}

pub fn stop(home: &Home, name: Option<&str>) -> Result<(), String> {
    if let Some(name) = name {
        components::control(home, name, "stop")?;
        println!("Component {name} will stop within a few seconds");
        return Ok(());
    }
    Svc::from_config(Home(home.0.clone()), &home.config()?).stop()?;
    println!("Worker stopped");
    Ok(())
}

pub fn restart(home: &Home, name: Option<&str>) -> Result<(), String> {
    if let Some(name) = name {
        components::control(home, name, "restart")?;
        println!("Component {name} will restart within a few seconds");
        return Ok(());
    }
    let svc = Svc::from_config(Home(home.0.clone()), &home.config()?);
    svc.stop()?;
    svc.start()?;
    println!("Worker restarted");
    Ok(())
}

pub fn logs(home: &Home, lines: usize) -> Result<(), String> {
    let text = std::fs::read_to_string(home.file("worker.log")).map_err(|_| "There is no log yet".to_string())?;
    let all: Vec<&str> = text.lines().collect();
    println!("{}", all[all.len().saturating_sub(lines)..].join("\n"));
    Ok(())
}

pub fn component(home: &Home, action: &str, name: Option<&str>, cwd: Option<String>, env: Vec<String>, argv: Vec<String>) -> Result<(), String> {
    match action {
        "list" => {
            let entries = components::load(home)?;
            let status = components::read_status(home);
            if entries.is_empty() {
                println!("No components. Add one with: agent-worker component add NAME -- COMMAND ...");
            }
            for e in entries {
                let n = e["name"].as_str().unwrap();
                println!("{n}: {}  {}", status[n]["state"].as_str().unwrap_or("not started yet"), e["command"].as_array().unwrap().iter().map(|a| a.as_str().unwrap()).collect::<Vec<_>>().join(" "));
            }
        }
        "add" => {
            let name = name.ok_or("A component name is required")?;
            let mut entry = json!({"name": name, "command": argv});
            if let Some(cwd) = cwd {
                entry["cwd"] = json!(cwd);
            }
            if !env.is_empty() {
                let mut map = serde_json::Map::new();
                for item in env {
                    let (k, v) = item.split_once('=').filter(|(k, _)| !k.is_empty()).ok_or_else(|| format!("--env wants NAME=VALUE, got {item:?}"))?;
                    map.insert(k.into(), json!(v));
                }
                entry["env"] = Value::Object(map);
            }
            let added = components::add(home, &entry)?;
            println!("Added {}: the worker starts it within a few seconds and restarts it if it exits", added["name"].as_str().unwrap());
        }
        "remove" => {
            let name = name.ok_or("A component name is required")?;
            components::remove(home, name)?;
            println!("Removed {name}");
        }
        other => return Err(format!("Unknown component action {other}")),
    }
    Ok(())
}

// --------------------------------------------------------------------------- update
fn write_update_status(home: &Home, state: &str, extra: Value) {
    let mut data = json!({"state": state, "at": now_unix()});
    if let (Some(map), Some(extra)) = (data.as_object_mut(), extra.as_object()) {
        map.extend(extra.clone());
    }
    let _ = atomic_json(&home.file("update-status.json"), &data);
}

/// Ask the service to stop admitting work and wait for running jobs (up to `wait` seconds). True when idle.
async fn drain(home: &Home, config: &Value, on: bool, wait: u64) -> bool {
    let url = format!("http://127.0.0.1:{}/drain", port_of(config));
    let deadline = Instant::now() + Duration::from_secs(wait);
    loop {
        let response = client().post(&url).bearer_auth(home.token()).json(&json!({"drain": on})).timeout(Duration::from_secs(10)).send().await;
        let Ok(response) = response else { return true }; // the service is not running: nothing to wait for
        if !response.status().is_success() || !on {
            return response.status().is_success();
        }
        let active = response.json::<Value>().await.ok().and_then(|b| b["active"].as_u64()).unwrap_or(0);
        if active == 0 {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

async fn wait_serving(home: &Home, config: &Value, version: &str) -> bool {
    let deadline = Instant::now() + Duration::from_secs(HEALTH_TIMEOUT);
    while Instant::now() < deadline {
        if let Some(h) = local_health(home, config).await {
            if h["worker_version"].as_str() == Some(version) {
                return true;
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    false
}

/// Replace the binary with `new_bytes` (already verified): drain, stop, swap, start, check; roll back on failure.
pub async fn apply_update(home: &Home, config: &Value, new_bytes: Vec<u8>, drain_timeout: u64) -> Result<String, String> {
    let version = config::file_version(&new_bytes);
    let svc = Svc::from_config(Home(home.0.clone()), config);
    let was_running = svc.running();
    if was_running {
        println!("Waiting up to {drain_timeout} s for running jobs to finish");
        if !drain(home, config, true, drain_timeout).await {
            drain(home, config, false, 0).await;
            return Err(format!("Jobs were still running after {drain_timeout} s; nothing was changed. Try again later or use --drain-timeout"));
        }
    }
    println!("Stopping the worker");
    svc.stop()?;
    let exe = home.exe();
    let (staged, old) = (home.file(&format!("{}.new", Home::exe_name())), home.file(&format!("{}.old", Home::exe_name())));
    let _ = std::fs::remove_file(&old);
    std::fs::write(&staged, &new_bytes).map_err(|e| format!("Could not write the new worker: {e}"))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&staged, std::fs::Permissions::from_mode(0o755));
    }
    // A running program cannot be overwritten on Windows but it can be renamed, so the old one moves aside first.
    std::fs::rename(&exe, &old).map_err(|e| format!("Could not move the old worker aside: {e}"))?;
    if let Err(e) = std::fs::rename(&staged, &exe) {
        let _ = std::fs::rename(&old, &exe);
        return Err(format!("Could not install the new worker: {e}"));
    }
    if was_running {
        println!("Starting the new worker");
        let started = svc.start();
        if started.is_err() || !wait_serving(home, config, &version).await {
            let why = started.err().unwrap_or_else(|| format!("The new worker did not become healthy within {HEALTH_TIMEOUT} s; see {}", home.file("worker.log").display()));
            println!("Update failed ({why}); restoring the previous worker");
            let _ = svc.stop();
            let _ = std::fs::remove_file(&exe);
            let _ = std::fs::rename(&old, &exe);
            let _ = svc.start();
            return Err(why);
        }
    }
    let _ = std::fs::remove_file(&old);
    Ok(version)
}

pub async fn update(home: &Home, force: bool, drain_timeout: u64, from_file: Option<String>, link: Option<(String, String)>) -> Result<(), String> {
    let config = home.config()?;
    let started = now_unix();
    let result = async {
        let bytes = match (&from_file, &link) {
            (Some(path), _) => std::fs::read(path).map_err(|e| e.to_string())?,
            // An update link: this program is the new worker, so it installs itself over the old one.
            (None, Some(_)) => std::fs::read(std::env::current_exe().map_err(|e| e.to_string())?).map_err(|e| e.to_string())?,
            (None, None) => {
                let server = config["server"].as_str().ok_or("worker.json has no server address")?.trim_end_matches('/').to_string();
                let id = config::relay_id(&config);
                println!("Fetching the current worker from {server}");
                let response = client().get(format!("{server}/relay/{id}/update/binary")).query(&[("os", config::os_name()), ("arch", "x86_64")]).bearer_auth(home.token()).timeout(Duration::from_secs(300)).send().await.map_err(|e| format!("Could not reach the server: {e}"))?;
                if !response.status().is_success() {
                    return Err(format!("The server refused the download (HTTP {}): this PC may no longer be registered or the server has no worker for this OS", response.status().as_u16()));
                }
                let expected = response.headers().get("x-worker-version").and_then(|v| v.to_str().ok()).map(String::from);
                let bytes = response.bytes().await.map_err(|e| e.to_string())?.to_vec();
                if let Some(expected) = expected {
                    let actual = config::file_version(&bytes);
                    if actual != expected {
                        return Err(format!("The downloaded worker does not match its version (expected {expected}, got {actual})"));
                    }
                }
                bytes
            }
        };
        let version = config::file_version(&bytes);
        let installed = std::fs::read(home.exe()).map(|b| config::file_version(&b)).unwrap_or_default();
        if version == installed && !force {
            println!("Already up to date (version {version})");
            write_update_status(home, "updated", json!({"version": version, "started_at": started}));
            return Ok(());
        }
        println!("Updating {} -> {version}", if installed.is_empty() { "unknown" } else { &installed });
        write_update_status(home, "running", json!({"version": version, "started_at": started}));
        let applied = apply_update(home, &config, bytes, drain_timeout).await?;
        write_update_status(home, "updated", json!({"version": applied, "started_at": started}));
        println!("Updated to {applied}");
        Ok(())
    }
    .await;
    if let Err(error) = &result {
        write_update_status(home, "failed", json!({"error": error.chars().take(300).collect::<String>(), "started_at": started}));
    }
    if let Some((server, code)) = link {
        let body = match &result { Ok(()) => json!({"ok": true}), Err(e) => json!({"ok": false, "error": e.chars().take(300).collect::<String>()}) };
        let _ = client().post(format!("{}/update/{code}/done", server.trim_end_matches('/'))).json(&body).timeout(Duration::from_secs(10)).send().await;
    }
    result
}

// --------------------------------------------------------------------------- uninstall
fn remove_cli(config: &Value) {
    for link in config["cli_links"].as_array().into_iter().flatten().filter_map(|l| l.as_str()) {
        let _ = std::fs::remove_file(link);
    }
    #[cfg(windows)]
    if let Some(entry) = config["path_entry"].as_str() {
        let script = format!("$e = [Environment]::GetEnvironmentVariable('Path','User'); if ($e) {{ $p = ($e -split ';') | Where-Object {{ $_ -and ($_ -ne '{}') }}; [Environment]::SetEnvironmentVariable('Path', ($p -join ';'), 'User') }}", entry.replace('\'', "''"));
        let _ = crate::svc::run("powershell", &["-NoProfile", "-NonInteractive", "-Command", &script]);
    }
}

pub async fn uninstall(home: &Home, yes: bool, keep_registration: bool) -> Result<(), String> {
    let config = home.config()?;
    let id = config::name_of(&config);
    let relay = config::relay_id(&config);
    if !yes {
        print!("Remove worker {id} from this PC and from the fleet? [y/N] ");
        use std::io::Write;
        std::io::stdout().flush().ok();
        let mut answer = String::new();
        std::io::stdin().read_line(&mut answer).ok();
        if !["y", "yes"].contains(&answer.trim().to_lowercase().as_str()) {
            println!("Nothing was changed");
            return Err(String::new());
        }
    }
    let mut registered = false;
    if !keep_registration {
        if let Some(server) = config["server"].as_str() {
            match client().delete(format!("{}/relay/{relay}/registration", server.trim_end_matches('/'))).bearer_auth(home.token()).timeout(Duration::from_secs(20)).send().await {
                Ok(r) => {
                    registered = r.status().as_u16() == 200;
                    if ![200, 401, 404].contains(&r.status().as_u16()) {
                        println!("Warning: the server answered {} when removing this PC from the fleet; remove it in the Fleet tab", r.status().as_u16());
                    }
                }
                Err(e) => println!("Warning: could not reach the server to remove this PC from the fleet ({e}); remove it in the Fleet tab"),
            }
        }
    }
    let svc = Svc::from_config(Home(home.0.clone()), &config);
    if let Err(e) = svc.stop() {
        println!("Warning: {e}");
    }
    svc.remove();
    remove_cli(&config);
    #[cfg(windows)]
    {
        // This program runs from inside the folder: a detached helper removes it once we have exited.
        use std::os::windows::process::CommandExt;
        let _ = std::process::Command::new("cmd").raw_arg(format!("/c ping -n 4 127.0.0.1 >nul & rmdir /s /q \"{}\"", home.0.display())).creation_flags(crate::procutil::DETACHED_PROCESS | crate::procutil::CREATE_NEW_PROCESS_GROUP | crate::procutil::CREATE_NO_WINDOW).stdin(std::process::Stdio::null()).stdout(std::process::Stdio::null()).stderr(std::process::Stdio::null()).spawn();
    }
    #[cfg(not(windows))]
    {
        let _ = std::fs::remove_dir_all(&home.0);
    }
    println!("Worker {id} removed from this PC{}", if keep_registration { "" } else if registered { " and from the fleet" } else { "" });
    Ok(())
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

/// `agent-worker boot [enable|disable|status]`: start with the PC, before anyone logs in.
pub fn boot(home: &Home, action: &str) -> Result<(), String> {
    let mut config = home.config()?;
    let mut svc = Svc::from_config(Home(home.0.clone()), &config);
    match action {
        "status" => {
            println!("{}", if svc.starts_at_boot() { "Starts when the PC starts, even before anyone logs in" } else { "Starts when you log in (run `agent-worker boot enable` to start with the PC)" });
            Ok(())
        }
        "enable" | "disable" => {
            let enable = action == "enable";
            svc.set_boot(enable)?;
            config["boot"] = serde_json::json!(enable);
            home.save_config(&config).map_err(|e| e.to_string())?;
            if enable && !svc.running() {
                svc.start()?;
            }
            println!("{}", if enable { "The worker now starts when the PC starts, even before anyone logs in" } else { "The worker starts when you log in again" });
            Ok(())
        }
        other => Err(format!("Unknown boot action {other:?}; use enable, disable or status")),
    }
}
