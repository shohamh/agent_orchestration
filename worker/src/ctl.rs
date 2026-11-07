//! The `agent-worker` management commands: status, start, stop, restart, update, uninstall, logs, version, component.
use crate::components;
use crate::config::{self, port_of, Home};
use crate::connector::Revocation;
use crate::svc::Svc;
use crate::util::{atomic_json, now_unix, read_json};
use serde_json::{json, Value};
use std::time::{Duration, Instant};

pub const DRAIN_TIMEOUT: u64 = 600;
const HEALTH_TIMEOUT: u64 = 90;

fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .build()
        .unwrap()
}

const REVOCATION_REMEDIATION: &str =
    "Run `agent-worker uninstall`, then install again using a new Fleet install command.";

fn connection_is_current(running: bool, fresh: bool, connector: &Value, revoked: bool) -> bool {
    running && fresh && connector["connected"] == true && !revoked
}

fn revocation_json_fields(revocation: Option<&Revocation>) -> Value {
    match revocation {
        Some(revocation) => {
            json!({"revoked": true, "revocation": revocation.to_json(), "remediation": REVOCATION_REMEDIATION})
        }
        None => json!({"revoked": false, "revocation": null, "remediation": null}),
    }
}

fn revocation_human_output(revocation: Option<&Revocation>) -> String {
    match revocation {
        Some(revocation) => format!(
            "Fleet       Worker unregistered from the Fleet\nMessage     {}\nRecovery    {}\n",
            revocation.message, REVOCATION_REMEDIATION
        ),
        None => String::new(),
    }
}

/// The version of the worker installed in this folder (not of the program that happens to be running this command).
fn installed_version(home: &Home) -> String {
    std::fs::read(home.exe())
        .map(|bytes| config::file_version(&bytes))
        .unwrap_or_else(|_| config::self_version())
}

pub async fn local_health(home: &Home, config: &Value) -> Option<Value> {
    let response = client()
        .get(format!("http://127.0.0.1:{}/health", port_of(config)))
        .bearer_auth(home.token())
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .ok()?;
    if response.status().is_success() {
        response.json().await.ok()
    } else {
        None
    }
}

pub async fn status(home: &Home, json_output: bool) -> Result<i32, String> {
    let config = home.config()?;
    let svc = Svc::from_config(Home(home.0.clone()), &config);
    let running = svc.running();
    let health = if running {
        local_health(home, &config).await
    } else {
        None
    };
    let connector = read_json(&home.file("runtime").join("connector.json")).unwrap_or(json!({}));
    let fresh = now_unix() - connector["at"].as_f64().unwrap_or(0.0) < 30.0;
    let revocation = Revocation::from_persisted(&connector["revocation"]);
    let connected = connection_is_current(running, fresh, &connector, revocation.is_some());
    let components = components::read_status(home);
    let update = read_json(&home.file("update-status.json"));
    let active = health.as_ref().and_then(|h| h["active"].as_u64());
    let draining = health
        .as_ref()
        .map(|h| h["draining"] == true)
        .unwrap_or(false);
    if json_output {
        let mut output = json!({"name": config::name_of(&config), "uuid": config::relay_id(&config), "version": installed_version(home), "home": home.0, "server": config["server"], "service": svc.describe(),
            "service_exists": svc.exists(), "starts_at_boot": svc.starts_at_boot(), "autostart": svc.autostart(), "auto_restart": svc.kind != crate::svc::Kind::Process && svc.auto_restart, "running": running, "pid": home.pid(), "serving": health.is_some(), "active_jobs": active, "draining": draining, "connected": connected,
            "components": components, "update": update});
        let revocation_fields = revocation_json_fields(revocation.as_ref());
        output
            .as_object_mut()
            .unwrap()
            .extend(revocation_fields.as_object().unwrap().clone());
        println!("{}", serde_json::to_string_pretty(&output).unwrap());
    } else {
        println!(
            "Worker      {}  ({})  version {}",
            config::name_of(&config),
            config::relay_id(&config),
            installed_version(home)
        );
        println!("Service     {}", svc.describe());
        let mut state = format!(
            "State       {}",
            if running { "running" } else { "stopped" }
        );
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
        println!(
            "Server      {}  {}",
            config["server"].as_str().unwrap_or("?"),
            if connected {
                "connected"
            } else {
                "not connected"
            }
        );
        print!("{}", revocation_human_output(revocation.as_ref()));
        if let Some(map) = components.as_object() {
            let mut names: Vec<_> = map.keys().collect();
            names.sort();
            for name in names {
                let info = &map[name];
                println!(
                    "Component   {name}: {}{}",
                    info["state"].as_str().unwrap_or("?"),
                    info["pid"]
                        .as_u64()
                        .map(|p| format!(" (pid {p})"))
                        .unwrap_or_default()
                );
            }
        }
        if let Some(u) = update {
            println!(
                "Last update {}{}",
                u["state"].as_str().unwrap_or("?"),
                u["error"]
                    .as_str()
                    .map(|e| format!(": {e}"))
                    .unwrap_or_default()
            );
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
        return Err(format!(
            "The startup entry is missing ({}); reinstall with a new install command",
            svc.describe()
        ));
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
    let text = std::fs::read_to_string(home.file("worker.log"))
        .map_err(|_| "There is no log yet".to_string())?;
    let all: Vec<&str> = text.lines().collect();
    println!("{}", all[all.len().saturating_sub(lines)..].join("\n"));
    Ok(())
}

pub fn component(
    home: &Home,
    action: &str,
    name: Option<&str>,
    cwd: Option<String>,
    env: Vec<String>,
    argv: Vec<String>,
) -> Result<(), String> {
    match action {
        "list" => {
            let entries = components::load(home)?;
            let status = components::read_status(home);
            if entries.is_empty() {
                println!(
                    "No components. Add one with: agent-worker component add NAME -- COMMAND ..."
                );
            }
            for e in entries {
                let n = e["name"].as_str().unwrap();
                println!(
                    "{n}: {}  {}",
                    status[n]["state"].as_str().unwrap_or("not started yet"),
                    e["command"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|a| a.as_str().unwrap())
                        .collect::<Vec<_>>()
                        .join(" ")
                );
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
                    let (k, v) = item
                        .split_once('=')
                        .filter(|(k, _)| !k.is_empty())
                        .ok_or_else(|| format!("--env wants NAME=VALUE, got {item:?}"))?;
                    map.insert(k.into(), json!(v));
                }
                entry["env"] = Value::Object(map);
            }
            let added = components::add(home, &entry)?;
            println!(
                "Added {}: the worker starts it within a few seconds and restarts it if it exits",
                added["name"].as_str().unwrap()
            );
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
pub(crate) async fn drain(home: &Home, config: &Value, on: bool, wait: u64) -> bool {
    let url = format!("http://127.0.0.1:{}/drain", port_of(config));
    let deadline = Instant::now() + Duration::from_secs(wait);
    loop {
        let response = client()
            .post(&url)
            .bearer_auth(home.token())
            .json(&json!({"drain": on}))
            .timeout(Duration::from_secs(10))
            .send()
            .await;
        let Ok(response) = response else { return true }; // the service is not running: nothing to wait for
        if !response.status().is_success() || !on {
            return response.status().is_success();
        }
        let active = response
            .json::<Value>()
            .await
            .ok()
            .and_then(|b| b["active"].as_u64())
            .unwrap_or(0);
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
pub async fn apply_update(
    home: &Home,
    config: &Value,
    new_bytes: Vec<u8>,
    drain_timeout: u64,
) -> Result<String, String> {
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
    let (staged, old) = (
        home.file(&format!("{}.new", Home::exe_name())),
        home.file(&format!("{}.old", Home::exe_name())),
    );
    let _ = std::fs::remove_file(&old);
    std::fs::write(&staged, &new_bytes)
        .map_err(|e| format!("Could not write the new worker: {e}"))?;
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
            let why = started.err().unwrap_or_else(|| {
                format!(
                    "The new worker did not become healthy within {HEALTH_TIMEOUT} s; see {}",
                    home.file("worker.log").display()
                )
            });
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

pub async fn update(
    home: &Home,
    force: bool,
    drain_timeout: u64,
    from_file: Option<String>,
    link: Option<(String, String)>,
) -> Result<(), String> {
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
        write_update_status(
            home,
            "failed",
            json!({"error": error.chars().take(300).collect::<String>(), "started_at": started}),
        );
    }
    if let Some((server, code)) = link {
        let body = match &result {
            Ok(()) => json!({"ok": true}),
            Err(e) => json!({"ok": false, "error": e.chars().take(300).collect::<String>()}),
        };
        let _ = client()
            .post(format!(
                "{}/update/{code}/done",
                server.trim_end_matches('/')
            ))
            .json(&body)
            .timeout(Duration::from_secs(10))
            .send()
            .await;
    }
    result
}

// --------------------------------------------------------------------------- uninstall
fn remove_cli(config: &Value) {
    for link in config["cli_links"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|l| l.as_str())
    {
        let _ = std::fs::remove_file(link);
    }
    #[cfg(windows)]
    if let Some(entry) = config["path_entry"].as_str() {
        let script = format!("$e = [Environment]::GetEnvironmentVariable('Path','User'); if ($e) {{ $p = ($e -split ';') | Where-Object {{ $_ -and ($_ -ne '{}') }}; [Environment]::SetEnvironmentVariable('Path', ($p -join ';'), 'User') }}", entry.replace('\'', "''"));
        let _ = crate::svc::run(
            "powershell",
            &["-NoProfile", "-NonInteractive", "-Command", &script],
        );
    }
}

fn is_revoked_registration_response(status: u16, body: &Value) -> bool {
    status == 410 && body["error"] == "worker_revoked"
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
                    let status = r.status().as_u16();
                    let already_revoked = if status == 410 {
                        r.json::<Value>().await.map(|body| is_revoked_registration_response(status, &body)).unwrap_or(false)
                    } else {
                        false
                    };
                    registered = status == 200 || already_revoked;
                    if ![200, 401, 404].contains(&status) && !already_revoked {
                        println!("Warning: the server answered {status} when removing this PC from the fleet; remove it in the Fleet tab");
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
        let _ = std::process::Command::new("cmd")
            .raw_arg(format!(
                "/c ping -n 4 127.0.0.1 >nul & rmdir /s /q \"{}\"",
                home.0.display()
            ))
            .creation_flags(
                crate::procutil::DETACHED_PROCESS
                    | crate::procutil::CREATE_NEW_PROCESS_GROUP
                    | crate::procutil::CREATE_NO_WINDOW,
            )
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn();
    }
    #[cfg(not(windows))]
    {
        let _ = std::fs::remove_dir_all(&home.0);
    }
    println!(
        "Worker {id} removed from this PC{}",
        if keep_registration {
            ""
        } else if registered {
            " and from the fleet"
        } else {
            ""
        }
    );
    Ok(())
}

/// `agent-worker boot [enable|disable|status]`: start with the PC, before anyone logs in. On Windows this needs Administrator
/// rights once; a normal shell gets the UAC prompt, which is answered on the PC.
pub fn boot(home: &Home, action: &str, as_user: Option<String>) -> Result<(), String> {
    let mut config = home.config()?;
    let mut svc = Svc::from_config(Home(home.0.clone()), &config);
    match action {
        "status" => {
            println!("{}", match svc.autostart() {
                "boot" => "Starts when the PC starts, even before anyone logs in",
                "none" => "Does not start by itself (run `agent-worker start` after a restart)",
                _ => "Starts when you log in (run `agent-worker boot enable` to start with the PC)",
            });
            Ok(())
        }
        "enable" | "disable" => {
            let enable = action == "enable";
            if let Some(expected) = &as_user {
                // the elevated copy: UAC may have been answered with another administrator's credentials, which must not own this worker's task
                if !expected.eq_ignore_ascii_case(&crate::svc::whoami()) {
                    return Err(format!("The administrator prompt was approved as {}, but this worker belongs to {expected}. Approve it as {expected}.", crate::svc::whoami()));
                }
            }
            if svc.kind == crate::svc::Kind::Task
                && !crate::procutil::is_elevated()
                && svc.starts_at_boot() != enable
            {
                if std::env::var_os("AGENT_WORKER_NO_ELEVATE").is_some() {
                    return Err("Starting before anyone logs in needs Administrator rights. Run this in an Administrator PowerShell: agent-worker boot enable".into());
                }
                println!("Windows will ask for administrator approval (UAC)...");
                let exe = std::env::current_exe().map_err(|e| e.to_string())?;
                let args = vec![
                    "--home".into(),
                    home.0.to_string_lossy().to_string(),
                    "boot".into(),
                    action.to_string(),
                    "--as-user".into(),
                    crate::svc::whoami(),
                ];
                let code = crate::procutil::run_elevated(&exe, &args)?;
                if code != 0 {
                    return Err(format!("The administrator step did not finish (exit code {code}); the worker keeps starting {}", if enable { "when you log in" } else { "when the PC starts" }));
                }
                // The elevated copy only changed the task. The files in the worker's folder are written here, as the user, so they stay the user's.
                config["boot"] = serde_json::json!(enable);
                home.save_config(&config).map_err(|e| e.to_string())?;
                svc = Svc::from_config(Home(home.0.clone()), &config);
            } else if as_user.is_some() {
                svc.set_boot(enable)?; // running elevated on behalf of the user's own command: change nothing else
                return Ok(());
            } else {
                svc.set_boot(enable)?;
                config["boot"] = serde_json::json!(enable);
                home.save_config(&config).map_err(|e| e.to_string())?;
            }
            if enable && !svc.running() {
                svc.start()?;
            }
            println!(
                "{}",
                if enable {
                    "The worker now starts when the PC starts, even before anyone logs in"
                } else {
                    "The worker starts when you log in again"
                }
            );
            Ok(())
        }
        other => Err(format!(
            "Unknown boot action {other:?}; use enable, disable or status"
        )),
    }
}

/// `agent-worker autorestart [on|off|status]`: restart the worker when it crashes or is killed, or leave it down.
pub fn autorestart(home: &Home, action: &str, as_user: Option<String>) -> Result<(), String> {
    let mut config = home.config()?;
    let svc = Svc::from_config(Home(home.0.clone()), &config);
    match action {
        "status" => {
            println!("{}", match (svc.kind, svc.auto_restart) {
                (crate::svc::Kind::Process, _) => "Not restarted if it stops: it runs as a plain background process (no startup entry)",
                (_, true) => "Restarted automatically if it crashes or is killed (run `agent-worker autorestart off` to turn this off)",
                (_, false) => "Not restarted if it crashes or is killed; it starts again at the next start-up or with `agent-worker start`",
            });
            Ok(())
        }
        "on" | "off" => {
            let on = action == "on";
            if svc.kind == crate::svc::Kind::Process {
                return Err("This worker has no startup entry (it runs as a plain background process), so nothing restarts it. Reinstall with a start-up choice to use this".into());
            }
            if let Some(expected) = &as_user {
                if !expected.eq_ignore_ascii_case(&crate::svc::whoami()) {
                    return Err(format!("The administrator prompt was approved as {}, but this worker belongs to {expected}. Approve it as {expected}.", crate::svc::whoami()));
                }
                // the elevated copy: the user's command already saved the setting; only the machine-level task is rebuilt here
                return svc.install();
            }
            if svc.auto_restart == on {
                println!("Auto restart is already {action}");
                return Ok(());
            }
            let was_running = svc.running();
            let previous = config.clone();
            config["auto_restart"] = serde_json::json!(on);
            home.save_config(&config).map_err(|e| e.to_string())?;
            let svc = Svc::from_config(Home(home.0.clone()), &config);
            let applied = if svc.kind == crate::svc::Kind::Task
                && svc.boot
                && !crate::procutil::is_elevated()
            {
                // the boot task was made by an administrator and is rebuilt by one
                if std::env::var_os("AGENT_WORKER_NO_ELEVATE").is_some() {
                    Err("Changing a task that starts at boot needs Administrator rights. Run this in an Administrator PowerShell: agent-worker autorestart".to_string() + " " + action)
                } else {
                    println!("Windows will ask for administrator approval (UAC)...");
                    let exe = std::env::current_exe().map_err(|e| e.to_string())?;
                    let args = vec![
                        "--home".into(),
                        home.0.to_string_lossy().to_string(),
                        "autorestart".into(),
                        action.to_string(),
                        "--as-user".into(),
                        crate::svc::whoami(),
                    ];
                    match crate::procutil::run_elevated(&exe, &args) {
                        Ok(0) => Ok(()),
                        Ok(code) => Err(format!(
                            "The administrator step did not finish (exit code {code})"
                        )),
                        Err(e) => Err(e),
                    }
                }
            } else {
                svc.install()
            };
            if let Err(why) = applied {
                let _ = home.save_config(&previous); // nothing changed on the PC, so the setting must not say it did
                return Err(why);
            }
            if was_running && !svc.running() {
                svc.start()?; // rebuilding a Windows task ends its running instance
            }
            println!(
                "{}",
                if on {
                    "The worker is now restarted automatically if it crashes or is killed"
                } else {
                    "The worker is no longer restarted automatically: if it stops it stays down until the next start-up or `agent-worker start`"
                }
            );
            Ok(())
        }
        other => Err(format!(
            "Unknown autorestart action {other:?}; use on, off or status"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_output_reports_unregistration_and_recovery_in_human_and_json_forms() {
        let revocation = Revocation {
            reason: "worker_revoked".into(),
            message: "This worker has been unregistered from the fleet.".into(),
            at: 1234.5,
        };

        let json = revocation_json_fields(Some(&revocation));
        assert_eq!(json["revoked"], true);
        assert_eq!(json["revocation"]["reason"], "worker_revoked");
        assert_eq!(
            json["revocation"]["message"],
            "This worker has been unregistered from the fleet."
        );
        assert_eq!(json["revocation"]["at"], 1234.5);
        assert_eq!(
            json["remediation"],
            "Run `agent-worker uninstall`, then install again using a new Fleet install command."
        );

        let human = revocation_human_output(Some(&revocation));
        assert!(human.contains("Worker unregistered from the Fleet"));
        assert!(human.contains("This worker has been unregistered from the fleet."));
        assert!(human.contains(
            "Run `agent-worker uninstall`, then install again using a new Fleet install command."
        ));
        assert!(!connection_is_current(
            true,
            true,
            &json!({"connected": true}),
            true
        ));
        assert!(connection_is_current(
            true,
            true,
            &json!({"connected": true}),
            false
        ));
    }
    #[test]
    fn uninstall_treats_only_protocol_revocation_as_already_removed() {
        assert!(is_revoked_registration_response(
            410,
            &json!({"error": "worker_revoked"})
        ));
        assert!(!is_revoked_registration_response(
            410,
            &json!({"error": "other"})
        ));
        assert!(!is_revoked_registration_response(
            401,
            &json!({"error": "worker_revoked"})
        ));
    }
}
