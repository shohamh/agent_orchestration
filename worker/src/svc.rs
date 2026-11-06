//! The platform's per-user startup entry for the runner: a Windows scheduled task (at logon, restarted if it fails),
//! a `systemd --user` unit (`Restart=always`), a launchd agent, or a plain background process as the fallback.
use crate::config::Home;
use crate::procutil::{kill_tree, pid_alive, spawn_detached};
use serde_json::Value;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

#[cfg(windows)]
use std::os::windows::process::CommandExt;

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum Kind {
    Process,
    Task,
    Systemd,
    Launchd,
}

impl Kind {
    pub fn name(&self) -> &'static str {
        match self {
            Kind::Process => "process",
            Kind::Task => "task",
            Kind::Systemd => "systemd",
            Kind::Launchd => "launchd",
        }
    }

    pub fn parse(name: &str) -> Kind {
        match name {
            "task" => Kind::Task,
            "systemd" => Kind::Systemd,
            "launchd" => Kind::Launchd,
            _ => Kind::Process,
        }
    }

    /// The best startup mechanism on this PC.
    pub fn detect() -> Kind {
        if cfg!(windows) {
            Kind::Task
        } else if cfg!(target_os = "macos") {
            Kind::Launchd
        } else if run("systemctl", &["--user", "show-environment"]).map(|o| o.0 == 0).unwrap_or(false) {
            Kind::Systemd
        } else {
            Kind::Process
        }
    }
}

/// Run a program to completion; (exit code, stdout, stderr).
pub fn run(program: &str, args: &[&str]) -> std::io::Result<(i32, String, String)> {
    let mut command = Command::new(program);
    command.args(args).stdin(Stdio::null());
    #[cfg(windows)]
    command.creation_flags(crate::procutil::CREATE_NO_WINDOW);
    let out = command.output()?;
    Ok((out.status.code().unwrap_or(-1), String::from_utf8_lossy(&out.stdout).to_string(), String::from_utf8_lossy(&out.stderr).to_string()))
}

pub struct Svc {
    pub home: Home,
    pub kind: Kind,
    pub unit: String,
}

impl Svc {
    pub fn from_config(home: Home, config: &Value) -> Svc {
        let name = crate::config::name_of(config);
        Svc { kind: Kind::parse(config["service"].as_str().unwrap_or("process")), unit: config["unit"].as_str().map(String::from).unwrap_or_else(|| default_unit(&name)), home }
    }

    pub fn running(&self) -> bool {
        match self.kind {
            Kind::Systemd => run("systemctl", &["--user", "is-active", "--quiet", &format!("{}.service", self.unit)]).map(|o| o.0 == 0).unwrap_or(false),
            _ => pid_alive(self.home.pid()),
        }
    }

    pub fn exists(&self) -> bool {
        match self.kind {
            Kind::Task => run("schtasks", &["/Query", "/TN", &self.unit]).map(|o| o.0 == 0).unwrap_or(false),
            Kind::Systemd => self.unit_path().exists(),
            Kind::Launchd => self.plist_path().exists(),
            Kind::Process => true,
        }
    }

    pub fn describe(&self) -> String {
        match self.kind {
            Kind::Task => format!("Windows scheduled task {} (starts when you log in; restarted if it fails)", self.unit),
            Kind::Systemd => format!("systemd user service {} (restarted if it fails)", self.unit),
            Kind::Launchd => format!("launchd agent {} (restarted if it fails)", self.unit),
            Kind::Process => "background process (no startup entry)".into(),
        }
    }

    fn unit_path(&self) -> PathBuf {
        PathBuf::from(std::env::var("HOME").unwrap_or_default()).join(".config/systemd/user").join(format!("{}.service", self.unit))
    }

    fn plist_path(&self) -> PathBuf {
        PathBuf::from(std::env::var("HOME").unwrap_or_default()).join("Library/LaunchAgents").join(format!("{}.plist", self.unit))
    }

    fn wait_running(&self, timeout: u64) -> bool {
        let deadline = Instant::now() + Duration::from_secs(timeout);
        while Instant::now() < deadline {
            if self.running() {
                return true;
            }
            std::thread::sleep(Duration::from_millis(500));
        }
        self.running()
    }

    pub fn start(&self) -> Result<(), String> {
        if self.running() {
            return Ok(());
        }
        match self.kind {
            Kind::Task => {
                let (code, out, err) = run("schtasks", &["/Run", "/TN", &self.unit]).map_err(|e| e.to_string())?;
                if code != 0 {
                    return Err(format!("Could not start the scheduled task {}: {}", self.unit, if err.trim().is_empty() { out.trim() } else { err.trim() }));
                }
            }
            Kind::Systemd => {
                let (code, _, err) = run("systemctl", &["--user", "start", &format!("{}.service", self.unit)]).map_err(|e| e.to_string())?;
                if code != 0 {
                    return Err(format!("systemctl start failed: {}", err.trim()));
                }
            }
            Kind::Launchd => {
                let domain = format!("gui/{}", unsafe_uid());
                let _ = run("launchctl", &["bootstrap", &domain, &self.plist_path().to_string_lossy()]);
                let _ = run("launchctl", &["kickstart", &format!("{domain}/{}", self.unit)]);
            }
            Kind::Process => {
                spawn_detached(&self.home.exe(), &["run".into(), "--home".into(), self.home.0.to_string_lossy().to_string()], Some(&self.home.0), Some(&self.home.file("worker.log"))).map_err(|e| e.to_string())?;
            }
        }
        if self.wait_running(40) { Ok(()) } else { Err(format!("The worker did not start; see {}", self.home.file("worker.log").display())) }
    }

    pub fn stop(&self) -> Result<(), String> {
        match self.kind {
            Kind::Task => {
                let _ = run("schtasks", &["/End", "/TN", &self.unit]);
            }
            Kind::Systemd => {
                let _ = run("systemctl", &["--user", "stop", &format!("{}.service", self.unit)]);
            }
            Kind::Launchd => {
                let _ = run("launchctl", &["bootout", &format!("gui/{}/{}", unsafe_uid(), self.unit)]);
            }
            Kind::Process => {}
        }
        let pid = self.home.pid();
        if pid != 0 && !kill_tree(pid, Duration::from_secs(15)) {
            return Err("The worker did not stop".into());
        }
        let _ = std::fs::remove_file(self.home.file("worker.pid"));
        Ok(())
    }

    pub fn remove(&self) {
        match self.kind {
            Kind::Task => {
                let _ = run("schtasks", &["/End", "/TN", &self.unit]);
                let _ = run("schtasks", &["/Delete", "/TN", &self.unit, "/F"]);
            }
            Kind::Systemd => {
                let _ = run("systemctl", &["--user", "disable", "--now", &format!("{}.service", self.unit)]);
                let _ = std::fs::remove_file(self.unit_path());
                let _ = run("systemctl", &["--user", "daemon-reload"]);
            }
            Kind::Launchd => {
                let _ = run("launchctl", &["bootout", &format!("gui/{}/{}", unsafe_uid(), self.unit)]);
                let _ = std::fs::remove_file(self.plist_path());
            }
            Kind::Process => {}
        }
    }

    /// Create the startup entry (not started here).
    pub fn install(&self) -> Result<(), String> {
        let exe = self.home.exe().to_string_lossy().to_string();
        let home = self.home.0.to_string_lossy().to_string();
        match self.kind {
            Kind::Task => {
                let user = run("whoami", &[]).map_err(|e| e.to_string())?.1.trim().to_string();
                let xml = task_xml(&user, &exe, &home);
                let path = self.home.file("task.xml");
                let mut bytes = vec![0xFF, 0xFE];
                bytes.extend(xml.encode_utf16().flat_map(|u| u.to_le_bytes()));
                std::fs::write(&path, bytes).map_err(|e| e.to_string())?;
                let (code, out, err) = run("schtasks", &["/Create", "/TN", &self.unit, "/XML", &path.to_string_lossy(), "/F"]).map_err(|e| e.to_string())?;
                let _ = std::fs::remove_file(&path);
                if code != 0 {
                    return Err(format!("Could not create the scheduled task: {}", if err.trim().is_empty() { out.trim() } else { err.trim() }));
                }
            }
            Kind::Systemd => {
                let unit = format!(
                    "[Unit]\nDescription=Agent orchestration execution worker ({})\nAfter=network-online.target\n\n[Service]\nExecStart=\"{exe}\" run --home \"{home}\"\nRestart=always\nRestartSec=5\nKillMode=control-group\n\n[Install]\nWantedBy=default.target\n",
                    self.unit
                );
                let path = self.unit_path();
                std::fs::create_dir_all(path.parent().unwrap()).map_err(|e| e.to_string())?;
                std::fs::write(&path, unit).map_err(|e| e.to_string())?;
                run("systemctl", &["--user", "daemon-reload"]).map_err(|e| e.to_string())?;
                let (code, _, err) = run("systemctl", &["--user", "enable", &format!("{}.service", self.unit)]).map_err(|e| e.to_string())?;
                if code != 0 {
                    return Err(format!("systemctl enable failed: {}", err.trim()));
                }
            }
            Kind::Launchd => {
                let plist = format!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n<plist version=\"1.0\"><dict>\n<key>Label</key><string>{}</string>\n<key>ProgramArguments</key><array><string>{exe}</string><string>run</string><string>--home</string><string>{home}</string></array>\n<key>RunAtLoad</key><true/><key>KeepAlive</key><true/>\n</dict></plist>\n",
                    self.unit
                );
                let path = self.plist_path();
                std::fs::create_dir_all(path.parent().unwrap()).map_err(|e| e.to_string())?;
                std::fs::write(&path, plist).map_err(|e| e.to_string())?;
            }
            Kind::Process => {}
        }
        Ok(())
    }
}

pub fn default_unit(id: &str) -> String {
    if cfg!(windows) { format!("AgentWorker-{id}") } else { format!("agent-worker-{id}") }
}

fn unsafe_uid() -> u32 {
    #[cfg(unix)]
    unsafe {
        libc::getuid()
    }
    #[cfg(not(unix))]
    0
}

/// The scheduled task: starts when this user logs in, restarts a minute after a failure (up to 999 times), has no time
/// limit and keeps going on battery. Runs as the user with ordinary rights, so no administrator is needed.
fn task_xml(user: &str, exe: &str, home: &str) -> String {
    let esc = |s: &str| s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;");
    format!(
        r#"<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.4" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo><Description>Agent orchestration execution worker</Description></RegistrationInfo>
  <Triggers><LogonTrigger><Enabled>true</Enabled><UserId>{user}</UserId></LogonTrigger></Triggers>
  <Principals><Principal id="Author"><UserId>{user}</UserId><LogonType>InteractiveToken</LogonType><RunLevel>LeastPrivilege</RunLevel></Principal></Principals>
  <Settings>
    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>
    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>
    <StartWhenAvailable>true</StartWhenAvailable>
    <AllowHardTerminate>true</AllowHardTerminate>
    <ExecutionTimeLimit>PT0S</ExecutionTimeLimit>
    <RestartOnFailure><Interval>PT1M</Interval><Count>999</Count></RestartOnFailure>
    <Enabled>true</Enabled>
    <Hidden>false</Hidden>
  </Settings>
  <Actions Context="Author"><Exec><Command>{exe}</Command><Arguments>run --home "{home}"</Arguments><WorkingDirectory>{home}</WorkingDirectory></Exec></Actions>
</Task>
"#,
        user = esc(user),
        exe = esc(exe),
        home = esc(home)
    )
}
