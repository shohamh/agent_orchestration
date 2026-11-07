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
        // For tests and PCs where a startup entry is unwanted: AGENT_WORKER_SERVICE=process runs the worker as a plain background process.
        if std::env::var("AGENT_WORKER_SERVICE")
            .map(|v| v == "process")
            .unwrap_or(false)
        {
            Kind::Process
        } else if cfg!(windows) {
            Kind::Task
        } else if cfg!(target_os = "macos") {
            Kind::Launchd
        } else if run("systemctl", &["--user", "show-environment"])
            .map(|o| o.0 == 0)
            .unwrap_or(false)
        {
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
    Ok((
        out.status.code().unwrap_or(-1),
        String::from_utf8_lossy(&out.stdout).to_string(),
        String::from_utf8_lossy(&out.stderr).to_string(),
    ))
}

pub struct Svc {
    /// restart the worker when it crashes or is killed (the form's choice; on by default)
    pub auto_restart: bool,
    /// start at boot, before anyone logs in (Windows: a boot trigger; Linux: lingering)
    pub boot: bool,
    pub home: Home,
    pub kind: Kind,
    pub unit: String,
}

impl Svc {
    pub fn from_config(home: Home, config: &Value) -> Svc {
        let name = crate::config::name_of(config);
        Svc {
            auto_restart: config["auto_restart"] != false,
            boot: config["boot"] == true,
            kind: Kind::parse(config["service"].as_str().unwrap_or("process")),
            unit: config["unit"]
                .as_str()
                .map(String::from)
                .unwrap_or_else(|| default_unit(&name)),
            home,
        }
    }

    /// Does the worker start when the PC starts, without anyone logging in?
    pub fn starts_at_boot(&self) -> bool {
        match self.kind {
            Kind::Task => self.boot,
            Kind::Systemd => run(
                "loginctl",
                &["show-user", &whoami(), "-p", "Linger", "--value"],
            )
            .map(|o| o.1.trim() == "yes")
            .unwrap_or(false),
            _ => false,
        }
    }

    /// Turn starting at boot on or off. Windows needs an Administrator shell (a boot trigger is a machine-level task);
    /// on Linux lingering lets the user's systemd instance run without a login.
    pub fn set_boot(&mut self, enable: bool) -> Result<(), String> {
        if self.kind == Kind::Systemd && self.starts_at_boot() == enable {
            return Ok(()); // already so; loginctl would ask for rights it does not need
        }
        match self.kind {
            Kind::Task => {
                if enable && !crate::procutil::is_elevated() {
                    return Err("Starting before anyone logs in needs Administrator rights once. Open PowerShell with \"Run as administrator\" and run: agent-worker boot enable".into());
                }
                self.boot = enable;
                self.install()?;
                Ok(())
            }
            Kind::Systemd => {
                let verb = if enable { "enable-linger" } else { "disable-linger" };
                let (code, out, err) = run("loginctl", &[verb, &whoami()]).map_err(|e| e.to_string())?;
                if code != 0 {
                    return Err(format!("loginctl {verb} failed ({}). Ask an administrator to run: sudo loginctl {verb} {}", if err.trim().is_empty() { out.trim() } else { err.trim() }, whoami()));
                }
                Ok(())
            }
            _ => Err("Starting at boot needs a Windows Scheduled Task or a systemd user service; this worker runs as a plain background process".into()),
        }
    }

    pub fn running(&self) -> bool {
        match self.kind {
            Kind::Systemd => run(
                "systemctl",
                &[
                    "--user",
                    "is-active",
                    "--quiet",
                    &format!("{}.service", self.unit),
                ],
            )
            .map(|o| o.0 == 0)
            .unwrap_or(false),
            // A task that starts at boot runs in a session of its own whose processes this user's shell cannot always see: ask the Task Scheduler as well
            Kind::Task => pid_alive(self.home.pid()) || self.task_running(),
            _ => pid_alive(self.home.pid()),
        }
    }

    /// Does the Task Scheduler say the task is running? (The state is read as an enum name, which is not translated.)
    fn task_running(&self) -> bool {
        let script = format!(
            "(Get-ScheduledTask -TaskName '{}' -ErrorAction SilentlyContinue).State -eq 'Running'",
            self.unit.replace("'", "''")
        );
        run(
            "powershell",
            &["-NoProfile", "-NonInteractive", "-Command", &script],
        )
        .map(|o| o.1.trim() == "True")
        .unwrap_or(false)
    }

    pub fn exists(&self) -> bool {
        match self.kind {
            Kind::Task => run("schtasks", &["/Query", "/TN", &self.unit])
                .map(|o| o.0 == 0)
                .unwrap_or(false),
            Kind::Systemd => self.unit_path().exists(),
            Kind::Launchd => self.plist_path().exists(),
            Kind::Process => true,
        }
    }

    /// "logon", "boot" or "none": when this worker starts by itself.
    pub fn autostart(&self) -> &'static str {
        if self.kind == Kind::Process {
            "none"
        } else if self.starts_at_boot() {
            "boot"
        } else {
            "logon"
        }
    }

    pub fn describe(&self) -> String {
        match self.kind {
            Kind::Task => format!(
                "Windows scheduled task {} (starts {}; {})",
                self.unit,
                if self.boot {
                    "when the PC starts, even before anyone logs in"
                } else {
                    "when you log in"
                },
                if self.auto_restart {
                    "checked every minute and restarted if it is not running"
                } else {
                    "not restarted if it stops"
                }
            ),
            Kind::Systemd => format!(
                "systemd user service {} ({})",
                self.unit,
                if self.auto_restart {
                    "restarted if it fails"
                } else {
                    "not restarted if it stops"
                }
            ),
            Kind::Launchd => format!(
                "launchd agent {} ({})",
                self.unit,
                if self.auto_restart {
                    "restarted if it fails"
                } else {
                    "not restarted if it stops"
                }
            ),
            Kind::Process => "background process (no startup entry)".into(),
        }
    }

    fn unit_path(&self) -> PathBuf {
        PathBuf::from(std::env::var("HOME").unwrap_or_default())
            .join(".config/systemd/user")
            .join(format!("{}.service", self.unit))
    }

    fn plist_path(&self) -> PathBuf {
        PathBuf::from(std::env::var("HOME").unwrap_or_default())
            .join("Library/LaunchAgents")
            .join(format!("{}.plist", self.unit))
    }

    fn wait_running(&self, timeout: u64) -> bool {
        let deadline = Instant::now() + Duration::from_secs(timeout);
        while Instant::now() < deadline {
            if self.running() {
                return true;
            }
            std::thread::sleep(Duration::from_millis(if self.kind == Kind::Task {
                1000
            } else {
                500
            }));
        }
        self.running()
    }

    pub fn start(&self) -> Result<(), String> {
        let _ = std::fs::remove_file(self.home.file("stopped"));
        if self.running() {
            return Ok(());
        }
        match self.kind {
            Kind::Task => {
                // A task made by an administrator (boot start) may not let a normal user change or run it: then start the
                // worker directly. The task's own minute check finds it running and leaves it alone.
                let _ = run("schtasks", &["/Change", "/TN", &self.unit, "/ENABLE"]);
                let ran = run("schtasks", &["/Run", "/TN", &self.unit])
                    .map(|o| o.0 == 0)
                    .unwrap_or(false);
                if !ran || !self.wait_running(10) {
                    crate::procutil::spawn_detached(
                        &self.home.exe(),
                        &[
                            "run".into(),
                            "--home".into(),
                            self.home.0.to_string_lossy().to_string(),
                        ],
                        Some(&self.home.0),
                        Some(&self.home.file("worker.log")),
                    )
                    .map_err(|e| format!("Could not start the worker: {e}"))?;
                }
            }
            Kind::Systemd => {
                let (code, _, err) = run(
                    "systemctl",
                    &["--user", "start", &format!("{}.service", self.unit)],
                )
                .map_err(|e| e.to_string())?;
                if code != 0 {
                    return Err(format!("systemctl start failed: {}", err.trim()));
                }
            }
            Kind::Launchd => {
                let domain = format!("gui/{}", unsafe_uid());
                let _ = run(
                    "launchctl",
                    &["bootstrap", &domain, &self.plist_path().to_string_lossy()],
                );
                let _ = run(
                    "launchctl",
                    &["kickstart", &format!("{domain}/{}", self.unit)],
                );
            }
            Kind::Process => {
                spawn_detached(
                    &self.home.exe(),
                    &[
                        "run".into(),
                        "--home".into(),
                        self.home.0.to_string_lossy().to_string(),
                    ],
                    Some(&self.home.0),
                    Some(&self.home.file("worker.log")),
                )
                .map_err(|e| e.to_string())?;
            }
        }
        if self.wait_running(40) {
            Ok(())
        } else {
            Err(format!(
                "The worker did not start; see {}",
                self.home.file("worker.log").display()
            ))
        }
    }

    pub fn stop(&self) -> Result<(), String> {
        match self.kind {
            Kind::Task => {
                // the task re-runs every minute; a stopped worker leaves this marker so a re-run exits at once even when the task cannot be disabled
                let _ = std::fs::write(
                    self.home.file("stopped"),
                    "stopped by agent-worker stop; agent-worker start removes this",
                );
                // The task re-runs every minute to bring back a worker that was killed; disabled, a stopped worker stays stopped until `start`.
                let _ = run("schtasks", &["/Change", "/TN", &self.unit, "/DISABLE"]);
                let _ = run("schtasks", &["/End", "/TN", &self.unit]);
            }
            Kind::Systemd => {
                let _ = run(
                    "systemctl",
                    &["--user", "stop", &format!("{}.service", self.unit)],
                );
            }
            Kind::Launchd => {
                let _ = run(
                    "launchctl",
                    &["bootout", &format!("gui/{}/{}", unsafe_uid(), self.unit)],
                );
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
                let _ = run(
                    "systemctl",
                    &[
                        "--user",
                        "disable",
                        "--now",
                        &format!("{}.service", self.unit),
                    ],
                );
                let _ = std::fs::remove_file(self.unit_path());
                let _ = run("systemctl", &["--user", "daemon-reload"]);
            }
            Kind::Launchd => {
                let _ = run(
                    "launchctl",
                    &["bootout", &format!("gui/{}/{}", unsafe_uid(), self.unit)],
                );
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
                let user = run("whoami", &[])
                    .map_err(|e| e.to_string())?
                    .1
                    .trim()
                    .to_string();
                let xml = task_xml(&user, &exe, &home, self.boot, self.auto_restart);
                let path = self.home.file("task.xml");
                let mut bytes = vec![0xFF, 0xFE];
                bytes.extend(xml.encode_utf16().flat_map(|u| u.to_le_bytes()));
                std::fs::write(&path, bytes).map_err(|e| e.to_string())?;
                let (code, out, err) = run(
                    "schtasks",
                    &[
                        "/Create",
                        "/TN",
                        &self.unit,
                        "/XML",
                        &path.to_string_lossy(),
                        "/F",
                    ],
                )
                .map_err(|e| e.to_string())?;
                let _ = std::fs::remove_file(&path);
                if code != 0 {
                    return Err(format!(
                        "Could not create the scheduled task: {}",
                        if err.trim().is_empty() {
                            out.trim()
                        } else {
                            err.trim()
                        }
                    ));
                }
            }
            Kind::Systemd => {
                let unit = format!(
                    "[Unit]\nDescription=Agent orchestration execution worker ({})\nAfter=network-online.target\n\n[Service]\nExecStart=\"{exe}\" run --home \"{home}\"\nRestart={restart}\nRestartSec=5\nKillMode=control-group\n\n[Install]\nWantedBy=default.target\n",
                    self.unit,
                    restart = if self.auto_restart { "always" } else { "no" }
                );
                let path = self.unit_path();
                std::fs::create_dir_all(path.parent().unwrap()).map_err(|e| e.to_string())?;
                std::fs::write(&path, unit).map_err(|e| e.to_string())?;
                run("systemctl", &["--user", "daemon-reload"]).map_err(|e| e.to_string())?;
                let (code, _, err) = run(
                    "systemctl",
                    &["--user", "enable", &format!("{}.service", self.unit)],
                )
                .map_err(|e| e.to_string())?;
                if code != 0 {
                    return Err(format!("systemctl enable failed: {}", err.trim()));
                }
            }
            Kind::Launchd => {
                let plist = format!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n<plist version=\"1.0\"><dict>\n<key>Label</key><string>{}</string>\n<key>ProgramArguments</key><array><string>{exe}</string><string>run</string><string>--home</string><string>{home}</string></array>\n<key>RunAtLoad</key><true/><key>KeepAlive</key><{keep}/>\n</dict></plist>\n",
                    self.unit,
                    keep = if self.auto_restart { "true" } else { "false" }
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
    if cfg!(windows) {
        format!("AgentWorker-{id}")
    } else {
        format!("agent-worker-{id}")
    }
}

pub fn whoami() -> String {
    std::env::var("USER")
        .or_else(|_| std::env::var("LOGNAME"))
        .unwrap_or_else(|_| {
            run("whoami", &[])
                .map(|o| o.1.trim().to_string())
                .unwrap_or_default()
        })
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
fn task_xml(user: &str, exe: &str, home: &str, boot: bool, auto_restart: bool) -> String {
    // auto_restart off: no one-minute check and no restart on failure; the logon and boot triggers still start it
    let (check_trigger, restart_on_failure) = if auto_restart {
        ("<TimeTrigger><Repetition><Interval>PT1M</Interval><StopAtDurationEnd>false</StopAtDurationEnd></Repetition><StartBoundary>2020-01-01T00:00:00</StartBoundary><Enabled>true</Enabled></TimeTrigger>", "<RestartOnFailure><Interval>PT1M</Interval><Count>999</Count></RestartOnFailure>")
    } else {
        ("", "")
    };
    // boot: also start when the PC starts, before anyone logs in. S4U runs the task as this user without a stored password
    // (and without network credentials, which the worker does not need: it only connects out to the server).
    let (boot_trigger, logon_type) = if boot {
        ("<BootTrigger><Enabled>true</Enabled></BootTrigger>", "S4U")
    } else {
        ("", "InteractiveToken")
    };
    let esc = |s: &str| {
        s.replace('&', "&amp;")
            .replace('<', "&lt;")
            .replace('>', "&gt;")
    };
    format!(
        r#"<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.4" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo><Description>Agent orchestration execution worker</Description></RegistrationInfo>
  <Triggers>{boot_trigger}<LogonTrigger><Enabled>true</Enabled><UserId>{user}</UserId></LogonTrigger>{check_trigger}</Triggers>
  <Principals><Principal id="Author"><UserId>{user}</UserId><LogonType>{logon_type}</LogonType><RunLevel>LeastPrivilege</RunLevel></Principal></Principals>
  <Settings>
    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>
    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>
    <StartWhenAvailable>true</StartWhenAvailable>
    <AllowHardTerminate>true</AllowHardTerminate>
    <ExecutionTimeLimit>PT0S</ExecutionTimeLimit>
    {restart_on_failure}
    <Enabled>true</Enabled>
    <Hidden>false</Hidden>
  </Settings>
  <Actions Context="Author"><Exec><Command>{exe}</Command><Arguments>run --home "{home}"</Arguments><WorkingDirectory>{home}</WorkingDirectory></Exec></Actions>
</Task>
"#,
        user = esc(user),
        exe = esc(exe),
        home = esc(home),
        boot_trigger = boot_trigger,
        check_trigger = check_trigger,
        restart_on_failure = restart_on_failure,
        logon_type = logon_type
    )
}

#[cfg(test)]
mod tests {
    use super::task_xml;

    #[test]
    fn the_task_starts_at_logon_and_every_minute_and_only_with_boot_also_at_startup() {
        let normal = task_xml("PC-me", "C:/w/x.exe", "C:/w", false, true);
        assert!(
            normal.contains("<LogonTrigger>")
                && normal.contains("<TimeTrigger>")
                && normal.contains("InteractiveToken")
        );
        assert!(!normal.contains("BootTrigger") && !normal.contains("S4U"));
        let boot = task_xml("PC-me", "C:/w/x.exe", "C:/w", true, true);
        assert!(
            boot.contains("<BootTrigger>")
                && boot.contains("<LogonType>S4U</LogonType>")
                && !boot.contains("InteractiveToken")
        );
        assert!(
            boot.contains("<LogonTrigger>"),
            "still starts at logon if the boot start was missed"
        );
    }

    #[test]
    fn without_auto_restart_the_task_has_no_minute_check_and_no_restart_on_failure() {
        let off = task_xml("PC-me", "C:/w/x.exe", "C:/w", false, false);
        assert!(
            off.contains("<LogonTrigger>")
                && !off.contains("TimeTrigger")
                && !off.contains("RestartOnFailure")
        );
        let on = task_xml("PC-me", "C:/w/x.exe", "C:/w", false, true);
        assert!(on.contains("TimeTrigger") && on.contains("RestartOnFailure"));
        let boot_off = task_xml("PC-me", "C:/w/x.exe", "C:/w", true, false);
        assert!(boot_off.contains("BootTrigger") && !boot_off.contains("TimeTrigger"));
    }

    #[test]
    fn names_with_markup_are_escaped() {
        assert!(task_xml("a&b<c>", "x", "y", true, true).contains("a&amp;b&lt;c&gt;"));
    }
}
