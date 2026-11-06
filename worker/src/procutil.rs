//! Cross-platform process helpers: detached start, liveness, killing a whole tree.
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

#[cfg(windows)]
pub const CREATE_NO_WINDOW: u32 = 0x0800_0000;
#[cfg(windows)]
pub const DETACHED_PROCESS: u32 = 0x0000_0008;
#[cfg(windows)]
pub const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
#[cfg(windows)]
pub const CREATE_BREAKAWAY_FROM_JOB: u32 = 0x0100_0000;

/// Start a helper that must survive the worker being stopped by its service manager, which stops everything the service started:
/// a systemd unit's whole control group, a Scheduled Task's job. Used for the updater, which stops the worker itself.
pub fn spawn_outside_service(program: &Path, args: &[String], cwd: Option<&Path>, log: Option<&Path>) -> std::io::Result<u32> {
    #[cfg(unix)]
    {
        if std::env::var_os("INVOCATION_ID").is_some() {
            let mut command = Command::new("systemd-run");
            command.args(["--user", "--collect", "--quiet", "--unit", &format!("agent-worker-update-{}", std::process::id())]);
            if let Some(log) = log {
                command.arg("-p").arg(format!("StandardOutput=append:{}", log.display())).arg("-p").arg(format!("StandardError=append:{}", log.display()));
            }
            if let Some(cwd) = cwd {
                command.arg("-p").arg(format!("WorkingDirectory={}", cwd.display()));
            }
            command.arg("--").arg(program).args(args).stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::null());
            if command.status().map(|s| s.success()).unwrap_or(false) {
                return Ok(0);
            }
        }
    }
    #[cfg(windows)]
    {
        // leave the Scheduled Task's job object when the job allows it; otherwise fall back to a plain detached start
        if let Ok(pid) = spawn_with(program, args, cwd, log, DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW | CREATE_BREAKAWAY_FROM_JOB) {
            return Ok(pid);
        }
    }
    spawn_detached(program, args, cwd, log)
}

/// Start a program that keeps running after this process exits and shows no console window.
pub fn spawn_detached(program: &Path, args: &[String], cwd: Option<&Path>, log: Option<&Path>) -> std::io::Result<u32> {
    spawn_with(program, args, cwd, log, 0)
}

/// `flags`: Windows process creation flags (0 = detached, no window); ignored elsewhere.
fn spawn_with(program: &Path, args: &[String], cwd: Option<&Path>, log: Option<&Path>, flags: u32) -> std::io::Result<u32> {
    let _ = flags;
    let mut command = Command::new(program);
    command.args(args).stdin(Stdio::null());
    if let Some(cwd) = cwd {
        command.current_dir(cwd);
    }
    match log {
        Some(path) => {
            let file = std::fs::OpenOptions::new().create(true).append(true).open(path)?;
            command.stdout(file.try_clone()?).stderr(file);
        }
        None => {
            command.stdout(Stdio::null()).stderr(Stdio::null());
        }
    }
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        keep_std_handles_private();
        command.creation_flags(if flags != 0 { flags } else { DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW });
    }
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        unsafe {
            command.pre_exec(|| {
                libc::setsid();
                Ok(())
            });
        }
    }
    Ok(command.spawn()?.id())
}

#[cfg(windows)]
pub fn pid_alive(pid: u32) -> bool {
    use windows_sys::Win32::Foundation::{CloseHandle, STILL_ACTIVE};
    use windows_sys::Win32::System::Threading::{GetExitCodeProcess, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION};
    if pid == 0 {
        return false;
    }
    unsafe {
        let handle = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid);
        if handle.is_null() {
            return false;
        }
        let mut code = 0u32;
        let ok = GetExitCodeProcess(handle, &mut code) != 0;
        CloseHandle(handle);
        ok && code == STILL_ACTIVE as u32
    }
}

#[cfg(unix)]
pub fn pid_alive(pid: u32) -> bool {
    if pid == 0 {
        return false;
    }
    unsafe {
        if libc::kill(pid as i32, 0) == 0 {
            // A zombie is dead for our purposes.
            if let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat")) {
                if let Some(rest) = stat.rsplit(')').next() {
                    return rest.trim_start().chars().next() != Some('Z');
                }
            }
            true
        } else {
            std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
        }
    }
}

/// Stop a process and everything it started: politely first on Unix, then by force. True when it is gone.
pub fn kill_tree(pid: u32, timeout: Duration) -> bool {
    if !pid_alive(pid) {
        return true;
    }
    #[cfg(windows)]
    {
        terminate_tree(pid);
    }
    #[cfg(unix)]
    unsafe {
        let group = libc::getpgid(pid as i32);
        let target = if group > 0 { -group } else { pid as i32 };
        libc::kill(target, libc::SIGTERM);
        let deadline = Instant::now() + timeout;
        while pid_alive(pid) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(200));
        }
        if pid_alive(pid) {
            libc::kill(target, libc::SIGKILL);
        }
    }
    let deadline = Instant::now() + timeout;
    while pid_alive(pid) && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(200));
    }
    !pid_alive(pid)
}

/// A process tree owned by a job: killing it ends the child and everything the child started.
pub struct Tree {
    #[cfg(windows)]
    job: windows_sys::Win32::Foundation::HANDLE,
    #[cfg(unix)]
    pgid: i32,
}

unsafe impl Send for Tree {}
unsafe impl Sync for Tree {}

impl Tree {
    /// Put a freshly spawned child (made with `prepare_child`) under this tree.
    #[cfg(windows)]
    pub fn adopt(child: &tokio::process::Child) -> Option<Tree> {
        use windows_sys::Win32::System::JobObjects::*;
        unsafe {
            let job = CreateJobObjectW(std::ptr::null(), std::ptr::null());
            if job.is_null() {
                return None;
            }
            let mut info: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = std::mem::zeroed();
            info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
            SetInformationJobObject(job, JobObjectExtendedLimitInformation, &info as *const _ as *const _, std::mem::size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32);
            let handle = child.raw_handle()?;
            AssignProcessToJobObject(job, handle as _);
            Some(Tree { job })
        }
    }

    #[cfg(unix)]
    pub fn adopt(child: &tokio::process::Child) -> Option<Tree> {
        child.id().map(|pid| Tree { pgid: pid as i32 })
    }

    pub fn kill(&self) {
        #[cfg(windows)]
        unsafe {
            windows_sys::Win32::System::JobObjects::TerminateJobObject(self.job, 1);
        }
        #[cfg(unix)]
        unsafe {
            libc::kill(-self.pgid, libc::SIGKILL);
        }
    }
}

impl Drop for Tree {
    fn drop(&mut self) {
        #[cfg(windows)]
        unsafe {
            // KILL_ON_JOB_CLOSE ends anything still running.
            windows_sys::Win32::Foundation::CloseHandle(self.job);
        }
    }
}

/// Settings every step process needs: its own process group on Unix, no console window on Windows.
pub fn prepare_child(command: &mut tokio::process::Command) {
    #[cfg(windows)]
    command.creation_flags(CREATE_NO_WINDOW);
    #[cfg(unix)]
    command.process_group(0);
}

/// A child that outlives this command must not inherit its terminal or pipes: a caller that reads this command's output
/// (a CI log, `| tee`, a test) would otherwise wait for the worker to exit. Windows passes every inheritable handle on.
#[cfg(windows)]
fn keep_std_handles_private() {
    use windows_sys::Win32::Foundation::{SetHandleInformation, HANDLE_FLAG_INHERIT};
    use windows_sys::Win32::System::Console::{GetStdHandle, STD_ERROR_HANDLE, STD_INPUT_HANDLE, STD_OUTPUT_HANDLE};
    for which in [STD_INPUT_HANDLE, STD_OUTPUT_HANDLE, STD_ERROR_HANDLE] {
        // SAFETY: the handles belong to this process; failure (no console, closed handle) is harmless
        unsafe {
            let handle = GetStdHandle(which);
            if !handle.is_null() && handle as isize != -1 {
                SetHandleInformation(handle, HANDLE_FLAG_INHERIT, 0);
            }
        }
    }
}

/// End a process and its descendants, except this process: the updater is started by the worker and stops the worker, so it
/// is in the worker's tree and must survive it (`taskkill /T` would end it too).
#[cfg(windows)]
fn terminate_tree(root: u32) {
    use windows_sys::Win32::Foundation::{CloseHandle, INVALID_HANDLE_VALUE};
    use windows_sys::Win32::System::Diagnostics::ToolHelp::{CreateToolhelp32Snapshot, Process32FirstW, Process32NextW, PROCESSENTRY32W, TH32CS_SNAPPROCESS};
    use windows_sys::Win32::System::Threading::{OpenProcess, TerminateProcess, PROCESS_TERMINATE};
    let mut parents: Vec<(u32, u32)> = Vec::new();
    // SAFETY: a snapshot handle and a correctly sized PROCESSENTRY32W, both owned here
    unsafe {
        let snapshot = CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0);
        if snapshot != INVALID_HANDLE_VALUE {
            let mut entry: PROCESSENTRY32W = std::mem::zeroed();
            entry.dwSize = std::mem::size_of::<PROCESSENTRY32W>() as u32;
            let mut more = Process32FirstW(snapshot, &mut entry) != 0;
            while more {
                parents.push((entry.th32ProcessID, entry.th32ParentProcessID));
                more = Process32NextW(snapshot, &mut entry) != 0;
            }
            CloseHandle(snapshot);
        }
    }
    let me = std::process::id();
    let mut doomed = vec![root];
    let mut i = 0;
    while i < doomed.len() {
        let parent = doomed[i];
        for (child, ppid) in &parents {
            if *ppid == parent && *child != me && *child != parent && !doomed.contains(child) {
                doomed.push(*child);
            }
        }
        i += 1;
    }
    for pid in doomed.into_iter().rev() {
        // SAFETY: the handle is closed right after use
        unsafe {
            let handle = OpenProcess(PROCESS_TERMINATE, 0, pid);
            if !handle.is_null() {
                TerminateProcess(handle, 1);
                CloseHandle(handle);
            }
        }
    }
}

/// Is this process running with Administrator rights (always false off Windows)?
pub fn is_elevated() -> bool {
    #[cfg(windows)]
    {
        use windows_sys::Win32::Foundation::CloseHandle;
        use windows_sys::Win32::Security::{GetTokenInformation, TokenElevation, TOKEN_ELEVATION, TOKEN_QUERY};
        use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};
        // SAFETY: the token handle is closed; the out-structure is the size the API is told
        unsafe {
            let mut token = std::ptr::null_mut();
            if OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) == 0 {
                return false;
            }
            let mut elevation = TOKEN_ELEVATION { TokenIsElevated: 0 };
            let mut size = 0u32;
            let ok = GetTokenInformation(token, TokenElevation, &mut elevation as *mut _ as *mut _, std::mem::size_of::<TOKEN_ELEVATION>() as u32, &mut size);
            CloseHandle(token);
            ok != 0 && elevation.TokenIsElevated != 0
        }
    }
    #[cfg(not(windows))]
    {
        false
    }
}
