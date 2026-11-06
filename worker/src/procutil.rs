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

/// Start a program that keeps running after this process exits and shows no console window.
pub fn spawn_detached(program: &Path, args: &[String], cwd: Option<&Path>, log: Option<&Path>) -> std::io::Result<u32> {
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
        command.creation_flags(DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW);
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
        use std::os::windows::process::CommandExt;
        let _ = Command::new("taskkill").args(["/PID", &pid.to_string(), "/T", "/F"]).creation_flags(CREATE_NO_WINDOW).stdout(Stdio::null()).stderr(Stdio::null()).status();
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
