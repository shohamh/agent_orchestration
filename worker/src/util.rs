//! Small shared helpers: clocks, atomic JSON files, constant-time comparison and the log.
use serde_json::Value;
use std::fs;
use std::io::Write;
use std::path::Path;
use std::sync::{Mutex, OnceLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

static START: OnceLock<Instant> = OnceLock::new();
static LOG: Mutex<Option<fs::File>> = Mutex::new(None);

/// Seconds on a monotonic clock (the same role as Python's `time.monotonic`).
pub fn mono() -> f64 {
    START.get_or_init(Instant::now).elapsed().as_secs_f64() + 1000.0
}

pub fn now_unix() -> f64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs_f64()).unwrap_or(0.0)
}

pub fn iso_now() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Micros, false)
}

pub fn eq_const(a: &str, b: &str) -> bool {
    use subtle::ConstantTimeEq;
    a.as_bytes().ct_eq(b.as_bytes()).into()
}

pub fn read_json(path: &Path) -> Option<Value> {
    serde_json::from_slice(&fs::read(path).ok()?).ok()
}

/// Write a file so a reader never sees half of it; on Windows a reader can briefly hold the target, so retry.
pub fn atomic_write(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension(format!("tmp{}", std::process::id()));
    fs::write(&tmp, bytes)?;
    let mut last = None;
    for _ in 0..40 {
        match fs::rename(&tmp, path) {
            Ok(()) => return Ok(()),
            Err(e) => {
                last = Some(e);
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
        }
    }
    let _ = fs::remove_file(&tmp);
    Err(last.unwrap())
}

pub fn atomic_json(path: &Path, value: &Value) -> std::io::Result<()> {
    atomic_write(path, serde_json::to_vec(value).unwrap().as_slice())
}

/// Send the log to a file (used by the startup entry, which has no console) as well as stderr.
pub fn init_log(path: Option<&Path>) {
    if let Some(path) = path {
        if let Ok(file) = fs::OpenOptions::new().create(true).append(true).open(path) {
            *LOG.lock().unwrap() = Some(file);
        }
    }
}

pub fn log_line(component: &str, message: &str) {
    let line = format!("{} {} {}\n", chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.3f"), component, message);
    if let Some(file) = LOG.lock().unwrap().as_mut() {
        let _ = file.write_all(line.as_bytes());
    }
    eprint!("{line}");
}

#[macro_export]
macro_rules! log {
    ($component:expr, $($arg:tt)*) => { $crate::util::log_line($component, &format!($($arg)*)) };
}

/// A random lowercase hex string of `bytes` bytes.
pub fn random_hex(bytes: usize) -> String {
    let mut out = String::new();
    while out.len() < bytes * 2 {
        out.push_str(&uuid::Uuid::new_v4().simple().to_string());
    }
    out.truncate(bytes * 2);
    out
}
