//! Confluence page trees, through the pinned `confluence-markdown-exporter`: the same code a Python worker runs, so the
//! same Markdown, attachments and manifest. The PC needs no Python: the server holds a runtime (a relocatable CPython and
//! the exporter, built by ci/build-confluence-runtime.py) that this worker fetches the first time a run needs it.
//!
//! `AGENT_WORKER_CONFLUENCE_RUNTIME` replaces the download: an unpacked runtime folder or a runtime archive.
use crate::config;
use crate::procutil::{prepare_child, Tree};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;

const RUN_TIMEOUT: Duration = Duration::from_secs(15 * 60);
const DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(15 * 60);

struct Source {
    home: PathBuf,
    server: String,
    id: String,
    token: String,
}

static SOURCE: OnceLock<Source> = OnceLock::new();
static CACHE: OnceLock<PathBuf> = OnceLock::new();
static PREPARING: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// An installed worker fetches the runtime from its server, with its own token.
pub fn set_source(home: PathBuf, server: &str, id: &str, token: &str) {
    let _ = SOURCE.set(Source { home, server: server.trim_end_matches('/').to_string(), id: id.to_string(), token: token.to_string() });
}

/// Where a `serve` worker without a server unpacks a runtime archive.
pub fn set_cache(path: PathBuf) {
    let _ = CACHE.set(path);
}

fn python(path: &Path) -> PathBuf {
    if cfg!(windows) { path.join("python").join("python.exe") } else { path.join("python").join("bin").join("python3") }
}

fn is_runtime(path: &Path) -> bool {
    python(path).is_file() && path.join("packages").is_dir()
        && path.join("agents").join("flowkit").join("confluence_runner.py").is_file()
        && path.join("agents").join("flowkit").join("confluence_export.py").is_file()
}

/// The system's tar: every supported Windows and Linux has one. On Windows a tar earlier on the PATH (Git's) can't read
/// `C:\` paths, so it must be the system's own.
fn tar() -> PathBuf {
    if cfg!(windows) {
        if let Some(root) = std::env::var_os("SystemRoot") {
            return PathBuf::from(root).join("System32").join("tar.exe");
        }
    }
    PathBuf::from("tar")
}

/// Remove an unpublished download or unpack directory, including when its job is cancelled.
struct TemporaryPath(PathBuf);

impl Drop for TemporaryPath {
    fn drop(&mut self) {
        if self.0.is_dir() {
            let _ = std::fs::remove_dir_all(&self.0);
        } else {
            let _ = std::fs::remove_file(&self.0);
        }
    }
}

/// Unpack into a new folder, then move it into place: a partial runtime can never be mistaken for a complete one.
async fn unpack(archive: &Path, destination: &Path) -> Result<(), String> {
    let parent = destination.parent().ok_or("A runtime needs a parent folder")?;
    let partial = TemporaryPath(parent.join(format!(".unpacking-{}", crate::util::random_hex(6))));
    std::fs::create_dir_all(&partial.0).map_err(|e| e.to_string())?;
    let mut command = Command::new(tar());
    command.arg("-xzf").arg(archive).arg("-C").arg(&partial.0).stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::piped()).kill_on_drop(true);
    prepare_child(&mut command);
    let child = command.spawn().map_err(|e| format!("Could not run tar to unpack the Confluence runtime: {e}"))?;
    let _guard = KillOnDrop(Tree::adopt(&child));
    let output = child.wait_with_output().await.map_err(|e| e.to_string())?;
    if !output.status.success() {
        Err(format!("Could not unpack the Confluence runtime: {}", String::from_utf8_lossy(&output.stderr).trim()))
    } else if !is_runtime(&partial.0) {
        Err("The Confluence runtime archive is incomplete".to_string())
    } else {
        std::fs::rename(&partial.0, destination).map_err(|e| e.to_string())
    }
}

/// The newest runtime the server has, or the installed one when the server can't be reached.
async fn from_server(source: &Source) -> Result<PathBuf, String> {
    let folder = source.home.join("confluence-runtime");
    std::fs::create_dir_all(&folder).map_err(|e| e.to_string())?;
    let current = folder.join("current");
    let installed = std::fs::read_to_string(&current).ok().map(|v| v.trim().to_string()).filter(|v| is_runtime(&folder.join(v)));
    let client = reqwest::Client::builder().connect_timeout(Duration::from_secs(10)).timeout(DOWNLOAD_TIMEOUT).build().map_err(|e| e.to_string())?;
    let request = client.get(format!("{}/relay/{}/update/confluence-runtime", source.server, source.id)).bearer_auth(&source.token)
        .query(&[("os", config::os_name()), ("arch", "x86_64"), ("have", installed.as_deref().unwrap_or(""))]);
    let mut response = match request.send().await {
        Ok(response) => response,
        Err(_) => return installed.map(|v| folder.join(v)).ok_or_else(|| format!("Could not reach {} to fetch the Confluence runtime", source.server)),
    };
    let status = response.status().as_u16();
    if status == 304 {
        return installed.map(|version| folder.join(version)).ok_or_else(|| "The server returned an unchanged Confluence runtime, but none is installed".to_string());
    }
    if status != 200 {
        if let Some(version) = installed {
            return Ok(folder.join(version));
        }
        return Err(if status == 404 {
            "The server has no Confluence runtime for this system. Build it with worker/ci/build-confluence-runtime.py or get it with worker/ci/fetch-release.sh.".to_string()
        } else {
            format!("The server refused the Confluence runtime (HTTP {status})")
        });
    }
    let version = response.headers().get("x-runtime-version").and_then(|v| v.to_str().ok()).map(String::from).ok_or("The server did not say which runtime this is")?;
    let archive = TemporaryPath(folder.join(format!(".download-{}.tar.gz", crate::util::random_hex(6))));
    let mut output = tokio::fs::File::create(&archive.0).await.map_err(|e| e.to_string())?;
    let mut hash = Sha256::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| "The Confluence runtime download was interrupted".to_string())? {
        hash.update(&chunk);
        output.write_all(&chunk).await.map_err(|e| e.to_string())?;
    }
    output.flush().await.map_err(|e| e.to_string())?;
    drop(output);
    let actual = hex::encode(&hash.finalize()[..8]);
    if actual != version {
        return Err(format!("The downloaded Confluence runtime does not match its version (expected {version}, got {actual})"));
    }
    let destination = folder.join(&version);
    if !is_runtime(&destination) {
        let _ = std::fs::remove_dir_all(&destination);
        unpack(&archive.0, &destination).await?;
    }
    crate::util::atomic_write(&current, version.as_bytes()).map_err(|e| e.to_string())?;
    // Keep content-addressed versions: another export may still be importing modules from an earlier runtime.
    Ok(destination)
}

async fn from_override(path: PathBuf) -> Result<PathBuf, String> {
    if is_runtime(&path) {
        return Ok(path);
    }
    if !path.is_file() {
        return Err(format!("AGENT_WORKER_CONFLUENCE_RUNTIME is neither a runtime folder nor an archive: {}", path.display()));
    }
    let mut archive = tokio::fs::File::open(&path).await.map_err(|e| e.to_string())?;
    let mut hash = Sha256::new();
    let mut buffer = [0; 8192];
    loop {
        let count = archive.read(&mut buffer).await.map_err(|e| e.to_string())?;
        if count == 0 {
            break;
        }
        hash.update(&buffer[..count]);
    }
    let version = hex::encode(&hash.finalize()[..8]);
    let cache = CACHE.get().cloned().unwrap_or_else(std::env::temp_dir).join(format!("runtime-{version}"));
    if !is_runtime(&cache) {
        let _ = std::fs::remove_dir_all(&cache);
        std::fs::create_dir_all(cache.parent().unwrap()).map_err(|e| e.to_string())?;
        unpack(&path, &cache).await?;
    }
    Ok(cache)
}

async fn runtime() -> Result<PathBuf, String> {
    let _one_at_a_time = PREPARING.lock().await;
    if let Some(path) = std::env::var_os("AGENT_WORKER_CONFLUENCE_RUNTIME") {
        return from_override(PathBuf::from(path)).await;
    }
    match SOURCE.get() {
        Some(source) => from_server(source).await,
        None => Err("This worker has no server to fetch the Confluence runtime from".to_string()),
    }
}

/// Ends the exporter and everything it started if this run is cancelled.
struct KillOnDrop(Option<Tree>);

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        if let Some(tree) = &self.0 {
            tree.kill();
        }
    }
}

async fn run(mode: &str, item: &Value, target: &Path) -> Result<(), String> {
    let root = std::path::absolute(runtime().await?).map_err(|e| e.to_string())?;
    let python = python(&root);
    let search = std::env::join_paths([root.join("agents"), root.join("packages")]).map_err(|e| e.to_string())?;
    let mut command = Command::new(&python);
    // Nothing of a Python this PC may also have can leak in. Credentials stay in the environment, named by the source.
    command.args(["-u", "-m", "flowkit.confluence_runner"]).current_dir(root.join("agents")).env_remove("PYTHONHOME").env("PYTHONPATH", search).env("PYTHONNOUSERSITE", "1")
        .env("PYTHONUTF8", "1").env("FLOW_MANAGED_JOB", "1").stdin(Stdio::piped()).stdout(Stdio::null()).stderr(Stdio::piped()).kill_on_drop(true);
    prepare_child(&mut command);
    let mut child = command.spawn().map_err(|e| format!("Could not start the Confluence runtime: {e}"))?;
    let _guard = KillOnDrop(Tree::adopt(&child));
    let absolute = std::path::absolute(target).map_err(|e| e.to_string())?;
    #[derive(serde::Serialize)]
    struct Request<'a> {
        mode: &'a str,
        item: &'a Value,
        target: std::borrow::Cow<'a, str>,
    }
    let request = serde_json::to_vec(&Request { mode, item, target: absolute.to_string_lossy() }).map_err(|e| e.to_string())?;
    let mut stdin = child.stdin.take().ok_or("The Confluence runtime has no input")?;
    stdin.write_all(&request).await.map_err(|e| e.to_string())?;
    drop(stdin);
    let output = tokio::time::timeout(RUN_TIMEOUT, child.wait_with_output()).await.map_err(|_| "The Confluence export did not finish within 15 minutes".to_string())?.map_err(|e| e.to_string())?;
    if output.status.success() {
        return Ok(());
    }
    let error = String::from_utf8_lossy(&output.stderr);
    let error = error.trim();
    let start = error.char_indices().rev().nth(3999).map_or(0, |(offset, _)| offset);
    let tail = &error[start..];
    Err(if tail.is_empty() { format!("The Confluence export failed ({})", output.status) } else { tail.to_owned() })
}

/// Import a tree into `target`. An unsuccessful refresh cannot replace a previously complete import.
pub async fn import(item: &Value, target: &Path) -> Result<(), String> {
    run("import", item, target).await
}

/// Is the import still complete, and does it still match Confluence: tree, page versions and attachment versions?
pub async fn validate(item: &Value, target: &Path) -> Result<(), String> {
    run("validate", item, target).await
}
