//! Context on the worker: git repositories, files from this PC and Confluence page trees, the same sources and report as
//! agents/flowkit/environment.py. A flow's setup step then opens an agent to repair what failed and waits for a human.
use crate::artifacts;
use crate::confluence;
use crate::procutil::prepare_child;
use crate::program::Emitter;
use crate::session::{run_session, Inbox, Trees};
use crate::util::atomic_json;
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;
use tokio::process::Command;

const KINDS: [&str; 3] = ["repositories", "files", "confluence"];
const MAX_BYTES: u64 = 64 * 1024 * 1024;
const GIT_TIMEOUT: Duration = Duration::from_secs(300);

async fn git(args: &[&str]) -> Result<String, String> {
    let mut command = Command::new("git");
    command.args(args).env("GIT_TERMINAL_PROMPT", "0").stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped()).kill_on_drop(true);
    prepare_child(&mut command);
    let child = command.spawn().map_err(|e| format!("Could not run git: {e}"))?;
    let output = tokio::time::timeout(GIT_TIMEOUT, child.wait_with_output()).await.map_err(|_| "git did not finish within 5 minutes".to_string())?.map_err(|e| e.to_string())?;
    if output.status.success() {
        return Ok(String::from_utf8_lossy(&output.stdout).trim().to_string());
    }
    let error = String::from_utf8_lossy(&output.stderr);
    let tail: String = error.chars().rev().take(2000).collect::<Vec<_>>().into_iter().rev().collect();
    Err(if tail.is_empty() { "git failed".into() } else { tail })
}

fn target_path(workspace: &Path, item: &Value) -> Result<PathBuf, String> {
    let target = item["target"].as_str().ok_or("A source needs a target folder")?;
    artifacts::validate(&json!({target: {"data": "", "mode": 420}}))?;
    if target.split('/').any(|part| part.starts_with(".flow-")) {
        return Err("Context target uses a reserved runtime directory".into());
    }
    let path = workspace.join(target);
    if path.is_symlink() {
        return Err("Unsafe context destination".into());
    }
    Ok(path)
}

/// A branch other than the default exists only as origin/<branch> after a full clone.
async fn resolved_ref(reference: &str, target: &str) -> String {
    let commit = format!("{reference}^{{commit}}");
    match git(&["-C", target, "rev-parse", "--verify", "--quiet", &commit]).await {
        Ok(_) => reference.to_string(),
        Err(_) => format!("origin/{reference}"),
    }
}

async fn clone(item: &Value, target: &Path) -> Result<(), String> {
    let url = item["url"].as_str().ok_or("A repository needs a url")?;
    if url.starts_with('-') {
        return Err("Invalid git URL".into());
    }
    let reference = item["ref"].as_str().filter(|r| !r.is_empty());
    if reference.map(|r| r.starts_with('-')).unwrap_or(false) {
        return Err("Invalid git ref".into());
    }
    let path = target.to_string_lossy().to_string();
    if item["shallow"].as_bool().unwrap_or(false) {
        // A shallow clone stays within a run's transfer limit, but a commit id needs a full one.
        let mut args = vec!["clone", "--no-hardlinks", "--depth", "1"];
        if let Some(reference) = reference {
            args.extend(["--branch", reference]);
        }
        args.extend(["--", url, &path]);
        match git(&args).await {
            Ok(_) => return Ok(()),
            Err(problem) if reference.is_none() => return Err(problem),
            Err(_) => {
                let _ = std::fs::remove_dir_all(target);
            }
        }
    }
    git(&["clone", "--no-hardlinks", "--", url, &path]).await?;
    if let Some(reference) = reference {
        let resolved = resolved_ref(reference, &path).await;
        git(&["-C", &path, "checkout", "--detach", &resolved]).await?;
    }
    Ok(())
}

async fn validate_repository(item: &Value, target: &Path) -> Result<(), String> {
    let path = target.to_string_lossy().to_string();
    if git(&["-C", &path, "rev-parse", "--is-inside-work-tree"]).await? != "true" {
        return Err("Missing git worktree".into());
    }
    if git(&["-C", &path, "remote", "get-url", "origin"]).await?.as_str() != item["url"].as_str().unwrap_or("") {
        return Err("Repository origin differs from the requested source".into());
    }
    let head = git(&["-C", &path, "rev-parse", "HEAD"]).await?;
    if !git(&["-C", &path, "ls-files", "--deleted"]).await?.is_empty() {
        return Err("Repository has missing tracked files; restore the worktree before completing setup".into());
    }
    if let Some(reference) = item["ref"].as_str().filter(|r| !r.is_empty()) {
        let wanted = format!("{}^{{commit}}", resolved_ref(reference, &path).await);
        if head != git(&["-C", &path, "rev-parse", &wanted]).await? {
            return Err("Repository revision differs from requested ref".into());
        }
    }
    Ok(())
}

fn source_path(item: &Value) -> Result<PathBuf, String> {
    let source = item["source"].as_str().ok_or("A file source needs a source path")?;
    let expanded = match (source.strip_prefix("~/"), std::env::var_os(if cfg!(windows) { "USERPROFILE" } else { "HOME" })) {
        (Some(rest), Some(home)) => PathBuf::from(home).join(rest),
        _ => PathBuf::from(source),
    };
    if expanded.is_symlink() {
        return Err("Source file may not be a symlink".into());
    }
    Ok(expanded)
}

fn copy_source(item: &Value, target: &Path) -> Result<(), String> {
    let source = source_path(item)?;
    if source.is_dir() {
        return artifacts::restore(&artifacts::snapshot(&source, None)?, target);
    }
    let size = std::fs::metadata(&source).map_err(|e| format!("{}: {e}", source.display()))?.len();
    if size > MAX_BYTES {
        return Err("Context file exceeds transfer limit".into());
    }
    std::fs::copy(&source, target).map(|_| ()).map_err(|e| e.to_string())
}

fn validate_copy(item: &Value, target: &Path) -> Result<(), String> {
    let source = source_path(item)?;
    if source.is_dir() {
        if artifacts::snapshot(&source, None)? != artifacts::snapshot(target, None)? {
            return Err("Copied directory is incomplete or differs from source".into());
        }
        return Ok(());
    }
    let same = target.is_file() && std::fs::read(&source).map_err(|e| e.to_string())? == std::fs::read(target).map_err(|e| e.to_string())?;
    if same { Ok(()) } else { Err("Copied file is missing or differs from source".into()) }
}

/// Fetch every source, failures included in the report: one bad source doesn't hide the others.
pub async fn prepare_sources(config: &Value, workspace: &Path) -> Vec<Value> {
    let mut report = vec![];
    for kind in KINDS {
        for item in config[kind].as_array().into_iter().flatten() {
            let outcome = async {
                let target = target_path(workspace, item)?;
                if let Some(parent) = target.parent() {
                    std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
                }
                match kind {
                    "repositories" => {
                        if !target.exists() {
                            clone(item, &target).await?;
                        }
                        validate_repository(item, &target).await
                    }
                    "files" => copy_source(item, &target),
                    _ => confluence::import(item, &target).await,
                }
            }
            .await;
            let source = item["target"].clone();
            report.push(match outcome {
                Ok(()) => json!({"source": source, "kind": kind, "ok": true}),
                // Responses never echo headers or credentials.
                Err(problem) => json!({"source": source, "kind": kind, "ok": false, "error": problem.chars().take(2500).collect::<String>()}),
            });
        }
    }
    let _ = atomic_json(&workspace.join(".flow-setup-report.json"), &json!(report));
    report
}

async fn validate_sources(config: &Value, workspace: &Path) -> Vec<String> {
    let mut errors = vec![];
    for kind in KINDS {
        for item in config[kind].as_array().into_iter().flatten() {
            let outcome = async {
                let target = target_path(workspace, item)?;
                match kind {
                    "repositories" => validate_repository(item, &target).await,
                    "files" => validate_copy(item, &target),
                    _ => confluence::validate(item, &target).await,
                }
            }
            .await;
            if let Err(problem) = outcome {
                errors.push(format!("{}: {}", item["target"].as_str().unwrap_or(""), problem.chars().take(2000).collect::<String>()));
            }
        }
    }
    errors
}

fn report_rows(report: &[Value], emitter: &Emitter) {
    for item in report {
        let ok = item["ok"].as_bool().unwrap_or(false);
        emitter.emit("tool", json!({"id": format!("setup:{}", item["source"].as_str().unwrap_or("")), "name": format!("Prepare {}", item["kind"].as_str().unwrap_or("")),
            "summary": item["source"], "status": if ok { "ok" } else { "error" }, "output": item.get("error").cloned().unwrap_or(json!("Context copied"))}));
    }
}

/// The sources attached to one run in New card: fetched without a human, and a failure fails the run.
pub async fn prepare_attached(config: &Value, workspace: &Path, emitter: &Emitter) -> Result<Value, String> {
    // A Confluence page attached in the form uses this PC's standard credential variables when they are set.
    let mut config = config.clone();
    for item in config["confluence"].as_array_mut().into_iter().flatten() {
        for (key, name) in [("token_env", "CONFLUENCE_TOKEN"), ("username_env", "CONFLUENCE_EMAIL")] {
            if std::env::var_os(name).is_some() && item.get(key).is_none() {
                item[key] = json!(name);
            }
        }
    }
    for kind in ["repositories", "confluence"] {
        for item in config[kind].as_array().into_iter().flatten() {
            let target = item["target"].as_str().unwrap_or("");
            emitter.emit("tool", json!({"id": format!("context:{target}"), "name": format!("Prepare {kind}"), "summary": target, "status": "running"}));
        }
    }
    std::fs::create_dir_all(workspace).map_err(|e| e.to_string())?;
    let report = prepare_sources(&config, workspace).await;
    let mut failed = vec![];
    for item in &report {
        let ok = item["ok"].as_bool().unwrap_or(false);
        let source = item["source"].as_str().unwrap_or("");
        emitter.emit("tool", json!({"id": format!("context:{source}"), "name": format!("Prepare {}", item["kind"].as_str().unwrap_or("")), "summary": source,
            "status": if ok { "ok" } else { "error" }, "output": item.get("error").cloned().unwrap_or(json!("Ready"))}));
        if !ok {
            failed.push(format!("{source}: {}", item["error"].as_str().unwrap_or("")));
        }
    }
    if failed.is_empty() { Ok(json!({"sources": report})) } else { Err(format!("Could not prepare what was attached to this run:\n{}", failed.join("\n"))) }
}

/// A flow's setup step: fetch the sources, open an agent for repairs, and let dependents go only when validation passes.
pub async fn setup_environment(config: &Value, workspace: &Path, inbox: Inbox, emitter: Arc<Emitter>, trees: Trees) -> Result<Value, String> {
    let report = prepare_sources(config, workspace).await;
    report_rows(&report, &emitter);
    let agent = config["agent"].as_str().unwrap_or("codex");
    let mut instructions = format!("Prepare this environment and repair any source errors. Review .flow-setup-report.json. \
Do not change the requested source locations. Credentials are available only through worker environment variables. \
Explain any remaining login or network issues to the human. Wait for review.\n{config}\n{}", json!(report));
    loop {
        let session = json!({"goal": instructions, "agent": agent, "workspace": workspace.display().to_string(), "autoDone": false, "quietDemo": agent == "demo"});
        run_session(session, inbox.clone(), emitter.clone(), trees.clone()).await?;
        let errors = validate_sources(config, workspace).await;
        if errors.is_empty() {
            return Ok(json!({"sources": report, "validated": true}));
        }
        instructions = format!("Setup is incomplete. Repair these sources before marking done:\n{}", errors.join("\n"));
        emitter.emit("error", json!({"message": instructions, "will_retry": false}));
        emitter.emit("status", json!({"state": "awaiting_input", "reason": "Setup validation failed; dependencies remain blocked"}));
    }
}
