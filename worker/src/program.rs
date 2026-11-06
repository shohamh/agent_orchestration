//! Run one `program` step: render its command, start it in the job's workspace and stream its output.
use crate::procutil::{prepare_child, Tree};
use crate::template::{command_argv, list2cmdline, py_json, render};
use serde_json::{json, Value};
use std::path::Path;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;

const STDOUT_TAIL: usize = 40;

/// Receives the events a step produces (`log`, `step`).
pub type Emit = Arc<dyn Fn(Value) + Send + Sync>;

pub struct Emitter {
    seq: Mutex<u64>,
    sink: Emit,
}

fn clip(text: &str, limit: usize) -> String {
    let chars: Vec<char> = text.chars().collect();
    if chars.len() <= limit {
        return text.to_string();
    }
    let kept: String = chars[..limit - 40].iter().collect();
    format!("{kept}\n… [{} more chars]", chars.len() - limit + 40)
}

impl Emitter {
    pub fn new(sink: Emit) -> Emitter {
        Emitter { seq: Mutex::new(0), sink }
    }

    /// `{"seq", "ts", "type", ...fields}` with long text clipped as the session events do.
    pub fn emit(&self, kind: &str, fields: Value) {
        let seq = {
            let mut s = self.seq.lock().unwrap();
            *s += 1;
            *s
        };
        let mut event = json!({"seq": seq, "ts": (crate::util::now_unix() * 1000.0).round() / 1000.0, "type": kind});
        if let Some(map) = fields.as_object() {
            for (key, value) in map {
                let limit = match key.as_str() { "text" => 6000, "output" | "detail" => 2500, _ => 0 };
                event[key] = match (value.as_str(), limit) {
                    (Some(s), l) if l > 0 => json!(clip(s, l)),
                    _ => value.clone(),
                };
            }
        }
        (self.sink)(event);
    }
}

fn bounded(output: &Value) -> Value {
    let text = py_json(output);
    if text.chars().count() <= 60000 { output.clone() } else { json!({"truncated": true, "preview": text.chars().take(2000).collect::<String>()}) }
}

pub struct ProgramRun<'a> {
    pub step_id: &'a str,
    pub step: &'a Value,
    pub context: &'a Value,
    pub workspace: &'a Path,
    pub assets: &'a Path,
    pub workspaces: &'a Path,
    pub tree: Arc<Mutex<Option<Tree>>>,
}

pub async fn run_program(run: ProgramRun<'_>, emit: &Emitter) -> Result<Value, String> {
    let windows = cfg!(windows);
    let template = run.step.get("run").and_then(|r| r.as_str()).ok_or("A program step needs a run command")?;
    let flow_dir = run.assets.to_string_lossy().to_string();
    let expression = regex::Regex::new(r"\{\{\s*[a-zA-Z_][\w.]*\s*\}\}").unwrap().is_match(template);
    // cmd.exe cuts a command at a line break and has no reliable quoting, so on Windows a command that takes values
    // starts its program directly whenever it does not need the shell.
    let argv = if windows && expression { command_argv(template, run.context, &flow_dir)? } else { None };
    let command_text = match &argv {
        Some(argv) => list2cmdline(argv),
        None => render(template, run.context, &flow_dir, true, windows)?,
    };
    let output_file = run.workspace.join(format!(".flow-output-{}.json", run.step_id));
    let _ = std::fs::remove_file(&output_file);
    let context_json = py_json(run.context);

    let mut command = match &argv {
        Some(argv) => {
            let mut c = Command::new(&argv[0]);
            c.args(&argv[1..]);
            c
        }
        None => {
            #[cfg(windows)]
            {
                let mut c = Command::new(std::env::var("COMSPEC").unwrap_or_else(|_| "cmd.exe".into()));
                c.raw_arg(format!("/c \"{command_text}\""));
                c
            }
            #[cfg(not(windows))]
            {
                let mut c = Command::new("/bin/sh");
                c.arg("-c").arg(&command_text);
                c
            }
        }
    };
    command
        .current_dir(run.workspace)
        .env("FLOW_CONTEXT", &context_json)
        .env("FLOW_OUTPUT", &output_file)
        .env("FLOW_STEP", run.step_id)
        .env("FLOW_DIR", &flow_dir)
        .env("FLOW_ASSETS_DIR", &flow_dir)
        .env("FLOW_WORKSPACES", run.workspaces)
        .env("FLOW_MANAGED_JOB", "1")
        .env("PYTHONUTF8", "1")
        .env("PYTHONUNBUFFERED", "1")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    prepare_child(&mut command);
    emit.emit("step", json!({"state": "running", "kind": "program", "command": command_text}));
    let mut child = command.spawn().map_err(|e| format!("Could not start the step: {e}"))?;
    *run.tree.lock().unwrap() = Tree::adopt(&child);
    let mut stdin = child.stdin.take().unwrap();
    let (stdout, stderr) = (child.stdout.take().unwrap(), child.stderr.take().unwrap());
    let tail = Arc::new(Mutex::new(Vec::<String>::new()));

    // Feed the context, then read both streams to the end, emitting each line as it arrives.
    let feed = async move {
        let _ = stdin.write_all(context_json.as_bytes()).await;
        let _ = stdin.shutdown().await;
    };
    let read = |stream: Box<dyn tokio::io::AsyncRead + Unpin + Send>, name: &'static str, tail: Option<Arc<Mutex<Vec<String>>>>| async move {
        let mut reader = BufReader::new(stream);
        let mut buffer = Vec::new();
        loop {
            buffer.clear();
            match reader.read_until(b'\n', &mut buffer).await {
                Ok(0) | Err(_) => break,
                Ok(_) => {
                    let text = String::from_utf8_lossy(&buffer).trim_end_matches(['\r', '\n']).to_string();
                    if let Some(tail) = &tail {
                        let mut t = tail.lock().unwrap();
                        t.push(text.clone());
                        let excess = t.len().saturating_sub(STDOUT_TAIL);
                        t.drain(..excess);
                    }
                    emit.emit("log", json!({"stream": name, "text": text}));
                }
            }
        }
    };
    tokio::join!(feed, read(Box::new(stdout), "stdout", Some(tail.clone())), read(Box::new(stderr), "stderr", None));
    let status = child.wait().await.map_err(|e| e.to_string())?;
    let code = status.code().unwrap_or(-1);
    let tail_lines = tail.lock().unwrap().clone();
    if code != 0 {
        emit.emit("step", json!({"state": "failed", "kind": "program", "exit_code": code}));
        let last: Vec<String> = tail_lines.iter().rev().take(3).rev().cloned().collect();
        let mut summary = last.join(" | ");
        summary = summary.chars().take(500).collect();
        return Err(format!("step '{}' exited with code {code}: {summary}", run.step_id));
    }
    let output = match std::fs::read(&output_file) {
        Ok(bytes) => serde_json::from_slice::<Value>(&bytes).map_err(|e| format!("step '{}' wrote invalid JSON to FLOW_OUTPUT: {e}", run.step_id))?,
        Err(_) => json!({"stdout": tail_lines.join("\n")}),
    };
    emit.emit("step", json!({"state": "done", "kind": "program", "output": bounded(&output)}));
    Ok(output)
}
