//! Discovery of the software on this PC and what a step needs (flowkit/capabilities.py and fleet_matching.software_requirement).
use crate::template::{which, CMD_BUILTINS};
use crate::util::iso_now;
use serde_json::{json, Map, Value};

pub const AGENTS: &[&str] = &["demo", "codex", "claude", "opencode", "opencode2", "omp", "pi"];
const COMMON_BUILTINS: &[&str] = &["echo", "cd"];
const POSIX_BUILTINS: &[&str] = &[
    "alias", "bg", "break", "cd", "command", "continue", "echo", "eval", "exec", "exit", "export", "false", "fg", "getopts", "hash", "jobs", "pwd", "read", "readonly", "return", "set",
    "shift", "test", "times", "trap", "true", "type", "ulimit", "umask", "unalias", "unset", "wait",
];

pub fn result(status: &str) -> Value {
    json!({"status": status, "checked_at": iso_now(), "auth_status": "unknown"})
}

/// The one literal command a `run` string starts with, or None when it uses shell syntax that cannot be inferred safely.
pub fn first_word(run: &str, windows: bool) -> Option<String> {
    if run.contains(['\n', '\r', '`']) || run.contains("$(") {
        return None;
    }
    let (mut quote, mut escaped) = (None::<char>, false);
    for c in run.chars() {
        if escaped {
            escaped = false;
            continue;
        }
        if c == '\\' && !windows && quote != Some('\'') {
            escaped = true;
            continue;
        }
        if let Some(q) = quote {
            if c == q {
                quote = None;
            }
        } else if (c == '"' || c == '\'') && (!windows || c == '"') {
            quote = Some(c);
        } else if "|&;<>()".contains(c) {
            return None;
        }
    }
    if quote.is_some() {
        return None;
    }
    // The first token, split the way shlex does (POSIX: quotes removed; Windows: quotes kept, then stripped).
    let mut token = String::new();
    let (mut q, mut esc, mut started) = (None::<char>, false, false);
    for c in run.trim_start().chars() {
        if esc {
            token.push(c);
            esc = false;
            continue;
        }
        if c == '\\' && !windows && q != Some('\'') {
            esc = true;
            started = true;
            continue;
        }
        match q {
            Some(open) => {
                if c == open {
                    q = None;
                    if windows {
                        token.push(c);
                    }
                } else {
                    token.push(c);
                }
            }
            None => {
                if c.is_whitespace() {
                    if started {
                        break;
                    }
                } else if (c == '"' || c == '\'') && (!windows || c == '"') {
                    q = Some(c);
                    started = true;
                    if windows {
                        token.push(c);
                    }
                } else {
                    token.push(c);
                    started = true;
                }
            }
        }
    }
    let name = token.trim_matches('"').to_string();
    if name.is_empty() || name.contains(['{', '}', '*', '?', '=', '$', '%']) {
        return None;
    }
    Some(name)
}

pub fn shell_builtin(run: &str, windows: bool) -> bool {
    first_word(run, windows).map(|w| COMMON_BUILTINS.contains(&w.as_str())).unwrap_or(false)
}

pub fn infer_command(run: &str, windows: bool) -> Option<String> {
    let name = first_word(run, windows)?;
    if ["command", "exec", "eval", "call", "start"].contains(&name.to_lowercase().as_str()) || shell_builtin(run, windows) {
        return None;
    }
    Some(name)
}

/// What a step needs on the PC that runs it: an agent, a command, nothing (a portable builtin), or ambiguous.
pub fn software_requirement(step: &Value, windows: bool) -> Value {
    if step["kind"] == "agent" {
        return json!({"agent": step.get("agent").and_then(|a| a.as_str()).unwrap_or("demo")});
    }
    if let Some(command) = step.get("requires_command").and_then(|c| c.as_str()).filter(|c| !c.is_empty()) {
        return json!({"command": command});
    }
    let run = step.get("run").and_then(|r| r.as_str()).unwrap_or("");
    if let Some(command) = infer_command(run, windows) {
        return json!({"command": command});
    }
    if shell_builtin(run, windows) {
        return json!({});
    }
    json!({"ambiguous_command": true})
}

pub fn probe_command(name: &str) -> Value {
    if name.is_empty() || name.contains(['\0', '\r', '\n']) {
        return result("unknown");
    }
    let builtin = if cfg!(windows) { CMD_BUILTINS.contains(&name.to_lowercase().as_str()) } else { POSIX_BUILTINS.contains(&name) };
    result(if builtin || which(name).is_some() { "installed" } else { "absent" })
}

/// Agent sessions are not run by this worker yet (stage 2), so no agent is reported installed: the coordinator then
/// never places an agent step here instead of failing it in the middle.
pub fn probe_agent(name: &str) -> Value {
    result(if AGENTS.contains(&name) { "absent" } else { "unknown" })
}

pub fn probe(kind: &str, name: &str) -> Value {
    if kind == "agent" { probe_agent(name) } else { probe_command(name) }
}

pub fn refresh(commands: &std::collections::BTreeSet<String>) -> Value {
    let agents: Map<String, Value> = AGENTS.iter().map(|a| (a.to_string(), probe_agent(a))).collect();
    let commands: Map<String, Value> = commands.iter().map(|c| (c.clone(), probe_command(c))).collect();
    json!({"agents": agents, "commands": commands, "refreshed_at": iso_now()})
}

pub fn overdue(inventory: &Value) -> bool {
    inventory.get("refreshed_at").and_then(|t| t.as_str()).and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok()).map(|t| {
        let age = (chrono::Utc::now() - t.with_timezone(&chrono::Utc)).num_seconds();
        !(0..86400).contains(&age)
    }).unwrap_or(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_word_reads_one_literal_command_and_refuses_shell_syntax() {
        assert_eq!(first_word("python script.py --x", false).as_deref(), Some("python"));
        assert_eq!(first_word("  'my tool' a b", false).as_deref(), Some("my tool"));
        for run in ["a | b", "a && b", "echo $(x)", "a `b`", "a; b", "a > f", "echo 'open", "{{ input.cmd }} x", "A=1 run", "x\ny"] {
            assert_eq!(first_word(run, false), None, "{run}");
        }
        assert_eq!(first_word("\"C:\\Program Files\\x.exe\" /y", true).as_deref(), Some("C:\\Program Files\\x.exe"));
    }

    #[test]
    fn requirements_name_agents_commands_builtins_or_ambiguity() {
        use serde_json::json;
        assert_eq!(software_requirement(&json!({"kind": "agent", "agent": "codex"}), false), json!({"agent": "codex"}));
        assert_eq!(software_requirement(&json!({"kind": "program", "run": "git status"}), false), json!({"command": "git"}));
        assert_eq!(software_requirement(&json!({"kind": "program", "run": "x", "requires_command": "tool"}), false), json!({"command": "tool"}));
        assert_eq!(software_requirement(&json!({"kind": "program", "run": "echo hi"}), false), json!({}));
        assert_eq!(software_requirement(&json!({"kind": "program", "run": "a | b"}), false), json!({"ambiguous_command": true}));
        assert_eq!(software_requirement(&json!({"kind": "program", "run": "exec foo"}), false), json!({"ambiguous_command": true}));
    }

    #[test]
    fn a_builtin_is_installed_and_a_missing_program_absent() {
        assert_eq!(probe_command(if cfg!(windows) { "dir" } else { "cd" })["status"], "installed");
        assert_eq!(probe_command("definitely-not-a-real-program-xyz")["status"], "absent");
        assert_eq!(probe_command("a\nb")["status"], "unknown");
    }

    #[test]
    fn an_old_inventory_is_overdue() {
        assert!(overdue(&serde_json::json!({})));
        assert!(!overdue(&serde_json::json!({"refreshed_at": iso_now()})));
        assert!(overdue(&serde_json::json!({"refreshed_at": "2020-01-01T00:00:00+00:00"})));
    }
}
