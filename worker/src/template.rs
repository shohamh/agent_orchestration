//! `{{ input.x }}` / `{{ steps.<id>.output[.field...] }}` substitution in step commands, as flowkit/template.py does it.
use regex::Regex;
use serde_json::Value;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

fn expr() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\{\{\s*([a-zA-Z_][\w.]*)\s*\}\}").unwrap())
}

const CMD_SYNTAX: &str = "&|<>^()";
pub const CMD_BUILTINS: &[&str] = &[
    "assoc", "break", "call", "cd", "chdir", "cls", "color", "copy", "date", "del", "dir", "echo", "endlocal", "erase", "exit", "for", "ftype", "goto", "if", "md", "mkdir",
    "mklink", "move", "path", "popd", "prompt", "pushd", "rd", "rem", "ren", "rename", "rmdir", "set", "setlocal", "shift", "start", "time", "title", "type", "ver", "verify", "vol",
];

pub fn lookup<'a>(context: &'a Value, path: &str) -> Result<&'a Value, String> {
    let mut value = context;
    for part in path.split('.') {
        match value.get(part) {
            Some(next) if value.is_object() => value = next,
            _ => return Err(format!("{{{{ {path} }}}} is not available (known: input.*, steps.<needed step>.output)")),
        }
    }
    Ok(value)
}

/// Python's `json.dumps(value, ensure_ascii=False)`: the same separators (`, ` and `: `) so values read the same.
pub fn py_json(value: &Value) -> String {
    match value {
        Value::Null => "null".into(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => serde_json::to_string(s).unwrap(),
        Value::Array(items) => format!("[{}]", items.iter().map(py_json).collect::<Vec<_>>().join(", ")),
        Value::Object(map) => format!("{{{}}}", map.iter().map(|(k, v)| format!("{}: {}", serde_json::to_string(k).unwrap(), py_json(v))).collect::<Vec<_>>().join(", ")),
    }
}

fn text(found: &Value) -> String {
    match found {
        Value::String(s) => s.clone(),
        other => py_json(other),
    }
}

/// shlex.quote
pub fn sh_quote(s: &str) -> String {
    if s.is_empty() {
        return "''".into();
    }
    if s.chars().all(|c| c.is_ascii_alphanumeric() || "@%+=:,./-_".contains(c)) {
        return s.to_string();
    }
    format!("'{}'", s.replace('\'', "'\"'\"'"))
}

/// subprocess.list2cmdline
pub fn list2cmdline(args: &[String]) -> String {
    let mut out = Vec::new();
    for arg in args {
        let needs_quote = arg.is_empty() || arg.contains(' ') || arg.contains('\t');
        let mut result = String::new();
        let mut backslashes = 0;
        if needs_quote {
            result.push('"');
        }
        for c in arg.chars() {
            if c == '\\' {
                backslashes += 1;
            } else if c == '"' {
                result.push_str(&"\\".repeat(backslashes * 2 + 1));
                backslashes = 0;
                result.push('"');
            } else {
                if backslashes > 0 {
                    result.push_str(&"\\".repeat(backslashes));
                    backslashes = 0;
                }
                result.push(c);
            }
        }
        if backslashes > 0 {
            result.push_str(&"\\".repeat(backslashes));
        }
        if needs_quote {
            result.push_str(&"\\".repeat(backslashes));
            result.push('"');
        }
        out.push(result);
    }
    out.join(" ")
}

/// Replace expressions. With `shell` a value is quoted for the platform's shell.
pub fn render(template: &str, context: &Value, flow_dir: &str, shell: bool, windows: bool) -> Result<String, String> {
    if shell && windows {
        return Ok(render_cmd(template, context)?.replace("{flow_dir}", flow_dir));
    }
    let mut out = String::new();
    let mut at = 0;
    for m in expr().captures_iter(template) {
        let whole = m.get(0).unwrap();
        out.push_str(&template[at..whole.start()]);
        let value = text(lookup(context, &m[1])?);
        out.push_str(&if shell { sh_quote(&value) } else { value });
        at = whole.end();
    }
    out.push_str(&template[at..]);
    Ok(out.replace("{flow_dir}", flow_dir))
}

/// cmd.exe has no escape that survives every context, so a value is always in double quotes and may not contain
/// what cmd.exe would still act on there.
fn render_cmd(template: &str, context: &Value) -> Result<String, String> {
    let (mut out, mut at, mut quoted) = (String::new(), 0, false);
    for m in expr().captures_iter(template) {
        let whole = m.get(0).unwrap();
        let literal = &template[at..whole.start()];
        out.push_str(literal);
        if literal.matches('"').count() % 2 == 1 {
            quoted = !quoted;
        }
        let mut value = text(lookup(context, &m[1])?);
        if value.contains(['"', '%', '\r', '\n']) {
            return Err(format!(
                "{{{{ {} }}}} contains a double quote, percent sign or line break, which cmd.exe cannot pass on safely. Start the program directly (without pipes, redirection, built-in commands or %VARIABLES%), or read the value from the FLOW_CONTEXT environment variable.",
                &m[1]
            ));
        }
        let closing = if quoted { template[whole.end()..].starts_with('"') } else { true };
        if closing {
            let trailing = value.len() - value.trim_end_matches('\\').len();
            value.push_str(&"\\".repeat(trailing));
        }
        if quoted {
            out.push_str(&value);
        } else {
            out.push_str(&format!("\"{value}\""));
        }
        at = whole.end();
    }
    out.push_str(&template[at..]);
    Ok(out)
}

/// Find an executable on PATH (with PATHEXT on Windows), like shutil.which.
pub fn which(name: &str) -> Option<PathBuf> {
    let path = Path::new(name);
    let candidates = |dir: &Path| -> Vec<PathBuf> {
        let base = dir.join(name);
        if cfg!(windows) {
            let exts = std::env::var("PATHEXT").unwrap_or(".COM;.EXE;.BAT;.CMD".into());
            let mut out = vec![];
            if path.extension().is_some() {
                out.push(base.clone());
            }
            out.extend(exts.split(';').filter(|e| !e.is_empty()).map(|e| PathBuf::from(format!("{}{}", base.display(), e))));
            out
        } else {
            vec![base]
        }
    };
    let is_exec = |p: &Path| {
        p.is_file() && {
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::metadata(p).map(|m| m.permissions().mode() & 0o111 != 0).unwrap_or(false)
            }
            #[cfg(not(unix))]
            {
                true
            }
        }
    };
    if name.contains('/') || name.contains('\\') {
        return candidates(Path::new("")).into_iter().find(|p| is_exec(p));
    }
    for dir in std::env::split_paths(&std::env::var_os("PATH")?) {
        for candidate in candidates(&dir) {
            if is_exec(&candidate) {
                return Some(candidate);
            }
        }
    }
    None
}

/// Windows: the arguments of a command that can start without cmd.exe, or None. The command is split the way the C
/// runtime splits a command line and each value becomes part of its argument unchanged, so line breaks and quotes reach
/// the program exactly and a value can never start another command.
pub fn command_argv(template: &str, context: &Value, flow_dir: &str) -> Result<Option<Vec<String>>, String> {
    static PIECE: OnceLock<Regex> = OnceLock::new();
    let piece = PIECE.get_or_init(|| Regex::new(r"\{\{\s*([a-zA-Z_][\w.]*)\s*\}\}|\{flow_dir\}").unwrap());
    let mut pieces: Vec<(bool, String)> = vec![];
    let mut at = 0;
    for m in piece.captures_iter(template) {
        let whole = m.get(0).unwrap();
        pieces.push((false, template[at..whole.start()].to_string()));
        pieces.push((true, match m.get(1) {
            None => flow_dir.to_string(),
            Some(path) => text(lookup(context, path.as_str())?),
        }));
        at = whole.end();
    }
    pieces.push((false, template[at..].to_string()));
    let (mut args, mut current, mut started, mut quoted) = (Vec::<String>::new(), String::new(), false, false);
    for (is_value, chunk) in pieces {
        if is_value {
            current.push_str(&chunk);
            started = true;
            continue;
        }
        let chars: Vec<char> = chunk.chars().collect();
        let mut i = 0;
        while i < chars.len() {
            let c = chars[i];
            if c == '\\' {
                let mut end = i;
                while end < chars.len() && chars[end] == '\\' {
                    end += 1;
                }
                let count = end - i;
                if chars.get(end) == Some(&'"') {
                    current.push_str(&"\\".repeat(count / 2));
                    if count % 2 == 1 {
                        current.push('"');
                    }
                    i = end + (count % 2);
                } else {
                    current.push_str(&"\\".repeat(count));
                    i = end;
                }
                started = true;
            } else if c == '"' {
                if quoted && chars.get(i + 1) == Some(&'"') {
                    current.push('"');
                    i += 2;
                } else {
                    quoted = !quoted;
                    i += 1;
                }
                started = true;
            } else if (c == ' ' || c == '\t') && !quoted {
                if started {
                    args.push(std::mem::take(&mut current));
                    started = false;
                }
                i += 1;
            } else if c == '%' || (CMD_SYNTAX.contains(c) && !quoted) {
                return Ok(None);
            } else {
                current.push(c);
                started = true;
                i += 1;
            }
        }
    }
    if started {
        args.push(current);
    }
    if args.is_empty() || CMD_BUILTINS.contains(&args[0].to_lowercase().as_str()) {
        return Ok(None);
    }
    let Some(program) = which(&args[0]) else { return Ok(None) };
    let ext = program.extension().map(|e| e.to_string_lossy().to_lowercase()).unwrap_or_default();
    if ext != "exe" && ext != "com" {
        return Ok(None);
    }
    let mut argv = vec![program.to_string_lossy().to_string()];
    argv.extend(args.into_iter().skip(1));
    Ok(Some(argv))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn ctx() -> Value {
        json!({"input": {"topic": "edge caching", "n": 3, "list": ["a", 1], "quote": "it's"}, "steps": {"a": {"output": {"x": "y z"}}}})
    }

    #[test]
    fn plain_rendering_uses_python_json_for_non_text() {
        assert_eq!(render("t={{ input.topic }} n={{input.n}} l={{ input.list }} f={flow_dir}", &ctx(), "/f", false, false).unwrap(), "t=edge caching n=3 l=[\"a\", 1] f=/f");
    }

    #[test]
    fn posix_shell_values_are_quoted_like_shlex() {
        assert_eq!(render("echo {{ input.topic }} {{ input.quote }} {{ input.n }}", &ctx(), "", true, false).unwrap(), "echo 'edge caching' 'it'\"'\"'s' 3");
        assert_eq!(sh_quote(""), "''");
        assert_eq!(sh_quote("a.b/c-d"), "a.b/c-d");
    }

    #[test]
    fn missing_values_say_what_is_known() {
        let error = render("{{ input.nope }}", &ctx(), "", false, false).unwrap_err();
        assert!(error.contains("is not available"), "{error}");
        assert!(render("{{ steps.a.output.x }}", &ctx(), "", false, false).unwrap() == "y z");
    }

    #[test]
    fn cmd_values_are_quoted_and_dangerous_ones_refused() {
        assert_eq!(render("prog {{ input.topic }}", &ctx(), "", true, true).unwrap(), "prog \"edge caching\"");
        assert_eq!(render("prog \"{{ input.topic }}\"", &ctx(), "", true, true).unwrap(), "prog \"edge caching\"");
        let bad = json!({"input": {"v": "a\"b"}});
        assert!(render("prog {{ input.v }}", &bad, "", true, true).is_err());
    }

    #[test]
    fn list2cmdline_matches_python() {
        let args = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        assert_eq!(list2cmdline(&args(&["a b", "c", "d\"e", "f\\"])), "\"a b\" c d\\\"e f\\");
        assert_eq!(list2cmdline(&args(&["", "x y\\"])), "\"\" \"x y\\\\\"");
    }

    #[test]
    fn command_argv_splits_like_the_c_runtime_and_refuses_shell_syntax() {
        let c = json!({"input": {"v": "has space"}});
        let program = if cfg!(windows) { "cmd" } else { "sh" };
        // a program found on PATH with a .exe extension (Windows) is started directly
        let direct = command_argv(&format!("{program} /c {{{{ input.v }}}}"), &c, "");
        assert!(direct.is_ok());
        assert_eq!(command_argv("echo hi", &c, "").unwrap(), None, "builtins need cmd.exe");
        assert_eq!(command_argv("a | b", &c, "").unwrap(), None);
        assert_eq!(command_argv("a %PATH%", &c, "").unwrap(), None);
    }
}
