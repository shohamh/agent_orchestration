//! Portable, bounded file trees: the workspace a job receives and the one it returns (flowkit/artifacts.py).
use base64::{engine::general_purpose::STANDARD, Engine};
use regex::Regex;
use serde_json::{json, Map, Value};
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

pub const MAX_BYTES: usize = 64 * 1024 * 1024;
pub const MAX_FILES: usize = 10_000;

fn reserved() -> Regex {
    Regex::new(r"(?i)^(con|prn|aux|nul|com[1-9]|lpt[1-9])(\..*)?$").unwrap()
}

/// Check a `{name: {data: base64, mode: int}}` map the way the coordinator and the worker both require.
pub fn validate(files: &Value) -> Result<(), String> {
    let map = files.as_object().ok_or("Workspace exceeds 10000 files")?;
    if map.len() > MAX_FILES {
        return Err("Workspace exceeds 10000 files".into());
    }
    let reserved = reserved();
    let (mut seen, mut total) = (HashSet::new(), 0usize);
    for (name, entry) in map {
        let parts: Vec<&str> = name.split('/').collect();
        if name.is_empty() || name.contains('\\') || name.contains(':') || name.starts_with('/') || parts.iter().any(|p| p.is_empty() || *p == "." || *p == "..")
            || name.chars().any(|c| (c as u32) < 32)
            || parts.iter().any(|p| p.ends_with('.') || p.ends_with(' ') || reserved.is_match(p))
        {
            return Err(format!("Unsafe workspace path: {name:?}"));
        }
        if !seen.insert(name.to_lowercase()) {
            return Err(format!("Case collision: {name}"));
        }
        let mode = entry.get("mode").and_then(|m| m.as_i64());
        if !entry.is_object() || mode.map(|m| !(0..=0o777).contains(&m)).unwrap_or(true) {
            return Err(format!("Invalid file entry: {name}"));
        }
        let data = entry.get("data").and_then(|d| d.as_str()).ok_or_else(|| format!("Invalid file content: {name}"))?;
        total += STANDARD.decode(data).map_err(|_| format!("Invalid file content: {name}"))?.len();
        if total > MAX_BYTES {
            return Err("Workspace exceeds 64 MiB transfer limit".into());
        }
    }
    for name in &seen {
        let parts: Vec<&str> = name.split('/').collect();
        for end in 1..parts.len() {
            if seen.contains(&parts[..end].join("/")) {
                return Err(format!("File/directory collision: {name}"));
            }
        }
    }
    Ok(())
}

fn walk(root: &Path, dir: &Path, out: &mut Vec<PathBuf>) -> Result<(), String> {
    let mut entries: Vec<_> = fs::read_dir(dir).map_err(|e| e.to_string())?.filter_map(|e| e.ok()).collect();
    entries.sort_by_key(|e| e.file_name());
    for entry in entries {
        let path = entry.path();
        let kind = entry.file_type().map_err(|e| e.to_string())?;
        if kind.is_symlink() {
            return Err(format!("Symlinks cannot be transferred: {}", path.strip_prefix(root).unwrap_or(&path).display()));
        }
        if kind.is_dir() {
            walk(root, &path, out)?;
        } else if kind.is_file() {
            out.push(path);
        }
    }
    Ok(())
}

/// The files under `root` (names starting `.flow-` are internal and skipped). On Windows an unchanged file keeps the
/// mode it arrived with, since Windows has no execute bit to report.
pub fn snapshot(root: &Path, original: Option<&Value>) -> Result<Value, String> {
    let mut paths = vec![];
    if root.exists() {
        walk(root, root, &mut paths)?;
    }
    let (mut files, mut total) = (Map::new(), 0usize);
    for path in paths {
        let name = path.strip_prefix(root).unwrap().to_string_lossy().replace('\\', "/");
        if name.split('/').next().map(|p| p.starts_with(".flow-")).unwrap_or(false) {
            continue;
        }
        let bytes = fs::read(&path).map_err(|e| e.to_string())?;
        total += bytes.len();
        if total > MAX_BYTES || files.len() >= MAX_FILES {
            return Err("Workspace exceeds transfer limits (64 MiB / 10000 files)".into());
        }
        let data = STANDARD.encode(&bytes);
        #[cfg(unix)]
        let mode = {
            use std::os::unix::fs::PermissionsExt;
            (fs::metadata(&path).map_err(|e| e.to_string())?.permissions().mode() & 0o777) as i64
        };
        #[cfg(not(unix))]
        let mode = {
            let readonly = fs::metadata(&path).map(|m| m.permissions().readonly()).unwrap_or(false);
            let default = if readonly { 0o444 } else { 0o666 };
            original.and_then(|o| o.get(&name)).filter(|o| o.get("data").and_then(|d| d.as_str()) == Some(&data)).and_then(|o| o.get("mode").and_then(|m| m.as_i64())).unwrap_or(default)
        };
        let _ = original;
        files.insert(name, json!({"data": data, "mode": mode}));
    }
    let value = Value::Object(files);
    validate(&value)?;
    Ok(value)
}

pub fn restore(files: &Value, root: &Path) -> Result<(), String> {
    validate(files)?;
    fs::create_dir_all(root).map_err(|e| e.to_string())?;
    for (name, entry) in files.as_object().unwrap() {
        let path = root.join(name);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| e.to_string())?;
        }
        let bytes = STANDARD.decode(entry["data"].as_str().unwrap()).map_err(|e| e.to_string())?;
        fs::write(&path, bytes).map_err(|e| e.to_string())?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = fs::set_permissions(&path, fs::Permissions::from_mode(entry["mode"].as_u64().unwrap_or(0o644) as u32));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(data: &str) -> Value {
        json!({"data": STANDARD.encode(data), "mode": 0o644})
    }

    #[test]
    fn unsafe_names_and_collisions_are_refused() {
        for name in ["../x", "/abs", "a\\b", "a:b", "a//b", "con", "nul.txt", "trail.", "sp ", "x\u{1}y", "./a"] {
            assert!(validate(&json!({name: entry("x")})).is_err(), "{name}");
        }
        assert!(validate(&json!({"a": entry("x"), "A": entry("y")})).is_err(), "case collision");
        assert!(validate(&json!({"a": entry("x"), "a/b": entry("y")})).is_err(), "file under a file");
        assert!(validate(&json!({"dir/ok.txt": entry("x"), "other": entry("y")})).is_ok());
        assert!(validate(&json!({"a": {"data": "not base64!!", "mode": 420}})).is_err());
        assert!(validate(&json!({"a": {"data": "", "mode": 0o1000}})).is_err());
    }

    #[test]
    fn a_tree_round_trips_and_internal_files_stay_home() {
        let dir = tempfile::tempdir().unwrap();
        let files = json!({"src/main.txt": entry("hello"), "run.sh": {"data": STANDARD.encode("#!/bin/sh"), "mode": 0o755}});
        restore(&files, dir.path()).unwrap();
        fs::write(dir.path().join(".flow-internal.json"), "x").unwrap();
        let back = snapshot(dir.path(), Some(&files)).unwrap();
        assert_eq!(back["src/main.txt"]["data"], files["src/main.txt"]["data"]);
        assert!(back.get(".flow-internal.json").is_none());
        #[cfg(unix)]
        assert_eq!(back["run.sh"]["mode"], 0o755);
        #[cfg(windows)]
        assert_eq!(back["run.sh"]["mode"], 0o755, "an unchanged file keeps its mode on Windows");
    }
}
