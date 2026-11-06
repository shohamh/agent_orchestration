//! Confluence page trees as Markdown, without Python: the REST API, a small HTML converter and the same export layout as
//! agents/flowkit/confluence_export.py (`<page id>.md`, `attachments/<page id>/<attachment id><ext>`, `index.json`, and
//! a manifest of file hashes that shows later whether the import is still complete and current).
use crate::artifacts;
use crate::util::atomic_json;
use base64::Engine;
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashSet};
use std::path::Path;
use std::time::Duration;

const MANIFEST: &str = "confluence-export.json";
const EXPORTER: &str = "agent-worker-1";
const MAX_PAGES: usize = 2000;
const TIMEOUT: Duration = Duration::from_secs(600);

struct Client {
    http: reqwest::Client,
    base: String,
    authorization: Option<String>,
}

impl Client {
    fn new(item: &Value) -> Result<Client, String> {
        let base = item["base_url"].as_str().ok_or("A Confluence source needs a base_url")?.trim_end_matches('/').to_string();
        let credential = |key: &str| -> Result<Option<String>, String> {
            let Some(name) = item[key].as_str() else { return Ok(None) };
            std::env::var(name).ok().filter(|v| !v.is_empty()).map(Some).ok_or_else(|| format!("Missing worker credential: {name}"))
        };
        let (token, user) = (credential("token_env")?, credential("username_env")?);
        let authorization = match (user, token) {
            (Some(user), Some(token)) => Some(format!("Basic {}", base64::engine::general_purpose::STANDARD.encode(format!("{user}:{token}")))),
            (None, Some(token)) => Some(format!("Bearer {token}")),
            _ => None,
        };
        let http = reqwest::Client::builder().connect_timeout(Duration::from_secs(10)).timeout(Duration::from_secs(60)).build().map_err(|e| e.to_string())?;
        Ok(Client { http, base, authorization })
    }

    async fn get(&self, path: &str) -> Result<reqwest::Response, String> {
        let url = if path.starts_with("http://") || path.starts_with("https://") { path.to_string() } else { format!("{}{path}", self.base) };
        let mut last = String::new();
        for attempt in 0..3u64 {
            if attempt > 0 {
                tokio::time::sleep(Duration::from_secs(attempt * 2)).await;
            }
            let mut request = self.http.get(&url);
            if let Some(authorization) = &self.authorization {
                request = request.header("Authorization", authorization);
            }
            match request.send().await {
                Ok(response) if response.status().is_success() => return Ok(response),
                Ok(response) => {
                    let status = response.status();
                    last = format!("Confluence answered {} for {path}", status.as_u16());
                    if !(status.is_server_error() || status.as_u16() == 429) {
                        return Err(last);
                    }
                }
                Err(_) => last = format!("Could not reach Confluence for {path}"), // the error text can hold the URL's credentials
            }
        }
        Err(last)
    }

    async fn json(&self, path: &str) -> Result<Value, String> {
        self.get(path).await?.json().await.map_err(|_| format!("Confluence sent an unreadable answer for {path}"))
    }

    /// Every result of a paged listing.
    async fn all(&self, first: String) -> Result<Vec<Value>, String> {
        let (mut results, mut next) = (vec![], Some(first));
        while let Some(path) = next {
            let page = self.json(&path).await?;
            results.extend(page["results"].as_array().cloned().unwrap_or_default());
            if results.len() > MAX_PAGES {
                return Err(format!("Confluence tree exceeds {MAX_PAGES} pages"));
            }
            next = page["_links"]["next"].as_str().map(String::from);
        }
        Ok(results)
    }

    /// The page ids of the tree, the root first.
    async fn tree(&self, item: &Value) -> Result<Vec<Value>, String> {
        let root_id = item["page_id"].as_str().ok_or("A Confluence source needs a page_id")?;
        let root = self.json(&format!("/rest/api/content/{root_id}?expand=version")).await.map_err(|e| format!("Confluence root page is not accessible: {e}"))?;
        if root["id"].as_str() != Some(root_id) {
            return Err("Confluence root page is not accessible".into());
        }
        let mut pages = vec![root];
        if item["include_descendants"].as_bool().unwrap_or(true) {
            pages.extend(self.descendants(root_id).await?);
        }
        if pages.len() > MAX_PAGES {
            return Err(format!("Confluence tree exceeds {MAX_PAGES} pages"));
        }
        Ok(pages)
    }

    /// Sites differ in what they serve: search first, as the Python exporter does, then the older listings.
    async fn descendants(&self, root: &str) -> Result<Vec<Value>, String> {
        let searched = self.all(format!("/rest/api/content/search?cql=ancestor%3D{root}%20and%20type%3Dpage&expand=version&limit=100")).await;
        if let Ok(pages) = searched {
            return Ok(pages);
        }
        let listed = self.all(format!("/rest/api/content/{root}/descendant/page?expand=version&limit=100")).await;
        if let Ok(pages) = listed {
            return Ok(pages);
        }
        let (mut pages, mut waiting) = (vec![], vec![root.to_string()]);
        while let Some(parent) = waiting.pop() {
            for child in self.all(format!("/rest/api/content/{parent}/child/page?expand=version&limit=100")).await? {
                waiting.extend(child["id"].as_str().map(String::from));
                pages.push(child);
                if pages.len() > MAX_PAGES {
                    return Err(format!("Confluence tree exceeds {MAX_PAGES} pages"));
                }
            }
        }
        Ok(pages)
    }

    async fn attachments(&self, page: &str) -> Result<Vec<Value>, String> {
        self.all(format!("/rest/api/content/{page}/child/attachment?expand=version&limit=100")).await
    }
}

fn identity(item: &Value) -> Value {
    json!({"base_url": item["base_url"].as_str().unwrap_or("").trim_end_matches('/'), "page_id": item["page_id"],
        "include_descendants": item["include_descendants"].as_bool().unwrap_or(true),
        "attachments": item["attachments"].as_str().unwrap_or("referenced"), "exporter_version": EXPORTER})
}

fn percent_decode(text: &str) -> String {
    let bytes = text.as_bytes();
    let (mut out, mut at) = (Vec::with_capacity(bytes.len()), 0);
    while at < bytes.len() {
        if bytes[at] == b'%' && at + 2 < bytes.len() {
            let digits = std::str::from_utf8(&bytes[at + 1..at + 3]).ok().and_then(|d| u8::from_str_radix(d, 16).ok());
            if let Some(value) = digits {
                out.push(value);
                at += 3;
                continue;
            }
        }
        out.push(if bytes[at] == b'+' { b' ' } else { bytes[at] });
        at += 1;
    }
    String::from_utf8_lossy(&out).to_string()
}

fn extension(name: &str) -> String {
    match name.rfind('.') {
        Some(dot) if dot > 0 && name[dot + 1..].chars().all(|c| c.is_ascii_alphanumeric()) && name.len() - dot <= 12 => name[dot..].to_lowercase(),
        _ => String::new(),
    }
}

fn file_hashes(target: &Path) -> Result<Value, String> {
    // The normal workspace path and size rules apply to imports too.
    let files = artifacts::snapshot(target, None)?;
    let mut hashes = Map::new();
    for (name, entry) in files.as_object().into_iter().flatten() {
        if name != MANIFEST {
            let bytes = base64::engine::general_purpose::STANDARD.decode(entry["data"].as_str().unwrap_or("")).map_err(|e| e.to_string())?;
            hashes.insert(name.clone(), json!(hex::encode(Sha256::digest(&bytes))));
        }
    }
    Ok(Value::Object(hashes))
}

fn local_manifest(item: &Value, target: &Path) -> Result<Value, String> {
    let unavailable = || "Complete Confluence Markdown export manifest is unavailable; re-import the source".to_string();
    let data: Value = serde_json::from_slice(&std::fs::read(target.join(MANIFEST)).map_err(|_| unavailable())?).map_err(|_| unavailable())?;
    if data["source"] != identity(item) {
        return Err("Confluence export belongs to another source or exporter version".into());
    }
    if data["files"] != file_hashes(target)? {
        return Err("Confluence Markdown or attachments are missing or have changed; re-import the source".into());
    }
    if !data["pages"].as_array().map(|pages| pages.iter().any(|p| p["id"] == item["page_id"])).unwrap_or(false) {
        return Err("Confluence root page is missing from the export".into());
    }
    Ok(data)
}

fn version_of(entity: &Value) -> i64 {
    entity["version"]["number"].as_i64().unwrap_or(0)
}

/// The attachments this export keeps: all of them, none, or (the default) those the page refers to.
fn chosen<'a>(mode: &str, attachments: &'a [Value], html: Option<&str>) -> Vec<&'a Value> {
    attachments.iter().filter(|attachment| match mode {
        "disabled" => false,
        "all" => true,
        _ => match html {
            Some(html) => {
                let decoded = percent_decode(html);
                let title = attachment["title"].as_str().unwrap_or("\u{0}");
                decoded.contains(&format!("/{title}")) || decoded.contains(&format!("\"{title}\""))
            }
            None => true,
        },
    }).collect()
}

async fn export(client: &Client, item: &Value, stage: &Path) -> Result<Vec<Value>, String> {
    let mode = item["attachments"].as_str().unwrap_or("referenced");
    let tree = client.tree(item).await?;
    let ids: HashSet<String> = tree.iter().filter_map(|p| p["id"].as_str().map(String::from)).collect();
    let mut index = vec![];
    for listed in &tree {
        let id = listed["id"].as_str().ok_or("Confluence listed a page without an id")?.to_string();
        let page = client.json(&format!("/rest/api/content/{id}?expand=body.export_view,version,space")).await?;
        if version_of(listed) != 0 && version_of(&page) != version_of(listed) {
            return Err(format!("Confluence page {id} was inaccessible or changed during export"));
        }
        let html = page["body"]["export_view"]["value"].as_str().unwrap_or("");
        let attachments = if mode == "disabled" { vec![] } else { client.attachments(&id).await? };
        let kept = chosen(mode, &attachments, Some(html));
        let mut saved = BTreeMap::new();
        let mut local = BTreeMap::new();
        for attachment in &kept {
            let attachment_id = attachment["id"].as_str().ok_or("Confluence listed an attachment without an id")?;
            let title = attachment["title"].as_str().unwrap_or("");
            let download = attachment["_links"]["download"].as_str().ok_or("Confluence listed an attachment without a download link")?;
            let bytes = client.get(download).await?.bytes().await.map_err(|_| format!("Could not download attachment {title}"))?;
            let relative = format!("attachments/{id}/{attachment_id}{}", extension(title));
            let path = stage.join(&relative);
            std::fs::create_dir_all(path.parent().unwrap()).map_err(|e| e.to_string())?;
            std::fs::write(&path, &bytes).map_err(|e| e.to_string())?;
            saved.insert(attachment_id.to_string(), json!(version_of(attachment)));
            local.insert(title.to_string(), relative);
        }
        let title = page["title"].as_str().unwrap_or("").to_string();
        let links = Links { base: client.base.clone(), ids: &ids, attachments: local };
        let body = to_markdown(html, &links);
        let web = page["_links"]["webui"].as_str().map(|w| format!("{}{w}", client.base)).unwrap_or_default();
        let front = format!("---\ntitle: {}\npage_id: \"{id}\"\nversion: {}\nurl: {}\n---\n\n", json!(title), version_of(&page), json!(web));
        let file = format!("{id}.md");
        std::fs::write(stage.join(&file), format!("{front}# {title}\n\n{body}\n")).map_err(|e| e.to_string())?;
        index.push(json!({"id": id, "title": title, "version": version_of(&page), "file": file, "attachments": saved}));
    }
    Ok(index)
}

/// Import a tree into `target`. An unsuccessful refresh cannot replace a previously complete import.
pub async fn import(item: &Value, target: &Path) -> Result<(), String> {
    let parent = target.parent().ok_or("A Confluence target needs a parent folder")?;
    std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    let stage_root = parent.join(format!(".confluence-stage-{}", crate::util::random_hex(6)));
    let stage = stage_root.join("export");
    std::fs::create_dir_all(&stage).map_err(|e| e.to_string())?;
    let outcome = async {
        let client = Client::new(item)?;
        let index = tokio::time::timeout(TIMEOUT, export(&client, item, &stage)).await.map_err(|_| "The Confluence export exceeded its 10-minute timeout".to_string())??;
        atomic_json(&stage.join("index.json"), &json!(index)).map_err(|e| e.to_string())?;
        atomic_json(&stage.join(MANIFEST), &json!({"source": identity(item), "pages": index, "files": file_hashes(&stage)?})).map_err(|e| e.to_string())?;
        local_manifest(item, &stage)?;
        let backup = stage_root.join("previous");
        if target.exists() {
            std::fs::rename(target, &backup).map_err(|e| e.to_string())?;
        }
        if let Err(problem) = std::fs::rename(&stage, target) {
            if backup.exists() {
                let _ = std::fs::rename(&backup, target);
            }
            return Err(problem.to_string());
        }
        Ok(())
    }
    .await;
    let _ = std::fs::remove_dir_all(&stage_root);
    outcome
}

/// Is the import still complete, and does it still match Confluence: tree, page versions and attachment versions?
pub async fn validate(item: &Value, target: &Path) -> Result<(), String> {
    let data = local_manifest(item, target)?;
    let mode = item["attachments"].as_str().unwrap_or("referenced");
    let saved: BTreeMap<String, (i64, Value)> = data["pages"].as_array().into_iter().flatten()
        .map(|p| (p["id"].as_str().unwrap_or("").to_string(), (p["version"].as_i64().unwrap_or(0), p["attachments"].clone()))).collect();
    let client = Client::new(item)?;
    let tree = client.tree(item).await?;
    if tree.len() != saved.len() {
        return Err("Confluence page tree, page versions or attachments changed; re-import the source".into());
    }
    let changed = || "Confluence page tree, page versions or attachments changed; re-import the source".to_string();
    for page in &tree {
        let id = page["id"].as_str().unwrap_or("");
        let (version, attachments) = saved.get(id).ok_or_else(changed)?;
        if *version != version_of(page) {
            return Err(changed());
        }
        if mode != "disabled" {
            let remote = client.attachments(id).await?;
            for (attachment, was) in attachments.as_object().into_iter().flatten() {
                let now = remote.iter().find(|r| r["id"].as_str() == Some(attachment.as_str())).ok_or_else(changed)?;
                if json!(version_of(now)) != *was {
                    return Err(changed());
                }
            }
        }
    }
    Ok(())
}

// ------------------------------------------------------------------------------------------- HTML to Markdown
enum Node {
    Text(String),
    Element { tag: String, attrs: Vec<(String, String)>, children: Vec<Node> },
}

const VOID: [&str; 13] = ["area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "source", "track", "wbr"];

fn decode_entities(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(at) = rest.find('&') {
        out.push_str(&rest[..at]);
        rest = &rest[at..];
        let end = rest.char_indices().take(12).find(|(_, c)| *c == ';').map(|(i, _)| i);
        let decoded = end.and_then(|end| {
            let name = &rest[1..end];
            let value = match name {
                "amp" => Some('&'), "lt" => Some('<'), "gt" => Some('>'), "quot" => Some('"'), "apos" => Some('\''), "nbsp" => Some(' '),
                "mdash" => Some('—'), "ndash" => Some('–'), "hellip" => Some('…'), "rsquo" => Some('’'), "lsquo" => Some('‘'), "rdquo" => Some('”'), "ldquo" => Some('“'),
                _ => name.strip_prefix('#').and_then(|n| match n.strip_prefix(['x', 'X']) {
                    Some(hex) => u32::from_str_radix(hex, 16).ok(),
                    None => n.parse().ok(),
                }).and_then(char::from_u32),
            };
            value.map(|c| (c, end))
        });
        match decoded {
            Some((c, end)) => {
                out.push(c);
                rest = &rest[end + 1..];
            }
            None => {
                out.push('&');
                rest = &rest[1..];
            }
        }
    }
    out.push_str(rest);
    out
}

fn parse_attrs(text: &str) -> Vec<(String, String)> {
    let (mut attrs, chars) = (vec![], text.chars().collect::<Vec<_>>());
    let mut at = 0;
    while at < chars.len() {
        while at < chars.len() && (chars[at].is_whitespace() || chars[at] == '/') {
            at += 1;
        }
        let start = at;
        while at < chars.len() && !chars[at].is_whitespace() && chars[at] != '=' && chars[at] != '/' {
            at += 1;
        }
        if at == start {
            break;
        }
        let name: String = chars[start..at].iter().collect::<String>().to_lowercase();
        while at < chars.len() && chars[at].is_whitespace() {
            at += 1;
        }
        let mut value = String::new();
        if at < chars.len() && chars[at] == '=' {
            at += 1;
            while at < chars.len() && chars[at].is_whitespace() {
                at += 1;
            }
            if at < chars.len() && (chars[at] == '"' || chars[at] == '\'') {
                let quote = chars[at];
                at += 1;
                let begin = at;
                while at < chars.len() && chars[at] != quote {
                    at += 1;
                }
                value = chars[begin..at].iter().collect();
                at += 1;
            } else {
                let begin = at;
                while at < chars.len() && !chars[at].is_whitespace() {
                    at += 1;
                }
                value = chars[begin..at].iter().collect();
            }
        }
        attrs.push((name, decode_entities(&value)));
    }
    attrs
}

fn parse_html(html: &str) -> Vec<Node> {
    // Each open element is (tag, attributes, children); the bottom entry is the document.
    let mut stack: Vec<(String, Vec<(String, String)>, Vec<Node>)> = vec![(String::new(), vec![], vec![])];
    let mut rest = html;
    while !rest.is_empty() {
        let Some(open) = rest.find('<') else {
            stack.last_mut().unwrap().2.push(Node::Text(decode_entities(rest)));
            break;
        };
        if open > 0 {
            stack.last_mut().unwrap().2.push(Node::Text(decode_entities(&rest[..open])));
        }
        rest = &rest[open..];
        if let Some(after) = rest.strip_prefix("<!--") {
            rest = after.find("-->").map(|end| &after[end + 3..]).unwrap_or("");
            continue;
        }
        let Some(close) = rest.find('>') else {
            stack.last_mut().unwrap().2.push(Node::Text(decode_entities(rest)));
            break;
        };
        let inner = &rest[1..close];
        rest = &rest[close + 1..];
        if inner.starts_with('!') || inner.starts_with('?') {
            continue;
        }
        if let Some(name) = inner.strip_prefix('/') {
            let name = name.trim().to_lowercase();
            if let Some(at) = stack.iter().rposition(|(tag, _, _)| *tag == name) {
                if at > 0 {
                    while stack.len() > at {
                        let (tag, attrs, children) = stack.pop().unwrap();
                        stack.last_mut().unwrap().2.push(Node::Element { tag, attrs, children });
                    }
                }
            }
            continue;
        }
        let name_end = inner.find(|c: char| c.is_whitespace() || c == '/').unwrap_or(inner.len());
        let tag = inner[..name_end].to_lowercase();
        if tag.is_empty() {
            continue;
        }
        let attrs = parse_attrs(&inner[name_end..]);
        if tag == "script" || tag == "style" {
            let closing = format!("</{tag}");
            let lower = rest.to_ascii_lowercase();
            rest = lower.find(&closing).and_then(|at| rest[at..].find('>').map(|end| &rest[at + end + 1..])).unwrap_or("");
            continue;
        }
        if VOID.contains(&tag.as_str()) || inner.ends_with('/') {
            stack.last_mut().unwrap().2.push(Node::Element { tag, attrs, children: vec![] });
        } else {
            stack.push((tag, attrs, vec![]));
        }
    }
    while stack.len() > 1 {
        let (tag, attrs, children) = stack.pop().unwrap();
        stack.last_mut().unwrap().2.push(Node::Element { tag, attrs, children });
    }
    stack.pop().unwrap().2
}

struct Links<'a> {
    base: String,
    ids: &'a HashSet<String>,
    attachments: BTreeMap<String, String>,
}

impl Links<'_> {
    /// Exported pages and attachments become local paths; other links to this site become absolute.
    fn resolve(&self, url: &str) -> String {
        let local = url.strip_prefix(&self.base).filter(|r| r.starts_with('/')).unwrap_or(url);
        if !local.starts_with('/') || local.starts_with("//") {
            return url.to_string();
        }
        let (path, fragment) = local.split_once('#').map(|(p, f)| (p, Some(f))).unwrap_or((local, None));
        let (path_only, query) = path.split_once('?').unwrap_or((path, ""));
        if path_only.contains("/download/") {
            if let Some(name) = path_only.rsplit('/').next() {
                if let Some(relative) = self.attachments.get(&percent_decode(name)) {
                    return relative.clone();
                }
            }
        }
        let id_in_path = path_only.split_once("/pages/").and_then(|(_, rest)| rest.split('/').next()).filter(|p| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()));
        let id_in_query = query.split('&').find_map(|pair| pair.strip_prefix("pageId="));
        if let Some(id) = id_in_path.or(id_in_query).filter(|id| self.ids.contains(*id)) {
            return match fragment {
                Some(fragment) => format!("{id}.md#{fragment}"),
                None => format!("{id}.md"),
            };
        }
        // Root-relative: joined to the site, as a browser would.
        let site_end = self.base.find("://").map(|at| self.base[at + 3..].find('/').map(|p| at + 3 + p).unwrap_or(self.base.len())).unwrap_or(self.base.len());
        format!("{}{local}", &self.base[..site_end])
    }
}

fn attr<'a>(attrs: &'a [(String, String)], name: &str) -> Option<&'a str> {
    attrs.iter().find(|(key, _)| key == name).map(|(_, value)| value.as_str())
}

fn collapse(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut space = false;
    for c in text.chars() {
        if c.is_whitespace() {
            space = true;
        } else {
            if space {
                out.push(' ');
                space = false;
            }
            out.push(c);
        }
    }
    if space {
        out.push(' ');
    }
    out
}

fn plain_text(nodes: &[Node]) -> String {
    let mut out = String::new();
    for node in nodes {
        match node {
            Node::Text(text) => out.push_str(text),
            Node::Element { tag, children, .. } => {
                if tag == "br" {
                    out.push('\n');
                }
                out.push_str(&plain_text(children));
            }
        }
    }
    out
}

fn inline(nodes: &[Node], links: &Links) -> String {
    let mut out = String::new();
    for node in nodes {
        match node {
            Node::Text(text) => out.push_str(&collapse(text)),
            Node::Element { tag, attrs, children } => match tag.as_str() {
                "br" => out.push_str("  \n"),
                "strong" | "b" | "em" | "i" | "s" | "del" | "code" | "tt" => {
                    let mark = match tag.as_str() { "strong" | "b" => "**", "em" | "i" => "*", "s" | "del" => "~~", _ => "`" };
                    let text = inline(children, links);
                    let core = text.trim();
                    if !core.is_empty() {
                        let lead = if text.starts_with(char::is_whitespace) { " " } else { "" };
                        let trail = if text.ends_with(char::is_whitespace) { " " } else { "" };
                        out.push_str(&format!("{lead}{mark}{core}{mark}{trail}"));
                    }
                }
                "a" => {
                    let text = inline(children, links);
                    match attr(attrs, "href").filter(|h| !h.is_empty()) {
                        Some(href) => out.push_str(&format!("[{}]({})", text.trim(), links.resolve(href))),
                        None => out.push_str(&text),
                    }
                }
                "img" => {
                    if let Some(source) = attr(attrs, "src").filter(|s| !s.is_empty()) {
                        out.push_str(&format!("![{}]({})", attr(attrs, "alt").unwrap_or(""), links.resolve(source)));
                    }
                }
                _ => out.push_str(&inline(children, links)),
            },
        }
    }
    out
}

fn is_block(tag: &str) -> bool {
    ["p", "div", "section", "article", "header", "footer", "aside", "main", "h1", "h2", "h3", "h4", "h5", "h6", "ul", "ol", "pre", "blockquote", "table", "hr", "dl", "body", "html", "details"].contains(&tag)
}

fn table(nodes: &[Node], links: &Links) -> String {
    fn rows<'a>(nodes: &'a [Node], found: &mut Vec<&'a [Node]>) {
        for node in nodes {
            if let Node::Element { tag, children, .. } = node {
                if tag == "tr" {
                    found.push(children);
                } else if tag != "table" {
                    rows(children, found);
                }
            }
        }
    }
    let mut found = vec![];
    rows(nodes, &mut found);
    let cells: Vec<Vec<String>> = found.iter().map(|row| row.iter().filter_map(|cell| match cell {
        Node::Element { tag, children, .. } if tag == "td" || tag == "th" => Some(blocks(children, links).trim().replace('\n', " ").replace('|', "\\|")),
        _ => None,
    }).collect()).collect();
    let width = cells.iter().map(Vec::len).max().unwrap_or(0);
    if width == 0 {
        return String::new();
    }
    let line = |row: &Vec<String>| {
        let padded: Vec<String> = (0..width).map(|i| row.get(i).cloned().unwrap_or_default()).collect();
        format!("| {} |", padded.join(" | "))
    };
    let mut out = vec![line(&cells[0]), format!("|{}", " --- |".repeat(width))];
    out.extend(cells[1..].iter().map(line));
    out.join("\n")
}

fn list(tag: &str, children: &[Node], links: &Links) -> String {
    let mut out = vec![];
    let mut number = 0;
    for child in children {
        if let Node::Element { tag: item, children: inner, .. } = child {
            if item == "li" {
                number += 1;
                let mark = if tag == "ol" { format!("{number}. ") } else { "- ".to_string() };
                let body = blocks(inner, links);
                let indent = " ".repeat(mark.len());
                let lines: Vec<String> = body.trim().lines().enumerate().map(|(i, line)| {
                    if i == 0 { format!("{mark}{line}") } else if line.is_empty() { String::new() } else { format!("{indent}{line}") }
                }).collect();
                out.push(if lines.is_empty() { mark.trim_end().to_string() } else { lines.join("\n") });
            }
        }
    }
    out.join("\n")
}

/// Block-level Markdown: runs of inline content become paragraphs.
fn blocks(nodes: &[Node], links: &Links) -> String {
    let mut parts: Vec<String> = vec![];
    let mut run: Vec<&Node> = vec![];
    fn flush(run: &mut Vec<&Node>, parts: &mut Vec<String>, links: &Links) {
        if run.is_empty() {
            return;
        }
        let mut text = String::new();
        for node in run.drain(..) {
            text.push_str(&inline(std::slice::from_ref(node), links));
        }
        let text = text.trim().to_string();
        if !text.is_empty() {
            parts.push(text);
        }
    }
    for node in nodes {
        let block = matches!(node, Node::Element { tag, .. } if is_block(tag));
        if !block {
            run.push(node);
            continue;
        }
        flush(&mut run, &mut parts, links);
        let Node::Element { tag, children, attrs } = node else { continue };
        let text = match tag.as_str() {
            "h1" | "h2" | "h3" | "h4" | "h5" | "h6" => {
                let level = tag[1..].parse::<usize>().unwrap_or(2);
                format!("{} {}", "#".repeat(level), inline(children, links).trim())
            }
            "ul" | "ol" => list(tag, children, links),
            "table" => table(children, links),
            "hr" => "---".to_string(),
            "pre" => {
                let language = attr(attrs, "data-language").unwrap_or("");
                format!("```{language}\n{}\n```", plain_text(children).trim_matches('\n'))
            }
            "blockquote" => blocks(children, links).lines().map(|line| if line.is_empty() { ">".to_string() } else { format!("> {line}") }).collect::<Vec<_>>().join("\n"),
            _ => blocks(children, links),
        };
        if !text.trim().is_empty() {
            parts.push(text);
        }
    }
    flush(&mut run, &mut parts, links);
    parts.join("\n\n")
}

fn to_markdown(html: &str, links: &Links) -> String {
    blocks(&parse_html(html), links)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn convert(html: &str) -> String {
        let ids: HashSet<String> = ["2".to_string()].into_iter().collect();
        let links = Links { base: "https://acme.example/wiki".into(), ids: &ids, attachments: [("a b.png".to_string(), "attachments/1/att.png".to_string())].into_iter().collect() };
        to_markdown(html, &links)
    }

    #[test]
    fn common_markup_becomes_markdown() {
        assert_eq!(convert("<h2>Title</h2><p>Some <strong>bold</strong> and <em>soft</em> &amp; <code>x</code></p>"), "## Title\n\nSome **bold** and *soft* & `x`");
        assert_eq!(convert("<ul><li>one</li><li>two<ul><li>deep</li></ul></li></ul>"), "- one\n- two\n\n  - deep");
        assert_eq!(convert("<ol><li>a</li><li>b</li></ol>"), "1. a\n2. b");
        assert_eq!(convert("<pre>print(1)\nprint(2)</pre>"), "```\nprint(1)\nprint(2)\n```");
        assert_eq!(convert("<table><tr><th>A</th><th>B</th></tr><tr><td>1</td><td>x|y</td></tr></table>"), "| A | B |\n| --- | --- |\n| 1 | x\\|y |");
        assert_eq!(convert("<blockquote><p>quoted</p></blockquote><hr/><script>bad()</script>"), "> quoted\n\n---");
    }

    #[test]
    fn links_to_exported_pages_and_attachments_are_local() {
        assert_eq!(convert(r#"<a href="/wiki/spaces/T/pages/2/Child#part">c</a>"#), "[c](2.md#part)");
        assert_eq!(convert(r#"<a href="/pages/viewpage.action?pageId=2">c</a>"#), "[c](2.md)");
        assert_eq!(convert(r#"<a href="https://acme.example/wiki/spaces/T/pages/2/C">c</a>"#), "[c](2.md)");
        assert_eq!(convert(r#"<a href="/wiki/spaces/T/pages/9/Other">c</a>"#), "[c](https://acme.example/wiki/spaces/T/pages/9/Other)");
        assert_eq!(convert(r#"<a href="https://example.org/x">c</a>"#), "[c](https://example.org/x)");
        assert_eq!(convert(r#"<img src="/download/attachments/1/a%20b.png?version=1" alt="Pic">"#), "![Pic](attachments/1/att.png)");
    }

    #[test]
    fn broken_markup_never_panics() {
        for html in ["<", "<p", "</p>", "<a href=", "&", "&#xZZ;", "<p><b>open", "<!-- never closed", "<script>x", "é<é>é"] {
            let _ = convert(html);
        }
        assert_eq!(percent_decode("a%20b%zz%"), "a b%zz%");
    }
}
