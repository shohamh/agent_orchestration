//! The PC side of the relay: carry the coordinator's calls to this PC's own service. Everything is outbound.
//! A long poll to the server collects queued calls; each is made against the service on 127.0.0.1 and its answer sent
//! back, with bodies streamed both ways. The PC's /health answer is pushed every few seconds.
use crate::config::{self, Home};
use crate::util::{atomic_json, now_unix, read_json};
use futures_util::StreamExt;
use reqwest::{Body, Client, Method};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

const POLL_WAIT: u64 = 25;
const STATUS_EVERY: Duration = Duration::from_secs(5);
const BACKOFF: [u64; 5] = [1, 2, 5, 10, 30];

pub struct Connector {
    base: String,
    token: String,
    local: String,
    home: Home,
    client: Client,
    connected: AtomicBool,
}

impl Connector {
    pub fn new(server: &str, id: &str, token: &str, local_port: u16, home: Home) -> Arc<Connector> {
        let client = Client::builder().connect_timeout(Duration::from_secs(10)).pool_idle_timeout(Duration::from_secs(30)).build().expect("http client");
        Arc::new(Connector {
            base: format!("{}/relay/{}", server.trim_end_matches('/'), id),
            token: token.to_string(),
            local: format!("http://127.0.0.1:{local_port}"),
            home,
            client,
            connected: AtomicBool::new(false),
        })
    }

    fn authed(&self, builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        builder.bearer_auth(&self.token)
    }

    pub async fn run(self: Arc<Self>) {
        let status = self.clone();
        tokio::spawn(async move { status.push_status().await });
        let mut failures = 0usize;
        loop {
            match self.poll().await {
                Ok(calls) => {
                    if !self.connected.swap(true, Ordering::SeqCst) {
                        crate::log!("connector", "connected to the server");
                    }
                    failures = 0;
                    for call in calls {
                        let me = self.clone();
                        tokio::spawn(async move { me.handle(call).await });
                    }
                }
                Err(error) => {
                    if self.connected.swap(false, Ordering::SeqCst) || failures == 0 {
                        crate::log!("connector", "relay poll failed: {error}");
                    }
                    tokio::time::sleep(Duration::from_secs(BACKOFF[failures.min(BACKOFF.len() - 1)])).await;
                    failures += 1;
                }
            }
        }
    }

    async fn poll(&self) -> Result<Vec<Value>, String> {
        let response = self.authed(self.client.get(format!("{}/next", self.base)).query(&[("wait", POLL_WAIT)]).timeout(Duration::from_secs(POLL_WAIT + 15))).send().await.map_err(|e| e.to_string())?;
        match response.status().as_u16() {
            204 => Ok(vec![]),
            401 => Err("the server refused this PC's token".into()),
            s if s >= 400 => Err(format!("server answered {s}")),
            _ => response.json::<Vec<Value>>().await.map_err(|e| e.to_string()),
        }
    }

    fn write_state(&self) {
        let _ = atomic_json(&self.home.file("runtime").join("connector.json"), &json!({"connected": self.connected.load(Ordering::SeqCst), "at": now_unix()}));
    }

    fn update_state(&self) -> Value {
        match read_json(&self.home.file("update-status.json")) {
            Some(data) => {
                let mut out = json!({"update_state": data.get("state")});
                if let Some(error) = data.get("error").and_then(|e| e.as_str()) {
                    out["update_error"] = json!(error.chars().take(300).collect::<String>());
                }
                out
            }
            None => json!({}),
        }
    }

    async fn push_status(&self) {
        loop {
            self.write_state();
            let health = self.authed(self.client.get(format!("{}/health", self.local)).timeout(Duration::from_secs(10))).send().await;
            if let Ok(response) = health {
                if response.status().is_success() {
                    if let Ok(mut health) = response.json::<Value>().await {
                        health["worker_os"] = json!(config::os_name());
                        if let (Some(map), Some(extra)) = (health.as_object_mut(), self.update_state().as_object()) {
                            map.extend(extra.clone());
                        }
                        let _ = self.authed(self.client.post(format!("{}/status", self.base)).json(&health).timeout(Duration::from_secs(10))).send().await;
                    }
                }
            }
            tokio::time::sleep(STATUS_EVERY).await;
        }
    }

    async fn respond(&self, id: &str, headers: Vec<(&str, String)>, body: Option<Body>) {
        let mut request = self.authed(self.client.put(format!("{}/calls/{id}/response", self.base)));
        for (k, v) in headers {
            request = request.header(k, v);
        }
        if let Some(body) = body {
            request = request.body(body);
        }
        if let Ok(response) = request.send().await {
            let _ = response.bytes().await;
        }
    }

    /// A request from the server about this PC itself, never forwarded to the execution service.
    async fn control(&self, call: &Value) {
        let id = call["id"].as_str().unwrap_or("");
        let action = call["path"].as_str().unwrap_or("").rsplit('/').next().unwrap_or("");
        let (status, body) = match action {
            "update" => {
                if self.update_state()["update_state"] == "running" {
                    (202, json!({"started": false, "reason": "an update is already running"}))
                } else {
                    match std::env::current_exe().map_err(|e| e.to_string()).and_then(|exe| {
                        crate::procutil::spawn_detached(&exe, &["--home".into(), self.home.0.to_string_lossy().to_string(), "update".into(), "--background".into()], Some(&self.home.0), Some(&self.home.file("update.log"))).map_err(|e| e.to_string())
                    }) {
                        Ok(_) => (202, json!({"started": true})),
                        Err(e) => (500, json!({"error": e})),
                    }
                }
            }
            other => (404, json!({"error": format!("Unknown control action {other}")})),
        };
        self.respond(id, vec![("X-Relay-Status", status.to_string()), ("Content-Type", "application/json".into())], Some(Body::from(body.to_string()))).await;
    }

    async fn handle(&self, call: Value) {
        let id = call["id"].as_str().unwrap_or("").to_string();
        let path = call["path"].as_str().unwrap_or("/").to_string();
        if path.starts_with("/_control/") {
            self.control(&call).await;
            return;
        }
        let method = Method::from_bytes(call["method"].as_str().unwrap_or("GET").as_bytes()).unwrap_or(Method::GET);
        let query = call["query"].as_str().unwrap_or("");
        let url = format!("{}{}{}", self.local, path, if query.is_empty() { String::new() } else { format!("?{query}") });
        let mut request = self.client.request(method, url);
        if let Some(headers) = call["headers"].as_object() {
            for (k, v) in headers {
                if let Some(v) = v.as_str() {
                    request = request.header(k.as_str(), v);
                }
            }
        }
        if call["has_body"].as_bool().unwrap_or(false) {
            match self.authed(self.client.get(format!("{}/calls/{id}/body", self.base))).send().await {
                Ok(source) if source.status().is_success() => request = request.body(Body::wrap_stream(source.bytes_stream())),
                _ => return,
            }
        }
        match request.send().await {
            Ok(local) => {
                let mut headers = vec![("X-Relay-Status", local.status().as_u16().to_string())];
                if let Some(ct) = local.headers().get("content-type").and_then(|v| v.to_str().ok()) {
                    headers.push(("Content-Type", ct.to_string()));
                }
                let stream = local.bytes_stream().map(|chunk| chunk.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)));
                self.respond(&id, headers, Some(Body::wrap_stream(stream))).await;
            }
            Err(error) => {
                crate::log!("connector", "local service unreachable for {}: {error}", path);
                self.respond(&id, vec![("X-Relay-Drop", "1".into())], None).await;
            }
        }
    }
}
