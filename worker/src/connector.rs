//! The PC side of the relay: carry the coordinator's calls to this PC's own service. Everything is outbound.
//! A long poll to the server collects queued calls; each is made against the service on 127.0.0.1 and its answer sent
//! back, with bodies streamed both ways. The PC's /health answer is pushed every few seconds.
use crate::config::{self, Home};
use crate::util::{atomic_json, now_unix, read_json};
use futures_util::StreamExt;
use reqwest::{Body, Client, Method};
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::watch;

const POLL_WAIT: u64 = 25;
const STATUS_EVERY: Duration = Duration::from_secs(5);
const BACKOFF: [u64; 5] = [1, 2, 5, 10, 30];

const REVOCATION_MESSAGE: &str = "This worker has been unregistered from the fleet. Run agent-worker uninstall, then install again with a new Fleet install command.";

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Revocation {
    pub(crate) reason: String,
    pub(crate) message: String,
    pub(crate) at: f64,
}

impl Revocation {
    fn from_response(status: u16, body: &Value) -> Option<Self> {
        if status != 410 || body["error"] != "worker_revoked" {
            return None;
        }
        let message = body["message"]
            .as_str()
            .filter(|message| !message.trim().is_empty())
            .unwrap_or(REVOCATION_MESSAGE);
        Some(Revocation {
            reason: "worker_revoked".into(),
            message: message.to_string(),
            at: now_unix(),
        })
    }

    pub(crate) fn from_persisted(value: &Value) -> Option<Self> {
        Some(Revocation {
            reason: value["reason"].as_str()?.to_string(),
            message: value["message"].as_str()?.to_string(),
            at: value["at"].as_f64()?,
        })
        .filter(|revocation| revocation.reason == "worker_revoked")
    }

    pub(crate) fn to_json(&self) -> Value {
        json!({"reason": self.reason, "message": self.message, "at": self.at})
    }
}

struct ConnectorState {
    connected: bool,
    revocation: Option<Revocation>,
}

enum PollError {
    Transient(String),
    Revoked(Revocation),
}

pub struct Connector {
    base: String,
    token: String,
    local: String,
    home: Home,
    client: Client,
    state: Mutex<ConnectorState>,
    cutoff: watch::Sender<bool>,
}

impl Connector {
    pub fn new(server: &str, id: &str, token: &str, local_port: u16, home: Home) -> Arc<Connector> {
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .pool_idle_timeout(Duration::from_secs(30))
            .build()
            .expect("http client");
        let saved = read_json(&home.file("runtime").join("connector.json")).unwrap_or(json!({}));
        let revocation = Revocation::from_persisted(&saved["revocation"]);
        let (cutoff, _) = watch::channel(revocation.is_some());
        Arc::new(Connector {
            base: format!("{}/relay/{}", server.trim_end_matches('/'), id),
            token: token.to_string(),
            local: format!("http://127.0.0.1:{local_port}"),
            home,
            client,
            state: Mutex::new(ConnectorState {
                connected: false,
                revocation,
            }),
            cutoff,
        })
    }

    fn authed(&self, builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        builder.bearer_auth(&self.token)
    }

    fn is_revoked(&self) -> bool {
        self.state.lock().unwrap().revocation.is_some()
    }

    fn persist_state(&self, state: &ConnectorState) {
        let revocation = state.revocation.as_ref().map(Revocation::to_json);
        if let Err(error) = atomic_json(
            &self.home.file("runtime").join("connector.json"),
            &json!({"connected": state.connected, "at": now_unix(), "revoked": state.revocation.is_some(), "revocation": revocation}),
        ) {
            crate::log!("connector", "could not persist connector state: {error}");
        }
    }

    fn write_state(&self) {
        let state = self.state.lock().unwrap();
        self.persist_state(&state);
    }

    fn set_connected(&self, connected: bool) -> bool {
        let mut state = self.state.lock().unwrap();
        if state.revocation.is_some() {
            return false;
        }
        let changed = state.connected != connected;
        if changed {
            state.connected = connected;
            self.persist_state(&state);
        }
        changed
    }

    fn set_revocation(&self, revocation: Revocation) {
        {
            let mut state = self.state.lock().unwrap();
            if state.revocation.is_none() {
                state.connected = false;
                state.revocation = Some(revocation);
                self.persist_state(&state);
            }
        }
        self.cutoff.send_replace(true);
    }

    async fn inspect_gone(&self, response: reqwest::Response) {
        if let Ok(body) = response.json::<Value>().await {
            if let Some(revocation) = Revocation::from_response(410, &body) {
                self.set_revocation(revocation);
            }
        }
    }

    pub async fn run(self: Arc<Self>) {
        if self.is_revoked() {
            return;
        }
        let mut cutoff = self.cutoff.subscribe();
        let status = self.clone();
        let status_cutoff = self.cutoff.subscribe();
        tokio::spawn(async move { status.push_status(status_cutoff).await });
        let mut failures = 0usize;
        loop {
            if self.is_revoked() || *cutoff.borrow() {
                return;
            }
            let result = tokio::select! {
                _ = cutoff.changed() => return,
                result = self.poll() => result,
            };
            match result {
                Ok(calls) => {
                    if self.is_revoked() {
                        return;
                    }
                    if self.set_connected(true) {
                        crate::log!("connector", "connected to the server");
                    }
                    failures = 0;
                    for call in calls {
                        if self.is_revoked() {
                            return;
                        }
                        let me = self.clone();
                        tokio::spawn(async move { me.handle(call).await });
                    }
                }
                Err(PollError::Revoked(revocation)) => {
                    crate::log!("connector", "worker unregistered from the fleet");
                    self.set_revocation(revocation);
                    return;
                }
                Err(PollError::Transient(error)) => {
                    let changed = self.set_connected(false);
                    if changed || failures == 0 {
                        crate::log!("connector", "relay poll failed: {error}");
                    }
                    if self.is_revoked() {
                        return;
                    }
                    let delay = Duration::from_secs(BACKOFF[failures.min(BACKOFF.len() - 1)]);
                    tokio::select! {
                        _ = cutoff.changed() => return,
                        _ = tokio::time::sleep(delay) => {}
                    }
                    failures += 1;
                }
            }
        }
    }

    async fn poll(&self) -> Result<Vec<Value>, PollError> {
        if self.is_revoked() {
            return Err(PollError::Transient(
                "worker unregistered from the fleet".into(),
            ));
        }
        let response = self
            .authed(
                self.client
                    .get(format!("{}/next", self.base))
                    .query(&[("wait", POLL_WAIT)])
                    .timeout(Duration::from_secs(POLL_WAIT + 15)),
            )
            .send()
            .await
            .map_err(|e| PollError::Transient(e.to_string()))?;
        let status = response.status().as_u16();
        match status {
            204 => Ok(vec![]),
            410 => {
                let body = response.json::<Value>().await.unwrap_or(Value::Null);
                match Revocation::from_response(status, &body) {
                    Some(revocation) => Err(PollError::Revoked(revocation)),
                    None => Err(PollError::Transient("server answered 410".into())),
                }
            }
            401 => Err(PollError::Transient(
                "the server refused this PC's token".into(),
            )),
            s if s >= 400 => Err(PollError::Transient(format!("server answered {s}"))),
            _ => response
                .json::<Vec<Value>>()
                .await
                .map_err(|e| PollError::Transient(e.to_string())),
        }
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

    async fn push_status(&self, mut cutoff: watch::Receiver<bool>) {
        loop {
            if self.is_revoked() || *cutoff.borrow() {
                return;
            }
            self.write_state();
            let health = tokio::select! {
                _ = cutoff.changed() => return,
                result = self.authed(self.client.get(format!("{}/health", self.local)).timeout(Duration::from_secs(10))).send() => result,
            };
            if let Ok(response) = health {
                if response.status().is_success() {
                    let parsed = tokio::select! {
                        _ = cutoff.changed() => return,
                        result = response.json::<Value>() => result,
                    };
                    if let Ok(mut health) = parsed {
                        health["worker_os"] = json!(config::os_name());
                        if let (Some(map), Some(extra)) =
                            (health.as_object_mut(), self.update_state().as_object())
                        {
                            map.extend(extra.clone());
                        }
                        if self.is_revoked() {
                            return;
                        }
                        // The first poll is held for up to 25 seconds, so an accepted status push proves reachability.
                        let pushed = tokio::select! {
                            _ = cutoff.changed() => return,
                            result = self.authed(self.client.post(format!("{}/status", self.base)).json(&health).timeout(Duration::from_secs(10))).send() => result,
                        };
                        if let Ok(pushed) = pushed {
                            if pushed.status().as_u16() == 410 {
                                self.inspect_gone(pushed).await;
                                if self.is_revoked() {
                                    return;
                                }
                            } else if pushed.status().is_success() && self.set_connected(true) {
                                crate::log!("connector", "connected to the server");
                            }
                        }
                    }
                }
            }
            if self.is_revoked() {
                return;
            }
            tokio::select! {
                _ = cutoff.changed() => return,
                _ = tokio::time::sleep(STATUS_EVERY) => {}
            }
        }
    }

    async fn respond(&self, id: &str, headers: Vec<(&str, String)>, body: Option<Body>) {
        if self.is_revoked() {
            return;
        }
        let mut request = self.authed(
            self.client
                .put(format!("{}/calls/{id}/response", self.base)),
        );
        for (k, v) in headers {
            request = request.header(k, v);
        }
        if let Some(body) = body {
            request = request.body(body);
        }
        if let Ok(response) = request.send().await {
            if response.status().as_u16() == 410 {
                self.inspect_gone(response).await;
            } else {
                let _ = response.bytes().await;
            }
        }
    }

    /// A request from the server about this PC itself, never forwarded to the execution service.
    async fn control(&self, call: &Value) {
        let id = call["id"].as_str().unwrap_or("");
        let action = call["path"]
            .as_str()
            .unwrap_or("")
            .rsplit('/')
            .next()
            .unwrap_or("");
        let (status, body) = match action {
            "update" => {
                // an updater that died (power loss, killed) leaves "running" behind: after ten minutes it no longer counts
                let stale = read_json(&self.home.file("update-status.json"))
                    .and_then(|d| d["at"].as_f64())
                    .map(|at| now_unix() - at > 600.0)
                    .unwrap_or(false);
                if self.update_state()["update_state"] == "running" && !stale {
                    (
                        202,
                        json!({"started": false, "reason": "an update is already running"}),
                    )
                } else {
                    match std::env::current_exe()
                        .map_err(|e| e.to_string())
                        .and_then(|exe| {
                            crate::procutil::spawn_outside_service(
                                &exe,
                                &[
                                    "--home".into(),
                                    self.home.0.to_string_lossy().to_string(),
                                    "update".into(),
                                    "--background".into(),
                                ],
                                Some(&self.home.0),
                                Some(&self.home.file("update.log")),
                            )
                            .map_err(|e| e.to_string())
                        }) {
                        Ok(_) => (202, json!({"started": true})),
                        Err(e) => (500, json!({"error": e})),
                    }
                }
            }
            other => (
                404,
                json!({"error": format!("Unknown control action {other}")}),
            ),
        };
        self.respond(
            id,
            vec![
                ("X-Relay-Status", status.to_string()),
                ("Content-Type", "application/json".into()),
            ],
            Some(Body::from(body.to_string())),
        )
        .await;
    }

    async fn handle(&self, call: Value) {
        if self.is_revoked() {
            return;
        }
        let id = call["id"].as_str().unwrap_or("").to_string();
        let path = call["path"].as_str().unwrap_or("/").to_string();
        if path.starts_with("/_control/") {
            self.control(&call).await;
            return;
        }
        let method = Method::from_bytes(call["method"].as_str().unwrap_or("GET").as_bytes())
            .unwrap_or(Method::GET);
        let query = call["query"].as_str().unwrap_or("");
        let url = format!(
            "{}{}{}",
            self.local,
            path,
            if query.is_empty() {
                String::new()
            } else {
                format!("?{query}")
            }
        );
        let mut request = self.client.request(method, url);
        if let Some(headers) = call["headers"].as_object() {
            for (k, v) in headers {
                if let Some(v) = v.as_str() {
                    request = request.header(k.as_str(), v);
                }
            }
        }
        if call["has_body"].as_bool().unwrap_or(false) {
            match self
                .authed(self.client.get(format!("{}/calls/{id}/body", self.base)))
                .send()
                .await
            {
                Ok(source) if source.status().is_success() => {
                    request = request.body(Body::wrap_stream(source.bytes_stream()))
                }
                Ok(source) if source.status().as_u16() == 410 => {
                    self.inspect_gone(source).await;
                    return;
                }
                _ => return,
            }
        }
        if self.is_revoked() {
            return;
        }
        match request.send().await {
            Ok(local) => {
                let mut headers = vec![("X-Relay-Status", local.status().as_u16().to_string())];
                if let Some(ct) = local
                    .headers()
                    .get("content-type")
                    .and_then(|v| v.to_str().ok())
                {
                    headers.push(("Content-Type", ct.to_string()));
                }
                let stream = local.bytes_stream().map(|chunk| {
                    chunk.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
                });
                self.respond(&id, headers, Some(Body::wrap_stream(stream)))
                    .await;
            }
            Err(error) => {
                crate::log!(
                    "connector",
                    "local service unreachable for {}: {error}",
                    path
                );
                self.respond(&id, vec![("X-Relay-Drop", "1".into())], None)
                    .await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;
    use axum::routing::get;
    use axum::{Json, Router};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::time::timeout;

    #[test]
    fn only_a_protocol_410_is_a_revocation() {
        let response = json!({"error": "worker_revoked", "message": "removed"});
        assert_eq!(
            Revocation::from_response(StatusCode::GONE.as_u16(), &response)
                .map(|r| (r.reason, r.message)),
            Some(("worker_revoked".into(), "removed".into()))
        );
        assert!(Revocation::from_response(StatusCode::UNAUTHORIZED.as_u16(), &response).is_none());
        assert!(
            Revocation::from_response(StatusCode::TOO_MANY_REQUESTS.as_u16(), &response).is_none()
        );
        assert!(
            Revocation::from_response(StatusCode::INTERNAL_SERVER_ERROR.as_u16(), &response)
                .is_none()
        );
        assert!(
            Revocation::from_response(StatusCode::GONE.as_u16(), &json!({"error": "other"}))
                .is_none()
        );
    }

    #[tokio::test]
    async fn a_revocation_is_persisted_and_prevents_polling_after_restart() {
        let polls = Arc::new(AtomicUsize::new(0));
        let app = Router::new().route(
            "/relay/worker/next",
            get({
                let polls = polls.clone();
                move || {
                    let polls = polls.clone();
                    async move {
                        polls.fetch_add(1, Ordering::SeqCst);
                        (
                            StatusCode::GONE,
                            Json(json!({
                                "error": "worker_revoked",
                                "message": "This worker was unregistered from the fleet."
                            })),
                        )
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let dir = tempfile::tempdir().unwrap();
        let home = Home(dir.path().to_path_buf());
        let server_url = format!("http://{address}");
        let connector = Connector::new(&server_url, "worker", "token", 0, Home(home.0.clone()));

        timeout(Duration::from_secs(2), connector.clone().run())
            .await
            .unwrap();
        connector.write_state(); // later status-state writes must retain the permanent cutoff
        let state = read_json(&home.file("runtime").join("connector.json")).unwrap();
        assert_eq!(state["revoked"], true);
        assert_eq!(state["revocation"]["reason"], "worker_revoked");
        assert_eq!(
            state["revocation"]["message"],
            "This worker was unregistered from the fleet."
        );
        assert!(state["revocation"]["at"].as_f64().is_some());
        assert_eq!(polls.load(Ordering::SeqCst), 1);

        let restarted = Connector::new(&server_url, "worker", "token", 0, Home(home.0.clone()));
        timeout(Duration::from_secs(2), restarted.run())
            .await
            .unwrap();
        assert_eq!(polls.load(Ordering::SeqCst), 1);
        server.abort();
    }
}
