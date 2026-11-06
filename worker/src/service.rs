//! The execution service: the HTTP protocol coordinators use to submit, follow, authorize and cancel step jobs.
//! It matches the Python service (flowkit/worker_service.py), so the coordinator code needs no change.
use crate::capabilities;
use crate::gate::{Acquire, Gate, Sampler, MAX_SLOTS};
use crate::job::{Job, JobState};
use crate::util::{atomic_json, eq_const, mono, now_unix, read_json};
use axum::body::Body;
use axum::extract::{DefaultBodyLimit, Path, Query, Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{json, Value};
use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub struct Config {
    pub id: String,
    pub uuid: String,
    pub slots: u64,
    pub token: String,
    pub root: PathBuf,
    pub version: String,
}

pub struct App {
    pub identity: Value,
    pub token: String,
    pub root: PathBuf,
    pub version: String,
    pub gate: Gate,
    pub gate_id: String,
    pub count: u64,
    pub jobs: Mutex<HashMap<String, Arc<Job>>>,
    pub draining: AtomicBool,
    pub active: AtomicU64,
    pub inventory: Mutex<Value>,
    pub inventory_lock: tokio::sync::Mutex<()>,
}

/// An error a client can read: `{"error": message}` with the status the Python service uses.
pub struct ApiError(pub StatusCode, pub String);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(json!({"error": self.1}))).into_response()
    }
}

fn bad(message: impl Into<String>) -> ApiError {
    ApiError(StatusCode::BAD_REQUEST, message.into())
}

type ApiResult = Result<Json<Value>, ApiError>;

impl App {
    pub fn new(config: Config) -> Arc<App> {
        let _ = std::fs::create_dir_all(&config.root);
        let gate = Gate::open(config.root.join("gate.json"), config.slots);
        let inventory = read_json(&config.root.join("capabilities.json")).filter(|v| v.is_object()).unwrap_or(json!({}));
        let hostname = sysinfo::System::host_name().unwrap_or_else(|| "unknown".into());
        Arc::new(App {
            identity: json!({"id": config.id, "uuid": config.uuid, "hostname": hostname, "pool": "default", "tags": []}),
            token: config.token,
            root: config.root,
            version: config.version,
            gate_id: gate.gate_id(),
            gate,
            count: config.slots,
            jobs: Mutex::new(HashMap::new()),
            draining: AtomicBool::new(false),
            active: AtomicU64::new(0),
            inventory: Mutex::new(inventory),
            inventory_lock: tokio::sync::Mutex::new(()),
        })
    }

    pub fn termination_path(&self, job_id: &str) -> PathBuf {
        self.root.join(".terminated").join(format!("{job_id}.json"))
    }

    /// Durable proof that a job's local cleanup finished and its id can never run again.
    pub fn record_termination(&self, job_id: &str) -> Result<(), String> {
        if !self.gate.revoke_absent(job_id, Some(&self.gate_id)) {
            return Err("The job still has an active capacity lease".into());
        }
        atomic_json(&self.termination_path(job_id), &json!({"terminated": true, "gate_id": self.gate_id})).map_err(|e| e.to_string())
    }

    pub async fn refresh_inventory(&self, extra: &[String]) -> Value {
        let _guard = self.inventory_lock.lock().await;
        let mut names: BTreeSet<String> = self.inventory.lock().unwrap().get("commands").and_then(|c| c.as_object()).map(|c| c.keys().filter(|k| !k.is_empty() && k.len() <= 260).cloned().collect()).unwrap_or_default();
        names.extend(extra.iter().cloned());
        let updated = tokio::task::spawn_blocking(move || capabilities::refresh(&names)).await.unwrap();
        let _ = atomic_json(&self.root.join("capabilities.json"), &updated);
        *self.inventory.lock().unwrap() = updated.clone();
        updated
    }
}

fn valid_uuid(text: &str) -> Option<String> {
    uuid::Uuid::parse_str(text).ok().map(|u| u.hyphenated().to_string())
}

// --------------------------------------------------------------------------- routes
pub fn router(app: Arc<App>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/drain", post(drain))
        .route("/capabilities", get(inventory))
        .route("/capabilities/refresh", post(refresh_capabilities))
        .route("/capacity/settings", post(configure))
        .route("/jobs", post(create))
        .route("/jobs/{id}", get(status).delete(cancel))
        .route("/jobs/{id}/input", post(receive))
        .route("/jobs/{id}/start", post(authorize))
        .layer(DefaultBodyLimit::max(190 * 1024 * 1024))
        .layer(middleware::from_fn_with_state(app.clone(), auth))
        .with_state(app)
}

async fn auth(State(app): State<Arc<App>>, request: Request, next: Next) -> Response {
    if !app.token.is_empty() {
        let given = request.headers().get("authorization").and_then(|v| v.to_str().ok()).unwrap_or("");
        if !eq_const(given, &format!("Bearer {}", app.token)) {
            return (StatusCode::UNAUTHORIZED, "401: Unauthorized").into_response();
        }
    }
    next.run(request).await
}

async fn health(State(app): State<Arc<App>>) -> ApiResult {
    let mut out = app.identity.clone();
    out["slots"] = json!(app.count);
    out["active"] = json!(app.active.load(Ordering::SeqCst));
    out["capacity"] = app.gate.metrics();
    out["draining"] = json!(app.draining.load(Ordering::SeqCst));
    out["worker_os"] = json!(crate::config::os_name());
    if !app.version.is_empty() {
        out["worker_version"] = json!(app.version);
    }
    Ok(Json(out))
}

/// An update asks the service to stop admitting work; running jobs finish. `{"drain": false}` lifts it.
async fn drain(State(app): State<Arc<App>>, Json(body): Json<Value>) -> ApiResult {
    let on = match (body.as_object(), body.get("drain")) {
        (Some(map), Some(Value::Bool(b))) if map.len() == 1 => *b,
        _ => return Err(bad("Body must be {\"drain\": true|false}")),
    };
    app.draining.store(on, Ordering::SeqCst);
    Ok(Json(json!({"draining": on, "active": app.active.load(Ordering::SeqCst)})))
}

async fn inventory(State(app): State<Arc<App>>) -> ApiResult {
    let overdue = capabilities::overdue(&app.inventory.lock().unwrap());
    if overdue {
        app.refresh_inventory(&[]).await;
    }
    Ok(Json(app.inventory.lock().unwrap().clone()))
}

async fn refresh_capabilities(State(app): State<Arc<App>>, body: Option<Json<Value>>) -> ApiResult {
    let data = body.map(|b| b.0).unwrap_or(json!({}));
    if !data.is_object() {
        return Err(bad("Body must be an object"));
    }
    let commands = data.get("commands").cloned().unwrap_or(json!([]));
    let list: Vec<String> = commands.as_array().ok_or_else(|| bad("commands must be a list of executable names"))?.iter().map(|c| c.as_str().map(String::from)).collect::<Option<_>>().ok_or_else(|| bad("commands must be a list of executable names"))?;
    if list.iter().any(|c| c.is_empty() || c.len() > 260 || c.contains(['\0', '\r', '\n'])) {
        return Err(bad("commands must be a list of executable names"));
    }
    Ok(Json(app.refresh_inventory(&list).await))
}

/// The coordinator's registry pushes this PC's admission limits (and nothing else).
async fn configure(State(app): State<Arc<App>>, headers: HeaderMap, body: bytes::Bytes) -> ApiResult {
    let data: Value = serde_json::from_slice(&body).map_err(|_| bad("Invalid JSON body"))?;
    let map = data.as_object().filter(|m| m.len() == 3 && ["slots", "cpu_limit", "ram_limit"].iter().all(|k| m.contains_key(*k))).ok_or_else(|| bad("Body must contain exactly slots, cpu_limit and ram_limit"))?;
    let slots = map["slots"].as_u64().filter(|s| (1..=MAX_SLOTS).contains(s) && map["slots"].is_u64()).ok_or_else(|| bad(format!("slots must be a whole number from 1 to {MAX_SLOTS}")))?;
    let percent = |key: &str| map[key].as_f64().filter(|v| *v > 0.0 && *v <= 100.0);
    let (cpu, ram) = (percent("cpu_limit"), percent("ram_limit"));
    let (Some(cpu), Some(ram)) = (cpu, ram) else { return Err(bad("cpu_limit and ram_limit must be percentages above 0 and at most 100")) };
    let expected = headers.get("x-flow-gate-id").and_then(|v| v.to_str().ok());
    if let Some(expected) = expected {
        if expected != app.gate_id {
            return Err(bad("Worker capacity gate identity changed"));
        }
    }
    let revision = match headers.get("x-flow-settings-revision").and_then(|v| v.to_str().ok()) {
        Some(raw) => {
            if !raw.is_ascii() || raw.is_empty() || !raw.bytes().all(|b| b.is_ascii_digit()) || raw.len() > 30 {
                return Err(bad("Invalid worker settings revision"));
            }
            Some(raw.parse::<i64>().map_err(|_| bad("Invalid worker settings revision"))?)
        }
        None => None,
    };
    app.gate.configure(slots, cpu, ram, expected, revision).map_err(bad)?;
    Ok(Json(json!({"settings": app.gate.metrics()["settings"]})))
}

fn find(app: &App, id: &str) -> Result<Arc<Job>, ApiError> {
    app.jobs.lock().unwrap().get(id).cloned().ok_or_else(|| ApiError(StatusCode::NOT_FOUND, "404: Not Found".into()))
}

async fn create(State(app): State<Arc<App>>, Json(mut config): Json<Value>) -> ApiResult {
    let raw_id = config.get("id").and_then(|i| i.as_str()).ok_or_else(|| bad("'id'"))?.to_string();
    let job_id = valid_uuid(&raw_id).ok_or_else(|| bad("badly formed hexadecimal UUID string"))?;
    for key in ["start", "files", "assets"] {
        if config.get(key).is_none() {
            return Err(bad(format!("'{key}'")));
        }
    }
    if let Some(expected) = config.get("gate_id").and_then(|g| g.as_str()) {
        if expected != app.gate_id || expected != app.gate.gate_id() {
            return Err(bad("Worker capacity gate identity changed"));
        }
    }
    if !app.jobs.lock().unwrap().contains_key(&job_id) {
        if app.termination_path(&job_id).exists() {
            return Err(bad("A terminated job ID cannot be reused"));
        }
        if app.jobs.lock().unwrap().len() >= 100 {
            return Err(bad("Execution worker has too many retained jobs; retry later"));
        }
        let start = config["start"].clone();
        let run_id = start.get("runId").and_then(|r| r.as_str()).ok_or_else(|| bad("'runId'"))?.to_string();
        if run_id.is_empty() || run_id.len() > 150 || !run_id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-') {
            return Err(bad("Invalid run id"));
        }
        if start.get("session").is_some() {
            return Err(bad("This worker does not run agent sessions yet"));
        }
        let flow = start.get("flow").ok_or_else(|| bad("'flow'"))?;
        let step_id = start.get("step").and_then(|s| s.as_str()).ok_or_else(|| bad("'step'"))?;
        let step = flow.get("steps").and_then(|s| s.get(step_id)).ok_or_else(|| bad("Unknown step"))?.clone();
        let kind = step.get("kind").and_then(|k| k.as_str()).unwrap_or("");
        let requirement = capabilities::software_requirement(&step, cfg!(windows));
        if kind != "program" && kind != "agent" {
            return Err(bad(format!("Unknown step kind {kind:?}")));
        }
        if let Some(command) = requirement.get("command").and_then(|c| c.as_str()) {
            app.refresh_inventory(&[command.to_string()]).await;
        }
        crate::artifacts::validate(&config["files"]).map_err(bad)?;
        crate::artifacts::validate(&config["assets"]).map_err(bad)?;
        config["requirement"] = requirement;
        config["runId"] = json!(run_id);
        if let Some(expected) = config.get("gate_id").and_then(|g| g.as_str()) {
            if expected != app.gate_id {
                return Err(bad("Worker capacity gate identity changed"));
            }
        }
        // A concurrent submit or cancel may have registered or terminated this id while the inventory refreshed.
        if app.jobs.lock().unwrap().contains_key(&job_id) {
            return Ok(Json(json!({"id": job_id})));
        }
        if app.termination_path(&job_id).exists() {
            return Err(bad("A terminated job ID cannot be reused"));
        }
        let job = Job::new(app.clone(), job_id.clone(), config);
        app.jobs.lock().unwrap().insert(job_id.clone(), job.clone());
        job.spawn();
    }
    Ok(Json(json!({"id": job_id})))
}

#[derive(serde::Deserialize)]
struct After {
    after: Option<i64>,
}

async fn status(State(app): State<Arc<App>>, Path(id): Path<String>, Query(q): Query<After>) -> ApiResult {
    let job = find(&app, &id)?;
    job.touch();
    let snapshot = job.snapshot(q.after.unwrap_or(0).max(0) as usize);
    Ok(Json(snapshot))
}

async fn receive(State(app): State<Arc<App>>, Path(id): Path<String>, Json(item): Json<Value>) -> ApiResult {
    let job = find(&app, &id)?;
    if !matches!(job.state(), JobState::Queued | JobState::Running) {
        return Err(bad("Job has finished"));
    }
    job.input(item);
    Ok(Json(json!({"ok": true})))
}

async fn authorize(State(app): State<Arc<App>>, Path(id): Path<String>, Json(body): Json<Value>) -> Result<Json<Value>, Response> {
    let job = find(&app, &id).map_err(|e| e.into_response())?;
    job.touch();
    let nonce = body.get("nonce").and_then(|n| n.as_str());
    if !job.authorize_with(nonce) {
        return Err((StatusCode::CONFLICT, Json(json!({"error": "No start authorization is pending for this job"}))).into_response());
    }
    Ok(Json(json!({"ok": true})))
}

async fn cancel(State(app): State<Arc<App>>, Path(id): Path<String>, headers: HeaderMap) -> Result<Json<Value>, Response> {
    let conflict = |message: &str| (StatusCode::CONFLICT, Json(json!({"error": message}))).into_response();
    let expected = headers.get("x-flow-gate-id").and_then(|v| v.to_str().ok());
    let current = app.gate.gate_id();
    if current != app.gate_id || expected.map(|e| e != current).unwrap_or(false) {
        return Err(conflict("Worker capacity gate identity changed"));
    }
    let known = app.jobs.lock().unwrap().contains_key(&id);
    if !known {
        let terminated = read_json(&app.termination_path(&id)).map(|p| p["terminated"] == true && p["gate_id"] == json!(current)).unwrap_or(false);
        if terminated {
            return Ok(Json(json!({"ok": true, "terminated": true})));
        }
        let canonical = valid_uuid(&id).ok_or_else(|| ApiError(StatusCode::NOT_FOUND, "404: Not Found".into()).into_response())?;
        if !app.gate.revoke_absent(&canonical, Some(&current)) {
            return Err(conflict("A previous execution may still be running"));
        }
        // The revocation prevents any delayed submit from acquiring capacity; check this process once more.
        if !app.jobs.lock().unwrap().contains_key(&id) {
            app.record_termination(&canonical).map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, e).into_response())?;
            return Ok(Json(json!({"ok": true, "terminated": true})));
        }
    }
    let job = find(&app, &id).map_err(|e| e.into_response())?;
    if !job.cancel_and_wait(Duration::from_secs(30)).await {
        return Err((StatusCode::SERVICE_UNAVAILABLE, Json(json!({"error": "Worker cleanup did not confirm process termination"}))).into_response());
    }
    if !app.gate.revoke_absent(&id, Some(&current)) {
        return Err(conflict("A previous execution may still be running"));
    }
    job.mark_cancelled_if_active();
    app.record_termination(&id).map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, e).into_response())?;
    Ok(Json(json!({"ok": true, "terminated": true})))
}

// --------------------------------------------------------------------------- background tasks
pub fn spawn_background(app: Arc<App>) {
    // CPU and RAM, once a second. A missing measurement stays unknown, never zero.
    let a = app.clone();
    tokio::spawn(async move {
        let mut sampler = Sampler::new();
        loop {
            let sampled = tokio::task::block_in_place(|| sampler.sample());
            if let Some((cpu, ram)) = sampled {
                a.gate.observe(cpu, ram, mono());
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });
    // Coordinators that stopped polling lose their jobs; finished jobs are forgotten after five minutes.
    let a = app.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            let now = mono();
            let jobs: Vec<(String, Arc<Job>)> = a.jobs.lock().unwrap().iter().map(|(k, v)| (k.clone(), v.clone())).collect();
            for (id, job) in jobs {
                let idle = now - job.last_seen();
                if matches!(job.state(), JobState::Queued | JobState::Running) && idle > 45.0 {
                    job.cancel_and_wait(Duration::from_secs(30)).await;
                    job.set_state_error(JobState::Cancelled, "Coordinator lease expired");
                } else if job.finished() && idle > 300.0 {
                    a.jobs.lock().unwrap().remove(&id);
                    let dir = a.root.join(&id);
                    let _ = tokio::task::spawn_blocking(move || std::fs::remove_dir_all(dir)).await;
                }
            }
        }
    });
    // The daily capability refresh.
    let a = app.clone();
    tokio::spawn(async move {
        a.refresh_inventory(&[]).await;
        loop {
            tokio::time::sleep(Duration::from_secs(3600)).await;
            let overdue = capabilities::overdue(&a.inventory.lock().unwrap());
            if overdue {
                a.refresh_inventory(&[]).await;
            }
        }
    });
}

pub fn try_acquire(app: &App, job: &str, gate_id: Option<&str>, revision: Option<i64>) -> Acquire {
    app.gate.try_acquire(job, gate_id, revision)
}

pub fn unix_now() -> f64 {
    now_unix()
}

pub fn _unused(_: Body) {}
