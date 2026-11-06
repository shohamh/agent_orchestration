//! Per-PC admission: slots, CPU/RAM averaged over 10 seconds against limits, a cooldown after each start,
//! settings revisions that refuse stale pushes, and a gate identity. One service per PC, so the state lives in
//! this process and only the settings, identity and revocations are persisted (`gate.json`).
use crate::util::{atomic_json, mono, now_unix, read_json};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Mutex;

pub const MAX_SLOTS: u64 = 100;
const WINDOW: f64 = 10.0;
const COOLDOWN: f64 = 10.0;

fn cooldown() -> f64 {
    // Tests start a fresh service per run; AGENT_WORKER_TEST_FAST shortens the cooldown and primes the samples, as the Python fixture does.
    if std::env::var_os("AGENT_WORKER_TEST_FAST").is_some() { 0.3 } else { COOLDOWN }
}
const LEASE: f64 = 30.0;
const MAX_REVOCATIONS: usize = 5000;

#[derive(Clone, Debug, PartialEq)]
pub struct Settings {
    pub slots: u64,
    pub cpu_limit: f64,
    pub ram_limit: f64,
}

struct Lease {
    fence: String,
    expires: f64,
    phase: &'static str,
}

struct State {
    gate_id: String,
    settings: Settings,
    revision: i64,
    samples: Vec<(f64, f64, f64)>,
    last_start: f64,
    leases: HashMap<String, Lease>,
    revoked: HashSet<String>,
    revoked_order: Vec<String>,
}

pub struct Gate {
    path: PathBuf,
    state: Mutex<State>,
}

pub enum Acquire {
    Granted { fence: String, expires: f64 },
    Refused(&'static str),
}

fn valid_settings(slots: u64, cpu: f64, ram: f64) -> bool {
    (1..=MAX_SLOTS).contains(&slots) && cpu > 0.0 && cpu <= 100.0 && ram > 0.0 && ram <= 100.0
}

impl Gate {
    pub fn open(path: PathBuf, slots: u64) -> Gate {
        let saved = read_json(&path);
        let get = |key: &str| saved.as_ref().and_then(|v| v.get(key)).cloned();
        let settings = get("settings")
            .and_then(|s| Some(Settings { slots: s.get("slots")?.as_u64()?, cpu_limit: s.get("cpu_limit")?.as_f64()?, ram_limit: s.get("ram_limit")?.as_f64()? }))
            .filter(|s| valid_settings(s.slots, s.cpu_limit, s.ram_limit))
            .unwrap_or(Settings { slots, cpu_limit: 90.0, ram_limit: 90.0 });
        let revoked_order: Vec<String> = get("revoked").and_then(|v| serde_json::from_value(v).ok()).unwrap_or_default();
        let state = State {
            gate_id: get("gate_id").and_then(|v| v.as_str().map(String::from)).unwrap_or_else(|| uuid::Uuid::new_v4().simple().to_string()),
            settings,
            revision: get("settings_revision").and_then(|v| v.as_i64()).unwrap_or(-1),
            samples: Vec::new(),
            last_start: mono() - cooldown(),
            leases: HashMap::new(),
            revoked: revoked_order.iter().cloned().collect(),
            revoked_order,
        };
        let gate = Gate { path, state: Mutex::new(state) };
        gate.save(&gate.state.lock().unwrap());
        if std::env::var_os("AGENT_WORKER_TEST_FAST").is_some() {
            let base = mono();
            for i in 0..11 {
                gate.observe(1.0, 1.0, base - 10.0 + i as f64);
            }
        }
        gate
    }

    fn save(&self, s: &State) {
        let value = json!({"gate_id": s.gate_id, "settings": {"slots": s.settings.slots, "cpu_limit": s.settings.cpu_limit, "ram_limit": s.settings.ram_limit},
            "settings_revision": s.revision, "revoked": s.revoked_order});
        let _ = atomic_json(&self.path, &value);
    }

    pub fn gate_id(&self) -> String {
        self.state.lock().unwrap().gate_id.clone()
    }

    pub fn configure(&self, slots: u64, cpu: f64, ram: f64, expected_gate: Option<&str>, revision: Option<i64>) -> Result<(), String> {
        if !valid_settings(slots, cpu, ram) {
            return Err("Invalid capacity settings".into());
        }
        let mut s = self.state.lock().unwrap();
        if let Some(expected) = expected_gate {
            if expected != s.gate_id {
                return Err("Worker capacity gate identity changed".into());
            }
        }
        let wanted = Settings { slots, cpu_limit: cpu, ram_limit: ram };
        match revision {
            Some(revision) => {
                if revision < s.revision || (revision == s.revision && s.settings != wanted) {
                    return Err("Stale worker settings revision".into());
                }
                s.revision = revision;
            }
            None => s.revision = -1, // a local override: the coordinator must resync
        }
        s.settings = wanted;
        self.save(&s);
        Ok(())
    }

    pub fn observe(&self, cpu: f64, ram: f64, t: f64) {
        if !(cpu.is_finite() && ram.is_finite() && (0.0..=100.0).contains(&cpu) && (0.0..=100.0).contains(&ram)) {
            return;
        }
        let mut s = self.state.lock().unwrap();
        if t < s.last_start {
            s.last_start = t;
        }
        if let Some(last) = s.samples.last() {
            if t <= last.0 || t - last.0 > 3.0 {
                s.samples.clear();
            }
        }
        s.samples.push((t, cpu, ram));
        while s.samples.len() > 2 && s.samples[1].0 <= t - WINDOW {
            s.samples.remove(0);
        }
    }

    fn window(s: &State, now: f64) -> Value {
        let rows = &s.samples;
        let mut result = json!({"state": "unknown", "cpu_percent": null, "ram_percent": null, "sampled_at": rows.last().map(|r| r.0),
            "observed_at": now_unix(), "window_seconds": 10, "ram_basis": "physical total minus available",
            "sample_age_seconds": rows.last().map(|r| now - r.0)});
        let reason = |result: &mut Value, why: &str| {
            result["reason"] = json!(why);
        };
        let Some(newest) = rows.last() else {
            reason(&mut result, "no_samples");
            return result;
        };
        let age = now - newest.0;
        if !(0.0..=3.0).contains(&age) {
            reason(&mut result, "stale");
            return result;
        }
        if rows[0].0 > newest.0 - WINDOW {
            reason(&mut result, "incomplete_window");
            return result;
        }
        let (end, start) = (newest.0, newest.0 - WINDOW);
        let mut totals = [0.0f64; 2];
        for pair in rows.windows(2) {
            let (a, b) = (pair[0], pair[1]);
            let dt = (b.0 - start.max(a.0)).max(0.0);
            totals[0] += b.1 * dt;
            totals[1] += b.2 * dt;
        }
        let _ = end;
        result["state"] = json!("known");
        result["reason"] = Value::Null;
        result["cpu_percent"] = json!(totals[0] / WINDOW);
        result["ram_percent"] = json!(totals[1] / WINDOW);
        result
    }

    pub fn metrics(&self) -> Value {
        let s = self.state.lock().unwrap();
        let mut m = Self::window(&s, mono());
        m["gate_id"] = json!(s.gate_id);
        m["settings_revision"] = json!(s.revision);
        m["settings"] = json!({"slots": s.settings.slots, "cpu_limit": s.settings.cpu_limit, "ram_limit": s.settings.ram_limit});
        m["leases"] = json!(s.leases.len());
        m
    }

    pub fn try_acquire(&self, job: &str, expected_gate: Option<&str>, expected_revision: Option<i64>) -> Acquire {
        let mut s = self.state.lock().unwrap();
        let now = mono();
        let m = Self::window(&s, now);
        if let Some(expected) = expected_gate {
            if expected != s.gate_id {
                return Acquire::Refused("gate_changed");
            }
        }
        if let Some(expected) = expected_revision {
            if expected != s.revision {
                return Acquire::Refused("settings_changed");
            }
        }
        if s.revoked.contains(job) {
            return Acquire::Refused("revoked");
        }
        let settings = s.settings.clone();
        let reason = if s.leases.len() as u64 >= settings.slots {
            Some("full_slots")
        } else if s.leases.values().any(|l| l.phase == "pending") {
            Some("pending_start")
        } else if now - s.last_start < cooldown() || s.samples.last().map(|l| l.0 - s.last_start < cooldown()).unwrap_or(false) {
            Some("cooldown")
        } else if m["state"] == "unknown" {
            Some("missing_metrics")
        } else if m["cpu_percent"].as_f64().unwrap_or(0.0) >= settings.cpu_limit {
            Some("high_cpu")
        } else if m["ram_percent"].as_f64().unwrap_or(0.0) >= settings.ram_limit {
            Some("high_ram")
        } else {
            None
        };
        if let Some(reason) = reason {
            return Acquire::Refused(reason);
        }
        let fence = uuid::Uuid::new_v4().simple().to_string();
        s.leases.insert(job.to_string(), Lease { fence: fence.clone(), expires: now + LEASE, phase: "pending" });
        Acquire::Granted { fence, expires: now + LEASE }
    }

    fn valid_locked(s: &State, job: &str, fence: &str) -> bool {
        s.leases.get(job).map(|l| l.fence == fence && l.expires > mono()).unwrap_or(false)
    }

    pub fn valid(&self, job: &str, fence: &str) -> bool {
        Self::valid_locked(&self.state.lock().unwrap(), job, fence)
    }

    pub fn confirm_started(&self, job: &str, fence: &str) -> Result<(), String> {
        let mut s = self.state.lock().unwrap();
        if !Self::valid_locked(&s, job, fence) {
            return Err("Capacity lease lost before start".into());
        }
        let already = s.leases.get(job).map(|l| l.phase == "started").unwrap_or(false);
        if !already {
            if let Some(lease) = s.leases.get_mut(job) {
                lease.phase = "started";
            }
            s.last_start = mono();
        }
        Ok(())
    }

    pub fn renew(&self, job: &str, fence: &str) -> bool {
        let mut s = self.state.lock().unwrap();
        if !Self::valid_locked(&s, job, fence) {
            return false;
        }
        if let Some(lease) = s.leases.get_mut(job) {
            lease.expires = mono() + LEASE;
        }
        true
    }

    /// Run `callback` only while the lease is still this attempt's: a publication after the lease expired is fenced out.
    pub fn publish<T>(&self, job: &str, fence: &str, callback: impl FnOnce() -> T) -> Result<T, String> {
        let s = self.state.lock().unwrap();
        if !Self::valid_locked(&s, job, fence) {
            return Err("Capacity lease expired; publication fenced".into());
        }
        Ok(callback())
    }

    fn revoke(&self, s: &mut State, job: &str) {
        if s.revoked.insert(job.to_string()) {
            s.revoked_order.push(job.to_string());
            if s.revoked_order.len() > MAX_REVOCATIONS {
                let old = s.revoked_order.remove(0);
                s.revoked.remove(&old);
            }
            self.save(s);
        }
    }

    /// Prevent a delayed submit from acquiring capacity, unless a live lease exists.
    pub fn revoke_absent(&self, job: &str, expected_gate: Option<&str>) -> bool {
        let mut s = self.state.lock().unwrap();
        if let Some(expected) = expected_gate {
            if expected != s.gate_id {
                return false;
            }
        }
        if s.leases.contains_key(job) {
            return false;
        }
        self.revoke(&mut s, job);
        true
    }

    /// Release this execution's lease and revoke its id.
    pub fn finish(&self, job: &str, expected_gate: Option<&str>) -> Result<(), String> {
        let mut s = self.state.lock().unwrap();
        if let Some(expected) = expected_gate {
            if expected != s.gate_id {
                return Err("Worker capacity gate identity changed during cleanup".into());
            }
        }
        s.leases.remove(job);
        self.revoke(&mut s, job);
        Ok(())
    }

    pub fn release(&self, job: &str) {
        self.state.lock().unwrap().leases.remove(job);
    }
}

/// CPU and RAM of the whole machine, sampled once a second.
pub struct Sampler {
    system: sysinfo::System,
}

impl Sampler {
    pub fn new() -> Sampler {
        let mut system = sysinfo::System::new();
        system.refresh_cpu_usage();
        Sampler { system }
    }

    /// (cpu percent, ram percent), or None until a second reading exists.
    pub fn sample(&mut self) -> Option<(f64, f64)> {
        self.system.refresh_cpu_usage();
        self.system.refresh_memory();
        let total = self.system.total_memory() as f64;
        if total <= 0.0 {
            return None;
        }
        let ram = 100.0 * (1.0 - self.system.available_memory() as f64 / total);
        let cpu = self.system.global_cpu_usage() as f64;
        Some((cpu.clamp(0.0, 100.0), ram.clamp(0.0, 100.0)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gate(slots: u64) -> Gate {
        let dir = tempfile::tempdir().unwrap();
        let g = Gate::open(dir.path().join("gate.json"), slots);
        std::mem::forget(dir);
        g
    }

    fn warm(g: &Gate, cpu: f64, ram: f64) {
        let base = mono();
        for i in 0..12 {
            g.observe(cpu, ram, base - 11.0 + i as f64);
        }
        g.state.lock().unwrap().last_start = base - 100.0;
    }

    #[test]
    fn admits_within_limits_then_enforces_slots_and_cooldown() {
        let g = gate(1);
        warm(&g, 10.0, 10.0);
        let Acquire::Granted { fence, .. } = g.try_acquire("a", None, None) else { panic!("should be admitted") };
        assert!(matches!(g.try_acquire("b", None, None), Acquire::Refused("full_slots")));
        g.confirm_started("a", &fence).unwrap();
        g.finish("a", None).unwrap();
        assert!(matches!(g.try_acquire("c", None, None), Acquire::Refused("cooldown")));
    }

    #[test]
    fn unknown_metrics_and_high_load_hold_work() {
        let g = gate(2);
        assert!(matches!(g.try_acquire("a", None, None), Acquire::Refused("missing_metrics")));
        warm(&g, 95.0, 10.0);
        assert!(matches!(g.try_acquire("a", None, None), Acquire::Refused("high_cpu")));
        warm(&g, 10.0, 95.0);
        assert!(matches!(g.try_acquire("a", None, None), Acquire::Refused("high_ram")));
    }

    #[test]
    fn settings_revisions_refuse_stale_pushes_and_gate_identity_is_checked() {
        let g = gate(1);
        g.configure(2, 80.0, 80.0, None, Some(5)).unwrap();
        assert!(g.configure(3, 80.0, 80.0, None, Some(4)).is_err());
        assert!(g.configure(3, 80.0, 80.0, None, Some(5)).is_err());
        assert!(g.configure(2, 80.0, 80.0, None, Some(5)).is_ok());
        assert!(g.configure(2, 80.0, 80.0, Some("other"), Some(6)).is_err());
        warm(&g, 10.0, 10.0);
        assert!(matches!(g.try_acquire("j", None, Some(4)), Acquire::Refused("settings_changed")));
        assert!(matches!(g.try_acquire("j", Some("nope"), None), Acquire::Refused("gate_changed")));
        assert!(g.configure(2, 80.0, 80.0, None, None).is_ok());
        assert_eq!(g.metrics()["settings_revision"], -1);
    }

    #[test]
    fn a_finished_or_revoked_job_id_never_acquires_again() {
        let g = gate(2);
        warm(&g, 10.0, 10.0);
        assert!(g.revoke_absent("x", None));
        assert!(matches!(g.try_acquire("x", None, None), Acquire::Refused("revoked")));
        let Acquire::Granted { .. } = g.try_acquire("y", None, None) else { panic!() };
        assert!(!g.revoke_absent("y", None), "a live lease cannot be revoked away");
    }

    #[test]
    fn publication_is_fenced_by_the_lease() {
        let g = gate(1);
        warm(&g, 10.0, 10.0);
        let Acquire::Granted { fence, .. } = g.try_acquire("a", None, None) else { panic!() };
        assert_eq!(g.publish("a", &fence, || 7).unwrap(), 7);
        assert!(g.publish("a", "wrong", || 7).is_err());
        g.release("a");
        assert!(g.publish("a", &fence, || 7).is_err());
    }

    #[test]
    fn settings_and_identity_survive_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("gate.json");
        let first = Gate::open(path.clone(), 1);
        first.configure(4, 70.0, 60.0, None, Some(9)).unwrap();
        let id = first.gate_id();
        let second = Gate::open(path, 1);
        assert_eq!(second.gate_id(), id);
        let m = second.metrics();
        assert_eq!((m["settings"]["slots"].as_u64(), m["settings_revision"].as_i64()), (Some(4), Some(9)));
    }
}
