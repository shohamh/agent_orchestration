/* A small JavaScript stand-in for the flow host's launch check (agents/flowkit/fleet_matching.py),
 * shared by the browser tests. It answers POST /api/launch/validate with the same shapes and
 * messages, so the dialogs can be exercised against matching, waiting and blocked fleets.
 * Not a test file itself (no .test.cjs suffix).
 */
const CHECKED = '2026-11-05T09:30:00+00:00';
const OLD_CHECK = '2026-11-04T18:15:00+00:00';
const AGENTS = ['demo', 'codex', 'claude', 'opencode', 'opencode2', 'omp', 'pi'];
const BUILTINS = new Set(['echo', 'cd', 'true', 'false', 'exit', 'set', 'export']);
const WAITING_ORDER = ['offline', 'credentials_unavailable', 'full_slots', 'high_cpu', 'high_ram', 'missing_metrics', 'software_unknown', 'software_stale'];
const RESOLUTIONS = {
  no_workers: 'Register a worker in the fleet tab.',
  unknown_worker: 'Register that worker in the fleet tab or select another worker.',
  pool: 'Add a worker to the selected pool in the fleet tab, or select another pool.',
  software_absent: 'Install the required software on a matching worker, then refresh its capabilities.',
  ambiguous_command: 'Add requires_command with the executable this program step runs.',
};

// pc-a: online, build pool, linux+gpu, python and codex installed. pc-b: offline (last known
// codex result cached), build and review pools. pc-c: online, review pool, codex verified absent.
function fleet() {
  return [
    { id: 'pc-a', pools: ['build'], tags: ['linux', 'gpu'], engines: ['hatchet', 'prefect', 'trigger'], online: true, slots: 2, active: 0, cpu_limit: 90, ram_limit: 90,
      capacity: { state: 'known', cpu_percent: 12, ram_percent: 40 },
      inventory: { agents: { codex: { status: 'installed', checked_at: CHECKED }, claude: { status: 'installed', checked_at: CHECKED } },
        commands: { python: { status: 'installed', checked_at: CHECKED }, bash: { status: 'installed', checked_at: CHECKED } } } },
    { id: 'pc-b', pools: ['build', 'review'], tags: ['linux'], engines: ['hatchet', 'prefect'], online: false, slots: 1, active: 0, cpu_limit: 90, ram_limit: 90,
      last_known: { agents: { codex: { status: 'installed', checked_at: OLD_CHECK } }, commands: {} } },
    { id: 'pc-c', pools: ['review'], tags: [], engines: ['hatchet', 'prefect', 'trigger'], online: true, slots: 1, active: 0, cpu_limit: 90, ram_limit: 90,
      capacity: { state: 'known', cpu_percent: 5, ram_percent: 30 },
      inventory: { agents: { codex: { status: 'absent', checked_at: CHECKED } }, commands: { python: { status: 'installed', checked_at: CHECKED } } } },
  ];
}

const reason = (code, message, extra = {}) => ({ code, message, ...extra });

function softwareRequirement(step) {
  if (step.kind === 'agent') return { agent: step.agent || 'demo' };
  if (step.requires_command) return { command: step.requires_command };
  const run = String(step.run || '').trim();
  if (/[|;&$`<>()]/.test(run)) return { ambiguous_command: true };
  const first = run.split(/\s+/)[0];
  if (!first) return { ambiguous_command: true };
  return BUILTINS.has(first) ? {} : { command: first };
}

function selectionFor(doc, stepId, execution) {
  const base = Object.keys(execution?.default || {}).length ? execution.default : doc.worker || {};
  const specific = execution?.steps?.[stepId] || doc.steps[stepId].worker || {};
  if (specific.id) return { ...specific };
  const result = { ...base, ...specific };
  if (specific.pool) { delete result.id; delete result.pools; }
  else if (specific.pools?.length) { delete result.id; delete result.pool; }
  return result;
}

function detected(worker, requirements) {
  let inventory = worker.inventory, cached = false;
  if (!inventory && worker.last_known) { inventory = worker.last_known; cached = true; }
  return ['agent', 'command'].filter((kind) => requirements[kind]).map((kind) => {
    const found = inventory?.[`${kind}s`]?.[requirements[kind]] || {};
    return { kind, name: requirements[kind], status: found.status || 'unknown', checked_at: found.checked_at || null, cached: cached && !!found.status };
  });
}

function matchWorker(worker, req) {
  const id = worker.id, incompatible = [], unknown = [];
  if (req.id && req.id !== id) incompatible.push(reason('worker_id', `The step is assigned to worker ${req.id}, not ${id}`));
  if (req.pool && !worker.pools.includes(req.pool)) incompatible.push(reason('pool', `${id} is not in pool ${req.pool}`, { pool: req.pool }));
  if (req.pools?.length && !req.pools.some((p) => worker.pools.includes(p))) incompatible.push(reason('pool', `${id} is not in ${req.pools.length === 1 ? 'pool' : 'any of the pools'} ${req.pools.join(', ')}`, { pools: req.pools }));
  const missing = (req.tags || []).filter((t) => !worker.tags.includes(t));
  if (missing.length) incompatible.push(reason('tags', `${id} lacks tags: ${missing.join(', ')}`, { missing }));
  if (req.engine && !worker.engines.includes(req.engine)) incompatible.push(reason('engine', `${id} is not registered for ${req.engine}`, { engine: req.engine }));
  for (const kind of ['agent', 'command']) {
    const name = req[kind];
    if (!name) continue;
    const found = worker.inventory?.[`${kind}s`]?.[name] || {};
    if (found.status === 'absent') incompatible.push(reason('software_absent', `${name} is not installed on ${id} (checked ${found.checked_at})`, { software: name, checked_at: found.checked_at }));
    else if (found.status !== 'installed') unknown.push(reason('software_unknown', `Whether ${name} is installed on ${id} is unknown`, { software: name }));
  }
  if (!incompatible.length) {
    if (!worker.online) unknown.push(reason('offline', `${id} is offline`));
    else if (worker.active >= worker.slots) unknown.push(reason('full_slots', `${id} has no free slot`));
  }
  const status = incompatible.length ? 'incompatible' : unknown.length ? 'unknown' : 'compatible';
  return { worker: id, status, reasons: incompatible.length ? incompatible : unknown, detected: detected(worker, req) };
}

function blocked(reasons, matches = [], req = {}) {
  const codes = [...new Set(reasons.map((r) => r.code))];
  const texts = codes.map((c) => (c === 'tags' ? `Add the missing tags to a worker${req.pool ? ` in pool ${req.pool}` : req.pools?.length ? ` in ${req.pools.length === 1 ? 'pool' : 'pools'} ${req.pools.join(', ')}` : ''}, or remove them from the requirement.` : RESOLUTIONS[c])).filter(Boolean);
  return { status: 'blocked', reasons, message: `No registered worker can run this step: ${reasons.map((r) => r.message).join('; ')}`,
    resolution: texts.join(' ') || RESOLUTIONS.no_workers, candidates: [], workers: matches };
}

function validateLaunch(req, workers) {
  if (req.ambiguous_command) return blocked([reason('ambiguous_command', 'The program executable cannot be determined safely; declare requires_command')]);
  if (req.id) {
    workers = workers.filter((w) => w.id === req.id);
    if (!workers.length) return blocked([reason('unknown_worker', `Worker ${req.id} is not registered`)]);
  }
  const matches = workers.map((w) => matchWorker(w, req));
  const compatible = matches.filter((m) => m.status === 'compatible').map((m) => m.worker);
  if (compatible.length) return { status: 'ready', reasons: [], candidates: compatible, workers: matches };
  const waiting = matches.filter((m) => m.status === 'unknown').flatMap((m) => m.reasons);
  if (waiting.length) {
    const present = new Set(waiting.map((r) => r.code));
    return { status: 'wait', reasons: waiting, waiting_reason: WAITING_ORDER.find((c) => present.has(c)) || waiting[0].code,
      message: `Waiting: ${waiting.map((r) => r.message).join('; ')}`, candidates: [], workers: matches };
  }
  return blocked(matches.flatMap((m) => m.reasons), matches, req);
}

const ENGINES = { hatchet: 'hatchet', prefect: 'prefect', 'trigger-dev': 'trigger', trigger: 'trigger' };
const clean = (selection) => Object.fromEntries(Object.entries(selection || {}).filter(([, v]) => (Array.isArray(v) ? v.length : v)));

// The board's POST /api/launch/validate: {flow|document|card, execution?, system?} -> [status, body].
function launchCheck(body, workers, flowsByName) {
  const engine = ENGINES[body.system];
  if (body.card) {
    if (!['hatchet', 'prefect'].includes(body.system)) return [400, { error: 'fleet placement of standalone cards is supported on Hatchet and Prefect' }];
    if (body.card.agent !== 'program' && !AGENTS.includes(body.card.agent)) return [400, { error: 'agent must be codex, claude, opencode, opencode2, omp, pi or demo' }];
    const wanted = clean(body.card.worker);
    if (!Object.keys(wanted).length) return [400, { error: 'choose a worker, a pool or tags to place the card' }];
    const software = body.card.agent === 'program' ? softwareRequirement({ kind: 'program', run: body.card.command }) : { agent: body.card.agent };
    if (software.ambiguous_command) delete software.ambiguous_command; // a program card is not checked in advance then
    const decision = validateLaunch({ ...wanted, ...software, ...(engine ? { engine } : {}) }, workers);
    return [200, { status: decision.status, fleet: true, card: decision }];
  }
  const doc = body.document || flowsByName[body.flow];
  if (!doc) return [400, { error: `unknown flow '${body.flow}'` }];
  const uses = 'execution' in body || !!doc.worker || Object.values(doc.steps).some((s) => s.worker);
  if (!uses) return [200, { status: 'ready', fleet: false, steps: {} }];
  const steps = Object.fromEntries(Object.keys(doc.steps).map((sid) => [sid,
    validateLaunch({ ...clean(selectionFor(doc, sid, body.execution)), ...softwareRequirement(doc.steps[sid]), ...(engine ? { engine } : {}) }, workers)]));
  const states = new Set(Object.values(steps).map((d) => d.status));
  return [200, { status: states.has('blocked') ? 'blocked' : states.has('wait') ? 'wait' : 'ready', fleet: true, steps }];
}

module.exports = { CHECKED, OLD_CHECK, fleet, launchCheck, validateLaunch, softwareRequirement };
