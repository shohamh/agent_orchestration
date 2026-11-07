/* Real Chromium checks for the fleet tab (fleet.js) against a local fake of the board's fleet API.
 * node --test board/static/fleet.browser.test.cjs
 * CHROME_PATH may override the installed Chrome executable; FLEET_SCREENSHOTS=<dir> saves screenshots.
 */
const { test, before, after } = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const os = require('node:os');
const { spawn } = require('node:child_process');

// ---------------------------------------------------------------- pure helpers (no browser)
const Fleet = require('./fleet.js');

test('instructions fit the selected engines and operating system and never hold a token', () => {
  const win = Fleet.instructions({ engines: ['hatchet'], os: 'windows', id: 'lab-pc', slots: 2 });
  const text = (doc) => doc.sections.map((s) => [s.title, ...s.steps.map((x) => `${x.text || ''}\n${x.code || ''}`)].join('\n')).join('\n') + doc.warnings.join('\n');
  const w = text(win);
  assert.match(w, /\$env:FLOW_WORKER_ID = 'lab-pc'/);
  assert.match(w, /agents\\\.venv\\Scripts\\python\.exe -u agents\\flow_execution_worker\.py/);
  assert.match(w, /\$env:FLOW_WORKER_SLOTS = '2'/);
  assert.match(w, /--no-index --find-links/);                       // offline install
  for (const os of ['windows', 'linux', 'macos']) {                  // the mirror only holds NPM_PROJECTS
    const t = text(Fleet.instructions({ engines: ['hatchet'], os, id: 'x' }));
    assert.match(t, /npm(\.cmd)? ci --prefix agents --registry <offline npm mirror URL>/, os);
    assert.match(t, /add agents to (it|NPM_PROJECTS)/, os);
    assert.match(t, /NPM_PROJECTS="trigger\.dev\/example-project agents"/, os);
  }
  assert.match(w, /FLOW_WORKER_TOKEN_LAB_PC=<generated token>/);    // placeholder only
  assert.match(w, /workers\.local\.env/);
  assert.match(w, /no Hatchet (token|credentials)/i);
  assert.doesNotMatch(w, /Trigger\.dev|Prefect/);
  const linux = text(Fleet.instructions({ engines: ['prefect'], os: 'linux', id: 'lab-pc' }));
  assert.match(linux, /export FLOW_WORKER_ID='lab-pc'/);
  assert.match(linux, /agents\/\.venv\/bin\/python -u agents\/flow_execution_worker\.py/);
  assert.match(linux, /\/var\/tmp\/agent-orchestration\/capacity\.db/);
  assert.doesNotMatch(linux, /\$env:/);
  assert.match(linux, /Prefect/);
  const mac = Fleet.instructions({ engines: ['trigger'], os: 'macos', id: 'mac-pc' });
  assert.ok(mac.warnings.some((x) => /macOS/.test(x) && /CPU\/RAM/.test(x)), 'macOS capacity limitation is stated');
  const trigger = text(mac);
  assert.match(trigger, /trigger dev/);
  assert.match(trigger, /cards .*coordinator/i);
  assert.match(trigger, /production .*not supported/i);
  // Adding an engine to a registered PC needs nothing new on it.
  const adding = text(Fleet.instructions({ engines: ['hatchet', 'prefect'], os: 'linux', id: 'pc-a', existing: ['hatchet'] }));
  assert.match(adding, /Nothing new is needed on the PC/);
  assert.doesNotMatch(adding, /pip install/);
  for (const doc of [win, mac]) assert.doesNotMatch(text(doc), /[A-Za-z0-9_-]{40,}/, 'no generated secret');
});

test('engine status words: Trigger.dev unconfirmed is never called connected', () => {
  const online = { online: true, status: 'online' };
  const unconfirmed = Fleet.engineText('trigger', { route: 'shared_service', coordinator: 'unconfirmed', state: 'unconfirmed', connected: false }, online);
  assert.match(unconfirmed, /^Trigger\.dev: Unconfirmed: .*trigger dev worker/);
  assert.doesNotMatch(unconfirmed, /Connected/);
  assert.match(Fleet.engineText('hatchet', { coordinator: 'running', state: 'connected', connected: true }, online), /^Hatchet: Connected/);
  assert.match(Fleet.engineText('trigger', { coordinator: 'unconfirmed', state: 'not_connected', connected: false }, { online: false, status: 'unreachable' }), /Not connected/);
});

test('tag suggestions autocomplete existing tags and offer to create new ones', () => {
  assert.deepEqual(Fleet.tagSuggestions('cu', ['cuda', 'linux', 'CUDA-12'], []), { matches: ['cuda', 'CUDA-12'], create: 'cu' });
  assert.deepEqual(Fleet.tagSuggestions('cuda', ['cuda', 'linux'], []), { matches: ['cuda'], create: null });
  assert.deepEqual(Fleet.tagSuggestions('cuda', ['cuda', 'linux'], ['cuda']), { matches: [], create: null });
  assert.deepEqual(Fleet.tagSuggestions('a,b', [], []).create, null);
  assert.deepEqual(Fleet.tagSuggestions('  ', ['x'], []), { matches: ['x'], create: null });
});

test('metric text: values, limits, unknown and stale are words, not colour', () => {
  const known = { state: 'known', cpu_percent: 42.4, ram_percent: 95, sample_age_seconds: 0.4 };
  assert.deepEqual(Fleet.metricView(known, 'cpu', 90, { online: true }), { value: '42%', detail: '10-second average · limit 90%', level: 'ok', label: 'CPU 42%, limit 90%' });
  const over = Fleet.metricView(known, 'ram', 90, { online: true });
  assert.equal(over.level, 'over'); assert.match(over.detail, /at or above the 90% limit: no new work/);
  assert.match(Fleet.metricView({ state: 'unknown', reason: 'stale', sample_age_seconds: 12.2 }, 'cpu', 90, { online: true }).detail, /stale: last sample 12 s ago/);
  assert.match(Fleet.metricView({ state: 'unknown', reason: 'incomplete_window' }, 'cpu', 90, { online: true }).detail, /collecting a full 10-second window/);
  assert.match(Fleet.metricView({ state: 'unknown', reason: 'no_samples' }, 'cpu', 90, { online: true }).detail, /no measurements yet/);
  assert.equal(Fleet.metricView(undefined, 'cpu', 90, { online: false }).value, 'unknown');
  assert.match(Fleet.metricView(known, 'cpu', 90, { online: true, staleSince: '10:42:05' }).detail, /not updated since 10:42:05/);
});

// ---------------------------------------------------------------- fake board fleet API
const iso = (minutesAgo = 0) => new Date(Date.now() - minutesAgo * 60000).toISOString();
const result = (status, minutesAgo = 5) => ({ status, checked_at: iso(minutesAgo), auth_status: 'unknown' });
const ENGINES = ['hatchet', 'prefect', 'trigger'];
let workers, tags, calls, fault, metrics, unregisterFault;
function reset() {
  installs.clear(); installCount = 0; updates.workers = []; updates.auto = false;
  calls = []; fault = { metrics: false }; unregisterFault = null;
  tags = ['cuda', 'linux', 'windows'];
  workers = new Map(Object.entries({
    'pc-a': { id: 'pc-a', pools: ['build', 'gpu'], tags: ['windows', 'cuda'], engines: ['hatchet', 'prefect'], slots: 2, cpu_limit: 90, ram_limit: 90,
      connection: 'configured', online: true, status: 'online', hostname: 'BUILD-01', active: 1, settings_sync: { state: 'applied' },
      inventory: { agents: { codex: result('installed'), claude: result('absent'), demo: result('installed'), pi: result('unknown') },
        commands: { git: result('installed'), cargo: result('absent') }, refreshed_at: iso(5), cached: false },
      _url: 'http://10.1.2.3:8096', _token_env: 'PC_A_TOKEN' },
    'pc-b': { id: 'pc-b', pools: ['build'], tags: ['linux'], engines: ENGINES, slots: 1, cpu_limit: 90, ram_limit: 90, connection: 'configured',
      online: false, status: 'unreachable', settings_sync: { state: 'pending' }, warnings: ['slots 500 in the registry is above the maximum of 100; 100 is enforced and stored at the next registry save'],
      inventory: { agents: { codex: result('installed', 600) }, commands: {}, refreshed_at: iso(600), cached: true },
      _url: 'http://10.1.2.4:8096', _token_env: 'PC_B_TOKEN' },
    'pc-c': { id: 'pc-c', pools: ['default'], tags: [], engines: ['hatchet'], slots: 1, cpu_limit: 90, ram_limit: 90, connection: 'awaiting',
      online: false, status: 'awaiting_connection', settings_sync: { state: 'pending' } },
  }));
  metrics = { 'pc-a': { state: 'known', reason: null, cpu_percent: 42.4, ram_percent: 63.2, sample_age_seconds: 0.5, window_seconds: 10,
    ram_basis: 'physical total minus available', leases: 1, settings: { slots: 2, cpu_limit: 90, ram_limit: 90 } } };
}
const coordinators = { hatchet: 'running', prefect: 'stopped', trigger: 'unconfirmed' };
function publicWorker(w) {
  const out = Object.fromEntries(Object.entries(w).filter(([k]) => !k.startsWith('_')));
  if (metrics[w.id] && w.online) out.capacity = metrics[w.id];
  const state = (e) => (!w.online || ['stopped', 'not_enabled'].includes(coordinators[e]) ? 'not_connected' : coordinators[e] === 'running' ? 'connected' : 'unconfirmed');
  out.engine_status = Object.fromEntries(w.engines.map((e) => [e, { route: 'shared_service', coordinator: coordinators[e], state: state(e), connected: state(e) === 'connected' }]));
  return out;
}
const listing = () => ({ workers: [...workers.values()].map(publicWorker), tags: [...new Set([...tags, ...[...workers.values()].flatMap((w) => w.tags)])].sort(),
  pools: [...new Set([...workers.values()].flatMap((w) => w.pools))].sort(), engines: ENGINES });
const LABEL = (v) => typeof v === 'string' && v.length > 0 && v.length <= 100 && v === v.trim() && !v.includes(',');
function save(body, existing) {
  const merged = { ...(existing || { slots: 1, cpu_limit: 90, ram_limit: 90, engines: ENGINES, tags: [], pools: ['default'], online: false, status: 'awaiting_connection' }), ...body };
  const { url, token_env, ...rest } = merged;
  const w = { ...rest, _url: url ?? existing?._url, _token_env: token_env ?? existing?._token_env };
  w.connection = w._url ? 'configured' : 'awaiting';
  if (!existing && w._url) w.status = 'unreachable';
  workers.set(w.id, w);
  return { worker: publicWorker(w), settings_sync: w.online ? { state: 'applied' } : { state: 'pending' } };
}

const updates = { latest: { windows: 'aaaa1111bbbb2222', linux: 'cccc3333dddd4444' }, auto: false, workers: [] };
const installs = new Map();
let installCount = 0;
const installCommands = (record, origin) => {
  const link = `${origin}/install/${record.code}/agent-worker`;
  return { powershell: `$f = Join-Path $env:TEMP 'agent-worker.exe'; iwr -UseBasicParsing '${link}' -OutFile $f; & $f install ${record.code} --server '${origin}'`,
    cmd: `curl.exe -fsSL "${link}" -o "%TEMP%\\agent-worker.exe" && "%TEMP%\\agent-worker.exe" install ${record.code} --server "${origin}"`,
    bash: `f=$(mktemp) && curl -fsSL '${link}' -o "$f" && chmod +x "$f" && "$f" install ${record.code} --server '${origin}'`, elevation: 'Run as a normal user. Do not use Administrator or sudo.' };
};
const installView = (r) => { const { commands, ...rest } = r; return rest; };
const STATIC = ['index.html', 'chat.js', 'card-search.js', 'chat.css', 'flow-builder.js', 'flow-builder.css', 'fleet.js', 'fleet.css', 'admin.js', 'admin.css', 'new-card.js', 'new-card.css', 'overlays.js', 'flow-graph.js', 'flow-graph.css'];
const server = http.createServer(async (request, response) => {
  const url = new URL(request.url, 'http://localhost');
  let raw = ''; for await (const chunk of request) raw += chunk;
  const body = raw ? JSON.parse(raw) : undefined;
  const json = (status, data) => { response.writeHead(status, { 'Content-Type': 'application/json' }); response.end(JSON.stringify(data)); };
  if (url.pathname.startsWith('/api/fleet')) calls.push({ method: request.method, path: url.pathname, body });
  try {
    if (url.pathname === '/api/fleet' && request.method === 'GET') return json(200, listing());
    if (/^\/api\/fleet\/workers\/[^/]+\/jobs$/.test(url.pathname)) return json(200, { jobs: [
      { run: 'run-live', step: 'card', state: 'running', at: Date.now() / 1000, title: 'Live job', system: 'hatchet', board: 'research', column: 'running' },
      { run: 'run-old', step: 'build', state: 'done', at: Date.now() / 1000 - 90, title: 'Old job', system: 'hatchet', board: 'research', column: 'done' },
      { run: 'run-gone', step: 'card', state: 'running', at: Date.now() / 1000 - 900 }] });
    if (url.pathname === '/api/fleet/metrics') {
      if (fault.metrics) return json(502, { error: 'The flow host is not reachable' });
      return json(200, { workers: listing().workers.map(({ inventory, ...w }) => w), engines: ENGINES });
    }
    if (url.pathname === '/api/fleet/tags' && request.method === 'POST') {
      if (!LABEL(body.name)) return json(400, { error: 'A tag is 1 to 100 characters without commas' });
      if (!tags.includes(body.name)) tags.push(body.name);
      return json(200, { tags: listing().tags });
    }
    if (url.pathname === '/api/fleet/workers' && request.method === 'POST') {
      if (workers.has(body.id)) return json(409, { error: `Worker ${body.id} is already registered; edit it to add engines, pools or tags` });
      return json(200, save(body, null));
    }
    let m = url.pathname.match(/^\/api\/fleet\/workers\/([^/]+)$/);
    if (m && request.method === 'PUT') {
      const existing = workers.get(decodeURIComponent(m[1]));
      if (!existing) return json(404, { error: 'Unknown worker' });
      return json(200, save({ ...body, id: existing.id }, existing));
    }
    if (m && request.method === 'DELETE') {
      if (unregisterFault) return json(unregisterFault.status, { error: unregisterFault.error });
      const id = decodeURIComponent(m[1]);
      workers.delete(id);
      for (const record of installs.values()) if (record.id === id && record.state === 'registered') record.state = 'unregistered';
      return json(200, { ok: true });
    }


    if (url.pathname === '/api/fleet/installs' && request.method === 'GET') {
      return json(200, { installs: [...installs.values()].filter((r) => r.state !== 'cancelled').map((r) => (r.state === 'pending' ? { ...installView(r), commands: r.commands } : installView(r))) });
    }
    if (url.pathname === '/api/fleet/installs' && request.method === 'POST') {
      if (workers.has(body.id)) return json(400, { error: `${body.id} is already registered; edit it instead` });
      if ([...installs.values()].some((r) => r.id === body.id && r.state === 'pending')) return json(400, { error: `${body.id} already has an install link waiting; cancel it first` });
      const code = `00000000-0000-4000-8000-${String(++installCount).padStart(12, '0')}`;
      const record = { code, id: body.id, os: body.os, autostart: body.autostart, state: 'pending', created_at: Date.now() / 1000, expires_at: Date.now() / 1000 + 1800, step: null, error: null, hostname: null, connected: false, form: body };
      record.commands = installCommands(record, body.server);
      installs.set(code, record);
      return json(200, { install: installView(record), commands: record.commands });
    }
    m = url.pathname.match(/^\/api\/fleet\/installs\/([^/]+)$/);
    if (m && request.method === 'DELETE') {
      const record = installs.get(m[1]);
      if (!record) return json(400, { error: 'Unknown install link' });
      record.state = 'cancelled';
      return json(200, { install: installView(record) });
    }
    if (url.pathname === '/api/fleet/updates' && request.method === 'GET') return json(200, updates);
    if (url.pathname === '/api/fleet/updates' && request.method === 'POST') {
      updates.auto = true;
      updates.workers = updates.workers.map((w) => (w.status === 'outdated' ? { ...w, status: 'updating' } : w));
      return json(200, updates);
    }
    if (url.pathname === '/api/fleet/updates' && request.method === 'DELETE') { updates.auto = false; return json(200, updates); }
    if (url.pathname === '/api/fleet/updates/link' && request.method === 'POST') {
      if (!['windows', 'linux'].includes(body.os)) return json(400, { error: 'Say which operating system the PC runs' });
      const code = `link${String(updates.workers.length).padStart(40, 'u')}`;
      const link = `${body.server}/update/${code}/agent-worker`;
      return json(200, { link: { code, id: body.worker, os: body.os, expires_at: Date.now() / 1000 + 1800 }, commands: {
        powershell: `$f = Join-Path $env:TEMP 'agent-worker-update.exe'; iwr -UseBasicParsing '${link}' -OutFile $f; & $f update --server '${body.server}' --code '${code}' --force`,
        cmd: `curl.exe -fsSL "${link}" -o "%TEMP%\\agent-worker-update.exe" && "%TEMP%\\agent-worker-update.exe" update --server "${body.server}" --code "${code}" --force`,
        bash: `f=$(mktemp) && curl -fsSL '${link}' -o "$f" && chmod +x "$f" && "$f" update --server '${body.server}' --code '${code}' --force`, local: 'agent-worker update' } });
    }
    if (url.pathname === '/__updates' && request.method === 'POST') { updates.workers = body.workers; return json(200, {}); }
    if (url.pathname === '/__install' && request.method === 'POST') {  // test hook: move an install along
      Object.assign(installs.get(body.code), body.set);
      return json(200, {});
    }
    m = url.pathname.match(/^\/api\/fleet\/workers\/([^/]+)\/refresh$/);
    if (m && request.method === 'POST') {
      const w = workers.get(decodeURIComponent(m[1]));
      if (!w.online) return json(409, { error: `${w.id} is not connected; its capabilities are checked when it connects` });
      w.inventory = { ...w.inventory, agents: { ...w.inventory.agents, claude: result('installed', 0) }, refreshed_at: iso(0) };
      return json(200, { inventory: w.inventory });
    }
    if (url.pathname === '/api/systems') return json(200, { systems: ['hatchet'], columns: ['scheduled', 'queued', 'running', 'waiting', 'done', 'stopped'] });
    if (url.pathname === '/api/live') {
      response.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache' });
      response.write(`data: ${JSON.stringify({ type: 'feed', live: true })}\n\n`);
      response.write(`data: ${JSON.stringify({ type: 'cards', reset: true, cards: [] })}\n\n`);
      return;
    }
    if (url.pathname === '/api/flows') return json(200, { flows: [], broken: {} });
    if (url.pathname === '/api/workers') return json(200, { workers: [] });
    if (url.pathname === '/api/schedules') return json(200, { schedules: [] });
    if (url.pathname.startsWith('/api/')) return json(404, { error: 'not in fake' });
    const file = url.pathname === '/' ? 'index.html' : url.pathname.replace('/static/', '');
    if (!STATIC.includes(file)) { response.writeHead(404).end(); return; }
    response.writeHead(200, { 'Content-Type': file.endsWith('.js') ? 'text/javascript' : file.endsWith('.css') ? 'text/css' : 'text/html' });
    response.end(fs.readFileSync(path.join(__dirname, file)));
  } catch (error) { json(500, { error: error.message }); }
});

// ---------------------------------------------------------------- browser
const errors = [];
let browser, profile, socket, call, evaluate, waitFor, base;
const shots = process.env.FLEET_SCREENSHOTS;
before(async () => {
  reset();
  const chromePath = process.env.CHROME_PATH || 'C:/Program Files/Google/Chrome/Application/chrome.exe';
  assert.ok(fs.existsSync(chromePath), 'Set CHROME_PATH to a Chromium browser');
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  base = `http://127.0.0.1:${server.address().port}`;
  profile = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-board-fleet-'));
  browser = spawn(chromePath, ['--headless=new', '--disable-gpu', '--no-first-run', '--no-default-browser-check', '--remote-debugging-port=0', `--user-data-dir=${profile}`, 'about:blank'], { stdio: 'ignore', windowsHide: true });
  let port;
  for (let i = 0; i < 300 && !port; i++) {
    try { const text = fs.readFileSync(path.join(profile, 'DevToolsActivePort'), 'utf8'); if (text.includes('\n')) port = text.split('\n')[0]; } catch {}
    if (!port) await new Promise((resolve) => setTimeout(resolve, 100));
  }
  assert.ok(port, 'Chrome debugging endpoint starts');
  const targets = await (await fetch(`http://127.0.0.1:${port}/json`)).json();
  socket = new WebSocket(targets.find((x) => x.type === 'page').webSocketDebuggerUrl);
  await new Promise((resolve) => socket.addEventListener('open', resolve, { once: true }));
  let id = 0; const pending = new Map();
  socket.addEventListener('message', (event) => {
    const data = JSON.parse(event.data);
    if (data.id) { const handler = pending.get(data.id); pending.delete(data.id); if (data.error) handler.reject(new Error(data.error.message)); else handler.resolve(data.result); }
    if (data.method === 'Runtime.exceptionThrown') errors.push(data.params.exceptionDetails.text + ': ' + (data.params.exceptionDetails.exception?.description || ''));
  });
  call = (method, params = {}) => new Promise((resolve, reject) => { const key = ++id; pending.set(key, { resolve, reject }); socket.send(JSON.stringify({ id: key, method, params })); });
  evaluate = async (expression) => {
    const r = await call('Runtime.evaluate', { expression, awaitPromise: true, returnByValue: true });
    if (r.exceptionDetails) throw new Error(`${expression}: ${r.exceptionDetails.exception?.description || r.exceptionDetails.text}`);
    return r.result.value;
  };
  waitFor = async (expression, what = expression) => {
    for (let i = 0; i < 100; i++) { if (await evaluate(expression)) return; await new Promise((resolve) => setTimeout(resolve, 75)); }
    throw new Error(`Timed out: ${what}`);
  };
  await call('Runtime.enable');
  await call('Page.enable');
  await call('Emulation.setDeviceMetricsOverride', { width: 1440, height: 1000, deviceScaleFactor: 1, mobile: false });
  await call('Page.navigate', { url: base });
  await waitFor('typeof Fleet === "object" && document.querySelector("#fleet-btn")');
  await evaluate('Fleet.config.metricsMs = 250');
});
after(async () => {
  if (socket) socket.close();
  browser?.kill();
  server.closeAllConnections();
  await new Promise((resolve) => server.close(resolve));
  try { fs.rmSync(profile, { recursive: true, force: true }); } catch {}
});

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const js = (value) => JSON.stringify(value);
const q = (selector) => `document.querySelector(${js(selector)})`;
const text = (selector) => evaluate(`(${q(selector)}?.textContent || '').replace(/\\s+/g, ' ')`);
async function shot(name) {
  if (!shots) return;
  fs.mkdirSync(shots, { recursive: true });
  const image = await call('Page.captureScreenshot', { format: 'png' });
  fs.writeFileSync(path.join(shots, `${name}.png`), Buffer.from(image.data, 'base64'));
}
async function centre(selector, label) {
  const r = await evaluate(`(() => { const all = [...document.querySelectorAll(${js(selector)})].filter(n => n.getClientRects().length && (${js(label ?? null)} === null || n.textContent.includes(${js(label ?? '')})));
    const n = all[0]; if (!n) return null; n.scrollIntoView({ block: 'nearest' }); const b = n.getBoundingClientRect(); return { x: b.left + b.width / 2, y: b.top + b.height / 2 }; })()`);
  assert.ok(r, `${selector}${label ? ` "${label}"` : ''} is visible`);
  return r;
}
async function mouse(type, { x, y }, buttons = 1) { await call('Input.dispatchMouseEvent', { type, x, y, button: 'left', buttons, clickCount: 1 }); }
async function click(selector, label) { const at = await centre(selector, label); await mouse('mouseMoved', at, 0); await mouse('mousePressed', at); await mouse('mouseReleased', at, 0); await sleep(40); }
const KEYS = { Enter: [13, '\r'], Tab: [9, ''], Escape: [27, ''], ArrowDown: [40, ''], ArrowUp: [38, ''] };
async function press(key) {
  const [code, t] = KEYS[key];
  await call('Input.dispatchKeyEvent', { type: t ? 'keyDown' : 'rawKeyDown', key, code: key, windowsVirtualKeyCode: code, text: t || undefined });
  await call('Input.dispatchKeyEvent', { type: 'keyUp', key, code: key, windowsVirtualKeyCode: code });
  await sleep(40);
}
async function type(selector, value) {
  await evaluate(`(() => { const n = ${q(selector)}; n.focus(); n.value = ''; })()`);
  await call('Input.insertText', { text: value });
  await sleep(40);
}
const worker = (id) => `.fl-worker[data-worker="${id}"]`;
const lastCall = (method, prefix) => calls.filter((c) => c.method === method && c.path.startsWith(prefix)).at(-1);
async function openFleet() {
  if (!(await evaluate('document.querySelector("#fleet-dialog").open'))) await click('#fleet-btn');
  await waitFor(`document.querySelector("#fleet-dialog").open && document.querySelectorAll(".fl-worker").length === ${workers.size}`, 'fleet tab lists every worker');
}
const noSecrets = async () => {
  const page = await evaluate(`document.documentElement.outerHTML + JSON.stringify(localStorage) + JSON.stringify(sessionStorage)`);
  for (const secret of ['10.1.2.3', '10.1.2.4', 'PC_A_TOKEN', 'PC_B_TOKEN', 'net-pc.internal', 'NET_PC_TOKEN']) assert.ok(!page.includes(secret), `the page never shows ${secret}`);
};

// ---------------------------------------------------------------- tests
test('fleet tab lists every PC with state, pools, tags, slots, per-engine status and capabilities', async () => {
  await openFleet();
  await shot('fleet-list');
  const a = await text(worker('pc-a'));
  for (const part of ['pc-a', 'BUILD-01', 'Online', 'build', 'gpu', 'windows', 'cuda', '1 of 2 slots in use']) assert.ok(a.includes(part), `pc-a shows ${part}: ${a}`);
  assert.match(await text(`${worker('pc-a')} [data-engine="hatchet"]`), /Hatchet.*Connected.*shared execution service/);
  assert.match(await text(`${worker('pc-a')} [data-engine="prefect"]`), /Prefect.*Not connected.*Prefect flow worker is not running/);
  assert.equal(await evaluate(`document.querySelectorAll('${worker('pc-a')} [data-engine]').length`), 2, 'only registered engines are listed');
  assert.match(await text(`${worker('pc-a')} [data-agent="codex"]`), /✓.*codex.*Installed.*checked \d{4}-\d{2}-\d{2} \d{2}:\d{2}/);
  assert.match(await text(`${worker('pc-a')} [data-agent="claude"]`), /✗.*Claude Code.*Absent/);
  assert.match(await text(`${worker('pc-a')} [data-agent="pi"]`), /\?.*pi.*Unknown/);
  assert.match(await text(`${worker('pc-a')} [data-command="git"]`), /✓.*git.*Installed/);
  assert.match(await text(`${worker('pc-a')} [data-command="cargo"]`), /✗.*cargo.*Absent/);
  assert.match(await text(`${worker('pc-a')} .fl-refreshed`), /Last refreshed \d{4}-\d{2}-\d{2} \d{2}:\d{2}/);
  // Glyphs are decorative: the words carry the state.
  assert.equal(await evaluate(`[...document.querySelectorAll('${worker('pc-a')} .fl-glyph')].every(n => n.getAttribute('aria-hidden') === 'true')`), true);
  const b = await text(worker('pc-b'));
  assert.match(b, /Offline/); assert.match(b, /Last known results.*not connected/);
  assert.match(b, /Warning: slots 500 in the registry is above the maximum of 100/);
  const c = await text(worker('pc-c'));
  assert.match(c, /Awaiting connection/);
  assert.match(c, /CPU 90%/); assert.match(c, /RAM 90%/); assert.match(c, /0 of 1 slot in use|1 slot/);
  assert.match(c, /Limits apply when the PC connects/);
  assert.equal(await evaluate(`${q(`${worker('pc-c')} [data-action="refresh"]`)}.disabled`), true, 'refresh needs a connected PC');
  await noSecrets();
});

test('live CPU and RAM: text values, labels, limits, unknown and stale, without announcing every tick', async () => {
  await openFleet();
  const cpu = `${worker('pc-a')} [data-metric="cpu"]`, ram = `${worker('pc-a')} [data-metric="ram"]`;
  assert.match(await text(cpu), /CPU 42% 10-second average · limit 90%/);
  assert.match(await text(ram), /RAM 63%/);
  assert.equal(await evaluate(`${q(cpu)}.getAttribute('aria-label')`), 'CPU 42%, limit 90%');
  assert.equal(await evaluate(`${q(`${cpu} .fl-bar`)}.getAttribute('aria-hidden')`), 'true');
  assert.match(await text(worker('pc-a')), /RAM is physical memory in use \(total minus available\)/);
  assert.match(await text(`${worker('pc-c')} [data-metric="cpu"]`), /unknown.*PC not connected/);
  const live = () => text('.fl-live');
  metrics['pc-a'] = { ...metrics['pc-a'], cpu_percent: 95.2 };
  await waitFor(`/95%/.test(${q(cpu)}.textContent)`, 'live update');
  assert.match(await text(cpu), /at or above the 90% limit: no new work/);
  assert.equal(await evaluate(`${q(cpu)}.classList.contains('fl-over')`), true);
  assert.doesNotMatch(await live(), /%/, 'metric ticks are not announced');
  metrics['pc-a'] = { ...metrics['pc-a'], state: 'unknown', reason: 'stale', cpu_percent: null, ram_percent: null, sample_age_seconds: 12.3 };
  await waitFor(`/stale: last sample 12 s ago/.test(${q(cpu)}.textContent)`, 'stale sample');
  assert.match(await text(cpu), /CPU unknown/);
  metrics['pc-a'] = { ...metrics['pc-a'], reason: 'incomplete_window', sample_age_seconds: 0.2 };
  await waitFor(`/collecting a full 10-second window/.test(${q(ram)}.textContent)`, 'incomplete window');
  metrics['pc-a'] = { ...metrics['pc-a'], state: 'known', reason: null, cpu_percent: 12, ram_percent: 30 };
  await waitFor(`/12%/.test(${q(cpu)}.textContent)`);
  fault.metrics = true;  // the board cannot reach the flow host: values stay, marked stale
  await waitFor(`!document.querySelector('.fl-stale').hidden`, 'stale banner');
  assert.match(await text('.fl-stale'), /not updated since \d{2}:\d{2}:\d{2}/);
  assert.match(await text(cpu), /12%.*not updated since/);
  fault.metrics = false;
  await waitFor(`document.querySelector('.fl-stale').hidden`, 'fresh again');
  workers.get('pc-a').online = false; workers.get('pc-a').status = 'unreachable';
  await waitFor(`/pc-a is now offline/.test(document.querySelector('.fl-live').textContent)`, 'a state change is announced');
  assert.match(await text(`${worker('pc-a')} .fl-state`), /Offline/);
  workers.get('pc-a').online = true; workers.get('pc-a').status = 'online';
  await waitFor(`/pc-a is now online/.test(document.querySelector('.fl-live').textContent)`);
  assert.equal(await evaluate(`document.querySelector('.fl-live').getAttribute('aria-live')`), 'polite');
});

test('register an offline PC: engine and OS instructions, several engines and pools, tag autocomplete and inline creation', async () => {
  await openFleet();
  await click('.fl-top button', 'Register by URL (advanced)');
  await waitFor(`!document.querySelector('.fl-form').hidden`, 'registration form');
  const steps = () => text('.fl-instructions');
  assert.equal(await evaluate(`${q('.fl-form input[name="engine"][value="hatchet"]')}.checked`), true, 'Hatchet is preselected');
  assert.equal(await evaluate(`${q('.fl-form input[name="os"][value="windows"]')}.checked`), true);
  assert.match(await steps(), /\$env:FLOW_WORKER_ID/);
  assert.match(await steps(), /flow_execution_worker\.py/);
  assert.doesNotMatch(await steps(), /trigger dev/);
  await click('.fl-form label', 'Linux');
  await waitFor(`/export FLOW_WORKER_ID/.test(document.querySelector('.fl-instructions').textContent)`, 'Linux instructions');
  assert.doesNotMatch(await steps(), /\$env:/);
  await click('.fl-form label', 'macOS');
  await waitFor(`/macOS/.test(document.querySelector('.fl-instructions').textContent)`);
  assert.match(await steps(), /CPU\/RAM/);
  await click('.fl-form label', 'Windows');
  await click('.fl-form label', 'Trigger.dev');
  await click('.fl-form label', 'Prefect');
  await waitFor(`/trigger dev/.test(document.querySelector('.fl-instructions').textContent)`, 'Trigger.dev notes');
  assert.match(await steps(), /Prefect/);
  await type('.fl-form input[name="id"]', 'lab-pc');
  await waitFor(`/FLOW_WORKER_ID = 'lab-pc'/.test(document.querySelector('.fl-instructions').textContent)`, 'instructions use the worker ID');
  assert.equal(await evaluate(`${q('.fl-form input[name="token_env"]')}.value`), 'FLOW_WORKER_TOKEN_LAB_PC');
  assert.match(await steps(), /FLOW_WORKER_TOKEN_LAB_PC=<generated token>/);
  // Limits default to 90/90 and one slot.
  assert.deepEqual(await evaluate(`['slots','cpu_limit','ram_limit'].map(n => document.querySelector('.fl-form input[name="' + n + '"]').value)`), ['1', '90', '90']);
  assert.equal(await evaluate(`${q('.fl-form input[name="slots"]')}.max`), '100', 'the slot maximum matches the registry and the gate');
  // Pools: several, including a new one.
  assert.deepEqual(await evaluate(`[...document.querySelectorAll('.fl-form .fl-pools input:checked')].map(n => n.value)`), ['default'], 'a new PC starts in the default pool');
  await click('.fl-form .fl-pools label', 'build');
  await click('.fl-form .fl-pools label', 'default');
  await type('.fl-form input[name="new_pool"]', 'lab');
  await click('.fl-form button', 'Add pool');
  assert.deepEqual(await evaluate(`[...document.querySelectorAll('.fl-form .fl-pools input:checked')].map(n => n.value)`), ['build', 'lab']);
  // Tags: autocomplete from existing tags, then create one inline.
  await type('.fl-form input[name="tag"]', 'cu');
  await waitFor(`document.querySelector('.fl-tag-input').getAttribute('aria-expanded') === 'true'`, 'suggestions open');
  assert.deepEqual(await evaluate(`[...document.querySelectorAll('.fl-tag-options [role="option"]')].map(n => n.textContent)`), ['cuda', 'Create tag “cu”']);
  await press('ArrowDown');
  assert.equal(await evaluate(`document.querySelector('.fl-tag-input').getAttribute('aria-activedescendant') === document.querySelector('.fl-tag-options [role="option"]').id`), true);
  await press('Enter');
  await waitFor(`[...document.querySelectorAll('.fl-chosen-tags .fl-chip')].some(n => n.textContent.includes('cuda'))`, 'cuda chosen');
  await type('.fl-form input[name="tag"]', 'fpga');
  await waitFor(`[...document.querySelectorAll('.fl-tag-options [role="option"]')].some(n => n.textContent === 'Create tag “fpga”')`);
  await click('.fl-tag-options [role="option"]', 'Create tag');
  await waitFor(`[...document.querySelectorAll('.fl-chosen-tags .fl-chip')].some(n => n.textContent.includes('fpga'))`, 'fpga created');
  assert.deepEqual(lastCall('POST', '/api/fleet/tags').body, { name: 'fpga' });
  // A chosen tag is not offered again; once removed, the created tag is suggested like any other.
  await type('.fl-form input[name="tag"]', 'fp');
  await waitFor(`document.querySelector('.fl-tag-input').getAttribute('aria-expanded') === 'true'`);
  assert.deepEqual(await evaluate(`[...document.querySelectorAll('.fl-tag-options [role="option"]')].map(n => n.textContent)`), ['Create tag “fp”']);
  await click('.fl-chosen-tags button[aria-label="Remove tag fpga"]');
  await type('.fl-form input[name="tag"]', 'fp');
  await waitFor(`[...document.querySelectorAll('.fl-tag-options [role="option"]')].some(n => n.textContent === 'fpga')`, 'a created tag is offered next time');
  await press('ArrowDown'); await press('Enter');
  await waitFor(`[...document.querySelectorAll('.fl-chosen-tags .fl-chip')].map(n => n.firstChild.textContent).join() === 'cuda,fpga'`, 'fpga chosen again');
  await type('.fl-form input[name="tag"]', 'zz');
  await waitFor(`document.querySelector('.fl-tag-input').getAttribute('aria-expanded') === 'true'`);
  await press('Escape');  // closes the suggestions only, not the form or the tab
  assert.equal(await evaluate(`document.querySelector('.fl-tag-input').getAttribute('aria-expanded')`), 'false');
  assert.equal(await evaluate(`document.querySelector('#fleet-dialog').open && !document.querySelector('.fl-form').hidden`), true);
  await evaluate(`${q('.fl-form input[name="tag"]')}.value = ''`);
  await shot('fleet-register');
  await click('.fl-form button', 'Save');
  await waitFor(`document.querySelector('.fl-form').hidden && document.querySelector('${worker('lab-pc')}')`, 'saved and listed');
  const sent = lastCall('POST', '/api/fleet/workers').body;
  assert.deepEqual(sent, { id: 'lab-pc', engines: ['hatchet', 'prefect', 'trigger'], pools: ['build', 'lab'], tags: ['cuda', 'fpga'], slots: 1, cpu_limit: 90, ram_limit: 90, token_env: 'FLOW_WORKER_TOKEN_LAB_PC' });
  assert.match(await text('.fl-status'), /Saved lab-pc/);
  assert.match(await text('.fl-status'), /Limits apply when the PC connects/);
  assert.match(await text(worker('lab-pc')), /Awaiting connection/);
  assert.equal(await evaluate(`document.querySelectorAll('${worker('lab-pc')} [data-engine]').length`), 3);
  // A PC with an endpoint: the endpoint and token variable are write-only.
  await click('.fl-top button', 'Register by URL (advanced)');
  await type('.fl-form input[name="id"]', 'net-pc');
  await type('.fl-form input[name="url"]', 'https://net-pc.internal:8096');
  await type('.fl-form input[name="token_env"]', 'NET_PC_TOKEN');
  await click('.fl-form button', 'Save');
  await waitFor(`document.querySelector('.fl-form').hidden && document.querySelector('${worker('net-pc')}')`);
  assert.equal(lastCall('POST', '/api/fleet/workers').body.url, 'https://net-pc.internal:8096');
  assert.ok(!('token' in lastCall('POST', '/api/fleet/workers').body), 'a token value is never sent');
  await noSecrets();
});

test('the same PC connects to a second engine by editing it, never as a duplicate', async () => {
  await openFleet();
  await click('.fl-top button', 'Register by URL (advanced)');
  await type('.fl-form input[name="id"]', 'pc-a');
  await click('.fl-form label', 'Trigger.dev');
  await click('.fl-form label', 'Hatchet');  // only Trigger.dev
  await click('.fl-form button', 'Save');
  await waitFor(`/already registered/.test(document.querySelector('.fl-form-error').textContent)`, 'duplicate refused');
  await click('.fl-form-error button', 'Edit pc-a');
  await waitFor(`/Edit pc-a/.test(document.querySelector('.fl-form h3').textContent)`, 'edit form');
  assert.deepEqual(await evaluate(`[...document.querySelectorAll('.fl-form input[name="engine"]:checked')].map(n => n.value)`), ['hatchet', 'prefect', 'trigger']);
  assert.equal(await evaluate(`${q('.fl-form input[name="id"]')}.readOnly`), true);
  assert.match(await text('.fl-instructions'), /Nothing new is needed on the PC/);
  assert.match(await text('.fl-instructions'), /trigger dev/);
  assert.equal(await evaluate(`${q('.fl-form input[name="url"]')}.value`), '', 'the endpoint is never shown');
  assert.match(await text('.fl-form'), /endpoint is configured/i);
  await click('.fl-form button', 'Save');
  await waitFor(`document.querySelector('.fl-form').hidden`);
  const sent = lastCall('PUT', '/api/fleet/workers/pc-a').body;
  assert.deepEqual(sent.engines, ['hatchet', 'prefect', 'trigger']);
  assert.ok(!('url' in sent), 'a blank endpoint keeps the configured one');
  assert.equal(await evaluate(`document.querySelectorAll('${worker('pc-a')}').length`), 1);
  await waitFor(`document.querySelectorAll('${worker('pc-a')} [data-engine]').length === 3`);
  assert.match(await text(`${worker('pc-a')} [data-engine="trigger"]`), /Trigger\.dev: Unconfirmed/);
  assert.doesNotMatch(await text(`${worker('pc-a')} [data-engine="trigger"]`), /Connected/);
  await noSecrets();
});

test('settings edits: limits and slots, validated, with the sync state', async () => {
  await openFleet();
  await click(`${worker('pc-c')} button`, 'Edit settings');
  await waitFor(`/Edit pc-c/.test(document.querySelector('.fl-form h3').textContent)`);
  await type('.fl-form input[name="slots"]', '101');
  await click('.fl-form button', 'Save');
  await waitFor(`/Slots must be a whole number from 1 to 100/.test(document.querySelector('.fl-form-error').textContent)`, 'slot maximum');
  await type('.fl-form input[name="slots"]', '1');
  await type('.fl-form input[name="cpu_limit"]', '0');
  await click('.fl-form button', 'Save');
  await waitFor(`/CPU limit/.test(document.querySelector('.fl-form-error').textContent)`, 'client validation');
  const before = calls.length;
  await type('.fl-form input[name="cpu_limit"]', '75');
  await type('.fl-form input[name="ram_limit"]', '80');
  await type('.fl-form input[name="slots"]', '3');
  await click('.fl-form button', 'Save');
  await waitFor(`document.querySelector('.fl-form').hidden`);
  assert.ok(calls.slice(before).filter((c) => c.method === 'PUT').length === 1);
  const sent = lastCall('PUT', '/api/fleet/workers/pc-c').body;
  assert.deepEqual([sent.cpu_limit, sent.ram_limit, sent.slots], [75, 80, 3]);
  await waitFor(`/Saved pc-c.*Limits apply when the PC connects/.test(document.querySelector('.fl-status').textContent)`, 'pending sync shown');
  await waitFor(`/CPU 75%/.test(document.querySelector('${worker('pc-c')}').textContent)`);
  assert.match(await text(worker('pc-c')), /RAM 80%/);
  await click(`${worker('pc-a')} button`, 'Edit settings');
  await type('.fl-form input[name="cpu_limit"]', '60');
  await click('.fl-form button', 'Save');
  await waitFor(`document.querySelector('.fl-form').hidden`);
  await waitFor(`/applied on pc-a for new work; running work continues/.test(document.querySelector('.fl-status').textContent)`, 'sync state shown');
  await waitFor(`/limit 60%/.test(document.querySelector('${worker('pc-a')} [data-metric="cpu"]').textContent)`);
});

test('the list filters by words and fields; clicking a pool or tag filters by it', async () => {
  await openFleet();
  const shown = () => evaluate(`[...document.querySelectorAll('.fl-worker')].map((n) => n.dataset.worker)`);
  const filter = (query) => evaluate(`(() => { const n = document.querySelector('.fl-filter-input'); n.value = ${JSON.stringify(query)}; n.dispatchEvent(new Event('input')); })()`);
  const all = await shown();
  try {
    await filter('tag:cuda');
    assert.ok((await shown()).includes('pc-a'));
    assert.ok(!(await shown()).includes('pc-b'), 'pc-b has no such tag');
    assert.match(await text('.fl-filter-count'), /\d+ of \d+ PCs/);
    await click('.fl-filter button', 'Clear');
    assert.deepEqual(await shown(), all);
    await click(`${worker('pc-b')} button.fl-chip`, 'build');
    assert.equal(await evaluate(`document.querySelector('.fl-filter-input').value`), 'pool:build');
    assert.ok((await shown()).includes('pc-b') && !(await shown()).includes('pc-c'));
    await filter('nothing-like-this');
    assert.match(await text('.fl-list'), /No PCs match/);
  } finally { await filter(''); }
  assert.deepEqual(await shown(), all);
});

test('a PC shows what it runs now and has run; a run the board can\'t place can\'t be opened', async () => {
  await openFleet();
  await click(`${worker('pc-a')} [data-action="jobs"]`);
  await waitFor(`document.querySelector('[data-job="run-live"]')`, 'its jobs');
  assert.match(await text('[data-jobs="current"]'), /Live job/);
  assert.doesNotMatch(await text('[data-jobs="current"]'), /Old job/);
  assert.match(await text('[data-jobs="past"]'), /Old job.*build.*done/);
  assert.match(await text('[data-jobs="current"]'), /Run run-gone/);
  assert.equal(await evaluate(`document.querySelector('[data-job="run-gone"]').disabled`), true);
  await click('.fl-back');
  await waitFor(`document.querySelector('.fl-list').children.length > 0 && !document.querySelector('.fl-worker-detail')`, 'back to the list');
});

test('manual capability refresh updates results and timestamps; offline PCs explain why they cannot', async () => {
  await openFleet();
  const stamp = await text(`${worker('pc-a')} .fl-refreshed`);
  workers.get('pc-a').inventory.refreshed_at = iso(5);
  await click(`${worker('pc-a')} [data-action="refresh"]`);
  await waitFor(`/Installed/.test(document.querySelector('${worker('pc-a')} [data-agent="claude"]').textContent)`, 'refreshed results');
  assert.equal(lastCall('POST', '/api/fleet/workers/pc-a/refresh').method, 'POST');
  assert.notEqual(await text(`${worker('pc-a')} .fl-refreshed`), stamp);
  await waitFor(`/Capabilities of pc-a refreshed/.test(document.querySelector('.fl-live').textContent)`, 'the announcement');
  assert.match(await text(worker('pc-b')), /checked when it connects/);
  assert.equal(await evaluate(`${q(`${worker('pc-b')} [data-action="refresh"]`)}.disabled`), true);
});

test('unregister requires a named confirmation and removes the PC on success', async () => {
  await openFleet();
  await evaluate(`document.querySelector('${worker('pc-b')}').dispatchEvent(new MouseEvent('contextmenu', { bubbles: true, cancelable: true, button: 2 }))`);
  await waitFor('document.querySelector(".ctx-menu")', 'worker context menu opens');
  assert.match(await text('.ctx-menu'), /Unregister from Fleet/);
  await press('Escape');
  await click(`${worker('pc-b')} [data-action="unregister"]`);
  await waitFor('document.querySelector(".fl-unregister").open', 'unregister confirmation opens');
  assert.match(await text('.fl-unregister'), /Unregister pc-b\?.*removes pc-b from Fleet.*frees its name for a new worker.*Already-running work is not canceled synchronously/);
  assert.equal(calls.filter((c) => c.method === 'DELETE' && c.path === '/api/fleet/workers/pc-b').length, 0, 'opening confirmation does not unregister');
  await click('.fl-unregister button', 'Cancel');
  await waitFor('!document.querySelector(".fl-unregister").open', 'cancel closes confirmation');
  assert.equal(calls.filter((c) => c.method === 'DELETE' && c.path === '/api/fleet/workers/pc-b').length, 0, 'cancel leaves the PC registered');
  await click(`${worker('pc-b')} [data-action="unregister"]`);
  const box = await evaluate('document.querySelector(".fl-unregister").getBoundingClientRect().toJSON()');
  await mouse('mouseMoved', { x: Math.max(1, box.x - 20), y: box.y + box.height / 2 }, 0);
  await mouse('mousePressed', { x: Math.max(1, box.x - 20), y: box.y + box.height / 2 });
  await mouse('mouseReleased', { x: Math.max(1, box.x - 20), y: box.y + box.height / 2 }, 0);
  await waitFor('!document.querySelector(".fl-unregister").open', 'backdrop closes confirmation');
  assert.equal(calls.filter((c) => c.method === 'DELETE' && c.path === '/api/fleet/workers/pc-b').length, 0, 'backdrop cancels unregister');

  await click(`${worker('pc-b')} [data-action="unregister"]`);
  await click('.fl-unregister button', 'Unregister pc-b');
  await waitFor(`!document.querySelector('${worker('pc-b')}')`, 'removed PC leaves the list');
  assert.equal(lastCall('DELETE', '/api/fleet/workers/pc-b').method, 'DELETE');
  await waitFor(`/pc-b was removed from Fleet/.test(document.querySelector('.fl-live').textContent)`, 'success announcement');
  assert.equal(workers.has('pc-b'), false);
});

test('a registered disconnected install can be unregistered from its waiting card', async () => {
  reset();
  const code = '00000000-0000-4000-8000-000000000099';
  installs.set(code, { code, id: 'stuck-pc', os: 'windows', state: 'registered', created_at: Date.now() / 1000,
    expires_at: Date.now() / 1000 + 1800, step: 'register', error: null, hostname: null, connected: false });
  const connectedCode = '00000000-0000-4000-8000-000000000100';
  installs.set(connectedCode, { code: connectedCode, id: 'live-pc', os: 'windows', state: 'registered', connected: true, hostname: 'LIVE' });
  workers.set('stuck-pc', { id: 'stuck-pc', pools: ['default'], tags: [], engines: ['hatchet'], slots: 1, cpu_limit: 90, ram_limit: 90,
    connection: 'configured', online: false, status: 'unreachable', settings_sync: { state: 'pending' } });
  await openFleet();
  const card = '.fl-install-card[data-install="stuck-pc"]';
  await waitFor(`${q(card)}?.textContent.includes('Waiting for the worker to connect')`, 'registered worker status loads');
  assert.match(await text(card), /Registered\. Waiting for the worker to connect/);
  assert.equal(await evaluate(`!!${q(`${card} [data-action="unregister-install"]`)}`), true, 'disconnected registered install has an action');
  assert.equal(await evaluate(`!!${q('.fl-install-card[data-install="live-pc"] [data-action="unregister-install"]')}`), false, 'connected records do not get an action');
  await click(`${card} [data-action="unregister-install"]`);
  await waitFor('document.querySelector(".fl-unregister").open', 'confirmation opens');
  assert.match(await text('.fl-unregister'), /Unregister stuck-pc\?/);
  assert.equal(calls.filter((c) => c.method === 'DELETE' && c.path === '/api/fleet/workers/stuck-pc').length, 0);
  await click('.fl-unregister button', 'Unregister stuck-pc');
  await waitFor(`!${q(card)}`, 'stuck install disappears after successful unregister');
  assert.equal(lastCall('DELETE', '/api/fleet/workers/stuck-pc').path, '/api/fleet/workers/stuck-pc');
  assert.ok(calls.some((c) => c.method === 'GET' && c.path === '/api/fleet'));
  assert.ok(calls.some((c) => c.method === 'GET' && c.path === '/api/fleet/installs'));
  const failedCode = '00000000-0000-4000-8000-000000000101';
  installs.set(failedCode, { code: failedCode, id: 'stuck-fail', os: 'windows', state: 'registered', connected: false });
  workers.set('stuck-fail', { id: 'stuck-fail', pools: ['default'], tags: [], engines: ['hatchet'], slots: 1, cpu_limit: 90, ram_limit: 90,
    connection: 'configured', online: false, status: 'unreachable', settings_sync: { state: 'pending' } });
  unregisterFault = { status: 503, error: 'Flow host unavailable' };
  await click('.fl-top button', 'Reload');
  const failedCard = '.fl-install-card[data-install="stuck-fail"]';
  await waitFor(`!!${q(failedCard)}`, 'failed install is listed');
  await click(`${failedCard} [data-action="unregister-install"]`);
  await click('.fl-unregister button', 'Unregister stuck-fail');
  await waitFor(`/Could not unregister stuck-fail: Flow host unavailable/.test(document.querySelector('.fl-status').textContent)`);
  assert.ok(await evaluate(`!!${q(failedCard)}`), 'failed request preserves the install card');
  await evaluate('document.querySelector(\'[aria-label="Close fleet"]\').click()');
  await waitFor('!document.querySelector("#fleet-dialog").open', 'Fleet closes after unregister test');
  reset();
});
test('unregister handles a stale worker by refreshing and preserves the card on other errors', async () => {
  await openFleet();
  workers.delete('pc-c');
  unregisterFault = { status: 404, error: 'Unknown worker' };
  await click(`${worker('pc-c')} [data-action="unregister"]`);
  await click('.fl-unregister button', 'Unregister pc-c');
  await waitFor(`!document.querySelector('${worker('pc-c')}')`, 'stale worker is refreshed out of the list');
  await waitFor(`/already unregistered from Fleet/.test(document.querySelector('.fl-status').textContent)`);

  unregisterFault = { status: 503, error: 'Flow host unavailable' };
  await click(`${worker('pc-a')} [data-action="unregister"]`);
  await click('.fl-unregister button', 'Unregister pc-a');
  await waitFor(`/Could not unregister pc-a: Flow host unavailable/.test(document.querySelector('.fl-status').textContent)`);
  assert.ok(await evaluate(`!!document.querySelector('${worker('pc-a')}')`), 'failed request keeps the worker card');
});

test('closing the fleet tab stops polling', async () => {
  await openFleet();
  await press('Escape');
  await waitFor('!document.querySelector("#fleet-dialog").open', 'Escape closes');
  await sleep(400);
  const count = calls.filter((c) => c.path === '/api/fleet/metrics').length;
  await sleep(800);
  assert.equal(calls.filter((c) => c.path === '/api/fleet/metrics').length, count);
});

test('phone width: the list and the registration form never scroll sideways', async () => {
  await call('Emulation.setDeviceMetricsOverride', { width: 375, height: 812, deviceScaleFactor: 2, mobile: true });
  try {
    await openFleet();
    const sideways = () => evaluate(`(() => { const b = document.querySelector('.fl-body'); return b.scrollWidth - b.clientWidth; })()`);
    assert.ok(await sideways() <= 1, 'worker list fits');
    await click('.fl-top button', 'Register by URL (advanced)');
    await waitFor(`!document.querySelector('.fl-form').hidden`);
    assert.ok(await sideways() <= 1, 'form fits (code blocks scroll inside themselves)');
    await shot('fleet-phone');
    await click('.fl-form button', 'Cancel');
  } finally {
    await call('Emulation.setDeviceMetricsOverride', { width: 1440, height: 1000, deviceScaleFactor: 1, mobile: false });
  }
});

// ---------------------------------------------------------------- one-line install: pure helpers
test('install form validation mirrors the server and builds the request only when valid', () => {
  const valid = { id: 'build-pc', os: 'linux', engines: ['prefect', 'hatchet'], pool: 'build', tags: ['gpu', 'gpu', ' cuda '], slots: '2', cpu_limit: '80', ram_limit: '85.5', server: 'http://192.168.1.10:8090/' };
  const ok = Fleet.validateInstall(valid, { taken: ['pc-a'] });
  assert.deepEqual(ok.errors, {});
  assert.deepEqual(ok.body, { id: 'build-pc', os: 'linux', engines: ['hatchet', 'prefect'], pool: 'build', tags: ['gpu', 'cuda'], slots: 2, cpu_limit: 80, ram_limit: 85.5, server: 'http://192.168.1.10:8090', autostart: 'logon', auto_restart: true });
  const bad = (change, field) => { const r = Fleet.validateInstall({ ...valid, ...change }, { taken: ['pc-a'] }); assert.equal(r.body, null, field); assert.ok(r.errors[field], `${field} has a message`); };
  for (const id of ['', ' ', 'a b', '-x', 'x'.repeat(65), 'a/b', 'pc-a']) bad({ id }, 'id');
  bad({ os: '' }, 'os'); bad({ os: 'beos' }, 'os');
  bad({ engines: [] }, 'engines');
  for (const pool of ['', 'a,b', ' ']) bad({ pool }, 'pool');
  bad({ tags: ['a,b'] }, 'tags');
  for (const slots of ['0', '101', '1.5', '', 'x']) bad({ slots }, 'slots');
  for (const cpu_limit of ['0', '101', '', 'x', '-5']) bad({ cpu_limit }, 'cpu_limit');
  bad({ ram_limit: '0' }, 'ram_limit');
  for (const autostart of ['startup', 'BOOT', 7]) bad({ autostart }, 'autostart');
  for (const auto_restart of ['yes', 0, null]) bad({ auto_restart }, 'auto_restart');
  assert.equal(Fleet.validateInstall({ ...valid, auto_restart: false }).body.auto_restart, false);
  assert.equal(Fleet.validateInstall({ ...valid, autostart: 'boot' }).body.autostart, 'boot');
  assert.equal(Fleet.validateInstall({ ...valid, autostart: 'none' }).body.autostart, 'none');
  for (const server of ['', 'localhost:8090', 'http://localhost:8090', 'http://127.0.0.1:8090', 'http://[::1]:8090', 'http://0.0.0.0:8090', 'ftp://h:1', 'http://u:p@h:1', 'http://h:1/path', 'http://h:1?x=1', 'nope']) bad({ server }, 'server');
  assert.equal(Fleet.serverAddress('https://board.example.ts.net/').value, 'https://board.example.ts.net');
});

test('the start-up choices explain their consequences for the chosen operating system', () => {
  const windows = Fleet.autostartChoices('windows'), linux = Fleet.autostartChoices('linux');
  assert.deepEqual(windows.map((c) => c.value), ['logon', 'boot', 'none']);
  const by = (list, value) => list.find((c) => c.value === value).text;
  assert.match(by(windows, 'logon'), /not running until someone logs in/);
  assert.match(by(windows, 'logon'), /offline in the fleet/);
  assert.match(by(windows, 'boot'), /UAC prompt/);
  assert.match(by(windows, 'boot'), /without the user.s credentials/);
  assert.match(by(windows, 'boot'), /can fail until the user logs in/);
  assert.match(by(linux, 'boot'), /lingering/);
  assert.doesNotMatch(by(linux, 'boot'), /UAC/);
  assert.match(by(windows, 'none'), /agent-worker start/);
});

test('the auto restart text names the cost and the way back for each system and start-up choice', () => {
  assert.match(Fleet.autoRestartText('windows', 'logon'), /checks every minute/);
  assert.match(Fleet.autoRestartText('windows', 'logon'), /next logon or with agent-worker start/);
  assert.match(Fleet.autoRestartText('linux', 'boot'), /systemd starts it again/);
  assert.match(Fleet.autoRestartText('linux', 'boot'), /next PC start-up/);
  assert.match(Fleet.autoRestartText('windows', 'logon'), /agent-worker autorestart on\|off/);
  assert.match(Fleet.autoRestartText('windows', 'none'), /Not used here/);
});

test('install commands: Windows gets PowerShell and cmd, others get bash, and nobody is told to elevate', () => {
  const commands = { powershell: "$f = ...; & $f install c --server 'http://s'", cmd: 'curl.exe ... && install c --server "http://s"', bash: "f=$(mktemp) && ... install c --server 'http://s'" };
  assert.deepEqual(Fleet.installCommands('windows', commands).map((c) => c.id), ['powershell', 'cmd']);
  assert.deepEqual(Fleet.installCommands('linux', commands).map((c) => c.id), ['bash']);
  assert.match(Fleet.installCommands('macos', commands)[0].label, /macOS/);
  for (const os of ['windows', 'linux', 'macos']) for (const c of Fleet.installCommands(os, commands)) assert.match(c.hint, /normal user|normal Command Prompt/, `${os}: the hint names a normal user`);
});

test('install progress words: waiting, each step, failure with its step, registered, connected, expired, cancelled', () => {
  const now = 1_000_000_000_000;
  const record = (extra) => ({ state: 'pending', expires_at: now / 1000 + 600, step: null, error: null, connected: false, ...extra });
  assert.match(Fleet.installProgress(record(), now).text, /Waiting for you to run the command/);
  assert.equal(Fleet.installProgress(record({ step: 'python' }), now).text, 'Installing Python packages…');
  const failed = Fleet.installProgress(record({ step: 'npm', error: 'npm could not reach the registry' }), now);
  assert.deepEqual([failed.level, /while installing the agent adapters: npm could not reach/.test(failed.text)], ['error', true]);
  assert.match(Fleet.installProgress(record({ state: 'registered' }), now).text, /Waiting for the worker to connect/);
  assert.deepEqual(Fleet.installProgress(record({ state: 'registered', connected: true, hostname: 'PC-1' }), now), { text: 'Connected from PC-1.', level: 'ok' });
  assert.match(Fleet.installProgress(record({ expires_at: now / 1000 - 1 }), now).text, /expired/);
  assert.match(Fleet.installProgress(record({ state: 'cancelled' }), now).text, /Cancelled/);
  assert.match(Fleet.installProgress(null, now).text, /no longer listed/);
  assert.equal(Fleet.minutesLeft(now / 1000 + 61, now), 2);
  assert.equal(Fleet.minutesLeft(now / 1000 - 5, now), 0);
});

// ---------------------------------------------------------------- one-line install: in the browser
const fillInstall = async ({ id = 'build-pc', os = 'windows', server = 'http://192.168.1.10:8090' } = {}) => {
  await type('.fl-form input[name="id"]', id);
  await click(`.fl-form input[name="os"][value="${os}"]`);
  await type('.fl-form input[name="server"]', server);
};
const submitInstall = () => click('.fl-form button', 'Create install command');

test('register a PC: the form is checked first, and no install command exists until it is valid', async () => {
  await openFleet();
  await click('.fl-top button', 'Register PC');
  await waitFor('!document.querySelector(".fl-form").hidden && document.querySelector(".fl-form input[name=id]")');
  assert.equal(await evaluate('document.activeElement.name'), 'id', 'focus starts on the name');
  const before = calls.length;
  await evaluate('document.querySelector(".fl-form input[name=server]").value = "http://localhost:8090"');
  await submitInstall();
  await waitFor('document.querySelector(\'[data-error="id"]\').textContent.length > 0', 'name error shown');
  assert.match(await text('[data-error="server"]'), /cannot reach this server at localhost/);
  assert.match(await text('.fl-form-error'), /Fix these first/);
  assert.equal(await evaluate('document.querySelector(".fl-form input[name=id]").getAttribute("aria-invalid")'), 'true');
  assert.equal(calls.slice(before).filter((c) => c.method === 'POST').length, 0, 'nothing is sent while the form is invalid');
  assert.equal(await evaluate('!!document.querySelector(".fl-command")'), false, 'no command is shown');
  await type('.fl-form input[name="id"]', 'pc-a');
  await type('.fl-form input[name="server"]', 'http://192.168.1.10:8090');
  await submitInstall();
  assert.match(await text('[data-error="id"]'), /already registered/);
  assert.equal(calls.slice(before).filter((c) => c.method === 'POST').length, 0);
  await type('.fl-form input[name="id"]', 'build-pc');
  await type('.fl-form input[name="slots"]', '0');
  await submitInstall();
  assert.match(await text('[data-error="slots"]'), /whole number from 1 to 100/);
  assert.equal(calls.slice(before).filter((c) => c.method === 'POST').length, 0);
  await shot('install-form-errors');
});

test('a valid form creates the install link and shows the Windows commands with the elevation advice', async () => {
  await openFleet();
  await click('.fl-top button', 'Register PC');
  await waitFor('document.querySelector(".fl-form input[name=id]")');
  await fillInstall({ id: 'build-pc', os: 'windows' });
  await evaluate('(() => { const pool = document.querySelector(".fl-form select[name=pool]"); pool.value = "default"; pool.dispatchEvent(new Event("change", { bubbles: true })); })()');
  await click('.fl-form input[name="engine"][value="prefect"]');
  await type('.fl-form input[name="slots"]', '2');
  await type('.fl-form input[name="cpu_limit"]', '80');
  await evaluate('(() => { const i = document.querySelector(".fl-form input[name=tag]"); i.focus(); })()');
  await call('Input.insertText', { text: 'gpu' }); await press('Enter');
  await submitInstall();
  await waitFor('document.querySelectorAll(".fl-command").length === 2', 'both Windows commands shown');
  const sent = lastCall('POST', '/api/fleet/installs').body;
  assert.deepEqual(sent, { id: 'build-pc', os: 'windows', engines: ['hatchet', 'prefect'], pool: 'default', tags: ['gpu'], slots: 2, cpu_limit: 80, ram_limit: 90, server: 'http://192.168.1.10:8090', autostart: 'logon', auto_restart: true });
  const record = [...installs.values()][0];
  assert.equal(await text('[data-command="powershell"]'), `$f = Join-Path $env:TEMP 'agent-worker.exe'; iwr -UseBasicParsing 'http://192.168.1.10:8090/install/${record.code}/agent-worker' -OutFile $f; & $f install ${record.code} --server 'http://192.168.1.10:8090'`);
  assert.match(await text('[data-command="cmd"]'), /^curl\.exe -fsSL "http:\/\/192\.168\.1\.10:8090\/install\/.+\/agent-worker" -o/);
  assert.equal(await evaluate('document.querySelectorAll(".fl-command").length'), 2);
  assert.match(await text('.fl-elevation'), /Run as a normal user\. Do not use Administrator or sudo/);
  assert.match(await text('.fl-install-result'), /downloads the worker from this server/);
  await waitFor('/works once/.test(document.querySelector(".fl-install-expiry").textContent)', 'expiry note drawn by the first status check');
  assert.match(await text('.fl-install-expiry'), /works once and expires in about (29|30) min/);
  assert.match(await text('.fl-install-status'), /Waiting for you to run the command/);
  assert.equal(await evaluate('document.querySelector(\'[data-action="install-done"]\').hidden'), true);
  await click('[data-action="copy-powershell"]');
  await waitFor('/Copied|Select the command/.test(document.querySelector(\'[data-action="copy-powershell"]\').textContent)', 'copy gives feedback');
  await waitFor(`/command copied|Copy failed/.test(document.querySelector('.fl-live').textContent)`, 'the announcement');
  await shot('install-command-windows');
});

test('Linux and macOS get one bash command, no cmd variant', async () => {
  for (const [os, label] of [['linux', /Linux terminal/], ['macos', /macOS Terminal/]]) {
    reset();
    await openFleet();
    await click('.fl-top button', 'Register PC');
    await waitFor('document.querySelector(".fl-form input[name=id]")');
    await fillInstall({ id: `pc-${os}`, os });
    await submitInstall();
    await waitFor('document.querySelectorAll(".fl-command").length === 1', `${os} shows one command`);
    assert.match(await text('.fl-command-block h4'), label);
    assert.match(await text('[data-command="bash"]'), /^f=\$\(mktemp\) && curl -fsSL 'http:\/\/192\.168\.1\.10:8090\/install\/.+\/agent-worker' -o "\$f" && chmod \+x "\$f" && "\$f" install .+ --server 'http:\/\/192\.168\.1\.10:8090'$/);
    assert.match(await text('.fl-command-block .fl-hint'), /no sudo/);
    await click('.fl-form-actions button', 'Cancel this install');
    await waitFor('document.querySelector(".fl-form").hidden', 'form closes after cancelling');
  }
});

test('the form offers logon, boot and no autostart with their consequences, defaults to logon and sends the choice', async () => {
  await openFleet();
  await click('.fl-top button', 'Register PC');
  await waitFor('document.querySelector(".fl-form input[name=id]")');
  assert.equal(await evaluate('document.querySelectorAll(".fl-form input[name=autostart]").length'), 3);
  assert.equal(await evaluate('document.querySelector(".fl-form input[name=autostart]:checked").value'), 'logon');
  assert.match(await text('.fl-autostart-choice'), /not running until someone logs in/);
  assert.match(await text('.fl-autostart-choice'), /UAC prompt/);
  await click('.fl-form input[name="os"][value="linux"]');
  assert.match(await text('.fl-autostart-choice'), /lingering/);
  assert.doesNotMatch(await text('.fl-autostart-choice'), /UAC prompt/);
  await click('.fl-form input[name="os"][value="windows"]');
  await fillInstall({ id: 'boot-pc', os: 'windows' });
  await click('.fl-form input[name="autostart"][value="boot"]');
  await submitInstall();
  await waitFor('document.querySelectorAll(".fl-command").length === 2', 'commands shown');
  assert.equal(lastCall('POST', '/api/fleet/installs').body.autostart, 'boot');
  assert.match(await text('.fl-boot-note'), /UAC prompt/);
  assert.match(await text('.fl-boot-note'), /starts at logon instead/);
  await click('[data-action="cancel-install"]');
  await openFleet();
  await click('.fl-top button', 'Register PC');
  await waitFor('document.querySelector(".fl-form input[name=id]")');
  await fillInstall({ id: 'manual-pc', os: 'linux' });
  await click('.fl-form input[name="autostart"][value="none"]');
  await submitInstall();
  await waitFor('document.querySelectorAll(".fl-command").length === 1', 'bash command shown');
  assert.equal(lastCall('POST', '/api/fleet/installs').body.autostart, 'none');
  assert.equal(await evaluate('!!document.querySelector(".fl-boot-note")'), false);
  await click('[data-action="cancel-install"]');
});

test('the form lets auto restart be turned off, explains it and ignores it when nothing starts the worker', async () => {
  await openFleet();
  await click('.fl-top button', 'Register PC');
  await waitFor('document.querySelector(".fl-form input[name=id]")');
  assert.equal(await evaluate('document.querySelector(".fl-form input[name=auto_restart]").checked'), true);
  assert.match(await text('.fl-auto-restart'), /Restart automatically if the worker crashes or is killed/);
  assert.match(await text('.fl-auto-restart'), /checks every minute/);
  await click('.fl-form input[name="os"][value="linux"]');
  assert.match(await text('.fl-auto-restart'), /systemd starts it again/);
  await click('.fl-form input[name="os"][value="windows"]');
  await fillInstall({ id: 'norestart-pc', os: 'windows' });
  await click('.fl-form input[name="auto_restart"]');
  await submitInstall();
  await waitFor('document.querySelectorAll(".fl-command").length === 2', 'commands shown');
  assert.equal(lastCall('POST', '/api/fleet/installs').body.auto_restart, false);
  await click('[data-action="cancel-install"]');
  await openFleet();
  await click('.fl-top button', 'Register PC');
  await waitFor('document.querySelector(".fl-form input[name=id]")');
  await fillInstall({ id: 'plain-pc', os: 'windows' });
  await click('.fl-form input[name="autostart"][value="none"]');
  assert.equal(await evaluate('document.querySelector(".fl-form input[name=auto_restart]").disabled'), true);
  assert.match(await text('.fl-auto-restart'), /Not used here/);
  await submitInstall();
  await waitFor('document.querySelectorAll(".fl-command").length === 2', 'commands shown');
  assert.equal(lastCall('POST', '/api/fleet/installs').body.auto_restart, true, 'no startup entry: the setting stays at its default');
  await click('[data-action="cancel-install"]');
});

test('the install status follows the PC: steps, a failure with its step, registered, then connected with Done', async () => {
  reset();
  await openFleet();
  await click('.fl-top button', 'Register PC');
  await waitFor('document.querySelector(".fl-form input[name=id]")');
  await fillInstall({ id: 'live-pc', os: 'linux' });
  await submitInstall();
  await waitFor('document.querySelector(".fl-install-status")?.textContent.includes("Waiting for you")');
  const code = [...installs.keys()][0];
  const move = (set) => fetch(`${base}/__install`, { method: 'POST', body: JSON.stringify({ code, set }) });
  await move({ step: 'download' });
  await waitFor('document.querySelector(".fl-install-status").textContent === "Downloading the worker…"', 'download step shown');
  await move({ step: 'npm', error: 'npm could not install the agent adapters (is the npm registry reachable?)' });
  await waitFor('/Failed while installing the agent adapters: npm could not install/.test(document.querySelector(".fl-install-status").textContent)', 'failure shown with its step');
  assert.equal(await evaluate('document.querySelector(".fl-install-status").classList.contains("fl-error")'), true);
  await move({ step: 'register', error: null, state: 'registered' });
  await waitFor('/Registered\\. Waiting for the worker to connect/.test(document.querySelector(".fl-install-status").textContent)', 'registered shown');
  assert.equal(await evaluate('document.querySelector(\'[data-action="install-done"]\').hidden'), true);
  await move({ connected: true, hostname: 'PC-LIVE' });
  await waitFor('document.querySelector(".fl-install-status").textContent === "Connected from PC-LIVE."', 'connected shown');
  await waitFor('!document.querySelector(\'[data-action="install-done"]\').hidden', 'Done appears');
  await click('[data-action="install-done"]');
  await waitFor('document.querySelector(".fl-form").hidden && !document.querySelector(".fl-list").hidden', 'Done returns to the list');
  assert.equal(await evaluate('document.querySelector(".fl-installs").hidden'), true, 'a connected PC is no longer listed as pending');
});

test('a pending install survives closing the tab: it is listed with its command and can be cancelled', async () => {
  reset();
  await openFleet();
  await click('.fl-top button', 'Register PC');
  await waitFor('document.querySelector(".fl-form input[name=id]")');
  await fillInstall({ id: 'later-pc', os: 'windows' });
  await submitInstall();
  await waitFor('document.querySelectorAll(".fl-command").length === 2');
  await press('Escape');
  await waitFor('document.querySelector(".fl-form").hidden || !document.querySelector("#fleet-dialog").open', 'escape leaves the install view');
  if (await evaluate('document.querySelector("#fleet-dialog").open')) { await press('Escape'); }
  await waitFor('!document.querySelector("#fleet-dialog").open', 'dialog closed');
  await openFleet();
  await waitFor('!document.querySelector(".fl-installs").hidden && document.querySelector(\'[data-install="later-pc"]\')', 'pending install listed');
  assert.match(await text('[data-install="later-pc"]'), /Waiting for you to run the command/);
  await click('[data-install="later-pc"] [data-action="show-command"]');
  await waitFor('document.querySelectorAll(".fl-command").length === 2', 'the command is shown again');
  await click('.fl-form-actions button', 'Cancel this install');
  await waitFor('document.querySelector(".fl-form").hidden');
  assert.equal(lastCall('DELETE', '/api/fleet/installs/').method, 'DELETE');
  assert.equal(await evaluate('document.querySelector(".fl-installs").hidden'), true, 'a cancelled install disappears');
  await click('.fl-top button', 'Register PC');
  await waitFor('document.querySelector(".fl-form input[name=id]")');
  await fillInstall({ id: 'later-pc' });
  await submitInstall();
  await waitFor('document.querySelectorAll(".fl-command").length === 2', 'a cancelled name can be registered again');
});

test('an unregistered worker name is free and its old install is not shown as waiting', async () => {
  reset();
  const code = '00000000-0000-4000-8000-000000000099';
  installs.set(code, { code, id: 'released-pc', os: 'windows', state: 'unregistered', created_at: Date.now() / 1000,
    expires_at: Date.now() / 1000 + 1800, step: null, error: null, hostname: null, connected: false });
  await openFleet();
  assert.equal(await evaluate('document.querySelector(".fl-installs").hidden'), true, 'the old install does not look pending');
  await click('.fl-top button', 'Register PC');
  await waitFor('document.querySelector(".fl-form input[name=id]")');
  await fillInstall({ id: 'released-pc' });
  await submitInstall();
  await waitFor('document.querySelectorAll(".fl-command").length === 2', 'the released name can be used again');
});

test('the server refusing a name (already waiting) is shown on the form without losing the input', async () => {
  reset();
  await openFleet();
  await click('.fl-top button', 'Register PC');
  await waitFor('document.querySelector(".fl-form input[name=id]")');
  await fillInstall({ id: 'dup-pc' });
  await submitInstall();
  await waitFor('document.querySelectorAll(".fl-command").length === 2');
  await click('.fl-form-actions button', 'Cancel this install');   // cancel -> fine; now create a second waiting one via the API and try again
  installs.set('dupcode'.padEnd(43, 'x'), { code: 'dupcode'.padEnd(43, 'x'), id: 'dup-pc', os: 'linux', state: 'pending', created_at: 0, expires_at: Date.now() / 1000 + 900, commands: {}, step: null, error: null, connected: false });
  await click('.fl-top button', 'Register PC');
  await waitFor('document.querySelector(".fl-form input[name=id]")');
  await fillInstall({ id: 'dup-pc' });
  await submitInstall();
  await waitFor('/already has an install link waiting/.test(document.querySelector(".fl-form-error").textContent)', 'server message shown');
  assert.equal(await evaluate('document.querySelector(".fl-form input[name=id]").value'), 'dup-pc');
});

test('registering by URL stays available as the advanced path', async () => {
  reset();
  await openFleet();
  await click('.fl-top button', 'Register by URL (advanced)');
  await waitFor('document.querySelector(".fl-form input[name=token_env]")', 'the direct form opens');
  assert.match(await text('.fl-form'), /Setup instructions|Prepare the PC/);
});

test('phone width: the install form and its command never scroll sideways', async () => {
  reset();
  await call('Emulation.setDeviceMetricsOverride', { width: 360, height: 740, deviceScaleFactor: 2, mobile: true });
  try {
    await openFleet();
    await click('.fl-top button', 'Register PC');
    await waitFor('document.querySelector(".fl-form input[name=id]")');
    const wide = () => evaluate('document.documentElement.scrollWidth > innerWidth + 1 || document.querySelector(".fl").scrollWidth > document.querySelector(".fl").clientWidth + 1 || document.querySelector(".fl-body").scrollWidth > document.querySelector(".fl-body").clientWidth + 1');
    assert.equal(await wide(), false, 'the form fits');
    await fillInstall({ id: 'phone-pc', os: 'windows', server: 'http://192.168.100.200:8090' });
    await submitInstall();
    await waitFor('document.querySelectorAll(".fl-command").length === 2');
    assert.equal(await wide(), false, 'the command view fits');
    const style = await evaluate('(() => { const c = getComputedStyle(document.querySelector(".fl-command")); return [c.whiteSpace, c.overflowX, c.overflowWrap]; })()');
    assert.ok(style[0] === 'pre-wrap' || ['auto', 'scroll'].includes(style[1]), `the long command wraps or scrolls inside its box: ${style}`);
    await shot('install-command-phone');
  } finally {
    await call('Emulation.setDeviceMetricsOverride', { width: 1440, height: 1000, deviceScaleFactor: 1, mobile: false });
  }
});


// ---------------------------------------------------------------- worker updates
test('update words name each state and say when a command helps', () => {
  assert.deepEqual(Fleet.updateStatusText({ status: 'up_to_date' }), { text: 'Up to date', level: 'ok', link: false });
  assert.deepEqual(Fleet.updateStatusText({ status: 'updating' }), { text: 'Updating…', level: 'wait', link: false });
  assert.deepEqual(Fleet.updateStatusText({ status: 'waiting' }).link, true);
  assert.match(Fleet.updateStatusText({ status: 'waiting' }).text, /updates when it connects/);
  assert.equal(Fleet.updateStatusText({ status: 'failed', error: 'disk full' }).text, 'Update failed: disk full');
  assert.equal(Fleet.updateStatusText({ status: 'outdated' }).text, 'Update available');
  assert.equal(Fleet.updateStatusText({ status: 'nonsense' }).text, 'Unknown');
});

test('the worker cards show the name, the generated uuid and the worker version', async () => {
  reset();
  workers.get('pc-a').uuid = '11111111-2222-4333-8444-555555555555';
  updates.workers = [{ id: 'pc-a', version: 'aaaa1111bbbb2222', os: 'windows', online: true, status: 'up_to_date' }];
  await openFleet();
  await click('.fl-top button', 'Reload');  // the dialog may still show the previous test's data
  await waitFor(`document.querySelector('${worker('pc-a')} .fl-uuid')`, 'uuid shown');
  assert.equal(await text(`${worker('pc-a')} .fl-uuid`), '11111111-2222-4333-8444-555555555555');
  await waitFor(`/Worker version aaaa1111 · Up to date/.test(document.querySelector('${worker('pc-a')}').textContent)`, 'version line');
  assert.equal(await evaluate(`!!document.querySelector('${worker('pc-b')} .fl-uuid')`), false, 'a direct-URL PC has no generated uuid');
  await noSecrets();
});

test('the update view lists every PC with its state, updates them all and hands out a command for a PC that was down', async () => {
  reset();
  updates.auto = false;
  updates.workers = [
    { id: 'pc-a', version: 'aaaa1111bbbb2222', os: 'windows', online: true, status: 'up_to_date' },
    { id: 'pc-b', version: 'old00000old00000', os: 'linux', online: true, status: 'outdated' },
    { id: 'pc-c', version: null, os: null, online: false, status: 'waiting' },
    { id: 'pc-d', version: 'old11111old11111', os: 'windows', online: true, status: 'failed', error: 'the new worker did not start' },
  ];
  await openFleet();
  await click('.fl-top button', 'Update workers');
  await waitFor('document.querySelectorAll(".fl-update-row").length === 4', 'four rows');
  assert.match(await text('.fl-update-summary'), /1 up to date · 0 updating · 1 waiting for the PC to connect · 1 outdated · 1 failed/);
  assert.match(await text('[data-update="pc-c"]'), /Offline: updates when it connects/);
  assert.match(await text('[data-update="pc-d"]'), /Update failed: the new worker did not start/);
  assert.equal(await evaluate('!!document.querySelector(\'[data-update="pc-a"] button\')'), false, 'an up-to-date PC needs no command');
  // pc-b reported Linux: the command is for bash
  await type('.fl-updates input[name="server"]', 'http://192.168.1.10:8090');
  await click('[data-update="pc-b"] [data-action="update-link"]');
  await waitFor('document.querySelectorAll(".fl-update-link .fl-command").length === 1', 'bash command shown');
  const bodies = calls.filter((c) => c.path === '/api/fleet/updates/link').map((c) => c.body);
  assert.deepEqual(bodies.at(-1), { worker: 'pc-b', os: 'linux', server: 'http://192.168.1.10:8090' });
  assert.match(await text('.fl-update-link [data-command="bash"]'), /^f=\$\(mktemp\) && curl -fsSL 'http:\/\/192\.168\.1\.10:8090\/update\/.+\/agent-worker'.* update --server 'http:\/\/192\.168\.1\.10:8090' --code '.+' --force$/);
  assert.match(await text('.fl-update-link'), /agent-worker update/);
  await click('[data-action="update-all"]');
  await waitFor('/Updating…/.test(document.querySelector(\'[data-update="pc-b"]\').textContent)', 'pc-b is updating after Update all');
  assert.equal(lastCall('POST', '/api/fleet/updates').method, 'POST');
  assert.equal(await evaluate('!!document.querySelector(\'[data-update="pc-b"] button\')'), false, 'a PC that is updating needs no command');
  await click('[data-action="update-stop"]');
  assert.equal(lastCall('DELETE', '/api/fleet/updates').method, 'DELETE');
  await shot('updates-view');
});

test('phone width: the update view never scrolls sideways', async () => {
  reset();
  updates.workers = [{ id: 'a-pc-with-a-rather-long-name-to-check-wrapping', version: 'old00000old00000', os: 'windows', online: false, status: 'waiting' }];
  await call('Emulation.setDeviceMetricsOverride', { width: 360, height: 740, deviceScaleFactor: 2, mobile: true });
  try {
    await openFleet();
    await click('.fl-top button', 'Update workers');
    await waitFor('document.querySelectorAll(".fl-update-row").length === 1');
    await click('[data-action="update-link"]');
    await waitFor('document.querySelectorAll(".fl-update-link .fl-command").length >= 1');
    assert.equal(await evaluate('document.documentElement.scrollWidth > innerWidth + 1 || document.querySelector(".fl-body").scrollWidth > document.querySelector(".fl-body").clientWidth + 1'), false);
  } finally {
    await call('Emulation.setDeviceMetricsOverride', { width: 1440, height: 1000, deviceScaleFactor: 1, mobile: false });
  }
});

test('no uncaught page errors', () => {
  assert.deepEqual(errors, []);
});
