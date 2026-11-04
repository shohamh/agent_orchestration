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
let workers, tags, calls, fault, metrics;
function reset() {
  calls = []; fault = { metrics: false };
  tags = ['cuda', 'linux', 'windows'];
  workers = new Map(Object.entries({
    'pc-a': { id: 'pc-a', pools: ['build', 'gpu'], tags: ['windows', 'cuda'], engines: ['hatchet', 'prefect'], slots: 2, cpu_limit: 90, ram_limit: 90,
      connection: 'configured', online: true, status: 'online', hostname: 'BUILD-01', active: 1, settings_sync: { state: 'applied' },
      inventory: { agents: { codex: result('installed'), claude: result('absent'), demo: result('installed'), pi: result('unknown') },
        commands: { git: result('installed'), cargo: result('absent') }, refreshed_at: iso(5), cached: false },
      _url: 'http://10.1.2.3:8096', _token_env: 'PC_A_TOKEN' },
    'pc-b': { id: 'pc-b', pools: ['build'], tags: ['linux'], engines: ENGINES, slots: 1, cpu_limit: 90, ram_limit: 90, connection: 'configured',
      online: false, status: 'unreachable', settings_sync: { state: 'pending' },
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
  out.engine_status = Object.fromEntries(w.engines.map((e) => [e, { route: 'shared_service', coordinator: coordinators[e], connected: w.online && !['stopped', 'not_enabled'].includes(coordinators[e]) }]));
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
const STATIC = ['index.html', 'chat.js', 'chat.css', 'flow-builder.js', 'flow-builder.css', 'fleet.js', 'fleet.css'];
const server = http.createServer(async (request, response) => {
  const url = new URL(request.url, 'http://localhost');
  let raw = ''; for await (const chunk of request) raw += chunk;
  const body = raw ? JSON.parse(raw) : undefined;
  const json = (status, data) => { response.writeHead(status, { 'Content-Type': 'application/json' }); response.end(JSON.stringify(data)); };
  if (url.pathname.startsWith('/api/fleet')) calls.push({ method: request.method, path: url.pathname, body });
  try {
    if (url.pathname === '/api/fleet' && request.method === 'GET') return json(200, listing());
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
  const c = await text(worker('pc-c'));
  assert.match(c, /Awaiting connection/);
  assert.match(c, /CPU limit 90%/); assert.match(c, /RAM limit 90%/); assert.match(c, /0 of 1 slot in use|1 slot/);
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
  await click('.fl-top button', 'Register PC');
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
  await click('.fl-top button', 'Register PC');
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
  await click('.fl-top button', 'Register PC');
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
  assert.match(await text(`${worker('pc-a')} [data-engine="trigger"]`), /Trigger\.dev/);
  await noSecrets();
});

test('settings edits: limits and slots, validated, with the sync state', async () => {
  await openFleet();
  await click(`${worker('pc-c')} button`, 'Edit settings');
  await waitFor(`/Edit pc-c/.test(document.querySelector('.fl-form h3').textContent)`);
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
  await waitFor(`/CPU limit 75%/.test(document.querySelector('${worker('pc-c')}').textContent)`);
  assert.match(await text(worker('pc-c')), /RAM limit 80%/);
  await click(`${worker('pc-a')} button`, 'Edit settings');
  await type('.fl-form input[name="cpu_limit"]', '60');
  await click('.fl-form button', 'Save');
  await waitFor(`document.querySelector('.fl-form').hidden`);
  await waitFor(`/applied on pc-a for new work; running work continues/.test(document.querySelector('.fl-status').textContent)`, 'sync state shown');
  await waitFor(`/limit 60%/.test(document.querySelector('${worker('pc-a')} [data-metric="cpu"]').textContent)`);
});

test('manual capability refresh updates results and timestamps; offline PCs explain why they cannot', async () => {
  await openFleet();
  const stamp = await text(`${worker('pc-a')} .fl-refreshed`);
  workers.get('pc-a').inventory.refreshed_at = iso(5);
  await click(`${worker('pc-a')} [data-action="refresh"]`);
  await waitFor(`/Installed/.test(document.querySelector('${worker('pc-a')} [data-agent="claude"]').textContent)`, 'refreshed results');
  assert.equal(lastCall('POST', '/api/fleet/workers/pc-a/refresh').method, 'POST');
  assert.notEqual(await text(`${worker('pc-a')} .fl-refreshed`), stamp);
  assert.match(await text('.fl-live'), /Capabilities of pc-a refreshed/);
  assert.match(await text(worker('pc-b')), /checked when it connects/);
  assert.equal(await evaluate(`${q(`${worker('pc-b')} [data-action="refresh"]`)}.disabled`), true);
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
    await click('.fl-top button', 'Register PC');
    await waitFor(`!document.querySelector('.fl-form').hidden`);
    assert.ok(await sideways() <= 1, 'form fits (code blocks scroll inside themselves)');
    await shot('fleet-phone');
    await click('.fl-form button', 'Cancel');
  } finally {
    await call('Emulation.setDeviceMetricsOverride', { width: 1440, height: 1000, deviceScaleFactor: 1, mobile: false });
  }
});

test('no uncaught page errors', () => {
  assert.deepEqual(errors, []);
});
