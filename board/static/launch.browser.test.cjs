/* Real Chromium checks for fleet compatibility in the New card dialog (standalone cards and flow
 * launches) and for live placement status in the card drawer, against a local fake board API.
 * node --test board/static/launch.browser.test.cjs
 * CHROME_PATH may override the installed Chrome executable; FLOW_SCREENSHOTS=<dir> saves screenshots.
 */
const { test, before, after } = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const os = require('node:os');
const { spawn } = require('node:child_process');
const fake = require('./placement-fake.cjs');

// ---------------------------------------------------------------- fake board
const FLOWS = {
  'placed-flow': { name: 'placed-flow', description: 'Build, then review', input: { topic: 'x' },
    steps: { build: { kind: 'program', run: 'python build.py' }, review: { kind: 'agent', agent: 'codex', needs: ['build'], prompt: 'Review' } } },
  'ambiguous-flow': { name: 'ambiguous-flow', description: 'A pipeline', input: {}, steps: { pipe: { kind: 'program', run: 'cat $IN | sort' } } },
};
const CARDS = [
  { id: 'card-wait', title: 'Placed session', task: 'agent-session', agent: 'codex', status: 'RUNNING', column: 'running', session_state: 'starting', created_at: '2026-11-05T09:00:00Z' },
  { id: 'flow-wait', title: 'Placed flow', task: 'placed-flow', flow: 'placed-flow', status: 'RUNNING', column: 'running', created_at: '2026-11-05T09:01:00Z' },
];
let workers = fake.fleet();
const records = {};   // run id -> {step or 'card': execution record}
const calls = [];
const STATIC = ['index.html', 'chat.js', 'card-search.js', 'chat.css', 'flow-builder.js', 'flow-builder.css', 'fleet.js', 'fleet.css', 'overlays.js', 'flow-graph.js', 'flow-graph.css'];
const server = http.createServer(async (request, response) => {
  const url = new URL(request.url, 'http://localhost');
  let raw = ''; for await (const chunk of request) raw += chunk;
  const body = raw ? JSON.parse(raw) : undefined;
  const json = (status, data) => { response.writeHead(status, { 'Content-Type': 'application/json' }); response.end(JSON.stringify(data)); };
  if (url.pathname.startsWith('/api/') && url.pathname !== '/api/live') calls.push({ method: request.method, path: url.pathname, body });
  try {
    if (url.pathname === '/api/systems') return json(200, { systems: ['hatchet', 'prefect', 'trigger-dev'], columns: ['scheduled', 'queued', 'running', 'waiting', 'done', 'stopped'] });
    if (url.pathname === '/api/live') {
      response.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache' });
      const send = (m) => response.write(`data: ${JSON.stringify(m)}\n\n`);
      send({ type: 'feed', live: true });
      send({ type: 'cards', reset: true, cards: url.searchParams.get('system') === 'hatchet' ? CARDS : [] });
      if (url.searchParams.get('card') === 'card-wait') send({ type: 'rows', card: 'card-wait', reset: true, rows: [], more: false });
      return;
    }
    if (url.pathname === '/api/flows') return json(200, { flows: Object.values(FLOWS).map((f) => ({ name: f.name, description: f.description, input: f.input,
      steps: Object.fromEntries(Object.entries(f.steps).map(([id, s]) => [id, { kind: s.kind, needs: s.needs || [] }])) })), broken: {} });
    if (url.pathname === '/api/workers') return json(200, { workers: workers.map((w) => ({ id: w.id, pools: w.pools, pool: w.pools[0], tags: w.tags, online: w.online })) });
    if (url.pathname === '/api/launch/validate') { const [status, data] = fake.launchCheck(body, workers, FLOWS); return json(status, data); }
    let m = url.pathname.match(/^\/api\/boards\/([^/]+)\/([^/]+)\/cards$/);
    if (m && request.method === 'POST') {
      const placed = body.kind === 'flow' ? { flow: body.flow, execution: body.execution } : body.worker ? { card: { agent: body.agent, worker: body.worker } } : null;
      if (placed) {
        const [, launch] = fake.launchCheck({ ...placed, system: m[1] }, workers, FLOWS);
        if (launch.status === 'blocked') return json(409, { error: 'Cannot launch: blocked', launch });
      }
      return json(201, { id: 'new-run' });
    }
    m = url.pathname.match(/^\/api\/cards\/([^/]+)\/([^/]+)\/(execution|dag)$/);
    if (m && m[3] === 'execution') { const steps = records[m[2]] || {}; return json(200, { execution: steps.card || null, steps }); }
    if (m && m[3] === 'dag') {
      if (m[2] !== 'flow-wait') return json(200, { model: 'declared', nodes: [], edges: [] });
      const steps = records['flow-wait'] || {};
      return json(200, { model: 'declared', edges: [{ from: 'build', to: 'review', kind: 'depends' }], nodes: [
        { id: 'build', step: 'build', label: 'build', ref: 'build-ref', kind: 'program', column: 'running', status: 'RUNNING', restart_supported: true, execution: steps.build || null },
        { id: 'review', step: 'review', label: 'review', ref: null, kind: 'agent', agent: 'codex', column: 'pending', status: 'PENDING', restart_supported: true, execution: steps.review || null }] });
    }
    if (/\/(logs|transcript)$/.test(url.pathname)) return json(200, { rows: [], lines: [], start: 0, more: false, reset: true });
    const file = url.pathname === '/' ? 'index.html' : url.pathname.replace('/static/', '');
    if (!STATIC.includes(file)) { response.writeHead(404).end(); return; }
    response.writeHead(200, { 'Content-Type': file.endsWith('.js') ? 'text/javascript' : file.endsWith('.css') ? 'text/css' : 'text/html' });
    response.end(fs.readFileSync(path.join(__dirname, file)));
  } catch (error) { json(500, { error: error.message }); }
});

// ---------------------------------------------------------------- browser
const errors = [];
let browser, profile, socket, call, evaluate, waitFor;
const shots = process.env.FLOW_SCREENSHOTS;
before(async () => {
  const chromePath = process.env.CHROME_PATH || 'C:/Program Files/Google/Chrome/Application/chrome.exe';
  assert.ok(fs.existsSync(chromePath), 'Set CHROME_PATH to a Chromium browser');
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  profile = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-board-launch-'));
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
    for (let i = 0; i < 120; i++) { if (await evaluate(expression)) return; await new Promise((resolve) => setTimeout(resolve, 100)); }
    throw new Error(`Timed out: ${what}\n${await evaluate('document.querySelector("#n-placement")?.textContent || document.querySelector("#d-placement")?.textContent || ""')}`);
  };
  await call('Runtime.enable');
  await call('Page.enable');
  await call('Emulation.setDeviceMetricsOverride', { width: 1440, height: 1000, deviceScaleFactor: 1, mobile: false });
  await call('Page.navigate', { url: `http://127.0.0.1:${server.address().port}` });
  await waitFor('document.querySelector(".card")', 'the board shows its cards');
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
const text = (selector) => evaluate(`${q(selector)}?.textContent || ''`);
async function shot(name) {
  if (!shots) return;
  fs.mkdirSync(shots, { recursive: true });
  fs.writeFileSync(path.join(shots, `${name}.png`), Buffer.from((await call('Page.captureScreenshot', { format: 'png' })).data, 'base64'));
}
// Set a field the way a user does: real input/change events.
async function fill(selector, value) {
  await evaluate(`(() => { const n = ${q(selector)}; n.focus(); n.value = ${js(value)}; n.dispatchEvent(new Event('input', { bubbles: true })); n.dispatchEvent(new Event('change', { bubbles: true })); })()`);
}
// Choose a <select> option by its visible text.
async function pick(selector, label) {
  await evaluate(`(() => { const s = ${q(selector)}; const o = [...s.options].find(o => o.textContent.startsWith(${js(label)}));
    if (!o) throw new Error('no option ' + ${js(label)}); s.value = o.value; s.dispatchEvent(new Event('change', { bubbles: true })); })()`);
}
async function useSystem(name) {
  await evaluate(`[...document.querySelectorAll('#systems button')].find(b => b.textContent === ${js(name)}).click()`);
}
async function openNew(kind) {
  await evaluate('document.querySelector("#new-card").click()');
  await waitFor('document.querySelector("#new-dialog").open && document.querySelectorAll("#n-kind option").length > 3', 'New card opens with flows listed');
  if (kind) { await evaluate(`(() => { const s = ${q('#n-kind')}; s.value = ${js(kind)}; s.dispatchEvent(new Event('change', { bubbles: true })); })()`); }
}
async function closeNew() { await evaluate('document.querySelector("#new-dialog").close()'); await sleep(50); }
const placement = (status) => `document.querySelector('#n-placement [data-status="${status}"]')`;
const createDisabled = () => evaluate('document.querySelector("#n-create").disabled');
const lastCheck = () => calls.filter((c) => c.path === '/api/launch/validate').at(-1)?.body;
const cardPicker = '#n-card-worker select';
// Wait for the fake server to have seen something (a debounced launch check, a poll).
async function until(check, what) {
  for (let i = 0; i < 120; i++) { if (check()) return; await sleep(100); }
  throw new Error(`Timed out: ${what}`);
}
const checked = (execution) => () => JSON.stringify(lastCheck()?.execution) === JSON.stringify(execution);

// ---------------------------------------------------------------- tests
test('standalone card: the fleet selector is offered on Hatchet and Prefect only, and never with a host workspace', async () => {
  await openNew('agent-session');
  await waitFor(`${q(cardPicker)} && !${q(cardPicker)}.disabled`, 'the card worker selector is enabled on Hatchet');
  assert.match(await text('#n-card-worker'), /Any compatible fleet worker/);
  assert.equal(await evaluate(`${q('#n-card-worker .fb-note')}?.hidden ?? true`), true, 'no restriction note');
  await fill('#n-workspace', 'C:/work/repo');
  await waitFor(`${q(cardPicker)}.disabled`, 'a host workspace disables fleet placement');
  assert.match(await text('#n-card-worker'), /runs in a new workspace on that PC.*Clear the workspace/s);
  await fill('#n-workspace', '');
  await waitFor(`!${q(cardPicker)}.disabled`, 'clearing the workspace enables it again');
  await fill('#n-resume', 'session-1');
  await waitFor(`${q(cardPicker)}.disabled`, 'a resumed session is bound to its host workspace too');
  await fill('#n-resume', '');
  await closeNew();
  await useSystem('Trigger.dev · local');
  await openNew('agent-session');
  await waitFor(`${q(cardPicker)}.disabled`, 'disabled on Trigger.dev');
  assert.match(await text('#n-card-worker'), /Hatchet and Prefect.*Trigger\.dev.*coordinator/s);
  await closeNew();
  await useSystem('Prefect');
  await openNew('agent-session');
  await waitFor(`!${q(cardPicker)}.disabled`, 'enabled on Prefect');
  await closeNew();
  await useSystem('Hatchet');
});

test('standalone card: matching pool, tags and agent with last detected times; only a confirmed mismatch blocks', async () => {
  await openNew('agent-session');
  await pick('#n-agent', 'Codex');
  await pick(cardPicker, 'Pool: build');
  await until(() => lastCheck()?.card?.worker?.pool === 'build', 'the card selection is checked');
  await waitFor(placement('ready'), 'pool build is ready on pc-a');
  const ready = await text('#n-placement');
  assert.match(ready, /Can start now on pc-a/);
  assert.match(await text('#n-placement [data-worker="pc-a"]'), /Compatible.*codex installed.*checked/s);
  assert.equal(await evaluate(`!!document.querySelector('#n-placement [data-worker="pc-a"] time[datetime="${fake.CHECKED}"]')`), true, 'pc-a shows when codex was last detected');
  assert.match(await text('#n-placement [data-worker="pc-b"]'), /Might match.*pc-b is offline.*last known/s);
  assert.equal(await evaluate(`!!document.querySelector('#n-placement [data-worker="pc-b"] time[datetime="${fake.OLD_CHECK}"]')`), true, 'an offline PC shows its last known check');
  assert.deepEqual(lastCheck(), { card: { agent: 'codex', worker: { pool: 'build' } }, system: 'hatchet' });
  assert.equal(await createDisabled(), false);
  await shot('card-ready');
  // A required tag no worker in the pool has: blocked, explained, with how to fix it.
  await fill('#n-card-worker input', 'cuda');
  await waitFor(placement('blocked'), 'a missing tag blocks');
  const blocked = await text('#n-placement');
  assert.match(blocked, /pc-a lacks tags: cuda/);
  assert.match(blocked, /How to fix: Add the missing tags to a worker in pool build/);
  assert.match(blocked, /Launch is blocked/);
  assert.equal(await createDisabled(), true, 'a blocked card cannot be created');
  await shot('card-blocked');
  // pc-c lacks codex (verified absent) but pc-b, offline, might match: launch and wait.
  await fill('#n-card-worker input', '');
  await pick(cardPicker, 'Pool: review');
  await waitFor(placement('wait'), 'an offline possible match waits');
  const waiting = await text('#n-placement');
  assert.match(waiting, /Waits for a worker: a matching worker is offline/);
  assert.match(await text('#n-placement [data-worker="pc-c"]'), /Not compatible.*codex is not installed on pc-c/s);
  assert.equal(await createDisabled(), false, 'waiting never blocks launch');
  await fill('#n-goal', 'Fix the flaky test');
  await evaluate('document.querySelector("#n-create").click()');
  await waitFor('!document.querySelector("#new-dialog").open', 'created');
  const post = calls.filter((c) => c.method === 'POST' && c.path.endsWith('/cards')).at(-1);
  assert.deepEqual(post.body.worker, { pool: 'review' });
  assert.equal(post.body.workspace, '');
});

test('standalone card: a busy worker and an unchecked agent wait; default checks all workers', async () => {
  workers.find((w) => w.id === 'pc-a').active = 2;
  try {
    await openNew('agent-session');
    await pick('#n-agent', 'Codex');
    await pick(cardPicker, 'Worker: pc-a');
    await waitFor(`${placement('wait')} && /busy/.test(document.querySelector('#n-placement').textContent)`, 'a full PC waits as busy');
    assert.match(await text('#n-placement'), /pc-a has no free slot/);
    await pick('#n-agent', 'pi');
    await waitFor(`/Whether pi is installed on pc-a is unknown.*pi unknown, never checked/s.test(document.querySelector('#n-placement').textContent)`, 'the agent change is checked again');
    assert.deepEqual(lastCheck().card, { agent: 'pi', worker: { id: 'pc-a' } });
    await pick(cardPicker, 'Any compatible fleet worker');
    await until(() => lastCheck()?.card?.worker && Array.isArray(lastCheck().card.worker.tags), 'default fleet check');
    assert.deepEqual(lastCheck().card, { agent: 'pi', worker: { tags: [] } });
    await closeNew();
  } finally { workers = fake.fleet(); }
});

test('flow launch: per-step explanations follow the default worker and step overrides; a blocked launch cannot start', async () => {
  await openNew('flow:placed-flow');
  await waitFor(`document.querySelector('#n-placement [data-step="build"]')`, 'every step is checked when the flow is chosen');
  assert.deepEqual(lastCheck(), { flow: 'placed-flow', execution: { default: {}, steps: {} }, system: 'hatchet' });
  await pick('#n-worker-fields select[aria-label="Default worker / pool"]', 'Pool: build');
  await until(checked({ default: { pool: 'build' }, steps: {} }), 'the default is checked');
  await waitFor(`document.querySelector('#n-placement [data-step="build"] [data-status="ready"]') && document.querySelector('#n-placement [data-step="review"] [data-status="ready"]')`, 'both steps ready in pool build');
  assert.deepEqual(lastCheck().execution, { default: { pool: 'build' }, steps: {} });
  assert.match(await text('#n-placement [data-step="build"]'), /build.*Can start now on pc-a.*python installed/s);
  // A step override sends review to the review pool: pc-b is offline, pc-c lacks codex.
  await evaluate(`document.querySelector('#n-worker-fields details').open = true`);
  await pick('#n-worker-fields select[aria-label="review"]', 'Pool: review');
  await until(checked({ default: { pool: 'build' }, steps: { review: { pool: 'review' } } }), 'the override is sent');
  await waitFor(`document.querySelector('#n-placement [data-step="review"] [data-status="wait"]')`, 'the override is checked');
  assert.deepEqual(lastCheck().execution, { default: { pool: 'build' }, steps: { review: { pool: 'review' } } });
  assert.match(await text('#n-placement [data-step="review"]'), /Waits for a worker: a matching worker is offline/);
  assert.equal(await createDisabled(), false);
  // Default tags no worker has: blocked, explained and not startable.
  await fill('#n-worker-fields input[aria-label="Default worker / pool tags"]', 'cuda');
  await waitFor(`document.querySelector('#n-placement [data-step="build"] [data-status="blocked"]')`, 'missing tags block the step');
  assert.match(await text('#n-placement'), /build.*lacks tags: cuda.*How to fix/s);
  assert.match(await text('#n-placement'), /Launch is blocked/);
  assert.equal(await createDisabled(), true);
  await shot('flow-launch-blocked');
  await fill('#n-worker-fields input[aria-label="Default worker / pool tags"]', '');
  await waitFor(`!document.querySelector('#n-placement [data-status="blocked"]')`);
  assert.equal(await createDisabled(), false);
  await evaluate('document.querySelector("#n-create").click()');
  await waitFor('!document.querySelector("#new-dialog").open', 'started');
  const post = calls.filter((c) => c.method === 'POST' && c.path.endsWith('/cards')).at(-1);
  assert.deepEqual(post.body.execution, { default: { pool: 'build' }, steps: { review: { pool: 'review' } } });
});

test('flow launch: a program whose executable cannot be inferred needs requires_command before it can start', async () => {
  await openNew('flow:ambiguous-flow');
  await waitFor(`document.querySelector('#n-placement [data-step="pipe"] [data-status="blocked"]')`, 'the ambiguous step is blocked');
  const shown = await text('#n-placement');
  assert.match(shown, /executable cannot be determined safely/);
  assert.match(shown, /requires_command/);
  assert.equal(await createDisabled(), true);
  await closeNew();
});

test('live waiting: a placed card shows offline, busy and unchecked waits, where it runs, and a cancel keeps the reason', async () => {
  records['card-wait'] = { card: { state: 'waiting', engine: 'hatchet', waiting_reason: 'offline', intervention_needed: false,
    waiting: [{ code: 'offline', message: 'pc-b is offline' }], message: 'Waiting: pc-b is offline', worker: null } };
  await evaluate(`document.querySelector('.card[data-id="card-wait"]').click()`);
  await waitFor(`document.querySelector('#d-placement:not([hidden])') && /Waiting for a worker/.test(document.querySelector('#d-placement').textContent)`, 'the drawer shows the wait');
  assert.match(await text('#d-placement'), /a matching worker is offline.*pc-b is offline/s);
  await shot('drawer-card-waiting');
  records['card-wait'].card = { ...records['card-wait'].card, waiting_reason: 'full_slots', waiting: [{ code: 'full_slots', message: 'pc-a has no free slot' }] };
  await waitFor(`/busy.*pc-a has no free slot/s.test(document.querySelector('#d-placement').textContent)`, 'a busy worker is shown live');
  records['card-wait'].card = { ...records['card-wait'].card, waiting_reason: 'software_unknown', waiting: [{ code: 'software_unknown', message: 'Whether codex is installed on pc-a is unknown' }] };
  await waitFor(`/has not been checked/.test(document.querySelector('#d-placement').textContent)`, 'unknown software is shown live');
  records['card-wait'].card = { ...records['card-wait'].card, state: 'running', waiting_reason: null, waiting: [], worker: { id: 'pc-a', hostname: 'PC-A', pool: 'build', tags: ['linux'] } };
  await waitFor(`/Running on pc-a/.test(document.querySelector('#d-placement').textContent)`, 'then where it runs');
  records['card-wait'].card = { ...records['card-wait'].card, state: 'cancelled', worker: null, waiting_reason: 'offline', waiting: [{ code: 'offline', message: 'pc-b is offline' }] };
  await waitFor(`/Cancelled while waiting: a matching worker is offline/.test(document.querySelector('#d-placement').textContent)`, 'a cancel keeps why it never ran');
  await evaluate('document.querySelector("#d-close").click()');
});

test('live waiting: flow steps show waiting, intervention needed and timed out in the drawer and on the graph', async () => {
  records['flow-wait'] = {
    build: { state: 'waiting', engine: 'hatchet', waiting_reason: 'software_unknown', intervention_needed: false, worker: null,
      waiting: [{ code: 'software_unknown', message: 'Whether python is installed on pc-a is unknown' }], message: 'Waiting: Whether python is installed on pc-a is unknown' },
    review: { state: 'queued', engine: 'hatchet', waiting_reason: null, worker: null } };
  await evaluate(`document.querySelector('.card[data-id="flow-wait"]').click()`);
  await waitFor(`document.querySelector('#d-placement:not([hidden]) [data-step="build"]')`, 'the drawer lists placed steps');
  assert.match(await text('#d-placement [data-step="build"]'), /build.*Waiting for a worker: the required software has not been checked/s);
  assert.match(await text('#d-placement [data-step="review"]'), /review.*Queued/s);
  await waitFor(`/waiting for a worker/.test(document.querySelector('[aria-label^="build:"]')?.textContent || '')`, 'the graph shows the wait on the node');
  await evaluate(`document.querySelector('.fg-node[aria-label^="build:"]').dispatchEvent(new MouseEvent('click', { bubbles: true }))`);
  await waitFor(`/Waiting for a worker/.test(document.querySelector('#d-node').textContent)`, 'the step panel explains it');
  records['flow-wait'].build = { ...records['flow-wait'].build, waiting_reason: 'intervention_needed', intervention_needed: true,
    message: 'No registered worker can run this step: python is not installed on pc-a', resolution: 'Install the required software on a matching worker, then refresh its capabilities.' };
  await waitFor(`/Intervention needed.*python is not installed on pc-a.*How to fix: Install/s.test(document.querySelector('#d-placement').textContent)`, 'intervention needed is shown live');
  await shot('drawer-flow-intervention');
  records['flow-wait'].build = { ...records['flow-wait'].build, state: 'timed_out', timeout_minutes: 60, waiting_reason: 'software_unknown', intervention_needed: false, resolution: null };
  await waitFor(`/Timed out after 60 minutes while waiting: the required software has not been checked/.test(document.querySelector('#d-placement').textContent)`, 'a timeout keeps the last reason');
  assert.match(await text('#d-node'), /Timed out after 60 minutes/);
  await evaluate('document.querySelector("#d-close").click()');
  const polls = () => calls.filter((c) => c.path.endsWith('/execution')).length;
  const count = polls();
  await sleep(2600);
  assert.equal(polls(), count, 'closing the drawer stops polling');
});

test('no uncaught page errors', () => {
  assert.deepEqual(errors, []);
});
