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
let schedules = [];
const records = {};   // run id -> {step or 'card': execution record}
const calls = [];
const STATIC = ['index.html', 'chat.js', 'card-search.js', 'chat.css', 'flow-builder.js', 'flow-builder.css', 'fleet.js', 'fleet.css', 'admin.js', 'admin.css', 'new-card.js', 'new-card.css', 'overlays.js', 'flow-graph.js', 'flow-graph.css'];
const server = http.createServer(async (request, response) => {
  const url = new URL(request.url, 'http://localhost');
  let raw = ''; for await (const chunk of request) raw += chunk;
  if (url.pathname === '/api/uploads') {
    calls.push({ method: request.method, path: url.pathname, name: url.searchParams.get('name') });
    response.writeHead(url.searchParams.get('name') === 'broken.txt' ? 500 : 201, { 'Content-Type': 'application/json' });
    return response.end(JSON.stringify(url.searchParams.get('name') === 'broken.txt' ? { error: 'disk full' } : { id: 'a'.repeat(32), name: url.searchParams.get('name'), size: raw.length }));
  }
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
    if (url.pathname === '/api/fleet/tags') return json(200, { tags: ['gpu', 'linux'] });
    if (url.pathname === '/api/schedules' && request.method === 'GET') return json(200, { schedules: url.searchParams.get('system') === 'hatchet' ? schedules : [] });
    if (url.pathname === '/api/schedules/preview') return /bad/.test(body.cron || '') ? json(400, { error: 'cron needs five fields: minute hour day month weekday' }) : json(200, { next: [body.at || '2026-11-09T09:00:00Z', '2026-11-10T09:00:00Z'] });
    let found = url.pathname.match(/^\/api\/schedules\/([^/]+)(?:\/(\w+))?$/);
    if (found) {
      if (request.method === 'DELETE') schedules = schedules.filter((s) => s.id !== found[1]);
      else if (found[2] === 'disable') schedules = schedules.map((s) => (s.id === found[1] ? { ...s, enabled: false } : s));
      return json(200, found[2] === 'run' ? { run: 'new-run' } : { ok: true });
    }
    if (url.pathname === '/api/launch/validate') { const [status, data] = fake.launchCheck(body, workers, FLOWS); return json(status, data); }
    let m = url.pathname.match(/^\/api\/boards\/([^/]+)\/([^/]+)\/cards$/);
    if (m && request.method === 'POST') {
      const placed = body.kind === 'flow' ? { flow: body.flow, execution: body.execution } : body.worker ? { card: { agent: body.agent, worker: body.worker } } : null;
      if (placed) {
        const [, launch] = fake.launchCheck({ ...placed, system: m[1] }, workers, FLOWS);
        if (launch.status === 'blocked') return json(409, { error: 'Cannot launch: blocked', launch });
      }
      if (body.command === 'refused') return json(400, { error: 'The server refused this card' });
      if (body.schedule) {
        const item = { id: `s${schedules.length + 1}`, system: m[1], board: m[2], title: body.command || body.goal || body.flow, enabled: true, cron: body.schedule.cron || null, summary: body.schedule.summary, overlap: body.schedule.overlap || 'skip', next_at: body.schedule.at || '2026-11-09T09:00:00Z' };
        schedules.push(item);
        return json(201, { schedule: item, ...(body.schedule.runNow ? { id: 'new-run' } : {}) });
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
  await waitFor('document.querySelector("#new-dialog").open && flows.length > 0 && executionWorkers.length > 0', 'New card opens with flows and workers loaded');
  if (kind?.startsWith('flow:')) await chooseFlow(kind);
  else if (kind === 'program') await evaluate(`document.querySelector('input[name="n-mode"][value="program"]').click()`);
}
async function chooseFlow(kind) {
  await evaluate(`document.querySelector('input[name="n-mode"][value="flow"]').click()`);
  await waitFor(`document.querySelector('input[name="n-flow"][value=${js(kind)}]')`, 'the flow is offered as a card');
  await evaluate(`document.querySelector('input[name="n-flow"][value=${js(kind)}]').click()`);
}
const clickChip = (selector) => evaluate(`${q(selector)}.click()`);
const pool = (name) => `#n-pools [data-pool="${name}"]`;
async function addTag(name) {
  await evaluate(`(() => { const n = ${q('#n-tag-input')}; n.value = ${js(name)}; n.dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter', bubbles: true })); })()`);
}
const removeTag = (name) => clickChip(`#n-tags [data-tag="${name}"]`);
async function closeNew() { await evaluate('document.querySelector("#new-dialog").close()'); await sleep(50); }
const placement = (status) => `document.querySelector('#n-placement [data-status="${status}"]')`;
const createDisabled = () => evaluate('document.querySelector("#n-create").disabled');
const lastCheck = () => calls.filter((c) => c.path === '/api/launch/validate').at(-1)?.body;
const whereInput = '#n-tag-input';
// Wait for the fake server to have seen something (a debounced launch check, a poll).
async function until(check, what) {
  for (let i = 0; i < 120; i++) { if (check()) return; await sleep(100); }
  throw new Error(`Timed out: ${what}`);
}
const checked = (execution) => () => JSON.stringify(lastCheck()?.execution) === JSON.stringify(execution);

// ---------------------------------------------------------------- tests
test('what runs: agent, program and flow each ask for their own input', async () => {
  await openNew();
  const shown = (selector) => evaluate(`${q(selector)}.offsetParent !== null`);
  assert.equal(await evaluate('document.querySelector("#n-create").textContent'), 'Start agent');
  assert.equal(await shown('#n-goal'), true);
  assert.equal(await shown('#n-command'), false);
  assert.equal(await shown('#n-flow-picker'), false);
  await evaluate(`document.querySelector('input[name="n-mode"][value="program"]').click()`);
  assert.equal(await evaluate('document.querySelector("#n-create").textContent'), 'Run program');
  assert.equal(await shown('#n-goal'), false);
  assert.equal(await shown('#n-command'), true);
  assert.equal(await shown('#n-advanced'), false);
  await evaluate(`document.querySelector('input[name="n-mode"][value="flow"]').click()`);
  assert.equal(await shown('#n-flow-picker'), true);
  const cards = await text('#n-flow-cards');
  assert.match(cards, /placed-flow/);
  assert.match(cards, /Research report/);
  assert.equal(await evaluate(`document.querySelector('#n-kind').value`), `flow:${await evaluate("flows[0].name")}`, 'the first flow is selected');
  // A built-in demo has no attachments or worker choice.
  await evaluate(`document.querySelector('input[name="n-flow"][value="steerable-agent"]').click()`);
  assert.equal(await shown('#n-context'), false);
  assert.equal(await shown('#n-where-fields'), false);
  assert.equal(await shown('#n-steer-fields'), true);
  await shot('new-card-flow');
  await closeNew();
  await evaluate('newCard.reset()');
});

test('standalone card: workers can be chosen on Hatchet and Prefect only, and never with a host workspace', async () => {
  await openNew('agent-session');
  await waitFor(`!${q(whereInput)}.disabled`, 'worker selection is enabled on Hatchet');
  assert.match(await text('#n-pools'), /All pools/);
  assert.equal(await evaluate(`${q('#n-where-note')}.hidden`), true, 'no restriction note');
  await evaluate(`document.querySelector('#n-advanced').open = true`);
  await fill('#n-workspace', 'C:/work/repo');
  await waitFor(`${q(whereInput)}.disabled`, 'a host workspace disables fleet placement');
  assert.match(await text('#n-where-note'), /existing workspace or resumed session.*Clear them/s);
  await fill('#n-workspace', '');
  await waitFor(`!${q(whereInput)}.disabled`, 'clearing the workspace enables it again');
  await fill('#n-resume', 'session-1');
  await waitFor(`${q(whereInput)}.disabled`, 'a resumed session is bound to its host workspace too');
  await fill('#n-resume', '');
  await closeNew();
  await useSystem('Trigger.dev · local');
  await openNew('agent-session');
  await waitFor(`${q(whereInput)}.disabled`, 'disabled on Trigger.dev');
  assert.match(await text('#n-where-note'), /Trigger\.dev.*coordinator.*Hatchet or Prefect/s);
  await closeNew();
  await useSystem('Prefect');
  await openNew('agent-session');
  await waitFor(`!${q(whereInput)}.disabled`, 'enabled on Prefect');
  await closeNew();
  await useSystem('Hatchet');
});

test('standalone card: matching pool, tags and agent with last detected times; only a confirmed mismatch blocks', async () => {
  await openNew('agent-session');
  await pick('#n-agent', 'Codex');
  await clickChip(pool('build'));
  await until(() => lastCheck()?.card?.worker?.pools?.[0] === 'build', 'the card selection is checked');
  await waitFor(placement('ready'), 'pool build is ready on pc-a');
  assert.match(await text('#n-fit'), /2 of 3 workers match, 1 online/);
  const ready = await text('#n-placement');
  assert.match(ready, /Can start now on pc-a/);
  assert.match(await text('#n-placement [data-worker="pc-a"]'), /Compatible.*codex installed.*checked/s);
  assert.equal(await evaluate(`!!document.querySelector('#n-placement [data-worker="pc-a"] time[datetime="${fake.CHECKED}"]')`), true, 'pc-a shows when codex was last detected');
  assert.match(await text('#n-placement [data-worker="pc-b"]'), /Might match.*pc-b is offline.*last known/s);
  assert.equal(await evaluate(`!!document.querySelector('#n-placement [data-worker="pc-b"] time[datetime="${fake.OLD_CHECK}"]')`), true, 'an offline PC shows its last known check');
  assert.deepEqual(lastCheck(), { card: { agent: 'codex', worker: { pools: ['build'] } }, system: 'hatchet' });
  assert.equal(await createDisabled(), false);
  await shot('card-ready');
  // A required tag no worker in the pool has: blocked, explained, with how to fix it.
  await addTag('cuda');
  await waitFor(placement('blocked'), 'a missing tag blocks');
  const blocked = await text('#n-placement');
  assert.match(blocked, /pc-a lacks tags: cuda/);
  assert.match(blocked, /How to fix: Add the missing tags to a worker in pool build/);
  assert.match(blocked, /Launch is blocked/);
  assert.equal(await evaluate(`document.querySelector('#n-placement details').open`), true, 'the reason is open when it blocks');
  assert.match(await text('#n-fit'), /None of the 3/);
  assert.equal(await createDisabled(), true, 'a blocked card cannot be created');
  await shot('card-blocked');
  // pc-c lacks codex (verified absent) but pc-b, offline, might match: launch and wait.
  await removeTag('cuda');
  await clickChip(pool('build'));
  await clickChip(pool('review'));
  await waitFor(placement('wait'), 'an offline possible match waits');
  const waiting = await text('#n-placement');
  assert.match(waiting, /Waits for a worker: a matching worker is offline/);
  assert.match(await text('#n-placement [data-worker="pc-c"]'), /Not compatible.*codex is not installed on pc-c/s);
  assert.equal(await createDisabled(), false, 'waiting never blocks launch');
  await fill('#n-goal', 'Fix the flaky test');
  await evaluate('document.querySelector("#n-create").click()');
  await waitFor('!document.querySelector("#new-dialog").open', 'created');
  const post = calls.filter((c) => c.method === 'POST' && c.path.endsWith('/cards')).at(-1);
  assert.deepEqual(post.body.worker, { pools: ['review'] });
  assert.equal(post.body.workspace, '');
});

test('where it runs: several pools mean any of them, All pools clears them', async () => {
  await openNew('agent-session');
  await clickChip(pool('build'));
  await clickChip(pool('review'));
  await until(() => JSON.stringify(lastCheck()?.card?.worker) === JSON.stringify({ pools: ['build', 'review'] }), 'both pools are checked');
  assert.equal(await evaluate(`${q(pool('build'))}.getAttribute('aria-pressed')`), 'true');
  assert.equal(await evaluate(`${q(pool(''))}.getAttribute('aria-pressed')`), 'false');
  await clickChip(pool(''));
  await until(() => JSON.stringify(lastCheck()?.card?.worker) === JSON.stringify({ tags: [] }), 'All pools checks every worker');
  assert.equal(await evaluate(`${q(pool(''))}.getAttribute('aria-pressed')`), 'true');
  await closeNew();
});

test('standalone card: a busy worker and an unchecked agent wait; default checks all workers', async () => {
  workers.find((w) => w.id === 'pc-a').active = 2;
  try {
    await openNew('agent-session');
    await pick('#n-agent', 'Codex');
    await evaluate(`document.querySelector('#n-where-more').open = true`);
    await pick('#n-worker-id', 'pc-a');
    await waitFor(`${placement('wait')} && /busy/.test(document.querySelector('#n-placement').textContent)`, 'a full PC waits as busy');
    assert.match(await text('#n-placement'), /pc-a has no free slot/);
    assert.equal(await evaluate(`${q(pool('build'))}.disabled`), true, 'one worker replaces the pool choice');
    await pick('#n-agent', 'pi');
    await waitFor(`/Whether pi is installed on pc-a is unknown.*pi unknown, never checked/s.test(document.querySelector('#n-placement').textContent)`, 'the agent change is checked again');
    assert.deepEqual(lastCheck().card, { agent: 'pi', worker: { id: 'pc-a' } });
    await pick('#n-worker-id', 'Any matching worker');
    await until(() => lastCheck()?.card?.worker && Array.isArray(lastCheck().card.worker.tags), 'default fleet check');
    assert.deepEqual(lastCheck().card, { agent: 'pi', worker: { tags: [] } });
    await closeNew();
    await evaluate('newCard.reset()');
  } finally { workers = fake.fleet(); }
});

test('flow launch: per-step explanations follow the default worker and step overrides; a blocked launch cannot start', async () => {
  await openNew('flow:placed-flow');
  await waitFor(`document.querySelector('#n-placement [data-step="build"]')`, 'every step is checked when the flow is chosen');
  assert.deepEqual(lastCheck(), { flow: 'placed-flow', execution: { default: {}, steps: {} }, system: 'hatchet' });
  await clickChip(pool('build'));
  await until(checked({ default: { pools: ['build'] }, steps: {} }), 'the default is checked');
  await waitFor(`document.querySelector('#n-placement [data-step="build"] [data-status="ready"]') && document.querySelector('#n-placement [data-step="review"] [data-status="ready"]')`, 'both steps ready in pool build');
  assert.deepEqual(lastCheck().execution, { default: { pools: ['build'] }, steps: {} });
  assert.match(await text('#n-placement [data-step="build"]'), /build.*Can start now on pc-a.*python installed/s);
  // A step override sends review to the review pool: pc-b is offline, pc-c lacks codex.
  await evaluate(`document.querySelector('#n-where-more').open = true`);
  await pick('#n-worker-fields select[aria-label="review"]', 'Pool: review');
  await until(checked({ default: { pools: ['build'] }, steps: { review: { pool: 'review' } } }), 'the override is sent');
  await waitFor(`document.querySelector('#n-placement [data-step="review"] [data-status="wait"]')`, 'the override is checked');
  assert.deepEqual(lastCheck().execution, { default: { pools: ['build'] }, steps: { review: { pool: 'review' } } });
  assert.match(await text('#n-placement [data-step="review"]'), /Waits for a worker: a matching worker is offline/);
  assert.equal(await createDisabled(), false);
  // Default tags no worker has: blocked, explained and not startable.
  await addTag('cuda');
  await waitFor(`document.querySelector('#n-placement [data-step="build"] [data-status="blocked"]')`, 'missing tags block the step');
  assert.match(await text('#n-placement'), /build.*lacks tags: cuda.*How to fix/s);
  assert.match(await text('#n-placement'), /Launch is blocked/);
  assert.equal(await createDisabled(), true);
  await shot('flow-launch-blocked');
  await removeTag('cuda');
  await waitFor(`!document.querySelector('#n-placement [data-status="blocked"]')`);
  assert.equal(await createDisabled(), false);
  await evaluate('document.querySelector("#n-create").click()');
  await waitFor('!document.querySelector("#new-dialog").open', 'started');
  const post = calls.filter((c) => c.method === 'POST' && c.path.endsWith('/cards')).at(-1);
  assert.deepEqual(post.body.execution, { default: { pools: ['build'] }, steps: { review: { pool: 'review' } } });
});

test('flow launch: a program whose executable cannot be inferred needs requires_command before it can start', async () => {
  await openNew('flow:ambiguous-flow');
  await waitFor(`document.querySelector('#n-placement [data-step="pipe"] [data-status="blocked"]')`, 'the ambiguous step is blocked');
  const shown = await text('#n-placement');
  assert.match(shown, /executable cannot be determined safely/);
  assert.match(shown, /requires_command/);
  assert.equal(await createDisabled(), true);
  await closeNew();
  await evaluate('newCard.reset()');
});

test('program card: the command, its input and attachments reach the server', async () => {
  await openNew('program');
  await evaluate('document.querySelector("#n-create").click()');
  assert.match(await text('#n-error'), /Write the command/);
  assert.equal(await evaluate('document.activeElement.id'), 'n-command', 'the empty field gets focus');
  await fill('#n-command', 'python train.py --epochs 5');
  await fill('#n-program-input', 'use the small dataset');
  assert.equal(await evaluate(`${q('#n-error')}.hidden`), true, 'typing clears the error');
  // Repository and Confluence rows, with what is wrong said in the form.
  await evaluate(`document.querySelector('#n-add-repo').click()`);
  await evaluate(`document.querySelector('#n-add-confluence').click()`);
  await fill('#n-repos [data-field="url"]', 'not a url');
  await evaluate('document.querySelector("#n-create").click()');
  assert.match(await text('#n-error'), /git URL/);
  await fill('#n-repos [data-field="url"]', 'https://gitlab.com/team/project.git');
  assert.match(await text('#n-repos .nc-provider'), /GitLab/);
  await fill('#n-repos [data-field="ref"]', 'develop');
  await fill('#n-confluence [data-field="url"]', 'https://acme.atlassian.net/wiki/home');
  await evaluate('document.querySelector("#n-create").click()');
  assert.match(await text('#n-error'), /Confluence page/);
  await fill('#n-confluence [data-field="url"]', 'https://acme.atlassian.net/wiki/spaces/ENG/pages/123/Runbook');
  // Files upload as soon as they are attached; a failed one has to be dealt with.
  await evaluate(`newCardTest = async (name) => { const data = new DataTransfer(); data.items.add(new File(['hello'], name)); document.querySelector('#n-context').dispatchEvent(new DragEvent('drop', { dataTransfer: data, bubbles: true, cancelable: true })); }`);
  await evaluate(`newCardTest('notes.txt')`);
  await waitFor(`document.querySelector('#n-files li[data-state="ready"]')`, 'the file uploaded');
  await evaluate(`newCardTest('broken.txt')`);
  await waitFor(`document.querySelector('#n-files li[data-state="failed"]')`, 'the failed upload is shown');
  await evaluate('document.querySelector("#n-create").click()');
  assert.match(await text('#n-error'), /did not upload/);
  await evaluate(`document.querySelector('#n-files li[data-state="failed"] .nc-remove').click()`);
  await clickChip(pool('build'));
  await shot('new-card-program');
  await evaluate('document.querySelector("#n-create").click()');
  await waitFor('!document.querySelector("#new-dialog").open', 'started');
  const post = calls.filter((c) => c.method === 'POST' && c.path.endsWith('/cards')).at(-1);
  assert.equal(post.body.kind, 'program');
  assert.equal(post.body.command, 'python train.py --epochs 5');
  assert.deepEqual(post.body.worker, { pools: ['build'] });
  assert.deepEqual(post.body.context, { text: 'use the small dataset', files: [{ id: 'a'.repeat(32), name: 'notes.txt' }],
    repositories: [{ url: 'https://gitlab.com/team/project.git', ref: 'develop' }],
    confluence: [{ url: 'https://acme.atlassian.net/wiki/spaces/ENG/pages/123/Runbook', include_descendants: true }] });
  assert.equal(post.body.schedule, undefined, 'right now is not a schedule');
  // The form starts clean next time.
  await openNew();
  assert.equal(await evaluate(`${q('#n-command')}.value`), '');
  assert.equal(await evaluate(`document.querySelectorAll('#n-repos .nc-source, #n-files li').length`), 0);
  await closeNew();
});

test('an error from the server stays in the form, next to the button', async () => {
  await openNew('program');
  await fill('#n-command', 'refused');
  await evaluate('document.querySelector("#n-create").click()');
  await waitFor(`/refused this card/.test(document.querySelector('#n-error').textContent)`, 'the error is shown in the form');
  assert.equal(await evaluate('document.querySelector("#new-dialog").open'), true);
  assert.equal(await createDisabled(), false, 'it can be corrected and sent again');
  await closeNew();
  await evaluate('newCard.reset()');
});

test('when: later and repeating cards become schedules on the Scheduled column', async () => {
  await openNew('program');
  await fill('#n-command', 'nightly-backup');
  await evaluate(`document.querySelector('input[name="n-when"][value="later"]').click()`);
  assert.notEqual(await evaluate(`${q('#n-at')}.value`), '', 'a sensible time is filled in');
  assert.equal(await evaluate('document.querySelector("#n-create").textContent'), 'Schedule');
  await waitFor(`${q('#n-preview')}.hidden === false`, 'the next time is shown');
  await fill('#n-at', '2020-01-01T10:00');
  await evaluate('document.querySelector("#n-create").click()');
  assert.match(await text('#n-error'), /future/);
  await fill('#n-at', '2030-03-04T22:15');
  assert.equal(await evaluate(`${q('#n-overlap-row')}.hidden`), true, 'only a repeat can overlap');
  await pick('#n-repeat', 'Every weekday');
  assert.equal(await evaluate(`${q('#n-overlap-row')}.hidden`), false);
  assert.equal(await evaluate(`${q('#n-overlap')}.value`), 'skip', 'skipping is the default');
  assert.equal(await evaluate(`${q('#n-time')}.value`), '22:15', 'the repeat starts from the chosen time');
  await waitFor(`/Runs at/.test(${q('#n-preview')}.textContent)`, 'the preview lists runs');
  await shot('new-card-schedule');
  await evaluate('document.querySelector("#n-create").click()');
  await waitFor('!document.querySelector("#new-dialog").open', 'scheduled');
  const post = calls.filter((c) => c.method === 'POST' && c.path.endsWith('/cards')).at(-1);
  assert.equal(post.body.schedule.cron, '15 22 * * 1-5');
  assert.equal(post.body.schedule.summary, 'Every weekday at 22:15');
  assert.equal(post.body.schedule.overlap, 'skip');
  assert.equal(new Date(post.body.schedule.at).getTime(), new Date('2030-03-04T22:15').getTime());
  assert.equal(post.body.schedule.runNow, undefined);
  assert.ok(post.body.schedule.timezone, 'the time zone is sent');
  await waitFor(`document.querySelector('[data-col="scheduled"] [data-schedule="s1"]')`, 'it shows on the board');
  const shown = await text('[data-schedule="s1"]');
  assert.match(shown, /nightly-backup.*repeats.*Every weekday at 22:15.*next/s);
  assert.doesNotMatch(shown, /may overlap/);
  await shot('schedule-on-board');
  // Pause, then delete.
  await evaluate(`[...document.querySelectorAll('[data-schedule="s1"] button')].find((b) => b.textContent === 'Pause').click()`);
  await waitFor(`/paused/.test(document.querySelector('[data-schedule="s1"]').textContent)`, 'paused');
  await evaluate(`window.confirm = () => true; [...document.querySelectorAll('[data-schedule="s1"] button')].find((b) => b.textContent === 'Delete').click()`);
  await waitFor(`!document.querySelector('[data-schedule="s1"]')`, 'deleted');
});

test('when: right now and repeating starts one now; a wrong cron cannot be submitted', async () => {
  await openNew('program');
  await fill('#n-command', 'report');
  await pick('#n-repeat', 'Custom');
  await fill('#n-cron', 'bad cron');
  await waitFor(`/five fields/.test(${q('#n-preview')}.textContent)`, 'a wrong cron is explained');
  assert.equal(await createDisabled(), true);
  await fill('#n-cron', '0 9 * * *');
  await waitFor(`!document.querySelector("#n-create").disabled`, 'a valid cron can be sent');
  assert.equal(await evaluate('document.querySelector("#n-create").textContent'), 'Start and repeat');
  await pick('#n-overlap', 'Start another run');
  await evaluate('document.querySelector("#n-create").click()');
  await waitFor('!document.querySelector("#new-dialog").open', 'started');
  const post = calls.filter((c) => c.method === 'POST' && c.path.endsWith('/cards')).at(-1);
  assert.equal(post.body.schedule.runNow, true);
  assert.equal(post.body.schedule.cron, '0 9 * * *');
  assert.equal(post.body.schedule.at, undefined);
  assert.equal(post.body.schedule.overlap, 'start');
  await waitFor(`/runs may overlap/.test(document.querySelector('[data-schedule="s1"]')?.textContent || '')`, 'the card says its runs may overlap');
  await evaluate('document.querySelector("#new-card").click()');
  assert.equal(await evaluate(`${q('#n-overlap')}.value`), 'skip', 'the choice does not carry over to the next card');
  await closeNew();
  schedules = [];
  await evaluate('document.querySelector("#d-close")?.click(); loadSchedules()');
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
