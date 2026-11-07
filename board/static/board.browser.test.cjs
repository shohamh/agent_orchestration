/* The board on desktop in real Chromium, with a local fixture: lanes, what a card says, the cards
 * waiting on a person and the empty board.
 * node --test board/static/board.browser.test.cjs
 * CHROME_PATH may override the installed Chrome executable. No packages or backend needed.
 */
const { test, before, after } = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const os = require('node:os');
const { spawn } = require('node:child_process');

const ago = (seconds) => new Date(Date.now() - seconds * 1000).toISOString();
const COLUMNS = ['scheduled', 'queued', 'running', 'waiting', 'done', 'stopped'];
const MOVES = { stopped: 'cancel', queued: 'reorder' };
const CARDS = [
  { id: 'q1', title: 'Queued first', column: 'queued', status: 'QUEUED', agent: 'codex', created_at: ago(600), queue: { ahead: 0, running: 2, limit: 4, queued: 2 }, moves: MOVES },
  { id: 'q2', title: 'Queued second', column: 'queued', status: 'QUEUED', agent: 'codex', created_at: ago(500), queue: { ahead: 1, running: 2, limit: 4, queued: 2 }, moves: MOVES },
  { id: 'r1', title: 'Long run', column: 'running', status: 'RUNNING', agent: 'claude', created_at: ago(200000), started_at: ago(198000), last: 'step 3 of 8', workers: [{ id: 'pc-a' }, { id: 'pc-b' }] },
  { id: 'w1', title: 'Blocked on approval', task: 'agent-session', column: 'waiting', status: 'WAITING', agent: 'codex', session_state: 'approval', created_at: ago(900), started_at: ago(800) },
  { id: 'w2', title: 'Blocked on a question', task: 'agent-session', column: 'waiting', status: 'WAITING', agent: 'claude', session_state: 'question', created_at: ago(900), started_at: ago(400) },
  { id: 'w3', title: 'Waiting by itself', column: 'waiting', status: 'WAITING', agent: 'pi', created_at: ago(900), started_at: ago(300) },
  { id: 'w4', title: 'Agent still working', task: 'agent-session', column: 'running', status: 'RUNNING', agent: 'pi', session_state: 'working', created_at: ago(900), started_at: ago(100) },
  { id: 'd1', title: 'Finished', column: 'done', status: 'COMPLETED', agent: 'pi', created_at: ago(9000), started_at: ago(8000), finished_at: ago(8000 - 234960) },
  { id: 'd2', title: 'Finished session', task: 'agent-session', column: 'done', status: 'COMPLETED', session_state: 'approval', created_at: ago(900), started_at: ago(800), finished_at: ago(790) },
  { id: 'f1', title: 'Failed run', column: 'stopped', status: 'FAILED', agent: 'pi', error: 'Rate limited by provider', last: 'step 2 of 6', created_at: ago(900), started_at: ago(800), finished_at: ago(700) },
  { id: 'f2', title: 'Cancelled run', column: 'stopped', status: 'CANCELLED', agent: 'pi', last: 'step 1 of 6', created_at: ago(900), started_at: ago(800), finished_at: ago(700) },
];
// A long Done column for scrolling. "many" is a board big enough to skip layout off screen.
const LONG = [...CARDS, ...Array.from({ length: 40 }, (_, i) => ({ id: `long${i}`, title: `Older run ${i}`, column: 'done', status: 'COMPLETED', agent: 'pi', created_at: ago(9000 + i), started_at: ago(9000 + i), finished_at: ago(8000 + i) }))];
const MANY = Array.from({ length: 200 }, (_, i) => ({ id: `m${i}`, title: `Run ${i}`, column: COLUMNS[i % 6], status: 'RUNNING', created_at: ago(900 + i), started_at: ago(900), moves: MOVES }));
const FLOW_RUN = [{ id: 'run1', title: 'Flow run', task: 'demo', flow: 'demo', column: 'done', status: 'COMPLETED', created_at: ago(900), started_at: ago(800), finished_at: ago(700) }];
const DAG = { model: 'declared', edges: [{ from: 'n-a', to: 'n-b', kind: 'depends' }], nodes: [
  { id: 'n-a', step: 'a', label: 'a', ref: 'r-a', kind: 'program', column: 'done', status: 'COMPLETED', started_at: ago(800), finished_at: ago(760), restart_supported: true, execution: { state: 'done', worker: { id: 'pc-on', hostname: 'pc-on.lan', pool: 'build', tags: [] } } },
  { id: 'n-c', step: 'c', label: 'c', kind: 'agent', agent: 'pi', column: 'pending', status: 'PENDING' },
  { id: 'n-b', step: 'b', label: 'b', ref: 'r-b', kind: 'agent', agent: 'codex', column: 'stopped', status: 'FAILED', error: 'Exit code 2', started_at: ago(760), finished_at: ago(700), restart_supported: false, restart_reason: 'No replay here', backend_url: 'http://localhost:1/x' }] };
const SAVED = { a: { x: 400, y: 30 }, b: { x: 40, y: 200 }, c: { x: 400, y: 200 } };
const KINDS = [{ id: 'k-flow', title: 'A flow', flow: 'mirror-refresh', task: 'mirror-refresh', column: 'running', status: 'RUNNING', created_at: ago(900), started_at: ago(800) },
  { id: 'k-prog', title: 'A program', task: 'research-report', column: 'done', status: 'COMPLETED', created_at: ago(900), started_at: ago(800), finished_at: ago(700) }];
const session = (id, worker) => ({ id, title: id, task: 'agent-session', agent: 'codex', session_state: 'approval', column: 'waiting', status: 'WAITING', created_at: ago(900), started_at: ago(800), workers: [{ step: 'card', id: worker, state: 'running' }] });
const LINKS = [session('on-live', 'pc-on'), session('on-dead', 'pc-off'), session('on-lost', 'pc-unregistered'),
  { id: 'run-dead', title: 'run-dead', task: 'x', agent: 'pi', column: 'running', status: 'RUNNING', created_at: ago(900), started_at: ago(800), workers: [{ step: 'main', id: 'pc-off', state: 'running' }] },
  { id: 'run-ok', title: 'run-ok', task: 'x', agent: 'pi', column: 'running', status: 'RUNNING', created_at: ago(900), started_at: ago(800), workers: [{ step: 'old', id: 'pc-off', state: 'done' }, { step: 'main', id: 'pc-on', state: 'running' }] }];
const BOARDS = { research: LONG, empty: [], planned: [], many: MANY, flowrun: FLOW_RUN, kinds: KINDS, links: LINKS };
const SCHEDULES = {
  planned: [{ id: 'later1', system: 'hatchet', board: 'planned', title: 'Later program', enabled: true, cron: null, next_at: '2026-11-09T09:00:00Z', overlap: 'skip' }],
  flowrun: [{ id: 'later2', system: 'hatchet', board: 'flowrun', title: 'Next flow run', enabled: true, cron: '0 9 * * *', next_at: '2026-11-09T09:00:00Z', summary: 'Every day at 09:00', overlap: 'skip' }],
};

let holdSnapshot = false, workersDown = false;
const WORKERS = [{ id: 'pc-on', pools: ['build'], tags: [], online: true }, { id: 'pc-off', pools: ['build'], tags: [], online: false }];
const server = http.createServer((request, response) => {
  const url = new URL(request.url, 'http://localhost');
  const json = (data, status = 200) => { response.writeHead(status, { 'Content-Type': 'application/json' }); response.end(JSON.stringify(data)); };
  if (url.pathname === '/api/systems') return json({ systems: ['hatchet'], columns: COLUMNS });
  if (url.pathname === '/api/live') {
    response.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache' });
    response.write(`data: ${JSON.stringify({ type: 'feed', live: true })}\n\n`);
    if (!holdSnapshot) response.write(`data: ${JSON.stringify({ type: 'cards', reset: true, cards: BOARDS[url.searchParams.get('board')] || [] })}\n\n`);
    return;
  }
  if (url.pathname === '/api/flows') return json({ flows: [], broken: {} });
  if (url.pathname === '/api/schedules') return json({ schedules: url.searchParams.get('system') === 'hatchet' ? SCHEDULES[url.searchParams.get('board')] || [] : [] });
  if (url.pathname === '/api/fleet/tags') return json({ tags: [] });
  if (url.pathname.endsWith('/execution')) return json({ execution: null, steps: {} });
  if (url.pathname.endsWith('/run1/dag')) return json(DAG);
  if (url.pathname.endsWith('/dag')) return json({ model: 'declared', nodes: [], edges: [] });
  if (url.pathname === '/api/flow-documents/demo') return json({ name: 'demo', layout: { version: 1, positions: SAVED } });
  if (url.pathname === '/api/workers') return workersDown ? json({ error: 'The flow host is not reachable' }, 502) : json({ workers: WORKERS });
  if (url.pathname.startsWith('/api/')) return json({ error: 'not in fake' }, 404);
  const file = url.pathname === '/' ? 'index.html' : path.basename(url.pathname);
  const full = path.join(__dirname, file);
  if (!fs.existsSync(full)) { response.writeHead(404); response.end(); return; }
  response.writeHead(200, { 'Content-Type': file.endsWith('.js') ? 'text/javascript' : file.endsWith('.css') ? 'text/css' : 'text/html' });
  response.end(fs.readFileSync(full));
});

let browser, socket, profile, base;
const errors = [];
const pending = new Map();
let nextId = 0;
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const call = (method, params = {}) => new Promise((resolve, reject) => { const id = ++nextId; pending.set(id, { resolve, reject }); socket.send(JSON.stringify({ id, method, params })); });
async function evaluate(expression) {
  const result = await call('Runtime.evaluate', { expression, awaitPromise: true, returnByValue: true });
  if (result.exceptionDetails) throw new Error(`${expression}: ${result.exceptionDetails.exception?.description || result.exceptionDetails.text}`);
  return result.result.value;
}
async function waitFor(expression, label = expression) {
  for (let i = 0; i < 100; i++) { if (await evaluate(expression)) return; await sleep(50); }
  assert.fail(`timed out waiting for ${label}`);
}
async function openBoard(name) {
  await call('Page.navigate', { url: base });
  await waitFor('document.readyState === "complete"');
  await evaluate(`localStorage.setItem('boards', '["research","empty","planned","many","flowrun","kinds","links"]'); localStorage.setItem('board', '"${name}"'); localStorage.removeItem('hideFinished')`);
  await call('Page.navigate', { url: base });
  await waitFor('document.querySelectorAll("#columns .column").length === 6', 'the lanes');
  if (!holdSnapshot) await waitFor('loadedContext === contextKey()', 'the first card snapshot');
}
const onCard = (id, selector) => `document.querySelector('.card[data-id="${id}"] ${selector}')`;
const cardText = (id, selector) => evaluate(`${onCard(id, selector)}?.textContent ?? null`);

before(async () => {
  const chrome = process.env.CHROME_PATH || 'C:/Program Files/Google/Chrome/Application/chrome.exe';
  assert.ok(fs.existsSync(chrome), 'Set CHROME_PATH to a Chromium browser');
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  base = `http://127.0.0.1:${server.address().port}/`;
  profile = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-board-'));
  browser = spawn(chrome, ['--headless=new', '--disable-gpu', '--no-first-run', '--no-default-browser-check', '--remote-debugging-port=0', `--user-data-dir=${profile}`, 'about:blank'], { stdio: 'ignore', windowsHide: true });
  let port;
  for (let i = 0; i < 100 && !port; i++) {
    try { port = fs.readFileSync(path.join(profile, 'DevToolsActivePort'), 'utf8').split('\n')[0]; } catch (error) { if (!['ENOENT', 'EBUSY', 'EPERM'].includes(error.code)) throw error; }
    await sleep(100);
  }
  assert.ok(port, 'Chrome debugging endpoint starts');
  const targets = await (await fetch(`http://127.0.0.1:${port}/json`)).json();
  socket = new WebSocket(targets.find((t) => t.type === 'page').webSocketDebuggerUrl);
  await new Promise((resolve) => socket.addEventListener('open', resolve, { once: true }));
  socket.addEventListener('message', (event) => {
    const data = JSON.parse(event.data);
    if (data.id) { const handler = pending.get(data.id); pending.delete(data.id); if (data.error) handler.reject(new Error(data.error.message)); else handler.resolve(data.result); }
    if (data.method === 'Runtime.exceptionThrown') errors.push(data.params.exceptionDetails.exception?.description || data.params.exceptionDetails.text);
  });
  await call('Runtime.enable');
  await call('Page.enable');
  await call('Emulation.setDeviceMetricsOverride', { width: 1440, height: 900, deviceScaleFactor: 1, mobile: false });
});

after(async () => {
  socket?.close(); browser?.kill(); server.closeAllConnections(); server.close();
  await sleep(200);
  try { fs.rmSync(profile, { recursive: true, force: true }); } catch {}
});

test('a card shows a status only when it adds something to the column it is in', async () => {
  await openBoard('research');
  assert.equal(await cardText('q1', '.chip'), null, 'Queued in the Queued column');
  assert.equal(await cardText('d1', '.chip'), null, 'Completed in the Done column');
  assert.equal(await cardText('w4', '.chip'), null, 'a working agent in the Running column');
  assert.equal(await cardText('w1', '.chip'), 'Needs approval');
  assert.equal(await cardText('w2', '.chip'), 'Has a question');
  assert.equal(await cardText('f2', '.chip'), 'Cancelled', 'the column holds both failed and cancelled runs');
});

test('a failed card says why it failed, not how far it got', async () => {
  await openBoard('research');
  assert.equal(await cardText('f1', '.last'), 'Rate limited by provider');
  assert.equal(await evaluate(`${onCard('f1', '.last')}.classList.contains('is-error')`), true);
  assert.equal(await cardText('f1', '.chip'), null, 'the error already says it failed');
  assert.equal(await cardText('f2', '.last'), 'step 1 of 6');
  assert.equal(await cardText('r1', '.last'), 'step 3 of 8');
});

test('the meta line is short: who, where, position in the queue, and readable times', async () => {
  await openBoard('research');
  assert.equal(await cardText('q1', '.source'), 'codex · next up');
  assert.equal(await cardText('q2', '.source'), 'codex · 1 ahead');
  assert.equal(await cardText('r1', '.source'), 'claude · pc-a +1');
  assert.equal(await evaluate(`/limit/.test(document.querySelector('[data-col="queued"]').textContent)`), false, 'queue totals stay in the drawer');
  assert.equal(await cardText('r1', '.when'), '2d 7h');
  assert.equal(await evaluate(`${onCard('r1', '.when')}.getAttribute('aria-label')`), 'Running for 2d 7h');
  assert.equal(await cardText('d1', '.when'), '2d 17h', 'not 3916m');
});

test('each card shows what it runs, with the same icons as the graph', async () => {
  await openBoard('research');
  await waitFor(`document.querySelector('.card[data-id="q1"]')`);
  const kind = (id) => evaluate(`document.querySelector('.card[data-id="${id}"] .card-kind').getAttribute('aria-label')`);
  const icon = (id) => evaluate(`document.querySelector('.card[data-id="${id}"] .card-kind path').getAttribute('d')`);
  assert.equal(await kind('q1'), 'Agent');
  assert.equal(await icon('q1'), await evaluate('FlowGraph.ICONS.agent'));
  assert.equal(await kind('w1'), 'Agent', 'a coding session');
  await openBoard('kinds');
  await waitFor(`document.querySelector('.card[data-id="k-flow"]')`);
  assert.equal(await kind('k-flow'), 'Flow');
  assert.equal(await icon('k-flow'), await evaluate('FlowGraph.ICONS.flow'));
  assert.equal(await cardText('k-flow', '.source'), 'mirror-refresh', 'the icon says it is a flow');
  assert.equal(await kind('k-prog'), 'Program');
  assert.equal(await icon('k-prog'), await evaluate('FlowGraph.ICONS.program'));
  assert.equal(await evaluate(`document.querySelector('.card[data-id="k-flow"] .card-kind').classList.contains('live')`), true, 'running work pulses');
});

test('needs-you: the toolbar counts blocked sessions and walks through them, oldest first', async () => {
  await openBoard('research');
  assert.equal(await evaluate(`document.getElementById('needs-you').textContent`), '2 need you');
  assert.equal(await evaluate(`document.querySelectorAll('.card.needs-you').length`), 2, 'a finished session no longer needs anyone');
  const openWith = async () => { await evaluate(`document.getElementById('needs-you').click()`); return evaluate(`document.getElementById('d-title').textContent`); };
  assert.equal(await openWith(), 'Blocked on approval');
  assert.equal(await evaluate(`document.querySelector('.card.is-open').dataset.id`), 'w1', 'the board marks the open card');
  assert.match(await evaluate(`document.querySelector('.session-banner').textContent`), /Permission needed/);
  assert.equal(await openWith(), 'Blocked on a question');
  assert.equal(await evaluate(`document.querySelectorAll('.card.is-open').length`), 1);
  assert.equal(await openWith(), 'Blocked on approval', 'wraps round');
  await evaluate(`document.getElementById('d-close').click()`);
  assert.equal(await evaluate(`document.querySelectorAll('.card.is-open').length`), 0);
});

test('needs-you: the button is hidden when no one is waiting', async () => {
  await openBoard('many');
  await waitFor(`document.querySelectorAll('.card').length > 0`);
  assert.equal(await evaluate(`document.getElementById('needs-you').hidden`), true);
});

test('lanes scroll on their own: the page stays put and each lane keeps its header', async () => {
  await openBoard('research');
  await waitFor(`document.querySelector('.card[data-id="long0"]')`);
  await sleep(200);
  assert.equal(await evaluate(`document.documentElement.scrollHeight <= innerHeight`), true, 'the page has nothing to scroll');
  const result = await evaluate(`(async () => {
    const lane = document.querySelector('[data-col="done"]'), head = lane.querySelector('h2'), top = head.getBoundingClientRect().top;
    lane.scrollTop = 600;
    await new Promise((resolve) => requestAnimationFrame(() => requestAnimationFrame(resolve)));
    return { scrolled: lane.scrollTop, moved: head.getBoundingClientRect().top - top, other: document.querySelector('[data-col="queued"]').scrollTop };
  })()`);
  assert.ok(result.scrolled > 0, 'the lane scrolls');
  assert.equal(result.moved, 0, 'its header stays');
  assert.equal(result.other, 0, 'other lanes do not move');
  assert.equal(await evaluate(`(() => { renderBoard(); return document.querySelector('[data-col="done"]').scrollTop; })()`), result.scrolled, 'a live update leaves the lane where it was');
  const full = await evaluate(`(() => { const a = document.querySelector('[data-col="scheduled"]').getBoundingClientRect(), b = document.querySelector('[data-col="done"]').getBoundingClientRect(); return a.height === b.height && a.bottom <= innerHeight; })()`);
  assert.equal(full, true, 'an empty lane is as tall as a full one, so it is easy to drop on');
});

test('holding a dragged card at the bottom of a lane scrolls the lane', async () => {
  await openBoard('research');
  await waitFor(`document.querySelector('.card[data-id="q1"]')`);
  await sleep(200);
  const at = await evaluate(`(() => { const r = document.querySelector('.card[data-id="q1"]').getBoundingClientRect(), lane = document.querySelector('[data-col="queued"]').getBoundingClientRect(); return { x: r.x + r.width / 2, y: r.y + 10, bottom: lane.bottom - 10 }; })()`);
  await sleep(200); // the first redraws are over
  // Queued is short, so give it something to scroll.
  await evaluate(`document.querySelector('[data-col="queued"]').append(Object.assign(document.createElement('div'), { id: 'tall', style: 'height:2000px' }))`);
  await call('Input.dispatchMouseEvent', { type: 'mouseMoved', x: at.x, y: at.y });
  await call('Input.dispatchMouseEvent', { type: 'mousePressed', x: at.x, y: at.y, button: 'left', buttons: 1, clickCount: 1 });
  await call('Input.dispatchMouseEvent', { type: 'mouseMoved', x: at.x, y: at.y + 10, button: 'left', buttons: 1 });
  await call('Input.dispatchMouseEvent', { type: 'mouseMoved', x: at.x, y: at.bottom, button: 'left', buttons: 1 });
  await waitFor(`document.querySelector('[data-col="queued"]').scrollTop > 40`, 'the lane to scroll under the pointer');
  await call('Input.dispatchKeyEvent', { type: 'keyDown', key: 'Escape', code: 'Escape', windowsVirtualKeyCode: 27 });
  await call('Input.dispatchMouseEvent', { type: 'mouseReleased', x: at.x, y: at.bottom, button: 'left' });
  await waitFor(`!document.body.classList.contains('dragging')`);
});

test('cards measure their real height on small boards, so live updates do not make them jump; big boards skip layout', async () => {
  await openBoard('research');
  await waitFor(`document.querySelector('.card[data-id="q1"]')`);
  await sleep(200);
  const heights = await evaluate(`(async () => {
    const lane = document.querySelector('[data-col="queued"]'), fresh = lane.querySelector('.card').cloneNode(true);
    lane.append(fresh);
    const sync = fresh.getBoundingClientRect().height;
    await new Promise((resolve) => requestAnimationFrame(() => setTimeout(resolve, 50)));
    const later = fresh.getBoundingClientRect().height;
    fresh.remove();
    return { sync, later, many: document.getElementById('columns').classList.contains('many') };
  })()`);
  assert.equal(heights.many, false);
  assert.equal(heights.sync, heights.later);
  await openBoard('many');
  await waitFor(`document.getElementById('columns').classList.contains('many')`, 'the big board class');
  assert.equal(await evaluate(`getComputedStyle(document.querySelector('#columns .card')).contentVisibility`), 'auto');
});

test('an empty board invites a first card, but only once it is known to be empty', async () => {
  holdSnapshot = true;
  await openBoard('empty');
  await sleep(200);
  assert.equal(await evaluate(`document.getElementById('board-empty').hidden`), true, 'still loading: not known to be empty');
  assert.equal(await evaluate(`document.querySelector('[data-col="queued"] .empty').textContent`), 'Loading…', 'lanes appear straight away');
  holdSnapshot = false;
  await openBoard('empty');
  await waitFor(`!document.getElementById('board-empty').hidden`, 'the empty state');
  await evaluate(`document.getElementById('empty-new-card').click()`);
  assert.equal(await evaluate(`document.getElementById('new-dialog').open`), true);
  await evaluate(`document.getElementById('new-dialog').close()`);
  await openBoard('research');
  await waitFor(`document.querySelector('.card')`);
  assert.equal(await evaluate(`document.getElementById('board-empty').hidden`), true);
});

test('a board with scheduled cards is not presented as empty', async () => {
  await openBoard('planned');
  await waitFor(`document.querySelector('[data-schedule="later1"]')`, 'the scheduled card');
  assert.equal(await evaluate(`document.getElementById('board-empty').hidden`), true);
  assert.equal(await evaluate(`document.getElementById('columns').classList.contains('board-empty')`), false);
  assert.equal(await evaluate(`document.querySelector('[data-col="scheduled"] .col-count').textContent`), '1');
});

test('opening a worker job on another board replaces that board\'s schedules too', async () => {
  await openBoard('planned');
  await waitFor(`document.querySelector('[data-schedule="later1"]')`, 'the old board\'s schedule');
  await evaluate(`openJob({ system: 'hatchet', board: 'flowrun', run: 'run1' })`);
  await waitFor(`document.querySelector('[data-schedule="later2"]')`, 'the job board\'s schedule');
  assert.equal(await evaluate(`document.querySelector('[data-schedule="later1"]')`), null);
  await waitFor(`document.getElementById('d-title').textContent === 'Flow run'`, 'the job opens');
  await evaluate(`document.getElementById('d-close').click()`);
});

const rightClick = async (selector) => {
  const at = await evaluate(`(() => { const r = document.querySelector(${JSON.stringify(selector)}).getBoundingClientRect(); return { x: r.x + r.width / 2, y: r.y + r.height / 2 }; })()`);
  await call('Input.dispatchMouseEvent', { type: 'mouseMoved', x: at.x, y: at.y });
  await call('Input.dispatchMouseEvent', { type: 'mousePressed', x: at.x, y: at.y, button: 'right', buttons: 2, clickCount: 1 });
  await call('Input.dispatchMouseEvent', { type: 'mouseReleased', x: at.x, y: at.y, button: 'right', buttons: 0, clickCount: 1 });
  await waitFor(`document.querySelector('.ctx-menu')`, 'the menu');
  return at;
};
const cardlessText = (selector) => evaluate(`document.querySelector(${JSON.stringify(selector)}).textContent`);
const menuLabels = () => evaluate(`[...document.querySelectorAll('.ctx-item span')].map((n) => n.textContent)`);
const choose = (label) => evaluate(`[...document.querySelectorAll('.ctx-item')].find((n) => n.textContent.startsWith(${JSON.stringify(label)})).click()`);
const key = async (name, code = name) => { await call('Input.dispatchKeyEvent', { type: 'keyDown', key: name, code, windowsVirtualKeyCode: { Escape: 27, ArrowDown: 40, Enter: 13 }[name] }); await call('Input.dispatchKeyEvent', { type: 'keyUp', key: name, code }); };

test('right click on a card offers what the engine allows, and a lane offers board actions', async () => {
  await openBoard('research');
  await waitFor(`document.querySelector('.card[data-id="q2"]')`);
  await rightClick('.card[data-id="q1"]');
  assert.deepEqual(await menuLabels(), ['Open', 'Move to top', 'Move to bottom', 'Cancel run', 'Only codex cards', 'Copy title', 'Copy run ID']);
  assert.equal(await evaluate(`[...document.querySelectorAll('.ctx-item')].find((n) => n.textContent === 'Move to top').disabled`), true, 'already on top');
  await choose('Only codex cards');
  assert.equal(await evaluate(`document.getElementById('search').value`), 'agent:codex');
  assert.equal(await evaluate(`document.querySelector('.ctx-menu')`), null, 'choosing closes the menu');
  await evaluate(`(() => { const s = document.getElementById('search'); s.value = ''; s.dispatchEvent(new Event('input')); })()`);
  await waitFor(`document.querySelector('.card[data-id="w3"]')`);
  await rightClick('.card[data-id="w3"]');
  assert.equal((await menuLabels()).includes('Move to top'), false, 'a card with no moves offers none');
  await key('Escape');
  await waitFor(`!document.querySelector('.ctx-menu')`, 'Escape closes it');
  await rightClick('[data-col="scheduled"] .empty');
  assert.deepEqual(await menuLabels(), ['New card', 'Hide done / failed', 'Clear filter']);
  await key('ArrowDown');
  assert.equal(await evaluate(`document.activeElement.textContent`), 'Hide done / failed', 'arrow keys move through it');
  await key('Escape');
  await waitFor(`!document.querySelector('.ctx-menu')`);
});

test('hovering a card shows its details; a title shows as a quick tooltip', async () => {
  await openBoard('research');
  await waitFor(`document.querySelector('.card[data-id="r1"]')`);
  const at = await evaluate(`(() => { const r = document.querySelector('.card[data-id="r1"]').getBoundingClientRect(); return { x: r.x + 20, y: r.y + 20 }; })()`);
  await call('Input.dispatchMouseEvent', { type: 'mouseMoved', x: at.x, y: at.y });
  await waitFor(`document.querySelector('.hover-card.on')`, 'the hover card');
  const text = await evaluate(`document.querySelector('.hover-card').textContent`);
  assert.match(text, /Long run/); assert.match(text, /pc-a/); assert.match(text, /pc-b/); assert.match(text, /step 3 of 8/);
  await call('Input.dispatchMouseEvent', { type: 'mouseMoved', x: 5, y: 5 });
  await waitFor(`!document.querySelector('.hover-card')`, 'moving away hides it');
  const flows = await evaluate(`(() => { const r = document.getElementById('flows-btn').getBoundingClientRect(); return { x: r.x + 10, y: r.y + 10 }; })()`);
  await call('Input.dispatchMouseEvent', { type: 'mouseMoved', x: flows.x, y: flows.y });
  await waitFor(`document.querySelector('.tip.on')`, 'the tooltip');
  assert.match(await evaluate(`document.querySelector('.tip').textContent`), /Create and edit flows/);
  assert.equal(await evaluate(`document.getElementById('flows-btn').hasAttribute('title')`), false, 'the browser tooltip does not show as well');
  await call('Input.dispatchMouseEvent', { type: 'mouseMoved', x: 5, y: 5 });
});

test('a run shows its flow in the same viewer as the editor, at the saved layout, with a run\'s actions', async () => {
  await openBoard('flowrun');
  await waitFor(`document.querySelector('.card[data-id="run1"]')`);
  await evaluate(`document.querySelector('.card[data-id="run1"]').click()`);
  await waitFor(`document.querySelector('#d-dag .fg-canvas .fg-svg .fg-node[data-step="n-b"]')`, 'the shared viewer in the drawer');
  await sleep(300); // the drawer has finished sliding in
  // Viewing is shared: the same tools and step faces as the editor.
  assert.equal(await evaluate(`document.querySelectorAll('#d-dag .fg-tools button[aria-label="Zoom in"]').length`), 1);
  assert.equal(await cardlessText('[data-step="n-b"] .label'), 'b');
  assert.match(await cardlessText('[data-step="n-b"] .sub'), /failed/);
  assert.equal(await evaluate(`document.querySelector('[data-step="n-b"]').dataset.state`), 'stopped');
  // At the layout the flow's author saved, once it has loaded.
  await waitFor(`document.querySelector('[data-step="n-a"]').getAttribute('transform') === 'translate(400,30)'`, 'the saved layout');
  // Editing is not: no connection handles, and steps stay where they are.
  assert.equal(await evaluate(`document.querySelectorAll('#d-dag [data-handle], #d-dag .in').length`), 0);
  const box = await evaluate(`(() => { const r = document.querySelector('[data-step="n-a"] .box').getBoundingClientRect(); return { x: r.x + r.width / 2, y: r.y + r.height / 2 }; })()`);
  await call('Input.dispatchMouseEvent', { type: 'mouseMoved', x: box.x, y: box.y });
  await call('Input.dispatchMouseEvent', { type: 'mousePressed', x: box.x, y: box.y, button: 'left', buttons: 1, clickCount: 1 });
  await call('Input.dispatchMouseEvent', { type: 'mouseMoved', x: box.x + 60, y: box.y + 60, button: 'left', buttons: 1 });
  await call('Input.dispatchMouseEvent', { type: 'mouseReleased', x: box.x + 60, y: box.y + 60, button: 'left', buttons: 0, clickCount: 1 });
  assert.equal(await evaluate(`document.querySelector('[data-step="n-a"]').getAttribute('transform')`), 'translate(400,30)');
  // A step's right-click menu is about the run; nothing in it edits the flow.
  await rightClick('[data-step="n-b"] .box');
  assert.deepEqual(await menuLabels(), ['Show output and logs', 'Restart step', 'Open in Hatchet', 'Copy step name', 'Copy run ID', 'Edit this flow']);
  assert.equal(await evaluate(`[...document.querySelectorAll('.ctx-item')].find((n) => n.textContent === 'Restart step').disabled`), true, 'this engine can\'t replay it');
  await choose('Show output and logs');
  await waitFor(`/Exit code 2/.test(document.getElementById('d-node').textContent)`, 'the step panel');
  assert.equal(await evaluate(`document.querySelector('[data-step="n-b"]').classList.contains('selected')`), true);
  // Hover details come from the run: what happened, not how it's configured.
  assert.equal(await evaluate(`(() => { const r = document.querySelector('[data-step="n-b"] .box').getBoundingClientRect(); const top = document.elementFromPoint(r.x + 30, r.y + 20); return !!top.closest('[data-step="n-b"]'); })()`), true, 'the step panel never covers the graph');
  await call('Input.dispatchMouseEvent', { type: 'mouseMoved', x: 5, y: 5 });
  const a = await evaluate(`(() => { const r = document.querySelector('[data-step="n-b"] .box').getBoundingClientRect(); return { x: r.x + 30, y: r.y + 20 }; })()`);
  await call('Input.dispatchMouseEvent', { type: 'mouseMoved', x: a.x, y: a.y });
  await waitFor(`document.querySelector('.hover-card.on')`, 'the hover card');
  assert.match(await evaluate(`document.querySelector('.hover-card').textContent`), /Exit code 2.*codex/s);
  await call('Input.dispatchMouseEvent', { type: 'mouseMoved', x: 5, y: 5 });
  // "Edit this flow" goes to the editor, which has the same graph with the edits.
  await rightClick('#d-dag .fg-svg');
  assert.ok((await menuLabels()).includes('Edit this flow'));
  await key('Escape');
  await waitFor(`!document.querySelector('.ctx-menu')`);
  await evaluate(`document.getElementById('d-close').click()`);
});

test('work on a worker that can\'t be reached is not shown as live', async () => {
  await openBoard('links');
  await waitFor(`document.querySelector('.card[data-id="on-dead"] .chip')?.textContent === 'Worker offline'`, 'the offline card');
  assert.equal(await cardText('on-live', '.chip'), 'Needs approval');
  assert.equal(await cardText('on-lost', '.chip'), 'Connection unknown', 'not registered: can\'t be checked');
  assert.equal(await evaluate(`document.querySelector('.card[data-id="run-dead"] .card-kind').classList.contains('live')`), false, 'no pulse');
  assert.equal(await evaluate(`document.querySelector('.card[data-id="run-ok"] .card-kind').classList.contains('live')`), true, 'an offline PC only matters for steps still on it');
  assert.equal(await evaluate(`document.querySelector('.card[data-id="on-live"]').classList.contains('needs-you')`), true);
  assert.equal(await evaluate(`document.querySelector('.card[data-id="on-dead"]').classList.contains('needs-you')`), false, 'nothing you could do would reach it');
  assert.equal(await evaluate(`document.getElementById('needs-you').textContent`), '2 need you');
  // Its drawer says so and takes no input; a chip shows the PC and leads to it.
  await evaluate(`document.querySelector('.card[data-id="on-dead"]').click()`);
  await waitFor(`/pc-off is offline/.test(document.querySelector('.session-banner')?.textContent || '')`, 'the banner');
  assert.equal(await evaluate(`document.querySelector('.chat-composer textarea').disabled`), true);
  assert.match(await evaluate(`document.querySelector('.chat-composer textarea').placeholder`), /Waiting for pc-off to reconnect/);
  assert.equal(await evaluate(`document.querySelector('#d-state .worker-chip').textContent`), 'pc-off');
  assert.equal(await evaluate(`document.querySelector('#d-state .worker-chip').classList.contains('offline')`), true);
  await evaluate(`document.getElementById('d-close').click()`);
  // An online worker's session stays usable.
  await evaluate(`document.querySelector('.card[data-id="run-dead"]').click()`);
  await waitFor(`document.getElementById('d-title').textContent === 'run-dead'`);
  await evaluate(`document.getElementById('d-close').click()`);
  await evaluate(`document.querySelector('.card[data-id="on-live"]').click()`);
  await waitFor(`/Permission needed/.test(document.querySelector('.session-banner')?.textContent || '')`, 'a live session');
  assert.equal(await evaluate(`document.querySelector('.chat-composer textarea').disabled`), false);
  await evaluate(`document.getElementById('d-close').click()`);
});

test('a step shows its worker; a step that has not started says so rather than loading forever', async () => {
  await openBoard('flowrun');
  await waitFor(`document.querySelector('.card[data-id="run1"]')`);
  await evaluate(`document.querySelector('.card[data-id="run1"]').click()`);
  await waitFor(`document.querySelector('[data-step="n-c"]')`, 'the graph');
  assert.match(await evaluate(`document.querySelector('[data-step="n-a"] .sub').textContent`), /^pc-on · /);
  await evaluate(`selectNode('n-a')`);
  await waitFor(`document.querySelector('#d-node .worker-chip')?.textContent === 'pc-on'`, 'the step panel names its worker');
  await evaluate(`selectNode('n-c')`);
  await waitFor(`/hasn't started yet/.test(document.getElementById('d-node').textContent)`, 'the pending step');
  assert.equal(await evaluate(`document.querySelectorAll('#d-node .spinner').length`), 0);
  await evaluate(`document.getElementById('d-close').click()`);
});

test('clicking outside the drawer closes it, but not when switching cards, selecting text or dismissing a menu', async () => {
  await openBoard('research');
  await waitFor(`document.querySelector('.card[data-id="q1"]')`);
  const isOpen = () => evaluate(`document.getElementById('drawer').classList.contains('open')`);
  const centreOf = (selector) => evaluate(`(() => { const r = document.querySelector(${JSON.stringify(selector)}).getBoundingClientRect(); return { x: r.x + r.width / 2, y: r.y + r.height / 2 }; })()`);
  const press = (type, at, buttons) => call('Input.dispatchMouseEvent', { type, x: at.x, y: at.y, button: 'left', buttons, clickCount: 1 });
  const leftClick = async (at) => { await call('Input.dispatchMouseEvent', { type: 'mouseMoved', x: at.x, y: at.y }); await press('mousePressed', at, 1); await press('mouseReleased', at, 0); await sleep(30); };
  const lane = await evaluate(`(() => { const r = document.querySelector('[data-col="scheduled"]').getBoundingClientRect(); return { x: r.x + r.width / 2, y: r.bottom - 40 }; })()`);
  await leftClick(await centreOf('.card[data-id="q1"]'));
  await waitFor(`document.getElementById('d-title').textContent === 'Queued first'`, 'the drawer opens');
  // Another card switches to it, and the toolbar's "need you" walks to the next one.
  await leftClick(await centreOf('.card[data-id="q2"]'));
  await waitFor(`document.getElementById('d-title').textContent === 'Queued second'`, 'the drawer switches');
  assert.equal(await isOpen(), true);
  await leftClick(await centreOf('#needs-you'));
  assert.equal(await isOpen(), true);
  await leftClick(await centreOf('.card[data-id="q2"]'));
  // Clicks inside it, and selecting from inside to outside, leave it open.
  await leftClick(await centreOf('#d-title'));
  assert.equal(await isOpen(), true);
  const title = await centreOf('#d-title');
  await press('mousePressed', title, 1);
  await call('Input.dispatchMouseEvent', { type: 'mouseMoved', x: lane.x, y: lane.y, button: 'left', buttons: 1 });
  await press('mouseReleased', lane, 0);
  await sleep(30);
  assert.equal(await isOpen(), true, 'selecting text and letting go outside');
  // A right click opens a menu; clicking away closes the menu first, and the drawer only on the next click.
  await rightClick('[data-col="scheduled"] .empty');
  assert.equal(await isOpen(), true, 'right click');
  await leftClick(lane);
  await waitFor(`!document.querySelector('.ctx-menu')`, 'the menu closes');
  assert.equal(await isOpen(), true, 'dismissing the menu');
  // Anywhere else closes it.
  await leftClick(lane);
  await waitFor(`!document.getElementById('drawer').classList.contains('open')`, 'the drawer closes');
  assert.equal(await evaluate(`document.querySelectorAll('.card.is-open').length`), 0);
  // With nothing open, clicking around does nothing.
  await leftClick(lane);
  assert.equal(await isOpen(), false);
});

test('no uncaught page errors', () => {
  assert.deepEqual(errors, []);
});
