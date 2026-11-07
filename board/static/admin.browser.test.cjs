/* Real Chromium checks for Admin engine URL settings and live status.
 * node --test board/static/admin.browser.test.cjs
 * CHROME_PATH may override the installed Chrome executable. No packages or backend needed.
 */
const { test, before, after } = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const os = require('node:os');
const { spawn } = require('node:child_process');

const iso = () => new Date().toISOString();
const ENGINE_IDS = ['hatchet', 'trigger', 'trigger-dev', 'prefect'];
const records = new Map();
let calls = [];
function reset() {
  calls = [];
  records.clear();
  records.set('hatchet', { id: 'hatchet', label: 'Hatchet', enabled: true, api_url: 'http://hatchet.internal:8080', dashboard_url: 'http://hatchet.internal:8080/ui', status: 'reachable', checked_at: iso(), error: null });
  records.set('trigger', { id: 'trigger', label: 'Trigger.dev production', enabled: true, api_url: 'https://trigger.example.com', dashboard_url: 'https://cloud.trigger.dev', status: 'unreachable', checked_at: iso(), error: 'Connection refused' });
  records.set('trigger-dev', { id: 'trigger-dev', label: 'Trigger.dev local', enabled: false, api_url: 'http://127.0.0.1:8030', dashboard_url: 'http://127.0.0.1:8030', status: 'disabled', checked_at: null, error: null });
  records.set('prefect', { id: 'prefect', label: 'Prefect', enabled: true, api_url: 'http://prefect.internal:4200/api', dashboard_url: 'http://prefect.internal:4200', status: 'unreachable', checked_at: iso(), error: 'Service unavailable' });
}
reset();
const STATIC = ['index.html', 'overlays.js', 'chat.js', 'chat.css', 'card-search.js', 'flow-graph.js', 'flow-graph.css', 'flow-builder.js', 'flow-builder.css',
  'fleet.js', 'fleet.css', 'admin.js', 'admin.css', 'new-card.js', 'new-card.css'];
const server = http.createServer(async (request, response) => {
  const url = new URL(request.url, 'http://localhost');
  let raw = '';
  for await (const chunk of request) raw += chunk;
  const body = raw ? JSON.parse(raw) : undefined;
  const json = (status, data) => { response.writeHead(status, { 'Content-Type': 'application/json' }); response.end(JSON.stringify(data)); };
  if (url.pathname.startsWith('/api/admin/engines')) calls.push({ method: request.method, path: url.pathname, body });
  try {
    if (url.pathname === '/api/admin/engines' && request.method === 'GET') return json(200, { engines: ENGINE_IDS.map((id) => ({ ...records.get(id) })) });
    let match = url.pathname.match(/^\/api\/admin\/engines\/([^/]+)(\/check)?$/);
    if (match) {
      const [, encodedId, check] = match;
      const id = decodeURIComponent(encodedId);
      const record = records.get(id);
      if (!record) return json(404, { error: 'Unknown engine' });
      if (request.method === 'PUT' && !check) {
        record.api_url = body.api_url;
        record.dashboard_url = body.dashboard_url;
        record.checked_at = iso();
        return json(200, { ...record });
      }
      if (request.method === 'POST' && check) {
        if (!record.enabled) return json(409, { error: 'Engine is disabled' });
        record.status = 'reachable';
        record.checked_at = iso();
        record.error = null;
        return json(200, { ...record });
      }
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
    if (url.pathname === '/api/fleet/tags') return json(200, { tags: [] });
    if (url.pathname.startsWith('/api/')) return json(404, { error: 'not in fixture' });
    const file = url.pathname === '/' ? 'index.html' : url.pathname.replace('/static/', '');
    if (!STATIC.includes(file)) { response.writeHead(404).end(); return; }
    response.writeHead(200, { 'Content-Type': file.endsWith('.js') ? 'text/javascript' : file.endsWith('.css') ? 'text/css' : 'text/html' });
    response.end(fs.readFileSync(path.join(__dirname, file)));
  } catch (error) { json(500, { error: error.message }); }
});

const errors = [];
let browser, profile, socket, call, evaluate, waitFor, base;
before(async () => {
  reset();
  const chromePath = process.env.CHROME_PATH || 'C:/Program Files/Google/Chrome/Application/chrome.exe';
  assert.ok(fs.existsSync(chromePath), 'Set CHROME_PATH to a Chromium browser');
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  base = `http://127.0.0.1:${server.address().port}`;
  profile = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-board-admin-'));
  browser = spawn(chromePath, ['--headless=new', '--disable-gpu', '--no-first-run', '--no-default-browser-check', '--remote-debugging-port=0', `--user-data-dir=${profile}`, 'about:blank'], { stdio: 'ignore', windowsHide: true });
  let port;
  for (let i = 0; i < 300 && !port; i++) {
    try { const text = fs.readFileSync(path.join(profile, 'DevToolsActivePort'), 'utf8'); if (text.includes('\n')) port = text.split('\n')[0]; } catch {}
    if (!port) await new Promise((resolve) => setTimeout(resolve, 100));
  }
  assert.ok(port, 'Chrome debugging endpoint starts');
  const targets = await (await fetch(`http://127.0.0.1:${port}/json`)).json();
  socket = new WebSocket(targets.find((target) => target.type === 'page').webSocketDebuggerUrl);
  await new Promise((resolve) => socket.addEventListener('open', resolve, { once: true }));
  let id = 0;
  const pending = new Map();
  socket.addEventListener('message', (event) => {
    const data = JSON.parse(event.data);
    if (data.id) {
      const handler = pending.get(data.id);
      pending.delete(data.id);
      if (data.error) handler.reject(new Error(data.error.message));
      else handler.resolve(data.result);
    }
    if (data.method === 'Runtime.exceptionThrown') errors.push(data.params.exceptionDetails.text + ': ' + (data.params.exceptionDetails.exception?.description || ''));
  });
  call = (method, params = {}) => new Promise((resolve, reject) => {
    const key = ++id;
    pending.set(key, { resolve, reject });
    socket.send(JSON.stringify({ id: key, method, params }));
  });
  evaluate = async (expression) => {
    const result = await call('Runtime.evaluate', { expression, awaitPromise: true, returnByValue: true });
    if (result.exceptionDetails) throw new Error(`${expression}: ${result.exceptionDetails.exception?.description || result.exceptionDetails.text}`);
    return result.result.value;
  };
  waitFor = async (expression, label = expression) => {
    for (let i = 0; i < 120; i++) {
      if (await evaluate(expression)) return;
      await new Promise((resolve) => setTimeout(resolve, 75));
    }
    throw new Error(`Timed out: ${label}`);
  };
  await call('Runtime.enable');
  await call('Page.enable');
  await call('Emulation.setDeviceMetricsOverride', { width: 1440, height: 1000, deviceScaleFactor: 1, mobile: false });
  await call('Page.navigate', { url: base });
  await waitFor('typeof Admin === "object" && document.querySelector("#admin-btn")');
  await evaluate('Admin.config.pollMs = 120');
});
after(async () => {
  if (socket) socket.close();
  browser?.kill();
  server.closeAllConnections();
  await new Promise((resolve) => server.close(resolve));
  try { fs.rmSync(profile, { recursive: true, force: true }); } catch {}
});

const clickAt = async (x, y) => {
  await call('Input.dispatchMouseEvent', { type: 'mouseMoved', x, y });
  await call('Input.dispatchMouseEvent', { type: 'mousePressed', x, y, button: 'left', clickCount: 1 });
  await call('Input.dispatchMouseEvent', { type: 'mouseReleased', x, y, button: 'left', clickCount: 1 });
};
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const js = (value) => JSON.stringify(value);
const q = (selector) => `document.querySelector(${js(selector)})`;
const countGets = () => calls.filter((entry) => entry.method === 'GET' && entry.path === '/api/admin/engines').length;
const lastCall = (method, path) => calls.filter((entry) => entry.method === method && entry.path === path).at(-1);
const waitForSaved = async (id, body) => {
  for (let i = 0; i < 100; i++) {
    const record = records.get(id);
    if (record?.api_url === body.api_url && record?.dashboard_url === body.dashboard_url) return;
    await sleep(10);
  }
  throw new Error(`Timed out: ${id} URL settings were not saved`);
};

// ---------------------------------------------------------------- tests
test('Admin saves URLs, checks status, polls while open, respects disabled engines and stops when hidden or closed', async () => {
  assert.deepEqual(await evaluate('(() => { const svg = document.querySelector("#admin-btn svg"); return { gear: svg.querySelector("path")?.getAttribute("d").startsWith("M6.7 1.2"), viewBox: svg.getAttribute("viewBox") }; })()'),
    { gear: true, viewBox: '-1 -1 18 18' }, 'gear shape has padding so its outer teeth are not clipped');
  await evaluate('document.querySelector("#admin-btn").click()');
  await waitFor('document.querySelector("#admin-dialog").open && document.querySelectorAll(".ad-engine").length === 4', 'Admin opens with all four engines');
  await waitFor(`${q('.ad-engine[data-engine="hatchet"] .ad-state')}?.textContent === "Reachable"`, 'initial status loads');
  assert.equal(await evaluate('[...document.querySelectorAll(".ad-engine")].map(row => row.querySelector(".ad-engine-title").textContent.trim()).join("|")'),
    'Hatchet|Trigger.dev · production|Trigger.dev · local|Prefect');
  assert.equal(await evaluate(`${q('.ad-engine[data-engine="trigger"] .ad-state')}?.textContent`), 'Unreachable');
  assert.equal(await evaluate(`${q('.ad-engine[data-engine="trigger-dev"] .ad-state')}?.textContent`), 'Disabled');
  assert.equal(await evaluate(`${q('.ad-engine[data-engine="trigger-dev"] [data-action="check"]')}?.disabled`), true, 'disabled engines cannot be checked');
  assert.ok(await evaluate(`${q('.ad-engine[data-engine="hatchet"] .ad-checked')}?.textContent.includes("Last checked:")`));
  assert.equal(await evaluate('document.querySelectorAll("#admin-dialog input[type=password]").length'), 0, 'there are no credential inputs');
  assert.equal(await evaluate('document.querySelectorAll("#admin-dialog input").length'), 8, 'only API and dashboard URLs are editable');

  await evaluate(`(() => {
    const row = document.querySelector('.ad-engine[data-engine="hatchet"]');
    const api = row.querySelector('input[aria-label="Hatchet API URL"]');
    const dashboard = row.querySelector('input[aria-label="Hatchet dashboard URL"]');
    api.value = 'https://hatchet-new.example/api'; api.dispatchEvent(new Event('input', { bubbles: true }));
    dashboard.value = 'https://hatchet-new.example/'; dashboard.dispatchEvent(new Event('input', { bubbles: true }));
    row.querySelector('[data-action="save"]').click();
  })()`);
  await waitForSaved('hatchet', { api_url: 'https://hatchet-new.example/api', dashboard_url: 'https://hatchet-new.example/' });
  const saved = lastCall('PUT', '/api/admin/engines/hatchet');
  assert.deepEqual(saved.body, { api_url: 'https://hatchet-new.example/api', dashboard_url: 'https://hatchet-new.example/' }, 'the save sends only the two URL settings');
  assert.equal(await evaluate(`${q('.ad-engine[data-engine="hatchet"] input[aria-label="Hatchet API URL"]')}?.value`), 'https://hatchet-new.example/api');

  await evaluate(`document.querySelector('.ad-engine[data-engine="prefect"] [data-action="check"]').click()`);
  await waitFor(`${q('.ad-engine[data-engine="prefect"] .ad-state')}?.textContent === "Reachable"`, 'manual check returns and displays a fresh status');
  assert.ok(lastCall('POST', '/api/admin/engines/prefect/check'));
  assert.equal(lastCall('POST', '/api/admin/engines/prefect/check').body, undefined);
  assert.equal(lastCall('POST', '/api/admin/engines/trigger-dev/check'), undefined, 'disabled engines are not checked');

  records.get('hatchet').status = 'unreachable';
  records.get('hatchet').checked_at = iso();
  records.get('hatchet').error = 'Connection lost';
  await waitFor(`${q('.ad-engine[data-engine="hatchet"] .ad-state')}?.textContent === "Unreachable"`, 'open dialog polls updated engine status');
  assert.ok(countGets() >= 2, 'opening and polling issue GET requests');

  await evaluate('Object.defineProperty(document, "hidden", { configurable: true, value: true }); document.dispatchEvent(new Event("visibilitychange"))');
  await sleep(250);
  const hiddenGets = countGets();
  await sleep(350);
  assert.equal(countGets(), hiddenGets, 'polling stops while the document is hidden');
  const beforeResume = countGets();
  await evaluate('Object.defineProperty(document, "hidden", { configurable: true, value: false }); document.dispatchEvent(new Event("visibilitychange"))');
  for (let i = 0; i < 40 && countGets() <= beforeResume; i++) await sleep(25);
  assert.ok(countGets() > beforeResume, 'polling resumes when the document becomes visible');
  await waitFor('document.querySelector("#admin-dialog").open', 'Admin remains open after visibility resumes');
});

test('Admin polling stops when the dialog closes and the dialog never renders credential data', async () => {
  // The prior test resumes visibility; opening again exercises the fresh load and close lifecycle.
  if (!(await evaluate('document.querySelector("#admin-dialog").open'))) await evaluate('document.querySelector("#admin-btn").click()');
  await waitFor('document.querySelector("#admin-dialog").open');
  await evaluate('document.querySelector("#admin-dialog .ad-actions .close").click()');
  await waitFor('!document.querySelector("#admin-dialog").open', 'Admin closes');
  await sleep(350);
  const page = await evaluate('document.querySelector("#admin-dialog").innerHTML');
  assert.ok(!page.includes('HATCHET_CLIENT_TOKEN') && !page.includes('TRIGGER_SECRET_KEY') && !page.includes('PREFECT_API_KEY'));
  const gets = countGets();
  await sleep(350);
  assert.equal(countGets(), gets, 'polling stops after close');
  await evaluate('document.querySelector("#admin-btn").click()');
  await waitFor('document.querySelector("#admin-dialog").open', 'Admin reopens');
  const box = await evaluate('document.querySelector("#admin-dialog").getBoundingClientRect().toJSON()');
  await clickAt(Math.max(1, box.x - 20), box.y + box.height / 2);
  await waitFor('!document.querySelector("#admin-dialog").open', 'outside click closes Admin');
  assert.deepEqual(errors, []);
});

test('clicking outside every main app dialog closes it', async () => {
  const cases = [
    ['#new-card', '#new-dialog'],
    ['#flows-btn', '#flows-dialog'],
    ['#fleet-btn', '#fleet-dialog'],
    ['#admin-btn', '#admin-dialog'],
  ];
  for (const [button, selector] of cases) {
    await evaluate(`document.querySelector(${js(button)}).click()`);
    await waitFor(`document.querySelector(${js(selector)}).open`, `${selector} opens`);
    const box = await evaluate(`document.querySelector(${js(selector)}).getBoundingClientRect().toJSON()`);
    await clickAt(Math.max(1, box.x - 20), box.y + box.height / 2);
    await waitFor(`!document.querySelector(${js(selector)}).open`, `${selector} closes on backdrop click`);
  }
  assert.deepEqual(errors, []);
});
test('mobile Admin dialog renders every engine and keeps the settings list scrollable', async () => {
  await call('Emulation.setDeviceMetricsOverride', { width: 390, height: 844, deviceScaleFactor: 1, mobile: true });
  await evaluate('document.querySelector("#admin-btn").click()');
  await waitFor('document.querySelector("#admin-dialog").open && document.querySelectorAll(".ad-engine").length === 4', 'Admin loads all engine settings');
  await waitFor(`${q('.ad-status')}?.textContent.includes("refreshed")`, 'engine status loads');
  const layout = await evaluate(`(() => {
    const dialog = document.querySelector('#admin-dialog');
    const body = dialog.querySelector('.ad-body');
    const rows = [...dialog.querySelectorAll('.ad-engine')];
    return { dialog: dialog.getBoundingClientRect().toJSON(), bodyHeight: body.clientHeight, bodyScrollHeight: body.scrollHeight,
      rows: rows.map((row) => ({ visible: row.getBoundingClientRect().width > 0, name: row.querySelector('.ad-engine-title').textContent.trim() })) };
  })()`);
  assert.equal(layout.dialog.width, 390);
  assert.ok(layout.bodyHeight > 0 && layout.bodyScrollHeight > layout.bodyHeight, 'engine list can scroll within the viewport');
  assert.deepEqual(layout.rows.map((row) => row.name), ['Hatchet', 'Trigger.dev · production', 'Trigger.dev · local', 'Prefect']);
  assert.ok(layout.rows.every((row) => row.visible), 'all engine rows render');
  await evaluate('document.querySelector("#admin-dialog .ad-actions .close").click()');
  await waitFor('!document.querySelector("#admin-dialog").open');
  assert.deepEqual(errors, []);
});
