/* Real Chromium checks for the flow builder against a local fake of the flow-documents API
 * (docs/superpowers/specs/2026-11-03-workflow-builder-api.md).
 * node --test board/static/flow-builder.browser.test.cjs
 * CHROME_PATH may override the installed Chrome executable; FLOW_SCREENSHOTS=<dir> saves screenshots.
 */
const { test, before, after } = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const os = require('node:os');
const crypto = require('node:crypto');
const { spawn } = require('node:child_process');
const fake = require('./placement-fake.cjs');

// ---------------------------------------------------------------- fake flow host
// JSON is valid YAML, so the fake serialises documents as JSON. Real flow files use a tiny
// block-YAML reader that covers the subset flows/*.yaml use.
function scalar(text) {
  if (/^\[.*\]$/.test(text)) return text.slice(1, -1).split(',').map((x) => x.trim()).filter(Boolean).map(scalar);
  if (text.startsWith('[') || text.startsWith('{')) throw new Error(`unclosed ${text[0]}`);
  if (text === 'true' || text === 'false') return text === 'true';
  if (/^-?\d+(\.\d+)?$/.test(text)) return Number(text);
  if (/^(["']).*\1$/.test(text)) return text.slice(1, -1);
  return text.replace(/\s+#.*$/, '');
}
function miniYaml(text) {
  const lines = text.split('\n'), root = {}, stack = [{ indent: -1, obj: root }];
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    if (!line.trim() || line.trim().startsWith('#')) continue;
    const indent = line.match(/^ */)[0].length;
    const m = line.trim().match(/^([A-Za-z0-9_-]+):(?:\s+(.*))?$/);
    if (!m) throw new Error(`line ${i + 1}: expected "key: value"`);
    while (stack.at(-1).indent >= indent) stack.pop();
    const parent = stack.at(-1).obj, key = m[1], rest = (m[2] || '').trim();
    if (key in parent) throw new Error(`line ${i + 1}: duplicate key ${key}`);
    if (rest === '') { parent[key] = {}; stack.push({ indent, obj: parent[key] }); }
    else if (rest === '|') {
      const block = []; let j = i + 1, base = null;
      for (; j < lines.length; j++) {
        if (!lines[j].trim()) { block.push(''); continue; }
        const at = lines[j].match(/^ */)[0].length;
        if (at <= indent) break;
        base ??= at; block.push(lines[j].slice(base));
      }
      while (block.length && block.at(-1) === '') block.pop();
      parent[key] = `${block.join('\n')}\n`; i = j - 1;
    } else parent[key] = scalar(rest);
  }
  return root;
}
const serialize = (doc) => `${JSON.stringify(doc, null, 2)}\n`;
const revisionOf = (yaml) => `sha256:${crypto.createHash('sha256').update(yaml, 'utf8').digest('hex')}`;
function parse(yaml) {
  let doc;
  try { doc = yaml.trim().startsWith('{') ? JSON.parse(yaml) : miniYaml(yaml); }
  catch (error) { return { document: null, errors: [{ message: `YAML syntax error: ${error.message}`, step: null, field: null }], lossless: false }; }
  if (!doc || typeof doc !== 'object' || Array.isArray(doc)) return { document: null, errors: [{ message: 'A flow must be a mapping', step: null, field: null }], lossless: false };
  return { document: doc, errors: validate(doc), lossless: serialize(doc) === yaml };
}
function validate(doc) {
  const errors = [], err = (step, field, message) => errors.push({ message, step, field });
  if (!/^[a-z][a-z0-9-]{0,47}$/.test(doc.name || '')) err(null, 'name', 'name must be lowercase letters, digits and -');
  const steps = doc.steps || {}, needs = (id) => (Array.isArray(steps[id]?.needs) ? steps[id].needs : []);
  const reaches = (from, target, seen = new Set()) => needs(from).some((n) => n === target || (!seen.has(n) && seen.add(n) && reaches(n, target, seen)));
  for (const [id, step] of Object.entries(steps)) {
    for (const n of needs(id)) if (n === id) err(id, 'needs', `${id} cannot depend on itself`); else if (!steps[n]) err(id, 'needs', `needs unknown step ${n}`);
    if (needs(id).every((n) => n !== id) && reaches(id, id)) err(id, 'needs', `${id} is part of a dependency cycle`);
    if (step.kind === 'agent' && !step.prompt) err(id, 'prompt', 'an agent step needs a prompt');
    for (const key of ['run', 'prompt']) for (const m of String(step[key] ?? '').matchAll(/\{\{\s*(input|steps)\.([A-Za-z0-9_]+)/g)) {
      if (m[1] === 'input' && !(m[2] in (doc.input || {}))) err(id, key, `{{ input.${m[2]} }} is not a declared input`);
      if (m[1] === 'steps' && !needs(id).includes(m[2])) err(id, key, `{{ steps.${m[2]}… }} must name a step listed in needs`);
    }
  }
  return errors;
}

const MULTI_YAML = fs.readFileSync(path.join(__dirname, '..', '..', 'flows', 'multi-agent-design.yaml'), 'utf8');
const store = new Map();   // name -> {yaml | null, active | null, layout}
const calls = [];
const fault = { save: null, addRunOnActivate: false };
const runs = { hatchet: [{ system: 'hatchet', board: 'research', id: 'run-1', title: 'Design status page', status: 'RUNNING' }], prefect: [], trigger: null };
function reset() {
  store.clear(); calls.length = 0; fault.save = null; fault.addRunOnActivate = false; runs.hatchet.length = 1;
  store.set('multi-agent-design', { yaml: MULTI_YAML, active: MULTI_YAML, layout: null });
  store.set('other-flow', { yaml: serialize({ name: 'other-flow', description: 'Saved, never activated', input: { count: 2, fast: true, note: null },
    environment: { agent: 'codex', files: [{ source: 'docs/', target: 'context/' }] },
    steps: { only: { kind: 'program', run: 'echo hi', needs: [] }, helper: { kind: 'agent', needs: ['only'], agent: 'claude', prompt: 'Help', setup: { packages: ['jq'] }, future_key: 1 } } }), active: null, layout: null });
  store.set('old-flow', { yaml: null, active: serialize({ name: 'old-flow', input: {}, steps: { a: { kind: 'program', run: 'echo' } } }), layout: null });
}
// A removed flow (no draft) reports the active snapshot's YAML so it can be restored.
function docOf(name) {
  const entry = store.get(name), parsed = parse(entry.yaml ?? entry.active);
  const state = entry.yaml == null ? 'removed' : parsed.errors.length || !parsed.document ? 'broken' : !entry.active ? 'inactive' : entry.active === entry.yaml ? 'active' : 'changed';
  return { name, revision: entry.yaml == null ? null : revisionOf(entry.yaml), yaml: entry.yaml ?? entry.active, document: parsed.document, errors: parsed.errors, lossless: parsed.lossless,
    layout: entry.yaml == null ? null : entry.layout, state, activeRevision: entry.active ? revisionOf(entry.active) : null };
}
function previewOf(name) {
  const entry = store.get(name), change = entry.yaml == null ? 'remove' : !entry.active ? 'add' : entry.active === entry.yaml ? 'none' : 'update';
  const affected = change !== 'none';
  const engines = [
    { engine: 'hatchet', action: affected ? 'reload worker' : 'none', available: true, activeRuns: affected ? runs.hatchet.map((r) => ({ ...r })) : [] },
    { engine: 'prefect', action: affected ? 'reload worker' : 'none', available: true, activeRuns: affected ? [] : [] },
    { engine: 'trigger', action: affected ? 'regenerate tasks' : 'none', available: false, activeRuns: affected ? runs.trigger : [] }];
  const disruptive = engines.some((e) => e.action !== 'none' && (e.activeRuns === null || e.activeRuns.length));
  const revision = entry.yaml == null ? null : revisionOf(entry.yaml);
  return { name, revision, activeRevision: entry.active ? revisionOf(entry.active) : null, change, engines, disruptive,
    impact: crypto.createHash('sha256').update(JSON.stringify([revision, engines])).digest('hex').slice(0, 16) };
}
const STATIC = ['index.html', 'chat.js', 'card-search.js', 'chat.css', 'flow-builder.js', 'flow-builder.css', 'fleet.js', 'fleet.css', 'admin.js', 'admin.css', 'new-card.js', 'new-card.css', 'overlays.js', 'flow-graph.js', 'flow-graph.css'];
const server = http.createServer(async (request, response) => {
  const url = new URL(request.url, 'http://localhost');
  let raw = ''; for await (const chunk of request) raw += chunk;
  const body = raw ? JSON.parse(raw) : undefined;
  const json = (status, data) => { response.writeHead(status, { 'Content-Type': 'application/json' }); response.end(JSON.stringify(data)); };
  if (url.pathname.startsWith('/api/')) calls.push({ method: request.method, path: url.pathname, query: url.search, body });
  try {
    const m = url.pathname.match(/^\/api\/flow-documents(?:\/([^/]+))?(?:\/(layout|activation|activate|deactivate))?$/);
    if (m) {
      const [, rawName, sub] = m, name = rawName && decodeURIComponent(rawName);
      if (!name && request.method === 'GET') return json(200, { documents: [...store.keys()].sort().map(docOf) });
      if (name === 'parse' && request.method === 'POST') return json(200, parse(body.yaml));
      if (name === 'serialize' && request.method === 'POST') { const yaml = serialize(body.document); return json(200, { yaml, errors: validate(body.document) }); }
      const entry = store.get(name);
      if (!sub && request.method === 'GET') return entry ? json(200, docOf(name)) : json(404, { error: 'not found' });
      if (!sub && request.method === 'PUT') {
        if (fault.save) return json(fault.save, { error: 'disk full' });
        const parsed = parse(body.yaml);
        if (parsed.document && parsed.document.name !== name) return json(400, { errors: [{ message: `name must be ${name}`, step: null, field: 'name' }] });
        const current = entry?.yaml != null ? revisionOf(entry.yaml) : null;
        if (body.expectedRevision !== current) return json(409, { error: 'conflict', current: entry ? docOf(name) : null });
        store.set(name, { active: null, layout: null, ...entry, yaml: body.yaml });
        return json(200, docOf(name));
      }
      if (sub === 'layout' && request.method === 'PUT') { if (entry?.yaml == null) return json(404, { error: 'no draft' }); entry.layout = body.layout; return json(200, { ok: true }); }
      if (!sub && request.method === 'DELETE') {
        if (url.searchParams.get('revision') !== revisionOf(entry.yaml)) return json(409, { error: 'conflict', current: docOf(name) });
        if (entry.active) entry.yaml = null; else store.delete(name);
        return json(200, { ok: true });
      }
      if (sub === 'activation' && request.method === 'GET') return json(200, url.searchParams.get('disable') ? { ...previewOf(name), change: 'remove', revision: null } : previewOf(name));
      if (sub === 'deactivate' && request.method === 'POST') { entry.active = null; return json(200, { name, activeRevision: null, engines: [{ engine: 'hatchet', state: 'active', error: null }] }); }
      if (sub === 'activate' && request.method === 'POST') {
        if (fault.addRunOnActivate) { fault.addRunOnActivate = false; runs.hatchet.push({ system: 'hatchet', board: 'lab-a', id: 'run-2', title: 'Another run', status: 'RUNNING' }); }
        const pv = previewOf(name);
        if (body.revision !== pv.revision) return json(409, { error: 'conflict', preview: pv });
        if (body.impact !== pv.impact) return json(409, { error: 'impact changed', preview: pv });
        if (pv.disruptive && !body.confirm) return json(409, { error: 'confirmation required', preview: pv });
        entry.active = entry.yaml;
        return json(200, { name, activeRevision: entry.active ? revisionOf(entry.active) : null,
          engines: [{ engine: 'hatchet', state: 'active', error: null }, { engine: 'prefect', state: 'active', error: null }, { engine: 'trigger', state: 'unavailable', error: 'Trigger.dev dev server is not running' }] });
      }
      return json(405, { error: 'unexpected' });
    }
    if (url.pathname === '/api/systems') return json(200, { systems: ['hatchet'], columns: ['scheduled', 'queued', 'running', 'waiting', 'done', 'stopped'] });
    if (url.pathname === '/api/live') {
      response.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache' });
      response.write(`data: ${JSON.stringify({ type: 'feed', live: true })}\n\n`);
      response.write(`data: ${JSON.stringify({ type: 'cards', reset: true, cards: [] })}\n\n`);
      return;
    }
    if (url.pathname === '/api/flows') {
      const flows = [...store.entries()].filter(([, e]) => e.active).map(([name, e]) => { const d = parse(e.active).document; return { name, description: d.description || '', input: d.input || {}, steps: Object.fromEntries(Object.entries(d.steps).map(([id, s]) => [id, { kind: s.kind, needs: s.needs || [] }])) }; });
      return json(200, { flows, broken: {} });
    }
    if (url.pathname === '/api/launch/validate') {
      const active = Object.fromEntries([...store.entries()].filter(([, e]) => e.active).map(([name, e]) => [name, parse(e.active).document]));
      const [status, data] = fake.launchCheck(body, fake.fleet(), active);
      return json(status, data);
    }
    if (url.pathname === '/api/workers') return json(200, { workers: [{ id: 'pc-a', pool: 'gpu', tags: ['linux'], online: true }] });
    if (url.pathname === '/api/schedules') return json(200, { schedules: [] });
    if (url.pathname === '/api/fleet/tags') return json(200, { tags: ['linux'] });
    const file = url.pathname === '/' ? 'index.html' : url.pathname.replace('/static/', '');
    if (!STATIC.includes(file)) { response.writeHead(404).end(); return; }
    response.writeHead(200, { 'Content-Type': file.endsWith('.js') ? 'text/javascript' : file.endsWith('.css') ? 'text/css' : 'text/html' });
    response.end(fs.readFileSync(path.join(__dirname, file)));
  } catch (error) { json(500, { error: error.message }); }
});

// ---------------------------------------------------------------- browser
const errors = [];
let browser, profile, socket, call, evaluate, waitFor, base;
const shots = process.env.FLOW_SCREENSHOTS;
before(async () => {
  reset();
  const chromePath = process.env.CHROME_PATH || 'C:/Program Files/Google/Chrome/Application/chrome.exe';
  assert.ok(fs.existsSync(chromePath), 'Set CHROME_PATH to a Chromium browser');
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  base = `http://127.0.0.1:${server.address().port}`;
  profile = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-board-flows-'));
  browser = spawn(chromePath, ['--headless=new', '--disable-gpu', '--no-first-run', '--no-default-browser-check', '--remote-debugging-port=0', `--user-data-dir=${profile}`, 'about:blank'], { stdio: 'ignore', windowsHide: true });
  let port;
  for (let i = 0; i < 300 && !port; i++) {
    const active = path.join(profile, 'DevToolsActivePort');
    // Chrome may still be writing (and locking) the file on Windows: retry until a full line is there.
    try { const text = fs.readFileSync(active, 'utf8'); if (text.includes('\n')) port = text.split('\n')[0]; } catch {}
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
    for (let i = 0; i < 80; i++) { if (await evaluate(expression)) return; await new Promise((resolve) => setTimeout(resolve, 75)); }
    throw new Error(`Timed out: ${what}`);
  };
  await call('Runtime.enable');
  await call('Page.enable');
  await desktop();
  await call('Page.navigate', { url: base });
  await waitFor('document.readyState === "complete" && typeof FlowBuilder === "object" && document.querySelector("#flows-btn")');
});
after(async () => {
  if (socket) socket.close();
  browser?.kill();
  server.closeAllConnections();
  await new Promise((resolve) => server.close(resolve));
  try { fs.rmSync(profile, { recursive: true, force: true }); } catch {}
});

const desktop = () => call('Emulation.setDeviceMetricsOverride', { width: 1440, height: 1000, deviceScaleFactor: 1, mobile: false });
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const q = (selector) => `document.querySelector(${JSON.stringify(selector)})`;
const js = (value) => JSON.stringify(value);
async function shot(name) {
  if (!shots) return;
  fs.mkdirSync(shots, { recursive: true });
  const image = await call('Page.captureScreenshot', { format: 'png' });
  fs.writeFileSync(path.join(shots, `${name}.png`), Buffer.from(image.data, 'base64'));
}
// Centre of the first element matching selector (optionally whose text includes `text`).
async function centre(selector, text) {
  const r = await evaluate(`(() => { const all = [...document.querySelectorAll(${js(selector)})].filter(n => n.getClientRects().length && (${js(text ?? null)} === null || n.textContent.includes(${js(text ?? '')})));
    const n = all[0]; if (!n) return null; n.scrollIntoView({ block: 'nearest' }); const b = n.getBoundingClientRect(); return { x: b.left + b.width / 2, y: b.top + b.height / 2 }; })()`);
  assert.ok(r, `${selector}${text ? ` "${text}"` : ''} is visible`);
  return r;
}
async function mouse(type, { x, y }, buttons = 1) { await call('Input.dispatchMouseEvent', { type, x, y, button: 'left', buttons, clickCount: 1 }); }
async function click(selector, text) { const at = await centre(selector, text); await mouse('mouseMoved', at, 0); await mouse('mousePressed', at); await mouse('mouseReleased', at, 0); await sleep(30); }
async function drag(from, to) {
  await mouse('mouseMoved', from, 0); await mouse('mousePressed', from);
  for (let i = 1; i <= 8; i++) await mouse('mouseMoved', { x: from.x + ((to.x - from.x) * i) / 8, y: from.y + ((to.y - from.y) * i) / 8 });
  await mouse('mouseReleased', to, 0); await sleep(50);
}
const KEYS = { Enter: [13, '\r'], Tab: [9, ''], Delete: [46, ''], Escape: [27, ''], ArrowRight: [39, ''], ArrowDown: [40, ''], ' ': [32, ' '] };
async function press(key, modifiers = 0) {
  const [code, text] = KEYS[key];
  await call('Input.dispatchKeyEvent', { type: text ? 'keyDown' : 'rawKeyDown', key, code: key === ' ' ? 'Space' : key, windowsVirtualKeyCode: code, text: text || undefined, modifiers });
  await call('Input.dispatchKeyEvent', { type: 'keyUp', key, code: key === ' ' ? 'Space' : key, windowsVirtualKeyCode: code, modifiers });
  await sleep(30);
}
// Replace a field's text the way typing does (focus, select all, insert): fires real input events.
async function type(selector, text) {
  await evaluate(`(() => { const n = ${q(selector)}; n.focus(); n.select?.(); })()`);
  await call('Input.insertText', { text });
}
async function choose(selector, value) { await evaluate(`(() => { const n = ${q(selector)}; n.value = ${js(value)}; n.dispatchEvent(new Event('change', { bubbles: true })); })()`); }
const askOpen = () => evaluate('!!document.querySelector("dialog.fb-ask")?.open');
const askText = () => evaluate('document.querySelector("dialog.fb-ask").textContent');
async function answer(label) { await waitFor(`document.querySelector("dialog.fb-ask")?.open`, 'confirmation dialog'); await click('dialog.fb-ask .fb-ask-actions button', label); }
const nodeY = (id) => evaluate(`Number(document.querySelector('[data-step="${id}"]').getAttribute('transform').match(/,([-\\d.]+)\\)/)[1])`);
const nodeX = (id) => evaluate(`Number(document.querySelector('[data-step="${id}"]').getAttribute('transform').match(/\\(([-\\d.]+),/)[1])`);
// Elements sticking out of a container that should never scroll sideways.
const sideways = (selector) => evaluate(`(() => { const i = document.querySelector(${js(selector)}), r = i.getBoundingClientRect().right;
  return i.scrollWidth <= i.clientWidth ? [] : [...i.querySelectorAll('*')].filter(n => n.getBoundingClientRect().right > r + 0.5).map(n => n.tagName + '.' + n.className + ' ' + (n.textContent || '').slice(0, 40)); })()`);
const activateCalls = () => calls.filter((c) => c.path.endsWith('/activate')).length;
const lastPut = () => calls.filter((c) => c.method === 'PUT' && !c.path.endsWith('/layout')).at(-1);
const saveEnabled = () => evaluate(`![...document.querySelectorAll('.fb-actions button')].find(b => b.textContent === 'Save draft').disabled`);
async function openFlows() {
  await evaluate('document.querySelector("#flows-btn").click()');
  await waitFor('document.querySelector("#flows-dialog").open && document.querySelectorAll(".fb-flow-item").length >= 3', 'builder opens with the flow list');
}

// ---------------------------------------------------------------- tests
test('loads multi-agent-design: states in the list, parallel branches and the fan-in', async () => {
  await openFlows();
  await waitFor('document.querySelectorAll(".fg-node").length === 4', 'four steps drawn');
  assert.equal(await evaluate('document.querySelectorAll(".fg-edge").length'), 4);
  const states = await evaluate('[...document.querySelectorAll(".fb-flow-item")].map(b => b.querySelector(".fb-flow-name").textContent + ":" + b.querySelector(".fb-chip").textContent)');
  assert.deepEqual(states, ['multi-agent-design:Active', 'old-flow:Removed', 'other-flow:Not active']);
  assert.equal(await nodeY('architecture'), await nodeY('risk_review'), 'the two agents run in parallel on one row');
  assert.ok(await nodeY('implementation_plan') > await nodeY('architecture'), 'the join is below both branches');
  assert.ok(await nodeY('architecture') > await nodeY('brief'));
  assert.match(await evaluate(q('[data-step="architecture"]') + '.getAttribute("aria-label")'), /agent step, waits for brief/);
  assert.match(await evaluate(q('.fb-banner') + '.textContent'), /comments or formatting/, 'lossless: false is announced');
  assert.equal(await evaluate(`[...document.querySelectorAll('.fb-actions button')].find(b => b.textContent === 'Run…').disabled`), false, 'an active flow can run');
  assert.equal(await evaluate(`[...document.querySelectorAll('.fb-actions button')].find(b => b.textContent === 'Activate…').disabled`), true, 'nothing to activate');
  assert.equal(await saveEnabled(), false);
  assert.equal(await evaluate('document.querySelector(".fb-yaml").value'), MULTI_YAML);
  await shot('desktop-graph');
});

test('warns once before a graph edit rewrites non-canonical YAML; cancel keeps it', async () => {
  await click('[data-step="architecture"] .box');
  await waitFor(`document.querySelector('.fb-inspector h3')?.textContent === 'Step architecture'`);
  await type('.fb-inspector [data-focus="step:description"]', 'Codex designs');
  await answer('Cancel');
  await waitFor(`document.querySelector('.fb-inspector [data-focus="step:description"]').value === ''`, 'the edit is undone');
  assert.equal(await evaluate('document.querySelector(".fb-yaml").value'), MULTI_YAML);
  assert.equal(await saveEnabled(), false);
  assert.equal(calls.filter((c) => c.path.endsWith('/serialize')).length, 0);
  await type('.fb-inspector [data-focus="step:description"]', 'Codex designs');
  await answer('Edit and rewrite YAML');
  await waitFor(`document.querySelector('.fb-yaml').value.includes('"description": "Codex designs"')`, 'canonical YAML replaces the text');
  assert.equal(await askOpen(), false);
  assert.equal(await saveEnabled(), true, 'the document is dirty');
  assert.match(await evaluate(q('.fb-flow-item[aria-current="true"]') + '.textContent'), /unsaved/);
  await type('.fb-inspector [data-focus="step:description"]', 'Codex designs it');
  await sleep(400);
  assert.equal(await askOpen(), false, 'the warning is shown only once');
});

test('reference picker shows template syntax for inputs and direct prerequisites, and inserts at the caret', async () => {
  await evaluate(`(() => { const t = ${q('.fb-inspector [data-focus="step:prompt"]')}; t.focus(); t.setSelectionRange(0, 0); })()`);
  await click('.fb-inspector .fb-ref-btn', 'Insert reference');
  await waitFor('document.querySelector(".fb-refs")');
  const refs = await evaluate('[...document.querySelectorAll(".fb-refs .fb-ref code")].map(c => c.textContent)');
  assert.deepEqual(refs, ['{{ input.feature }}', '{{ steps.brief.output }}', '{{ steps.brief.output.stdout }}']);
  assert.equal(await evaluate('document.activeElement.classList.contains("fb-ref")'), true, 'focus moves into the picker');
  await click('.fb-refs .fb-ref', '{{ input.feature }}');
  await waitFor(`document.querySelector('.fb-inspector [data-focus="step:prompt"]').value.startsWith('{{ input.feature }}Work as a software architect')`, 'inserted at the caret');
  assert.equal(await evaluate('document.activeElement.dataset.focus'), 'step:prompt');
  assert.equal(await evaluate('document.activeElement.selectionStart'), '{{ input.feature }}'.length);
  await waitFor(`document.querySelector('.fb-yaml').value.includes('{{ input.feature }}Work as')`);
  // A step with two prerequisites offers both outputs and nothing transitive.
  await click('[data-step="implementation_plan"] .box');
  await click('.fb-inspector .fb-ref-btn', 'Insert reference');
  const join = await evaluate('[...document.querySelectorAll(".fb-refs .fb-ref code")].map(c => c.textContent)');
  assert.deepEqual(join, ['{{ input.feature }}', '{{ steps.architecture.output }}', '{{ steps.risk_review.output }}']);
  await press('Escape');
  assert.equal(await evaluate('!!document.querySelector(".fb-refs")'), false, 'Escape closes the picker');
  assert.equal(await evaluate('document.querySelector("#flows-dialog").open'), true, '…without closing the builder');
});

test('unsaved changes guard switching flows and starting a new one', async () => {
  await click('.fb-flow-item', 'other-flow');
  await answer('Keep editing');
  assert.equal(await evaluate(q('.fb-title') + '.textContent'), 'multi-agent-design');
  await click('.fb-new');
  await answer('Keep editing');
  assert.equal(await evaluate(q('.fb-title') + '.textContent'), 'multi-agent-design');
  await click('.fb-new');
  await answer('Discard changes');
  await waitFor(`document.querySelector('dialog.fb-ask')?.open && document.querySelector('dialog.fb-ask').textContent.includes('New flow')`);
  await call('Input.insertText', { text: 'Bad Name' });
  await click('dialog.fb-ask .fb-ask-actions button', 'Create');
  assert.match(await askText(), /Use lowercase letters/);
  await type('dialog.fb-ask input', 'fan-flow');
  await choose('dialog.fb-ask select', 'empty');
  await click('dialog.fb-ask .fb-ask-actions button', 'Create');
  await waitFor(`document.querySelector('.fb-title').textContent === 'fan-flow' && document.querySelectorAll('.fg-node').length === 1`, 'new flow from the empty template');
  assert.equal(await evaluate(q('.fb-state') + '.textContent'), 'Not saved yet', 'a new flow has no state chip yet');
  assert.equal(store.get('multi-agent-design').yaml, MULTI_YAML, 'discarding never saved anything');
});

test('builds parallel branches and a fan-in by dragging, the step list and the inspector; cycles are refused', async () => {
  for (const label of ['+ Agent step', '+ Agent step', '+ Program step']) { await click('.fg-tools button', label); await sleep(50); }
  await waitFor('document.querySelectorAll(".fg-node").length === 4');
  assert.equal(await evaluate('document.activeElement.dataset.focus'), 'step:id', 'a new step opens its form with the ID focused');
  await click('.fg-tools button', 'Fit');
  // Drag a connector from first_step's output handle onto agent_1.
  await drag(await centre('[data-handle="first_step"] .dot'), await centre('[data-step="agent_1"] .box'));
  await waitFor(`document.querySelector('.fg-edge[data-from="first_step"][data-to="agent_1"]')`, 'dragged dependency');
  // The accessible control in the step list.
  await click('.fb-tab', 'Steps');
  await choose('select[aria-label="Add dependency: agent_2 waits for…"]', 'first_step');
  await waitFor(`[...document.querySelectorAll('.fb-stage-title')].some(t => t.textContent.includes('2 run in parallel'))`, 'parallel branches listed as one stage');
  // Fan-in through the inspector.
  await click('.fb-step-open', 'program_1');
  await choose('.fb-inspector select.fb-add-dep', 'agent_1');
  await waitFor(`[...document.querySelectorAll('.fb-inspector .fb-need')].map(n => n.textContent).join() === 'agent_1×'`);
  await choose('.fb-inspector select.fb-add-dep', 'agent_2');
  await waitFor(`document.querySelectorAll('.fb-inspector .fb-need').length === 2`);
  // first_step -> program_1 would close a cycle: disabled in the menu, and refused when dragged.
  await click('.fb-step-open', 'first_step');
  assert.match(await evaluate(`[...${q('.fb-inspector select.fb-add-dep')}.options].find(o => o.value === 'program_1').textContent`), /would create a cycle/);
  assert.equal(await evaluate(`[...${q('.fb-inspector select.fb-add-dep')}.options].find(o => o.value === 'program_1').disabled`), true);
  await click('.fb-tab', 'Graph');
  await click('.fg-tools button', 'Fit');
  await drag(await centre('[data-handle="program_1"] .dot'), await centre('[data-step="first_step"] .box'));
  await waitFor(`/cycle/.test(document.querySelector('.fb-status').textContent)`, 'cycle refused with a message');
  assert.equal(await evaluate('document.querySelectorAll(".fg-edge").length'), 4);
  assert.equal(await nodeY('agent_1'), await nodeY('agent_2'));
  assert.ok(await nodeY('program_1') > await nodeY('agent_1'));
  await waitFor(`document.querySelector('.fb-yaml').value.includes('"program_1"') && !document.querySelector('.fb-yaml').readOnly`);
  // Save draft: a create (expectedRevision null) and never an activation.
  await click('.fb-actions button', 'Save draft');
  await waitFor(`/Saved draft fan-flow/.test(document.querySelector('.fb-status').textContent)`);
  const put = lastPut();
  assert.equal(put.path, '/api/flow-documents/fan-flow');
  assert.equal(put.body.expectedRevision, null);
  const saved = JSON.parse(put.body.yaml);
  assert.deepEqual(saved.steps.program_1.needs, ['agent_1', 'agent_2']);
  assert.deepEqual(saved.steps.agent_1.needs, ['first_step']);
  assert.deepEqual(saved.steps.agent_2.needs, ['first_step']);
  assert.equal(await saveEnabled(), false);
  assert.match(await evaluate(q('.fb-flow-item[aria-current="true"]') + '.textContent'), /fan-flow.*Not active/);
  // Moving a step saves layout only: no new draft save, not dirty, no activation.
  const puts = calls.filter((c) => c.method === 'PUT' && !c.path.endsWith('/layout')).length;
  const before = await nodeX('agent_2');
  await drag(await centre('[data-step="agent_2"] .label'), { ...(await centre('[data-step="agent_2"] .label')), x: (await centre('[data-step="agent_2"] .label')).x + 60 });
  await waitFor(`${before} !== Number(document.querySelector('[data-step="agent_2"]').getAttribute('transform').match(/\\(([-\\d.]+),/)[1])`);
  for (let i = 0; i < 30 && !calls.some((c) => c.path === '/api/flow-documents/fan-flow/layout'); i++) await sleep(100);
  const layout = calls.filter((c) => c.path === '/api/flow-documents/fan-flow/layout').at(-1);
  assert.ok(layout, 'layout saved');
  assert.equal(layout.body.layout.version, 1);
  assert.deepEqual(Object.keys(layout.body.layout.positions).sort(), ['agent_1', 'agent_2', 'first_step', 'program_1']);
  assert.equal(calls.filter((c) => c.method === 'PUT' && !c.path.endsWith('/layout')).length, puts);
  assert.equal(await saveEnabled(), false, 'layout never dirties the draft');
  assert.equal(activateCalls(), 0, 'saving a draft or a layout never activates');
  await shot('desktop-fan-in');
});

test('cycle errors from YAML show on nodes and in the problem list; invalid YAML stays in the editor', async () => {
  await click('.fb-tab', 'YAML');
  const doc = JSON.parse(await evaluate('document.querySelector(".fb-yaml").value'));
  doc.steps.first_step.needs = ['program_1'];
  await type('.fb-yaml', serialize(doc));
  await waitFor(`document.querySelectorAll('.fb-problem').length >= 4`, 'cycle errors listed');
  const problems = await evaluate('[...document.querySelectorAll(".fb-problem")].map(p => p.textContent)');
  assert.ok(problems.some((t) => /first_step · needs.*cycle/.test(t)), problems.join('\n'));
  await click('.fb-tab', 'Graph');
  assert.equal(await evaluate('document.querySelectorAll(".fg-node.has-error").length'), 4, 'every step in the cycle is marked');
  assert.match(await evaluate(q('[data-step="first_step"]') + '.getAttribute("aria-label")'), /1 problem/);
  await shot('desktop-cycle-errors');
  // Invalid YAML: the text stays, the graph keeps the last valid document, graph edits pause.
  await click('.fb-tab', 'YAML');
  const broken = serialize(doc).replace(/}\s*$/, '');
  await type('.fb-yaml', broken);
  await waitFor(`/YAML syntax error/.test(document.querySelector('.fb-problems').textContent)`);
  assert.equal(await evaluate('document.querySelector(".fb-yaml").value'), broken);
  assert.match(await evaluate(q('.fb-banner') + '.textContent'), /last valid version/);
  await click('.fb-tab', 'Graph');
  assert.equal(await evaluate('document.querySelectorAll(".fg-node").length'), 4);
  await click('.fg-tools button', '+ Program step');
  await waitFor(`/Fix it, or discard the YAML changes/.test(document.querySelector('.fb-status').textContent)`);
  assert.equal(await evaluate('document.querySelectorAll(".fg-node").length'), 4, 'no graph edit over invalid YAML');
  await click('.fb-tab', 'YAML');
  assert.equal(await evaluate('document.querySelector(".fb-yaml").value'), broken, 'switching tabs keeps the invalid text');
  await click('.fb-banner button', 'Discard YAML changes');
  await waitFor(`!/YAML syntax error/.test(document.querySelector('.fb-problems').textContent)`);
  // Jump from the problem list to the field and fix the cycle with the inspector.
  await click('.fb-problem', 'first_step');
  await waitFor(`document.querySelector('.fb-inspector h3').textContent === 'Step first_step'`);
  assert.match(await evaluate(q('.fb-inspector [data-field="needs"] .fb-field-error') + '.textContent'), /cycle/, 'the error is shown on the field');
  await click('.fb-inspector .fb-need', 'program_1');
  await waitFor(`document.querySelectorAll('.fb-problem').length === 0 && document.querySelector('.fb-problems').hidden`);
  await click('.fb-tab', 'Graph');
  assert.equal(await evaluate('document.querySelectorAll(".fg-node.has-error").length'), 0);
});

test('a failed save keeps the edits; a stale revision shows both versions and never overwrites silently', async () => {
  fault.save = 500;
  await click('.fb-actions button', 'Save draft');
  await waitFor(`/Save failed: disk full/.test(document.querySelector('.fb-status').textContent)`);
  assert.equal(await saveEnabled(), true, 'still dirty');
  assert.equal(document_has_cycle_free(), true);
  fault.save = null;
  // Someone else saves a different draft meanwhile.
  const external = parse(store.get('fan-flow').yaml).document;
  external.steps.external_step = { kind: 'program', run: 'echo external' };
  store.get('fan-flow').yaml = serialize(external);
  const externalRevision = revisionOf(store.get('fan-flow').yaml);
  await click('.fb-actions button', 'Save draft');
  await waitFor(`document.querySelector('dialog.fb-ask')?.open && /changed since you opened it/.test(document.querySelector('dialog.fb-ask').textContent)`);
  const text = await askText();
  assert.match(text, /Saved version.*5 steps.*external_step/s);
  assert.match(text, /Your version.*4 steps/s);
  assert.match(text, /Only in the saved version: external_step/);
  await shot('desktop-conflict');
  await answer('Overwrite with mine');
  await waitFor(`/Saved draft fan-flow/.test(document.querySelector('.fb-status').textContent)`);
  assert.equal(lastPut().body.expectedRevision, externalRevision, 'overwriting is an explicit save against the newer revision');
  assert.equal('external_step' in JSON.parse(store.get('fan-flow').yaml).steps, false);
  // And the other choice: load theirs.
  await click('[data-step="agent_1"] .box');
  await type('.fb-inspector [data-focus="step:description"]', 'mine');
  store.get('fan-flow').yaml = serialize(external);
  await click('.fb-actions button', 'Save draft');
  await answer('Load the saved version');
  await waitFor(`document.querySelectorAll('.fg-node').length === 5 && !!document.querySelector('[data-step="external_step"]')`);
  assert.equal(await saveEnabled(), false);
  assert.equal(activateCalls(), 0);
});
function document_has_cycle_free() { return !/cycle/.test(store.get('fan-flow').yaml); }

test('keyboard: Tab to a step, Enter selects, arrows nudge, Delete removes after confirmation', async () => {
  await evaluate(q('.fg-svg') + '.focus()');
  await press('Tab');
  assert.equal(await evaluate('document.activeElement.dataset.step'), 'first_step', 'steps are in the tab order; dependencies are not');
  await press('Enter');
  await waitFor(`document.querySelector('.fb-inspector h3').textContent === 'Step first_step'`);
  assert.equal(await evaluate('document.activeElement.dataset.step'), 'first_step', 'focus stays on the step');
  const x = await nodeX('first_step');
  const layouts = calls.filter((c) => c.path.endsWith('/layout')).length;
  await press('ArrowRight'); await press('ArrowRight');
  assert.equal(await nodeX('first_step'), x + 16);
  assert.equal(await evaluate('document.activeElement.dataset.step'), 'first_step');
  for (let i = 0; i < 30 && calls.filter((c) => c.path.endsWith('/layout')).length === layouts; i++) await sleep(100);
  assert.equal(calls.filter((c) => c.path.endsWith('/layout')).at(-1).body.layout.positions.first_step.x, x + 16);
  assert.equal(await saveEnabled(), false, 'nudging is layout only');
  await press('Delete');
  await waitFor(`document.querySelector('dialog.fb-ask')?.open`);
  assert.match(await askText(), /Delete step first_step\?.*removed from what these steps wait for: agent_1, agent_2/s);
  assert.equal(await evaluate('document.activeElement.textContent'), 'Keep step', 'the safe choice has focus');
  await press('Enter');
  await waitFor(`!document.querySelector('dialog.fb-ask').open`);
  assert.equal(await evaluate('!!document.querySelector(\'[data-step="first_step"]\')'), true, 'kept');
  assert.equal(await evaluate('document.activeElement.dataset.step'), 'first_step', 'focus returns to the step');
  await press('Delete');
  await waitFor(`document.querySelector('dialog.fb-ask')?.open`);
  await press('Tab');
  assert.equal(await evaluate('document.activeElement.textContent'), 'Delete step');
  await press('Enter');
  await waitFor(`!document.querySelector('[data-step="first_step"]')`);
  assert.equal(await evaluate('document.querySelectorAll(".fg-edge").length'), 2);
  assert.equal(await evaluate('document.activeElement === document.querySelector(".fg-svg")'), true);
  assert.equal(await saveEnabled(), true);
  await click('.fb-actions button', 'Save draft');
  await waitFor(`/Saved draft fan-flow/.test(document.querySelector('.fb-status').textContent)`);
});

test('long step IDs and descriptions are clipped in the graph and wrap in forms', async () => {
  const long = 'a_very_long_step_identifier_that_keeps_going_on1';
  assert.equal(long.length, 48);
  await click('[data-step="external_step"] .box');
  await type('.fb-inspector [data-focus="step:id"]', long);
  await press('Enter');
  await waitFor(`!!document.querySelector('[data-step="${long}"]')`, 'renamed');
  assert.equal(await evaluate(q('.fb-inspector [data-focus="step:id"]') + '.value'), long);
  await type('.fb-inspector [data-focus="step:description"]', 'An unusually long description '.repeat(8));
  const label = await evaluate(`(() => { const t = document.querySelector('[data-step="${long}"] .label'); return { text: t.textContent, width: t.getBBox().width }; })()`);
  assert.ok(label.text.endsWith('…'));
  assert.ok(label.width < FB_NODE_W - 24, `label fits inside its node (${label.width})`);
  assert.deepEqual(await sideways('.fb-inspector'), [], 'no sideways scroll in the inspector');
  await click('.fb-tab', 'Steps');
  assert.equal(await evaluate('(() => { const v = document.querySelector(".fb-steps-view"); return v.scrollWidth <= v.clientWidth; })()'), true, 'no sideways scroll in the list');
  // Renaming onto an existing ID is refused with a message on the field.
  await click('.fb-step-open', long);
  await type('.fb-inspector [data-focus="step:id"]', 'agent_1');
  await press('Enter');
  await waitFor(`/already exists/.test(document.querySelector('.fb-inspector [data-field="id"] .fb-field-error').textContent)`);
  assert.equal(await evaluate(`!!document.querySelector('[data-row="${long}"]')`), true);
  await click('.fb-tab', 'Graph');
  await shot('desktop-long-labels');
  assert.equal(await evaluate(`[...document.querySelectorAll('.fb-actions button')].find(b => b.textContent === 'Activate…').disabled`), true, 'unsaved drafts cannot be activated');
  await click(`[data-step="${long}"] .box`);
  assert.deepEqual(await sideways('.fb-inspector'), [], 'the long ID wraps everywhere in the form');
  await click('.fg-tools button', 'Flow settings');
  assert.deepEqual(await sideways('.fb-inspector'), [], 'flow settings fit');
  await click('.fb-actions button', 'Save draft');
  await waitFor(`/Saved draft fan-flow/.test(document.querySelector('.fb-status').textContent)`);
});
const FB_NODE_W = 184;

test('fields the forms do not cover (environment, setup, unknown keys) survive graph edits and stay visible', async () => {
  await click('.fb-flow-item', 'other-flow');
  await waitFor(`document.querySelector('.fb-title').textContent === 'other-flow' && document.querySelectorAll('.fg-node').length === 2`);
  await click('.fg-tools button', 'Flow settings');
  assert.match(await evaluate(q('.fb-inspector') + '.textContent'), /Environment setup.*setup_environment step runs first/s);
  assert.equal(await evaluate(`['count', 'fast', 'note'].map(k => document.querySelector('.fb-inspector [aria-label="' + k + ' type"]').value).join()`), 'number,boolean,empty');
  await click('[data-step="helper"] .box');
  assert.match(await evaluate(q('.fb-inspector .fb-extra') + '.textContent'), /Other settings.*setup.*future_key/s);
  await click('.fg-tools button', '+ Program step');
  await waitFor(`document.querySelector('.fb-yaml').value.includes('program_1') && !document.querySelector('.fb-yaml').readOnly`);
  const sent = calls.filter((c) => c.path.endsWith('/serialize')).at(-1).body.document;
  assert.deepEqual(sent.environment, { agent: 'codex', files: [{ source: 'docs/', target: 'context/' }] });
  assert.deepEqual(sent.steps.helper.setup, { packages: ['jq'] });
  assert.equal(sent.steps.helper.future_key, 1);
  assert.deepEqual(sent.input, { count: 2, fast: true, note: null });
  await click('.fb-flow-item', 'fan-flow');
  await answer('Discard changes');
  await waitFor(`document.querySelector('.fb-title').textContent === 'fan-flow' && !!document.querySelector('.fg-node')`);
});

test('graph: right click offers actions for a step, a dependency and the canvas; the menu sits inside the dialog', async () => {
  const fresh = 'menu-flow';
  store.set(fresh, { yaml: serialize({ name: fresh, input: {}, steps: { a: { kind: 'program', run: 'echo a', needs: [] }, b: { kind: 'agent', agent: 'codex', prompt: 'B', needs: ['a'], description: 'Second step' } } }), active: null, layout: null });
  await evaluate('document.querySelector("#flows-dialog").close()');
  await click('#flows-btn');
  await waitFor(`[...document.querySelectorAll('.fb-flow-name')].some((n) => n.textContent === '${fresh}')`, 'the flow is listed');
  await click('.fb-flow-item', fresh);
  await waitFor('document.querySelector(\'[data-step="b"]\')', 'the graph');
  await evaluate('document.getElementById("fb-tab-graph").click()');
  await sleep(100);
  const open = async (at) => {
    await mouse('mouseMoved', at, 0);
    await call('Input.dispatchMouseEvent', { type: 'mousePressed', x: at.x, y: at.y, button: 'right', buttons: 2, clickCount: 1 });
    await call('Input.dispatchMouseEvent', { type: 'mouseReleased', x: at.x, y: at.y, button: 'right', buttons: 0, clickCount: 1 });
    await waitFor('document.querySelector(".ctx-menu")', 'the menu');
    return evaluate('[...document.querySelectorAll(".ctx-item span")].map((n) => n.textContent)');
  };
  const pick = (label) => evaluate(`[...document.querySelectorAll('.ctx-item')].find((n) => n.textContent.startsWith(${JSON.stringify(label)})).click()`);
  const stepLabels = await open(await centre('[data-step="b"] .box'));
  assert.deepEqual(stepLabels, ['Edit step', 'Add agent step after', 'Add program step after', 'Duplicate', 'Copy step ID', 'Delete step']);
  assert.equal(await evaluate('document.querySelector(".ctx-menu").parentElement.id'), 'flows-dialog', 'not under the modal dialog');
  assert.match(await evaluate('document.querySelector(".fb-inspector h3").textContent'), /Step b/, 'right click selects it');
  await pick('Duplicate');
  await waitFor('document.querySelector(\'[data-step="b_copy"]\')', 'the copy');
  assert.equal(await evaluate('document.querySelector(\'[data-step="b_copy"]\').getAttribute("aria-label").includes("waits for a")'), true);

  // Undo and redo the duplicate.
  await click('.fg-tools button', 'Undo');
  await waitFor('!document.querySelector(\'[data-step="b_copy"]\')', 'undo removes the copy');
  await click('.fg-tools button', 'Redo');
  await waitFor('document.querySelector(\'[data-step="b_copy"]\')', 'redo restores it');
  await click('.fg-tools button', 'Undo');
  await waitFor('!document.querySelector(\'[data-step="b_copy"]\')');

  const canvas = await evaluate(`(() => { const r = document.querySelector('.fg-svg').getBoundingClientRect(); return { x: r.right - 60, y: r.top + 90 }; })()`);
  const canvasLabels = await open(canvas);
  assert.equal(canvasLabels[0], 'Add agent step here');
  assert.ok(canvasLabels.includes('Auto-layout'));
  await pick('Add agent step here');
  await waitFor('document.querySelector(\'[data-step="agent_1"]\')', 'a step added at the pointer');
  const placed = await evaluate(`(() => { const r = document.querySelector('[data-step="agent_1"] .box').getBoundingClientRect(); return { x: r.x + r.width / 2, y: r.y + r.height / 2 }; })()`);
  assert.ok(Math.abs(placed.y - canvas.y) < 90, 'near where the menu was opened');
  await click('.fg-tools button', 'Undo');
  await waitFor('!document.querySelector(\'[data-step="agent_1"]\')');

  // Dropping a new dependency on empty space offers to create the next step there.
  const handle = await centre('[data-handle="b"] .dot');
  await drag(handle, canvas);
  await waitFor('document.querySelector(".ctx-menu")', 'the add-a-step menu');
  await pick('Add program step here');
  await waitFor('document.querySelector(\'[data-step="program_1"]\')');
  assert.match(await evaluate('document.querySelector(\'[data-step="program_1"]\').getAttribute("aria-label")'), /waits for b/);

  // Hovering a step shows its details, and lights up what it connects to.
  const over = await centre('[data-step="a"] .box');
  // No button named: mouse() always says left, which the browser reports as a held button, and a drag shows no hover card.
  await call('Input.dispatchMouseEvent', { type: 'mouseMoved', x: over.x, y: over.y, button: 'none', buttons: 0 });
  await waitFor('document.querySelector(".hover-card.on")', 'the hover card');
  assert.match(await evaluate('document.querySelector(".hover-card").textContent'), /echo a/);
  assert.equal(await evaluate('document.querySelector(".fg-svg").classList.contains("tracing") && document.querySelector(\'[data-step="b"]\').classList.contains("lit")'), true);
  await call('Input.dispatchMouseEvent', { type: 'mouseMoved', x: 2, y: 2, button: 'none', buttons: 0 });
  store.delete(fresh);
  await click('.fb-close'); await sleep(50);
  if (await evaluate('document.querySelector(".fb-ask")?.open')) await click('.fb-ask button', 'Discard changes');
  await sleep(50);
  await evaluate('document.querySelector("#flows-btn").click()');
  await sleep(300);
});

test('dark theme uses the board\'s dark colours', async () => {
  await call('Emulation.setEmulatedMedia', { features: [{ name: 'prefers-color-scheme', value: 'dark' }] });
  await sleep(100);
  const colours = await evaluate(`({ dialog: getComputedStyle(document.querySelector('#flows-dialog')).backgroundColor,
    box: getComputedStyle(document.querySelector('.fg-node .box')).fill, label: getComputedStyle(document.querySelector('.fg-node .label')).fill,
    canvas: getComputedStyle(document.querySelector('.fg-canvas')).backgroundColor })`);
  assert.deepEqual(colours, { dialog: 'rgb(27, 31, 39)', box: 'rgb(27, 31, 39)', label: 'rgb(233, 235, 241)', canvas: 'rgb(14, 16, 20)' });
  await shot('desktop-dark');
  await call('Emulation.setEmulatedMedia', { features: [{ name: 'prefers-color-scheme', value: 'light' }] });
  assert.equal(await evaluate(`getComputedStyle(document.querySelector('.fg-node .box')).fill`), 'rgb(255, 255, 255)');
});

test('activation previews per-engine impact, needs explicit consent to interrupt, re-checks a changed impact, then Run opens New card', async () => {
  assert.equal(activateCalls(), 0, 'no draft save so far has activated anything');
  await click('.fb-actions button', 'Activate…');
  await waitFor(`document.querySelector('dialog.fb-ask')?.open`);
  let text = await askText();
  assert.match(text, /Hatchet reloads its flow worker.*1 active run will be interrupted.*Design status page/s);
  assert.match(text, /Prefect reloads its flow worker.*No active runs/s);
  assert.match(text, /Trigger\.dev regenerates its tasks · engine unavailable.*unknown/s);
  assert.equal(await evaluate(`[...document.querySelectorAll('dialog.fb-ask .fb-ask-actions button')].map(b => b.textContent).join('|')`), 'Not now|Activate and interrupt 1 run (and any unknown)');
  await shot('desktop-activation');
  await answer('Not now');
  await sleep(100);
  assert.equal(activateCalls(), 0, 'Not now defers');
  // A run starts between the preview and the apply: the server refuses and the UI shows the fresh impact.
  fault.addRunOnActivate = true;
  await click('.fb-actions button', 'Activate…');
  await answer('Activate and interrupt 1 run');
  await waitFor(`/changed since you looked/.test(document.querySelector('dialog.fb-ask')?.textContent || '')`, 'fresh preview after impact change');
  text = await askText();
  assert.match(text, /2 active runs will be interrupted.*Another run/s);
  const first = calls.filter((c) => c.path.endsWith('/activate'))[0];
  assert.equal(first.body.confirm, true);
  assert.equal(first.body.revision, revisionOf(store.get('fan-flow').yaml));
  await answer('Activate and interrupt 2 runs');
  await waitFor(`/Activation of fan-flow/.test(document.querySelector('dialog.fb-ask')?.textContent || '')`);
  text = await askText();
  assert.match(text, /Hatchet active.*Prefect active.*Trigger\.dev unavailable.*not running/s);
  assert.equal(calls.filter((c) => c.path.endsWith('/activate')).length, 2);
  const [stale, fresh] = calls.filter((c) => c.path.endsWith('/activate'));
  assert.notEqual(fresh.body.impact, stale.body.impact, 'the retry carries the re-checked impact');
  await answer('Close');
  await waitFor(`/fan-flow.*Active/.test(document.querySelector('.fb-flow-item[aria-current="true"]').textContent)`);
  // Run… hands over to the existing New card dialog with the flow selected.
  await click('.fb-actions button', 'Run…');
  await waitFor(`document.querySelector('#new-dialog').open && document.querySelector('#n-kind').value === 'flow:fan-flow'`, 'New card preselects the flow');
  assert.equal(await evaluate('document.querySelector("#flows-dialog").open'), false);
  await waitFor(`/agent_1/.test(document.querySelector('#n-flow-desc').textContent)`, 'New card shows the flow once the list is loaded');
  await evaluate('document.querySelector("#new-dialog").close()');
});

test('closing with unsaved changes asks first (close button and Escape)', async () => {
  await openFlows();
  await waitFor(`document.querySelector('.fb-title').textContent === 'fan-flow'`, 'reopens the last flow');
  await click('[data-step="agent_1"] .box');
  await type('.fb-inspector [data-focus="step:description"]', 'unsaved edit');
  await waitFor(`!document.querySelector('.fb-yaml').readOnly`);
  await click('.fb-close');
  await answer('Keep editing');
  assert.equal(await evaluate('document.querySelector("#flows-dialog").open'), true);
  await evaluate(q('.fg-svg') + '.focus()');
  await press('Escape');
  await answer('Discard changes');
  await waitFor('!document.querySelector("#flows-dialog").open');
  await openFlows();
  await waitFor(`document.querySelector('.fb-title').textContent === 'fan-flow' && ${q('.fb-flow-item[aria-current="true"]')}?.textContent.includes('unsaved') === false`);
  assert.equal(await saveEnabled(), false, 'discarded edits are gone');
});

test('a new step is panned into view, even when auto-layout places it off screen', async () => {
  await openFlows();
  await waitFor(`document.querySelector('.fb-title').textContent === 'fan-flow'`, 'reopens the last flow');
  await click('.fg-tools button', 'Auto-layout');
  for (let i = 0; i < 6; i++) await click('.fg-tools button[aria-label="Zoom in"]');
  const ids = () => evaluate('[...document.querySelectorAll(".fg-node")].map(n => n.dataset.step)');
  const inView = (id) => evaluate(`(() => { const s = document.querySelector('.fg-svg').getBoundingClientRect(), b = document.querySelector('[data-step="${id}"] .box').getBoundingClientRect();
    return b.left >= s.left && b.right <= s.right && b.top >= s.top && b.bottom <= s.bottom; })()`);
  for (let i = 0; i < 3; i++) {
    const before = await ids();
    await click('.fg-tools button', '+ Program step');
    await waitFor(`document.querySelectorAll(".fg-node").length === ${before.length + 1}`, 'the step is drawn');
    const added = (await ids()).find((id) => !before.includes(id));
    assert.equal(await inView(added), true, `${added} is fully on the canvas`);
  }
  assert.match(await evaluate(q('.fg-zoom') + '.textContent'), /^2\d\d%$/, 'panning keeps the zoom level');
  // A canvas narrower than the zoomed step: zoom out just enough to show it whole.
  await call('Emulation.setDeviceMetricsOverride', { width: 760, height: 900, deviceScaleFactor: 1, mobile: false });
  await sleep(200);
  for (let i = 0; i < 6; i++) await click('.fg-tools button[aria-label="Zoom in"]');
  const before = await ids();
  await click('.fg-tools button', '+ Program step');
  await waitFor(`document.querySelectorAll(".fg-node").length === ${before.length + 1}`, 'the step is drawn');
  const added = (await ids()).find((id) => !before.includes(id));
  assert.equal(await inView(added), true, `${added} is fully on the narrow canvas`);
  assert.ok(parseInt(await evaluate(q('.fg-zoom') + '.textContent')) < 250, 'the zoom drops only because the step could not fit');
  await call('Emulation.setDeviceMetricsOverride', { width: 1440, height: 1000, deviceScaleFactor: 1, mobile: false });
  await click('.fb-close');
  await answer('Discard changes');
  await waitFor('!document.querySelector("#flows-dialog").open');
});

test('phone: full screen, step list and form editing without sideways scrolling, 44px targets, touch graph', async () => {
  await evaluate('document.querySelector("#flows-dialog").close()');
  await call('Emulation.setDeviceMetricsOverride', { width: 375, height: 812, deviceScaleFactor: 2, mobile: true });
  await call('Emulation.setTouchEmulationEnabled', { enabled: true, maxTouchPoints: 5 });
  await evaluate('window.beforeReload = true');
  await call('Page.reload');
  await waitFor('!window.beforeReload && typeof openNewCard === "function" && document.querySelector("#flows-btn")', 'the reloaded page, with its script run');
  await openFlows();
  await waitFor(`document.querySelector('.fb-title').textContent === 'fan-flow' && document.querySelector('.fb-steps-view:not([hidden]) .fb-step-row')`, 'phones start on the step list');
  const fits = () => evaluate(`(() => { const d = document.querySelector('#flows-dialog').getBoundingClientRect();
    return d.width === 375 && document.documentElement.scrollWidth <= 375 && document.querySelector('.fb').scrollWidth <= 375
      && [...document.querySelectorAll('.fb *')].every(n => !n.getClientRects().length || n.getBoundingClientRect().right <= 375.5 || n.closest('.fb-flows, .fg-svg')); })()`);
  assert.equal(await fits(), true, 'full screen, nothing wider than the phone');
  const small = await evaluate(`[...document.querySelectorAll('.fb button, .fb select, .fb input:not([type=checkbox])')].filter(n => n.getClientRects().length && !n.closest('.fb-flows'))
    .map(n => [n.textContent || n.getAttribute('aria-label'), n.getBoundingClientRect().height]).filter(([, h]) => h < 44)`);
  assert.deepEqual(small, [], 'every visible control is at least 44px tall');
  await shot('phone-steps');
  // Add a step and edit it through the form.
  await click('.fb-steps-view button', '+ Agent step');
  await waitFor(`document.querySelector('.fb').classList.contains('fb-inspecting') && document.querySelector('.fb-inspector').getClientRects().length`, 'form opens full screen');
  assert.equal(await evaluate('document.querySelector(".fb-main").getClientRects().length'), 0);
  const added = await evaluate(`document.querySelector('.fb-inspector [data-focus="step:id"]').value`);
  await type('.fb-inspector [data-focus="step:id"]', 'phone_step');
  await press('Enter');
  await waitFor(`document.querySelector('.fb-inspector h3').textContent === 'Step phone_step'`);
  await choose('.fb-inspector select.fb-add-dep', 'agent_2');
  await choose('.fb-inspector [data-focus="step:agent"]', 'codex');
  await type('.fb-inspector [data-focus="step:prompt"]', 'Summarise ');
  await click('.fb-inspector .fb-ref-btn', 'Insert reference');
  await click('.fb-refs .fb-ref', '{{ steps.agent_2.output }}');
  await waitFor(`document.querySelector('.fb-inspector [data-focus="step:prompt"]').value === 'Summarise {{ steps.agent_2.output }}'`);
  await evaluate(`(() => { const r = ${q('.fb-inspector [data-focus="step:review"]')}; r.click(); })()`);
  await type('.fb-inspector [data-focus="step:retries"]', '2');
  await type('.fb-inspector [data-focus="step:phone_step:worker-tags"]', 'linux, gpu');
  assert.equal(await fits(), true, 'the form fits the phone');
  await shot('phone-form');
  await click('.fb-inspector .fb-back');
  await waitFor(`!document.querySelector('.fb').classList.contains('fb-inspecting') && document.querySelector('[data-row="phone_step"]')`);
  assert.notEqual(added, 'phone_step');
  assert.match(await evaluate(q('[data-row="phone_step"]') + '.textContent'), /Waits for.*agent_2/);
  // The list's own Add dependency control.
  await choose('select[aria-label="Add dependency: phone_step waits for…"]', 'agent_1');
  await waitFor(`/agent_1/.test(document.querySelector('[data-row="phone_step"] .fb-needs').textContent)`);
  // Graph view: a tap opens the form, a one-finger drag on the background pans.
  await click('.fb-tab', 'Graph');
  await waitFor('document.querySelectorAll(".fg-node").length === 5');
  const handle = await evaluate(`(() => { const b = document.querySelector('[data-handle="agent_1"] .hit').getBoundingClientRect(); return Math.min(b.width, b.height); })()`);
  assert.ok(handle >= 44, `connector handle is a 44px target (${handle})`);
  await shot('phone-graph');
  const tap = async (at) => { await call('Input.dispatchTouchEvent', { type: 'touchStart', touchPoints: [at] }); await call('Input.dispatchTouchEvent', { type: 'touchEnd', touchPoints: [] }); await sleep(50); };
  const before = await evaluate(q('.fg-viewport') + '.getAttribute("transform")');
  const canvas = await evaluate(`(() => { const b = document.querySelector('.fg-svg').getBoundingClientRect(); return { x: b.left + 12, y: b.bottom - 12 }; })()`);
  await call('Input.dispatchTouchEvent', { type: 'touchStart', touchPoints: [canvas] });
  for (let i = 1; i <= 6; i++) await call('Input.dispatchTouchEvent', { type: 'touchMove', touchPoints: [{ x: canvas.x + i * 10, y: canvas.y - i * 10 }] });
  await call('Input.dispatchTouchEvent', { type: 'touchEnd', touchPoints: [] });
  await sleep(50);
  assert.notEqual(await evaluate(q('.fg-viewport') + '.getAttribute("transform")'), before, 'one-finger drag pans');
  // Two fingers spreading apart zoom in.
  const zoom = await evaluate(q('.fg-zoom') + '.textContent');
  const mid = await evaluate(`(() => { const b = document.querySelector('.fg-svg').getBoundingClientRect(); return { x: b.left + b.width / 2, y: b.top + b.height / 2 }; })()`);
  await call('Input.dispatchTouchEvent', { type: 'touchStart', touchPoints: [{ x: mid.x - 30, y: mid.y, id: 1 }, { x: mid.x + 30, y: mid.y, id: 2 }] });
  for (let i = 1; i <= 6; i++) await call('Input.dispatchTouchEvent', { type: 'touchMove', touchPoints: [{ x: mid.x - 30 - i * 12, y: mid.y, id: 1 }, { x: mid.x + 30 + i * 12, y: mid.y, id: 2 }] });
  await call('Input.dispatchTouchEvent', { type: 'touchEnd', touchPoints: [] });
  await sleep(50);
  assert.ok(parseInt(await evaluate(q('.fg-zoom') + '.textContent')) > parseInt(zoom), 'pinch zooms in');
  await click('.fg-tools button', 'Fit');
  await tap(await centre('[data-step="phone_step"] .label'));
  await waitFor(`document.querySelector('.fb').classList.contains('fb-inspecting') && document.querySelector('.fb-inspector h3').textContent === 'Step phone_step'`, 'tap opens the form');
  await click('.fb-inspector .fb-back');
  assert.equal(await fits(), true);
  await click('.fb-actions button', 'Save draft');
  await waitFor(`/Saved draft fan-flow/.test(document.querySelector('.fb-status').textContent)`);
  const savedDoc = JSON.parse(store.get('fan-flow').yaml);
  assert.deepEqual(savedDoc.steps.phone_step, { kind: 'agent', agent: 'codex', review: true, prompt: 'Summarise {{ steps.agent_2.output }}', needs: ['agent_2', 'agent_1'], retries: 2, worker: { tags: ['linux', 'gpu'] } });
  await call('Emulation.setEmulatedMedia', { features: [{ name: 'prefers-color-scheme', value: 'dark' }] });
  await shot('phone-graph-dark');
  await click('.fb-tab', 'Steps');
  await shot('phone-steps-dark');
  await call('Emulation.setEmulatedMedia', { features: [] });
  await call('Emulation.setTouchEmulationEnabled', { enabled: false });
  await desktop();
});

test('fleet compatibility per step: matches with detection times, waits, explained mismatches, step overrides, a blocked draft still saves', async () => {
  store.set('fleet-flow', { yaml: serialize({ name: 'fleet-flow', description: 'Placed on the fleet', input: {}, worker: { pool: 'build' }, steps: {
    build: { kind: 'program', run: 'python build.py' },
    pipe: { kind: 'program', run: 'cat $IN | sort', needs: ['build'] },
    review: { kind: 'agent', agent: 'codex', prompt: 'Review', needs: ['build'], worker: { id: 'pc-c' } } } }), active: null, layout: null });
  await evaluate('document.querySelector("#flows-dialog").close()');
  await openFlows();
  await waitFor(`document.querySelector('.fb-title').textContent === 'fan-flow'`, 'reopens the last flow');
  await waitFor(`[...document.querySelectorAll('.fb-flow-item')].some(b => b.textContent.includes('fleet-flow'))`, 'the new draft is listed');
  // Clicked on the element itself: the list can re-render under a pointer position under load.
  await evaluate(`[...document.querySelectorAll('.fb-flow-item')].find((b) => b.textContent.includes('fleet-flow')).click()`);
  try { await waitFor(`document.querySelector('.fb-title').textContent === 'fleet-flow'`, 'fleet-flow opens'); }
  catch (error) { error.message += `; state: ${JSON.stringify(await evaluate(`({title: document.querySelector('.fb-title')?.textContent, ask: document.querySelector('dialog.fb-ask')?.open, status: document.querySelector('.fb-status')?.textContent, width: innerWidth, flow: document.querySelector('.fb')?.className})`))}`; throw error; }
  await click('.fb-tab', 'Graph');
  await waitFor(`document.querySelectorAll('.fg-node').length === 3`, 'fleet-flow graph renders');
  await click('[data-step="build"] .box');
  await waitFor(`document.querySelector('.fb-inspector .fb-placement[data-status="ready"]')`, 'the step is checked against the fleet');
  const check = calls.filter((c) => c.path === '/api/launch/validate').at(-1).body;
  assert.equal(check.document.name, 'fleet-flow');
  assert.deepEqual(check.execution, {}, 'checked the way the New card dialog launches it');
  assert.match(await evaluate(q('.fb-inspector .fb-placement') + '.textContent'), /Can start now on pc-a.*python installed.*checked/s);
  assert.equal(await evaluate(`!!document.querySelector('.fb-inspector .fb-placement [data-worker="pc-a"] time[datetime="${fake.CHECKED}"]')`), true, 'last detected time');
  assert.match(await evaluate(q('.fb-inspector .fb-placement-source') + '.textContent'), /default worker in Flow settings/, 'says where the selection comes from');
  // A tag no worker in the pool has: blocked and explained; the draft still saves.
  await evaluate(`document.querySelector('.fb-fold[data-fold="step-run"]').open = true`);
  await type('.fb-inspector [data-focus="step:build:worker-tags"]', 'cuda');
  await waitFor(`document.querySelector('.fb-inspector .fb-placement[data-status="blocked"]')`, 'missing tag blocks');
  const blocked = await evaluate(q('.fb-inspector .fb-placement') + '.textContent');
  assert.match(blocked, /pc-a lacks tags: cuda/);
  assert.match(blocked, /How to fix: Add the missing tags to a worker in pool build/);
  assert.match(await evaluate(q('.fb-inspector .fb-placement-source') + '.textContent'), /You can save this draft.*launch is blocked/s);
  await waitFor(`!document.querySelector('.fb-yaml').readOnly`);
  await click('.fb-actions button', 'Save draft');
  await waitFor(`/Saved draft fleet-flow/.test(document.querySelector('.fb-status').textContent)`, 'a blocked draft saves');
  assert.deepEqual(JSON.parse(store.get('fleet-flow').yaml).steps.build.worker, { tags: ['cuda'] });
  // A possible match that is offline: the step will wait.
  await type('.fb-inspector [data-focus="step:build:worker-pool"]', 'review');
  await type('.fb-inspector [data-focus="step:build:worker-tags"]', 'linux');
  await waitFor(`document.querySelector('.fb-inspector .fb-placement[data-status="wait"]')`, 'offline match waits');
  assert.match(await evaluate(q('.fb-inspector .fb-placement') + '.textContent'), /Waits for a worker: a matching worker is offline.*pc-b is offline/s);
  // A step override (worker ID) beats the flow default pool.
  await click('[data-step="review"] .box');
  await waitFor(`document.querySelector('.fb-inspector .fb-placement[data-status="blocked"]')`);
  const review = await evaluate(q('.fb-inspector .fb-placement') + '.textContent');
  assert.match(review, /codex is not installed on pc-c/);
  assert.doesNotMatch(review, /pc-a/, 'only the selected worker is considered');
  assert.match(await evaluate(q('.fb-inspector .fb-placement-source') + '.textContent'), /this step.s own worker setting/);
  await click('.fb-tab', 'Steps');
  assert.deepEqual(await evaluate(`['build', 'pipe', 'review'].map(id => document.querySelector('[data-row="' + id + '"] .fb-placement-chip')?.textContent)`), ['Waits', 'Blocked', 'Blocked']);
  await click('.fb-tab', 'Graph');
  await shot('desktop-placement');
});

test('an ambiguous program asks for requires_command; declaring it clears the block', async () => {
  await click('[data-step="pipe"] .box');
  await waitFor(`document.querySelector('.fb-inspector [data-field="requires_command"].invalid')`, 'requires_command is required');
  const field = await evaluate(`(() => { const w = document.querySelector('.fb-inspector [data-field="requires_command"]'), i = w.querySelector('input');
    return { error: w.querySelector('.fb-field-error').textContent, invalid: i.getAttribute('aria-invalid'), required: i.required }; })()`);
  assert.match(field.error, /cannot be inferred.*declare it/);
  assert.equal(field.invalid, 'true');
  assert.equal(field.required, true);
  assert.match(await evaluate(q('.fb-inspector .fb-placement') + '.textContent'), /executable cannot be determined safely/);
  await type('.fb-inspector [data-focus="step:requires_command"]', 'bash');
  await waitFor(`document.querySelector('.fb-inspector .fb-placement[data-status="ready"]')`, 'declared: ready');
  assert.match(await evaluate(q('.fb-inspector .fb-placement') + '.textContent'), /bash installed/);
  assert.equal(await evaluate(`document.querySelector('.fb-inspector [data-field="requires_command"]').classList.contains('invalid')`), false);
  await waitFor(`document.querySelector('.fb-yaml').value.includes('"requires_command": "bash"') && !document.querySelector('.fb-yaml').readOnly`);
  // Switching the step to an agent and back keeps the declaration (agent steps cannot have one).
  await choose('.fb-inspector [data-focus="step:kind"]', 'agent');
  await waitFor(`!document.querySelector('.fb-yaml').value.includes('requires_command') && !document.querySelector('.fb-yaml').readOnly`);
  await choose('.fb-inspector [data-focus="step:kind"]', 'program');
  await waitFor(`document.querySelector('.fb-yaml').value.includes('"requires_command": "bash"') && !document.querySelector('.fb-yaml').readOnly`);
  await click('.fb-actions button', 'Save draft');
  await waitFor(`/Saved draft fleet-flow/.test(document.querySelector('.fb-status').textContent)`);
  assert.equal(JSON.parse(store.get('fleet-flow').yaml).steps.pipe.requires_command, 'bash');
  await click('.fb-close');
  await waitFor('!document.querySelector("#flows-dialog").open');
});

test('no uncaught page errors', () => {
  assert.equal(activateCalls(), 2, 'only the explicitly confirmed activations reached the server');
  assert.deepEqual(errors, []);
});

const dismiss = async () => { for (let i = 0; i < 40; i++) { if (await evaluate('document.querySelector(".fb-ask")?.open')) { await click('.fb-ask button', 'Close'); return; } await sleep(50); } };
test('the sidebar filters flows; a switch enables or disables each, showing what it affects first; disabled flows are gray', async () => {
  const fresh = 'switch-flow';
  store.set(fresh, { yaml: serialize({ name: fresh, input: {}, steps: { a: { kind: 'program', run: 'echo a', needs: [] } } }), active: null, layout: null });
  store.set('other-switch', { yaml: serialize({ name: 'other-switch', input: {}, steps: { b: { kind: 'program', run: 'echo b', needs: [] } } }), active: null, layout: null });
  await evaluate('document.querySelector("#flows-dialog").close()');
  await click('#flows-btn');
  await waitFor(`document.querySelector('.fb-flow-item')`, 'the list');
  await waitFor(`[...document.querySelectorAll('.fb-flow-name')].some((n) => n.textContent === '${fresh}')`, 'the flow is listed');
  const names = () => evaluate('[...document.querySelectorAll(".fb-flow-name")].map((n) => n.textContent)');
  const box = '.fb-filter';
  await type(box, 'switch-flow');
  await evaluate(`document.querySelector('${box}').dispatchEvent(new Event('input'))`);
  assert.ok((await names()).includes(fresh));
  assert.ok(!(await names()).includes('other-switch'), 'filtered out');
  const row = `[...document.querySelectorAll('.fb-flow-row')].find((n) => n.textContent.includes('${fresh}'))`;
  assert.equal(await evaluate(`${row}.querySelector('.fb-switch').getAttribute('aria-checked')`), 'false');
  // Turning on goes through the activation preview.
  await evaluate(`${row}.querySelector('.fb-switch').click()`);
  await waitFor('document.querySelector(".fb-ask")?.open', 'the preview');
  assert.match(await evaluate('document.querySelector(".fb-ask").textContent'), new RegExp(`Activate ${fresh}`));
  await click('.fb-ask button', 'Activate');
  await sleep(100);
  await dismiss();  // the results of the activation
  await waitFor(`${row}.querySelector('.fb-switch').getAttribute('aria-checked') === 'true'`, 'enabled');
  // Turning off keeps the draft.
  await evaluate(`${row}.querySelector('.fb-switch').click()`);
  await waitFor('/Disabling/.test(document.querySelector(".fb-ask")?.textContent || "") && document.querySelector(".fb-ask").open', 'the disable preview');
  assert.match(await evaluate('document.querySelector(".fb-ask").textContent'), /saved draft stays/);
  await click('.fb-ask button', 'Disable');
  await sleep(100);
  await dismiss();
  await waitFor(`${row}.querySelector('.fb-switch').getAttribute('aria-checked') === 'false'`, 'disabled');
  assert.ok(calls.some((c) => c.method === 'POST' && c.path === `/api/flow-documents/${fresh}/deactivate`), 'asked the host to disable it');
  assert.equal(store.get(fresh).yaml != null, true, 'the draft stays');
  // The filter is cleared and the graph behaves: shift+scroll goes sideways; the ? stays open on click.
  await evaluate(`(() => { const n = document.querySelector('${box}'); n.value = ''; n.dispatchEvent(new Event('input')); })()`);
  await evaluate('document.getElementById("fb-tab-graph").click()');
  await sleep(100);
  const viewport = () => evaluate('document.querySelector(".fg-viewport").getAttribute("transform")');
  const before = await viewport();
  const mid = await centre('.fg-svg');
  await call('Input.dispatchMouseEvent', { type: 'mouseWheel', x: mid.x, y: mid.y, deltaX: 0, deltaY: 80, modifiers: 8 });
  await sleep(100);
  const [bx, by] = before.match(/-?[\d.]+/g).map(Number), [ax, ay] = (await viewport()).match(/-?[\d.]+/g).map(Number);
  assert.notEqual(ax, bx, 'moved sideways');
  assert.equal(ay, by, 'not up or down');
  await click('.fg-help');
  await waitFor('document.querySelector(".tip.pinned.on")', 'the help');
  assert.match(await evaluate('document.querySelector(".tip.pinned").textContent'), /Right-click/);
  await click('.fg-help');
  await waitFor('!document.querySelector(".tip.pinned")', 'closed again');
  store.delete(fresh); store.delete('other-switch');
  await click('.fb-close'); await sleep(50);
  if (await evaluate('document.querySelector(".fb-ask")?.open')) await click('.fb-ask button', 'Discard changes');
  await sleep(50);
  await evaluate('document.querySelector("#flows-btn").click()');
  await sleep(300);
});

test('a step shows how many workers can take it, and clicking opens the fleet filtered to what it needs', async () => {
  store.set('count-flow', { yaml: serialize({ name: 'count-flow', input: {}, worker: { pool: 'build' }, steps: { build: { kind: 'program', run: 'python build.py', needs: [] } } }), active: null, layout: null });
  await evaluate('document.querySelector("#flows-dialog").close()');
  await click('#flows-btn');
  await waitFor(`[...document.querySelectorAll('.fb-flow-name')].some((n) => n.textContent === 'count-flow')`, 'the flow');
  await click('.fb-flow-item', 'count-flow');
  await waitFor(`document.querySelector('.fb-title').textContent === 'count-flow'`);
  await evaluate(`document.querySelector('[data-fold="flow-where"]').open = true`);
  await waitFor(`document.querySelector('[data-where="build"] .fb-where-count')`, 'the count');
  assert.match(await evaluate(`document.querySelector('[data-where="build"] .fb-where-count').textContent`), /^\d+$/);
  assert.match(await evaluate(`document.querySelector('[data-where="build"]').getAttribute('title') || document.querySelector('[data-where="build"]').dataset.tip`), /workers can run build/);
  await evaluate(`document.querySelector('[data-where="build"]').click()`);
  await waitFor('document.querySelector("#fleet-dialog").open', 'the fleet');
  assert.match(await evaluate('document.querySelector(".fl-filter-input").value'), /pool:build/);
  assert.equal(await evaluate('document.querySelector("#flows-dialog").open'), true, 'over the editor, which stays where it was');
  await evaluate('document.querySelector(\'[aria-label="Close fleet"]\').click()');
  await waitFor('!document.querySelector("#fleet-dialog").open', 'back to the editor');
  store.delete('count-flow');
});
test('mobile Flow Builder keeps its filter usable and navigation icons visible', async () => {
  await call('Emulation.setDeviceMetricsOverride', { width: 390, height: 844, deviceScaleFactor: 1, mobile: true });
  await evaluate('document.querySelector("#flows-dialog").close()');
  await evaluate('document.querySelector("#flows-btn").click()');
  await waitFor('document.querySelector("#flows-dialog").open && document.querySelector(".fb-filter")', 'the mobile flow editor');
  const mobile = await evaluate(`(() => {
    const filter = document.querySelector('.fb-filter');
    return {
      filterWidth: filter.getBoundingClientRect().width,
      iconDisplay: ['#flows-btn', '#fleet-btn', '#admin-btn'].map((selector) => getComputedStyle(document.querySelector(selector + ' svg')).display),
      dialog: document.querySelector('#flows-dialog').getBoundingClientRect().toJSON()
    };
  })()`);
  assert.ok(mobile.filterWidth >= 120, `flow filter should have a usable width, got ${mobile.filterWidth}px`);
  assert.ok(mobile.iconDisplay.every((display) => display !== 'none'), 'all top-level navigation icons remain visible on phones');
  assert.equal(mobile.dialog.width, 390);
  await evaluate('document.querySelector(".fb-tab").click()');
  await waitFor('document.querySelector(".fg-help")', 'the graph controls');
  const graph = await evaluate(`(() => {
    const help = document.querySelector('.fg-help').getBoundingClientRect();
    return { helpWidth: help.width, helpHeight: help.height, canvasWidth: document.querySelector('.fg-canvas').getBoundingClientRect().width };
  })()`);
  assert.equal(graph.helpWidth, 44, 'touch help control is round and meets the 44px target');
  assert.equal(graph.helpHeight, 44);
  assert.ok(graph.canvasWidth <= 390, 'graph fits the phone viewport');
  await evaluate('document.querySelector("#flows-dialog").close()');
  await waitFor('!document.querySelector("#flows-dialog").open');
});
