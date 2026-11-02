/* Real Chromium integration checks with a local, deterministic API fixture.
 * node --test board/static/chat.browser.test.cjs
 * CHROME_PATH may override the installed Chrome executable. No packages or backend needed.
 */
const { test } = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const os = require('node:os');
const { spawn } = require('node:child_process');

test('chat supports approvals, typed answers, polling preservation, explicit done, and production guard', { timeout: 60000 }, async () => {
  const chromePath = process.env.CHROME_PATH || 'C:/Program Files/Google/Chrome/Application/chrome.exe';
  assert.ok(fs.existsSync(chromePath), 'Set CHROME_PATH to a Chromium browser');
  const posts = [], errors = [];
  const events = [
    { type: 'user', seq: 1, text: 'Fix the test' },
    { type: 'agent', seq: 2, message_id: 'm', text: 'I will ' },
    { type: 'agent', seq: 3, message_id: 'm', text: 'inspect it.' },
    { type: 'tool', seq: 4, id: 'read', name: 'Read file', status: 'running', summary: 'Opening test.py' },
    { type: 'tool', seq: 5, id: 'read', status: 'ok', output: 'assert actual == expected' },
    { type: 'approval', seq: 6, id: 'permission', summary: 'Run the tests?', detail: 'python -m unittest', options: [{ id: 'allow-123', label: 'Allow once', kind: 'allow_once' }, { id: 'deny-123', label: 'Deny', kind: 'reject_once' }] },
    { type: 'question', seq: 7, id: 'question', message: 'Choose how to fix it', schema: { type: 'object', required: ['count', 'choice', 'tags'], properties: {
      count: { type: 'integer', title: 'Retries', minimum: 1 }, choice: { type: 'boolean', title: 'Use cache', oneOf: [{ const: true, title: 'Enabled' }, { const: false, title: 'Disabled' }] },
      tags: { type: 'array', title: 'Targets', items: { enum: [1, 2] } }, other: { type: 'string', title: 'Other', pattern: '^ok$' },
    } } },
  ];
  let state = 'question';
  const card = () => ({ id: 'run1', title: 'Fix a failing test', task: 'agent-session', agent: 'demo', status: 'RUNNING', column: state === 'done' ? 'done' : 'waiting', session_state: state, progress: [], events, created_at: new Date().toISOString() });
  const server = http.createServer(async (request, response) => {
    try {
      const url = new URL(request.url, 'http://localhost');
      response.setHeader('Content-Type', 'application/json');
      if (request.method === 'POST') {
        let raw = ''; for await (const chunk of request) raw += chunk;
        const body = JSON.parse(raw || '{}'); posts.push({ url: url.pathname, body });
        if (body.type === 'approval') events.push({ type: 'approval_resolved', id: body.id, decision: 'allow', optionId: body.optionId });
        if (body.type === 'answer') {
          if (body.answers.other === 'invalid') events.push({ type: 'error', message: 'Please check your answer: pattern does not match', will_retry: false });
          else events.push({ type: 'answer', id: body.id, answers: body.answers });
        }
        if (body.type === 'message') { events.push({ type: 'user', text: body.text }); state = 'awaiting_review'; }
        if (body.type === 'done') state = 'done';
        response.end(JSON.stringify({ id: 'run1', ok: true })); return;
      }
      if (url.pathname === '/api/systems') { response.end(JSON.stringify({ systems: ['hatchet', 'trigger-dev', 'trigger'], columns: ['scheduled', 'queued', 'running', 'waiting', 'done', 'stopped'] })); return; }
      if (url.pathname.endsWith('/cards')) {
        const snapshot = { cards: [{ ...card(), title: url.pathname.includes('/trigger-dev/') ? 'Local agent session' : 'Fix a failing test' }] };
        if (url.pathname.includes('/hatchet/slow/')) await new Promise(resolve => setTimeout(resolve, 700));
        response.end(JSON.stringify(snapshot)); return;
      }
      if (url.pathname.endsWith('/dag')) { response.end(JSON.stringify({ nodes: [], edges: [] })); return; }
      const file = url.pathname === '/' ? 'index.html' : url.pathname.replace('/static/', '');
      if (!['index.html', 'chat.js', 'chat.css'].includes(file)) { response.writeHead(404).end(); return; }
      response.setHeader('Content-Type', file.endsWith('.js') ? 'text/javascript' : file.endsWith('.css') ? 'text/css' : 'text/html');
      response.end(fs.readFileSync(path.join(__dirname, file)));
    } catch (error) { response.writeHead(500).end(JSON.stringify({ error: error.message })); }
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const profile = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-board-chat-'));
  const browser = spawn(chromePath, ['--headless=new', '--disable-gpu', '--no-first-run', '--no-default-browser-check', '--remote-debugging-port=0', `--user-data-dir=${profile}`, 'about:blank'], { stdio: 'ignore', windowsHide: true });
  let socket;
  try {
    let port;
    for (let i = 0; i < 100; i++) {
      const active = path.join(profile, 'DevToolsActivePort');
      if (fs.existsSync(active)) { port = fs.readFileSync(active, 'utf8').split('\n')[0]; break; }
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    assert.ok(port, 'Chrome debugging endpoint starts');
    const targets = await (await fetch(`http://127.0.0.1:${port}/json`)).json();
    socket = new WebSocket(targets.find(x => x.type === 'page').webSocketDebuggerUrl);
    await new Promise(resolve => socket.addEventListener('open', resolve, { once: true }));
    let id = 0; const pending = new Map();
    socket.addEventListener('message', event => {
      const data = JSON.parse(event.data);
      if (data.id) { const handler = pending.get(data.id); pending.delete(data.id); if (data.error) handler.reject(new Error(data.error.message)); else handler.resolve(data.result); }
      if (data.method === 'Runtime.exceptionThrown') errors.push(data.params.exceptionDetails.text + ': ' + (data.params.exceptionDetails.exception?.description || ''));
    });
    const call = (method, params = {}) => new Promise((resolve, reject) => { const key = ++id; pending.set(key, { resolve, reject }); socket.send(JSON.stringify({ id: key, method, params })); });
    const evaluate = async expression => { const r = await call('Runtime.evaluate', { expression, awaitPromise: true, returnByValue: true }); if (r.exceptionDetails) throw new Error(r.exceptionDetails.exception?.description || r.exceptionDetails.text); return r.result.value; };
    const waitFor = async expression => { for (let i = 0; i < 80; i++) { if (await evaluate(expression)) return; await new Promise(resolve => setTimeout(resolve, 100)); } throw new Error(`Timed out: ${expression}`); };
    await call('Runtime.enable');
    await call('Emulation.setDeviceMetricsOverride', { width: 1440, height: 1000, deviceScaleFactor: 1, mobile: false });
    await call('Page.navigate', { url: `http://127.0.0.1:${server.address().port}` });
    await waitFor('document.querySelector(".card")');
    await evaluate('document.querySelector(".card").click()');
    await waitFor('document.querySelector(".question-card")');
    assert.equal(await evaluate('document.querySelectorAll(".chat-message.agent").length'), 1);
    assert.equal(await evaluate('document.querySelector(".chat-message.agent .message-text").textContent'), 'I will inspect it.');
    assert.equal(await evaluate('document.querySelectorAll(".tool-row").length'), 1);
    await evaluate(`document.querySelector('.tool-row').open = true;
      document.querySelector('.permission-detail').open = true;
      document.querySelector('[aria-label="Message to agent"]').value = 'Draft guidance';
      document.querySelector('[aria-label="Message to agent"]').dispatchEvent(new Event('input'));
      document.querySelector('[aria-label="Retries"]').value = '3';
      document.querySelector('[aria-label="Retries"]').dispatchEvent(new Event('input'));
      document.querySelectorAll('.question-option input')[1].click();
      document.querySelectorAll('.question-option input')[2].click();`);
    await new Promise(resolve => setTimeout(resolve, 100));
    events.push({ type: 'agent', message_id: 'm', text: ' Ready.' });
    await evaluate('refresh()');
    assert.equal(await evaluate('document.querySelector(".tool-row").open'), true);
    assert.equal(await evaluate('document.querySelector(".permission-detail").open'), true);
    assert.equal(await evaluate(`document.querySelector('[aria-label="Message to agent"]').value`), 'Draft guidance');
    assert.equal(await evaluate('document.querySelector("[aria-label=Retries]").value'), '3');
    assert.equal(await evaluate('document.querySelectorAll(".question-option input")[1].checked'), true);
    await evaluate('document.querySelector(".chat-log").scrollTop = 0');
    events.push({ type: 'agent', message_id: 'm', text: ' More activity.' });
    await evaluate('refresh()');
    assert.equal(await evaluate('document.querySelector(".chat-log").scrollTop'), 0);
    if (process.env.CHAT_SCREENSHOT) {
      const screenshot = await call('Page.captureScreenshot', { format: 'png' });
      fs.writeFileSync(process.env.CHAT_SCREENSHOT, Buffer.from(screenshot.data, 'base64'));
    }
    await evaluate(`Array.from(document.querySelectorAll('#systems button')).find(b => b.textContent.includes('local')).click()`);
    await waitFor('document.querySelector(".card .title")?.textContent === "Local agent session"');
    await evaluate('document.querySelector(".card").click()');
    await evaluate(`Array.from(document.querySelectorAll('#systems button')).find(b => b.textContent === 'Hatchet').click()`);
    await waitFor('document.querySelector(".card .title")?.textContent === "Fix a failing test"');
    await evaluate('document.querySelector(".card").click()');
    assert.equal(await evaluate('document.querySelector(".chat-log").scrollTop'), 0, 'a saved top-of-transcript position survives switching runs');
    assert.equal(await evaluate(`document.querySelector('[aria-label="Message to agent"]').value`), 'Draft guidance');
    await evaluate('document.querySelector(".approval-actions button").click()');
    await waitFor('document.querySelector(".approval-card .request-resolution")');
    assert.deepEqual(posts.at(-1), { url: '/api/cards/hatchet/run1/input', body: { type: 'approval', id: 'permission', optionId: 'allow-123' } });
    await evaluate(`document.querySelector('[aria-label="Other"]').value = 'invalid'; document.querySelector('[aria-label="Other"]').dispatchEvent(new Event('input')); document.querySelector('.question-card').requestSubmit()`);
    await waitFor('document.querySelector(".transcript-error")');
    assert.equal(await evaluate('document.querySelector(".question-card button")?.disabled === false'), true, 'a transport acknowledgement must not prevent correcting an answer rejected by the driver');
    assert.equal(await evaluate('document.querySelector(".question-card .request-resolution") !== null'), false);
    await evaluate(`document.querySelector('[aria-label="Other"]').value = 'ok'; document.querySelector('[aria-label="Other"]').dispatchEvent(new Event('input')); document.querySelector('.question-card').requestSubmit()`);
    await waitFor('document.querySelector(".question-card .request-resolution")');
    assert.deepEqual(posts.at(-1).body, { type: 'answer', id: 'question', answers: { count: 3, choice: false, tags: [1], other: 'ok' } });
    await evaluate('document.querySelector(".chat-composer").requestSubmit()');
    await waitFor('document.querySelector(".state-awaiting_review")');
    assert.equal(posts.at(-1).body.type, 'message');
    assert.equal(posts.filter(x => x.body.type === 'done').length, 0);
    await evaluate('document.querySelector(".finish-session").click()');
    await waitFor('document.querySelector(".state-done")');
    assert.equal(await evaluate('document.querySelector(".chat-composer textarea").disabled'), true);
    await evaluate(`boards.push('slow'); renderHeader(); document.querySelector('#board').value = 'slow'; document.querySelector('#board').dispatchEvent(new Event('change')); Array.from(document.querySelectorAll('#systems button')).find(b => b.textContent.includes('local')).click()`);
    await waitFor('document.querySelector(".card .title")?.textContent === "Local agent session"');
    await new Promise(resolve => setTimeout(resolve, 900));
    assert.equal(await evaluate('document.querySelector(".card .title").textContent'), 'Local agent session');
    await evaluate('Array.from(document.querySelectorAll("#systems button")).find(b => b.textContent.includes("production")).click(); document.querySelector("#new-card").click()');
    assert.equal(await evaluate('document.querySelector("#n-create").disabled'), true);
    assert.equal(await evaluate('document.querySelector("#n-production-note").hidden'), false);
    await call('Emulation.setDeviceMetricsOverride', { width: 390, height: 844, deviceScaleFactor: 1, mobile: false });
    await waitFor('document.querySelector(".card")');
    await evaluate('document.querySelector("#new-dialog").close(); document.querySelector("#d-close").click(); document.querySelector(".card").click()');
    assert.equal(await evaluate('document.querySelector("#drawer").getBoundingClientRect().width <= 390'), true);
    assert.deepEqual(errors, []);
  } finally {
    if (socket) socket.close();
    browser.kill();
    server.closeAllConnections();
    await new Promise(resolve => server.close(resolve));
    // Chrome can briefly retain file handles; leave only its disposable temp profile if still busy.
    try { fs.rmSync(profile, { recursive: true, force: true }); } catch {}
  }
});
