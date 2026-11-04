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

const chat = require('./chat.js');

// Rows with the same keys the server's transcript.coalesce gives them.
function rowsOf(events) {
  return chat.transcript(events).map(row => ({ ...row, key: row.type === 'acp_state' ? 'acp_state' : row.type === 'commands' ? 'commands' : row.type === 'agent' && row.message_id ? `m:${row.message_id}`
    : row.type === 'tool' ? `t:${row.id}` : row.type === 'approval' ? `a:${row.id}` : row.type === 'question' ? `q:${row.id}` : `e:${JSON.stringify(row)}` }));
}

test('chat supports approvals, typed answers, live-update preservation, paging, explicit done, and production guard', { timeout: 90000 }, async () => {
  const chromePath = process.env.CHROME_PATH || 'C:/Program Files/Google/Chrome/Application/chrome.exe';
  assert.ok(fs.existsSync(chromePath), 'Set CHROME_PATH to a Chromium browser');
  const posts = [], errors = [];
  let nodeApproval = null;
  const events = [
    { type: 'acp_state', capabilities: { promptCapabilities: { image: true, embeddedContext: true } }, usage: { used: 40, size: 100 }, configOptions: [{ id: 'permissions', name: 'Permission mode', type: 'select', category: 'mode', currentValue: 'ask', options: [{ value: 'ask', name: 'Ask' }, { value: 'auto', name: 'Automatic' }] }] },
    { type: 'commands', commands: [{ name: 'test', description: 'Run tests', input: { hint: 'path' } }, { name: 'plan', description: 'Plan work' }] },
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
  const card = () => ({ id: 'run1', title: 'Fix a failing test', task: 'agent-session', agent: 'demo', status: 'RUNNING', column: state === 'done' ? 'done' : 'waiting', session_state: state, last: null, created_at: '2026-01-01T00:00:00Z' });
  // A long session and a long plain log, for paging.
  const longRows = Array.from({ length: 250 }, (_, i) => ({ type: 'user', text: `message ${i}`, key: `e:${i}` }));
  longRows.unshift({ type: 'tool', id: 'old-tool', name: 'read', status: 'running', key: 't:old-tool' });
  const longLines = Array.from({ length: 2500 }, (_, i) => `line ${i}`);
  const longCards = [{ id: 'long1', title: 'Long session', task: 'agent-session', agent: 'demo', status: 'RUNNING', column: 'running', session_state: 'working', created_at: '2026-01-01T00:00:00Z' },
    { id: 'logs1', title: 'Long log', task: 'research-report', status: 'RUNNING', column: 'running', created_at: '2026-01-02T00:00:00Z' }];
  // The live stream: like the board's /api/live, a snapshot on connect and pushes after each change.
  const streams = new Set();
  const send = (stream, message) => stream.response.write(`data: ${JSON.stringify(message)}\n\n`);
  const flowCard = { id: 'flow1', title: 'Flow with steps', task: 'demo-flow', flow: 'demo-flow', status: 'COMPLETED', column: 'done', finished_at: '2026-01-01T00:00:01Z', backend_url: 'http://localhost:4200/runs/flow-run/flow1' };
  const flowNodes = [{ id: 'agent-node', step: 'talk', label: 'talk', ref: 'agent-ref', kind: 'agent', agent: 'demo', column: 'running', status: 'RUNNING', restart_supported: true, backend_url: 'http://localhost:4200/runs/task-run/agent-ref' },
    { id: 'program-node', step: 'compile', label: 'compile', ref: 'program-ref', kind: 'program', column: 'done', status: 'COMPLETED', restart_supported: true }];
  const cardsOf = stream => stream.board === 'flow' ? [flowCard] : stream.board === 'long' ? longCards : [{ ...card(), title: stream.system === 'trigger-dev' ? 'Local agent session' : 'Fix a failing test' }];
  const push = () => { for (const stream of streams) if (stream.board !== 'long') {
    send(stream, { type: 'cards', upsert: cardsOf(stream), remove: [] });
    if (stream.card) send(stream, { type: 'rows', card: stream.card, rows: rowsOf(events) });
  } };
  const server = http.createServer(async (request, response) => {
    try {
      const url = new URL(request.url, 'http://localhost');
      response.setHeader('Content-Type', 'application/json');
      if (request.method === 'POST') {
        let raw = ''; for await (const chunk of request) raw += chunk;
        const body = JSON.parse(raw || '{}'); posts.push({ url: url.pathname, body });
        // Holding the reply keeps the step's approval in flight, so the test can see its buttons disabled.
        if (url.pathname.includes('/nodes/') && body.type === 'approval') await nodeApproval?.promise;
        if (url.pathname.includes('/nodes/')) { response.end(JSON.stringify({ ok: true, id: 'flow1', replayed: url.pathname.endsWith('/restart') })); return; }
        if (body.type === 'approval') events.push({ type: 'approval_resolved', id: body.id, decision: 'allow', optionId: body.optionId });
        if (body.type === 'answer') {
          if (body.answers.other === 'invalid') events.push({ type: 'error', message: 'Please check your answer: pattern does not match', will_retry: false });
          else events.push({ type: 'answer', id: body.id, answers: body.answers });
        }
        if (body.type === 'message') { events.push({ type: 'user', text: body.text }); state = 'awaiting_review'; }
        if (body.type === 'done') state = 'done';
        response.end(JSON.stringify({ id: 'run1', ok: true })); push(); return;
      }
      if (url.pathname === '/api/live') {
        const stream = { response, system: url.searchParams.get('system'), board: url.searchParams.get('board'), card: url.searchParams.get('card'), node: url.searchParams.get('node') };
        response.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache' });
        if (stream.board === 'slow') await new Promise(resolve => setTimeout(resolve, 700));
        streams.add(stream); request.on('close', () => streams.delete(stream));
        send(stream, { type: 'feed', live: true });
        send(stream, { type: 'cards', reset: true, cards: cardsOf(stream) });
        if (stream.node === 'agent-ref') send(stream, { type: 'node-rows', card: 'flow1', node: 'agent-ref', state: 'question', reset: true, rows: rowsOf(events.slice(0, 9)), more: false });
        else if (stream.node === 'program-ref') send(stream, { type: 'node-lines', card: 'flow1', node: 'program-ref', reset: true, start: 1500, lines: longLines.slice(1500), more: true });
        else if (stream.card === 'long1') send(stream, { type: 'rows', card: 'long1', reset: true, rows: longRows.slice(-100), more: true });
        else if (stream.card === 'logs1') send(stream, { type: 'lines', card: 'logs1', reset: true, start: 1500, lines: longLines.slice(1500), more: true });
        else if (stream.card) send(stream, { type: 'rows', card: stream.card, reset: true, rows: rowsOf(events), more: false });
        return;
      }
      if (url.pathname.endsWith('/long1/transcript')) {
        await new Promise(resolve => setTimeout(resolve, 400)); // long enough to see the spinner
        const end = longRows.findIndex(row => row.key === url.searchParams.get('before')), start = Math.max(0, end - 100);
        response.end(JSON.stringify({ rows: longRows.slice(start, end), more: start > 0 })); return;
      }
      if (url.pathname.endsWith('/logs1/logs')) {
        await new Promise(resolve => setTimeout(resolve, 400));
        const end = Number(url.searchParams.get('before')), start = Math.max(0, end - 1000);
        response.end(JSON.stringify({ start, lines: longLines.slice(start, end), more: start > 0, reset: true })); return;
      }
      if (url.pathname === '/api/systems') { response.end(JSON.stringify({ systems: ['hatchet', 'trigger-dev', 'trigger'], columns: ['scheduled', 'queued', 'running', 'waiting', 'done', 'stopped'] })); return; }
      if (url.pathname.endsWith('/dag')) { response.end(JSON.stringify({ model: 'declared', nodes: url.pathname.includes('flow1') ? flowNodes : [], edges: [] })); return; }
      if (url.pathname === '/api/flows') { response.end(JSON.stringify({ flows: [{name:'worker-flow',input:{topic:'test'},steps:{build:{kind:'program',needs:[]},review:{kind:'agent',needs:['build']}}}], broken: {} })); return; }
      if (url.pathname === '/api/workers') { response.end(JSON.stringify({workers:[{id:'pc-a',pool:'builds',tags:['linux'],online:true},{id:'pc-b',pool:'review',tags:[],online:true}]})); return; }
      const file = url.pathname === '/' ? 'index.html' : url.pathname.replace('/static/', '');
      if (!['index.html', 'chat.js', 'chat.css', 'flow-builder.js', 'flow-builder.css'].includes(file)) { response.writeHead(404).end(); return; }
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
    const evaluate = async expression => { let r; try { r = await call('Runtime.evaluate', { expression, awaitPromise: true, returnByValue: true }); } catch (error) { throw new Error(`${expression}: ${error.message}`); } if (r.exceptionDetails) throw new Error(r.exceptionDetails.exception?.description || r.exceptionDetails.text); return r.result.value; };
    const waitFor = async expression => { for (let i = 0; i < 80; i++) { if (await evaluate(expression)) return; await new Promise(resolve => setTimeout(resolve, 100)); } throw new Error(`Timed out: ${expression}`); };
    await call('Runtime.enable');
    await call('Emulation.setDeviceMetricsOverride', { width: 1440, height: 1000, deviceScaleFactor: 1, mobile: false });
    await call('Page.navigate', { url: `http://127.0.0.1:${server.address().port}` });
    await waitFor('document.querySelector(".card")');
    await evaluate('document.querySelector(".card").click()');
    await waitFor('document.querySelector(".question-card")');
    assert.equal(await evaluate('document.querySelector(".context-usage").textContent'), 'Context 40%');
    await evaluate(`document.querySelector('[aria-label="Permission mode"]').value = 'auto'; document.querySelector('[aria-label="Permission mode"]').dispatchEvent(new Event('change'))`);
    for (let i = 0; i < 50 && !posts.some(p => p.body.type === 'config'); i++) await new Promise(resolve => setTimeout(resolve, 100));
    assert.ok(posts.some(p => p.body.type === 'config' && p.body.id === 'permissions' && p.body.value === 'auto'));
    await evaluate(`{ const input = document.querySelector('[aria-label="Attach images or files"]'); const transfer = new DataTransfer(); transfer.items.add(new File(['hello'], 'note.txt', { type: 'text/plain' })); input.files = transfer.files; input.dispatchEvent(new Event('change')); }`);
    assert.ok(await evaluate(`document.querySelector('.attachment-chip').textContent.includes('note.txt')`));
    await evaluate(`document.querySelector('[aria-label="Remove note.txt"]').click()`);
    await evaluate(`document.querySelector('[aria-label="Message to agent"]').value = '/'; document.querySelector('[aria-label="Message to agent"]').dispatchEvent(new Event('input'))`);
    assert.equal(await evaluate('document.querySelectorAll(".command-suggestions button").length'), 2);
    await evaluate(`document.querySelector('[aria-label="Message to agent"]').dispatchEvent(new KeyboardEvent('keydown', { key: 'ArrowDown', bubbles: true })); document.querySelector('[aria-label="Message to agent"]').dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter', bubbles: true }))`);
    assert.equal(await evaluate(`document.querySelector('[aria-label="Message to agent"]').value`), '/plan ');
    events.push({ type: 'commands', commands: [{ name: 'review', description: 'Review changes' }] }); push();
    await evaluate(`document.querySelector('[aria-label="Message to agent"]').value = '/re'; document.querySelector('[aria-label="Message to agent"]').dispatchEvent(new Event('input'))`);
    await waitFor('document.querySelector(".command-suggestions button")?.textContent.includes("/review")');
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
    push();
    await waitFor('document.querySelector(".chat-message.agent .message-text").textContent.endsWith("Ready.")');
    assert.equal(await evaluate('document.querySelector(".tool-row").open'), true);
    assert.equal(await evaluate('document.querySelector(".permission-detail").open'), true);
    assert.equal(await evaluate(`document.querySelector('[aria-label="Message to agent"]').value`), 'Draft guidance');
    assert.equal(await evaluate('document.querySelector("[aria-label=Retries]").value'), '3');
    assert.equal(await evaluate('document.querySelectorAll(".question-option input")[1].checked'), true);
    await evaluate('document.querySelector(".chat-log").scrollTop = 0');
    events.push({ type: 'agent', message_id: 'm', text: ' More activity.' });
    push();
    await waitFor('document.querySelector(".chat-message.agent .message-text").textContent.endsWith("More activity.")');
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
    await waitFor('window.innerWidth <= 390 && document.querySelector("#drawer").getBoundingClientRect().width <= 390');

    // Paging: a session opens on its newest 100 rows; scrolling to the top loads 100 more above,
    // with a spinner meanwhile and the rows on screen staying where they were.
    await call('Emulation.setDeviceMetricsOverride', { width: 1440, height: 1000, deviceScaleFactor: 1, mobile: false });
    await evaluate(`document.querySelector("#d-close").click(); Array.from(document.querySelectorAll('#systems button')).find(b => b.textContent === 'Hatchet').click();
      boards.push('long'); renderHeader(); document.querySelector('#board').value = 'long'; document.querySelector('#board').dispatchEvent(new Event('change'))`);
    await waitFor('document.querySelector(".card .title")?.textContent === "Long session"');
    await evaluate('document.querySelector(".card").click()');
    await waitFor('document.querySelectorAll(".chat-message.user").length === 100');
    assert.equal(await evaluate('document.querySelector(".chat-message.user .message-text").textContent'), 'message 150');
    assert.equal(await evaluate('document.querySelector(".chat-older button")?.textContent'), 'Load earlier messages');
    await evaluate('window.anchorRow = document.querySelectorAll(".chat-message.user")[0]; document.querySelector(".chat-log").scrollTop = 0; document.querySelector(".chat-log").dispatchEvent(new Event("scroll"))');
    await waitFor('document.querySelector(".chat-older .spinner")');
    await waitFor('document.querySelectorAll(".chat-message.user").length === 200');
    assert.equal(await evaluate('document.querySelector(".chat-message.user .message-text").textContent'), 'message 50');
    assert.equal(await evaluate('Math.abs(window.anchorRow.getBoundingClientRect().top - document.querySelector(".chat-log").getBoundingClientRect().top) < 200'), true,
      'the row that was at the top is still near the top after older rows load above it');
    await evaluate('document.querySelector(".chat-log").scrollTop = 0; document.querySelector(".chat-log").dispatchEvent(new Event("scroll"))');
    await waitFor('document.querySelectorAll(".chat-message.user").length === 250');
    assert.equal(await evaluate('document.querySelector(".chat-older")'), null, 'nothing older to load');
    for (const stream of streams) if (stream.card === 'long1') send(stream, { type: 'rows', card: 'long1', rows: [],
      patches: [{ type: 'tool', id: 'old-tool', name: 'read', status: 'ok', output: 'Older tool finished', key: 't:old-tool' }] });
    await evaluate('document.querySelector(".tool-row").open = true');
    await waitFor('document.querySelector(".tool-row").textContent.includes("Older tool finished")');

    // Ordinary logs: the newest 1000 lines, then 1000 more per scroll to the top.
    await evaluate('document.querySelector("#d-close").click(); document.querySelectorAll(".card")[1].click()');
    await waitFor('document.querySelectorAll("#d-progress li:not(.log-older)").length === 1000');
    assert.equal(await evaluate('document.querySelector("#d-progress li:not(.log-older)").textContent'), 'line 1500');
    await evaluate('const list = document.querySelector("#d-progress"); list.scrollTop = 0; list.dispatchEvent(new Event("scroll"))');
    await waitFor('document.querySelector("#d-progress .spinner")');
    await waitFor('document.querySelectorAll("#d-progress li:not(.log-older)").length === 2000');
    assert.equal(await evaluate('document.querySelector("#d-progress li:not(.log-older)").textContent'), 'line 500');
    assert.equal(await evaluate('document.querySelector("#d-progress").scrollTop > 0'), true, 'the list keeps its place instead of jumping to the oldest line');
    // The DAG agent uses the same permission/question/composer controls and addresses its own
    // step input, while a program step stays a bounded scrolling log.
    await evaluate(`closeDrawer(); boards.push('flow'); board = 'flow'; renderHeader(); cards = []; refresh()`);
    await waitFor('document.querySelector(".card .title")?.textContent === "Flow with steps"');
    await evaluate('document.querySelector(".card").click()');
    await waitFor('document.querySelectorAll(".dag-node").length === 2');
    await evaluate('selectNode("agent-node")');
    await waitFor('document.querySelector("#d-node .question-card")');
    let release; nodeApproval = { promise: new Promise(resolve => { release = resolve; }) };
    await evaluate(`document.querySelector('#d-node .approval-actions button').click()`);
    try { await waitFor(`document.querySelector('#d-node .approval-actions button').disabled`); }
    finally { release(); nodeApproval = null; }
    await waitFor(`!document.querySelector('#d-node .approval-actions button').disabled`);
    assert.equal(posts.at(-1).url, '/api/cards/hatchet/flow1/nodes/agent-ref/input');
    await evaluate(`document.querySelector('#d-node textarea[aria-label="Message to agent"]').value = 'step guidance'; document.querySelector('#d-node .chat-composer').requestSubmit()`);
    await waitFor(`document.querySelector('#d-node textarea[aria-label="Message to agent"]').value === ''`);
    assert.deepEqual(posts.at(-1), { url: '/api/cards/hatchet/flow1/nodes/agent-ref/input', body: { type: 'message', text: 'step guidance' } });
    assert.equal(await evaluate('document.querySelector("#d-node a.backend-link").target'), '_blank');
    await evaluate('selectNode("program-node")');
    await waitFor('document.querySelectorAll("#d-node ol.progress li:not(.log-older)").length === 1000');
    assert.equal(await evaluate('document.querySelector("#d-node ol.progress").scrollHeight > document.querySelector("#d-node ol.progress").clientHeight'), true);
    await evaluate('document.querySelector("#d-node .restart-step").click()');
    await waitFor('document.querySelector("#status").textContent.includes("Engine accepted step replay")');
    assert.equal(posts.at(-1).url, '/api/cards/hatchet/flow1/nodes/program-ref/restart');
    flowNodes[1].restart_supported = false;
    flowNodes[1].restart_reason = 'This engine cannot invalidate descendants';
    await evaluate('dag.final = false; refreshDag()');
    await waitFor('document.querySelector("#d-node").textContent.includes("cannot invalidate descendants")');
    assert.equal(await evaluate('document.querySelector("#d-node .restart-step").disabled'),true);
    assert.match(await evaluate('document.querySelector("#d-node").textContent'), /cannot invalidate descendants/);
    // A fast native replay can complete between board snapshots: terminal -> terminal still
    // needs a new DAG when the engine reports a new finished_at.
    flowNodes[1].output = { result: 'replayed output' };
    flowCard.finished_at = '2026-01-01T00:00:02Z'; push();
    await waitFor('document.querySelector("#d-node").textContent.includes("replayed output")');
    if (process.env.CHAT_NODE_SCREENSHOT) {
      await evaluate('selectNode("agent-node")');
      await waitFor('document.querySelector("#d-node .chat-composer")');
      await call('Emulation.setDeviceMetricsOverride', { width: 390, height: 844, deviceScaleFactor: 1, mobile: false });
      await evaluate('document.querySelector("#d-node").scrollIntoView({block:"end"})');
      const screenshot = await call('Page.captureScreenshot', { format: 'png' });
      fs.writeFileSync(process.env.CHAT_NODE_SCREENSHOT, Buffer.from(screenshot.data, 'base64'));
    }
    // Worker selection changes the launch payload for individual steps, including tag filters.
    await evaluate('closeDrawer(); document.querySelector("#new-card").click()');
    await waitFor(`document.querySelector('#n-kind option[value="flow:worker-flow"]')`);
    await evaluate(`document.querySelector('#n-kind').value='flow:worker-flow'; document.querySelector('#n-kind').dispatchEvent(new Event('change'))`);
    await waitFor(`document.querySelector('select[aria-label="build"]')`);
    await evaluate(`document.querySelector('select[aria-label="Default worker / pool"]').value=JSON.stringify({pool:'builds'});
      document.querySelector('select[aria-label="Default worker / pool"]').dispatchEvent(new Event('change'));
      document.querySelector('select[aria-label="review"]').value=JSON.stringify({id:'pc-b'});
      document.querySelector('select[aria-label="review"]').dispatchEvent(new Event('change'));
      document.querySelector('input[aria-label="build tags"]').value='linux, gpu';
      document.querySelector('input[aria-label="build tags"]').dispatchEvent(new Event('input'));
      document.querySelector('#new-form').requestSubmit(document.querySelector('#n-create'))`);
    await waitFor('!document.querySelector("#new-dialog").open');
    assert.deepEqual(posts.at(-1).body.execution, {default:{pool:'builds'},steps:{review:{id:'pc-b'},build:{tags:['linux','gpu']}}});
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
