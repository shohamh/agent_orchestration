/* Real Chromium checks for the board's filter box, "Hide done / failed" and column paging, against
 * a fake board API. node --test board/static/card-search.browser.test.cjs (CHROME_PATH overrides Chrome).
 */
const { test } = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const os = require('node:os');
const { spawn } = require('node:child_process');

const COLUMNS = ['scheduled', 'queued', 'running', 'waiting', 'done', 'stopped'];
const at = (minutes) => new Date(Date.UTC(2026, 10, 5, 8, minutes)).toISOString();
const CARDS = [
  { id: 'r1', column: 'running', status: 'RUNNING', title: 'Benchmark GPU scheduling', task: 'research-report', started_at: at(1),
    input: { goal: 'compare schedulers', depth: 3 }, workers: [{ step: 'fetch', id: 'gpu-02', hostname: 'gpu-02.lan', pool: 'gpu', tags: ['cuda'] }] },
  { id: 'q1', column: 'queued', status: 'QUEUED', title: 'Audit sbom tooling', task: 'research-report', input: { goal: 'list sbom tools' } },
  { id: 'f1', column: 'stopped', status: 'FAILED', title: 'Summarise otel sampling', task: 'summarise', error: 'Worker lost', finished_at: at(3) },
  // 120 finished cards: more than one page of the Done column.
  ...Array.from({ length: 120 }, (_, i) => ({ id: `d${i}`, column: 'done', status: 'COMPLETED', title: `Report ${i}`, task: 'research-report',
    finished_at: at(10 + i), output: { summary: i === 7 ? 'recommend pgvector' : 'fine' } })),
].map((c) => ({ system: 'hatchet', board: 'research', created_at: at(0), queue: {}, moves: {}, workers: [], ...c }));

test('the filter box, hide finished and column paging work on a live board', { timeout: 60000 }, async () => {
  const server = http.createServer((request, response) => {
    const url = new URL(request.url, 'http://localhost');
    if (url.pathname === '/api/live') {
      response.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache' });
      response.write(`data: ${JSON.stringify({ type: 'feed', live: true })}\n\n`);
      const fields = [...new Set(CARDS.flatMap((card) => Object.keys(card)))];
      const message = url.searchParams.get('compact') === '1'
        ? { type: 'cards', reset: true, fields, rows: CARDS.map((card) => fields.map((key) => card[key] ?? null)) }
        : { type: 'cards', reset: true, cards: CARDS };
      response.write(`data: ${JSON.stringify(message)}\n\n`);
      return;
    }
    response.setHeader('Content-Type', 'application/json');
    if (url.pathname === '/api/systems') return response.end(JSON.stringify({ systems: ['hatchet'], columns: COLUMNS }));
    if (url.pathname === '/api/flows') return response.end(JSON.stringify({ flows: [], broken: {} }));
    if (url.pathname === '/api/workers') return response.end(JSON.stringify({ workers: [] }));
    if (url.pathname.endsWith('/dag')) return response.end(JSON.stringify({ model: 'declared', nodes: [], edges: [] }));
    const file = url.pathname === '/' ? 'index.html' : url.pathname.replace('/static/', '');
    if (!['index.html', 'chat.js', 'card-search.js', 'chat.css', 'flow-builder.js', 'flow-builder.css', 'fleet.js', 'fleet.css', 'new-card.js', 'new-card.css', 'overlays.js', 'flow-graph.js', 'flow-graph.css'].includes(file)) return response.writeHead(404).end();
    response.setHeader('Content-Type', file.endsWith('.js') ? 'text/javascript' : file.endsWith('.css') ? 'text/css' : 'text/html');
    response.end(fs.readFileSync(path.join(__dirname, file)));
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const chromePath = process.env.CHROME_PATH || 'C:/Program Files/Google/Chrome/Application/chrome.exe';
  const profile = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-board-search-'));
  const browser = spawn(chromePath, ['--headless=new', '--disable-gpu', '--no-first-run', '--no-default-browser-check', '--remote-debugging-port=0', `--user-data-dir=${profile}`, 'about:blank'], { stdio: 'ignore', windowsHide: true });
  const errors = [];
  let socket;
  try {
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
      if (data.id) { const h = pending.get(data.id); pending.delete(data.id); data.error ? h.reject(new Error(data.error.message)) : h.resolve(data.result); }
      if (data.method === 'Runtime.exceptionThrown') errors.push(data.params.exceptionDetails.exception?.description || data.params.exceptionDetails.text);
    });
    const call = (method, params = {}) => new Promise((resolve, reject) => { const key = ++id; pending.set(key, { resolve, reject }); socket.send(JSON.stringify({ id: key, method, params })); });
    const evaluate = async (expression) => {
      const r = await call('Runtime.evaluate', { expression, awaitPromise: true, returnByValue: true });
      if (r.exceptionDetails) throw new Error(r.exceptionDetails.exception?.description || r.exceptionDetails.text);
      return r.result.value;
    };
    const waitFor = async (expression) => { for (let i = 0; i < 80; i++) { if (await evaluate(expression)) return; await new Promise((resolve) => setTimeout(resolve, 75)); } throw new Error(`Timed out: ${expression}`); };
    const frame = 'new Promise((resolve) => requestAnimationFrame(() => setTimeout(resolve, 0)))';
    const search = async (value) => evaluate(`(() => { const s = document.getElementById('search'); s.value = ${JSON.stringify(value)}; s.dispatchEvent(new Event('input', { bubbles: true })); return ${frame}; })()`);
    const ids = () => evaluate(`[...document.querySelectorAll('#columns .card')].map((n) => n.dataset.id)`);
    const column = (col) => evaluate(`(() => { const s = document.querySelector('[data-col="${col}"]'); return { count: s.querySelector('h2 span:last-child').textContent, cards: s.querySelectorAll('.card').length, text: s.textContent }; })()`);
    const status = () => evaluate(`document.getElementById('status').textContent`);

    await call('Runtime.enable');
    await call('Emulation.setDeviceMetricsOverride', { width: 1440, height: 1000, deviceScaleFactor: 1, mobile: false });
    await call('Page.navigate', { url: `http://127.0.0.1:${server.address().port}` });
    await waitFor(`document.querySelectorAll('#columns .card').length > 0`);

    // Paging: Done shows one page, newest first, and Show more adds the next.
    let done = await column('done');
    assert.deepEqual([done.count, done.cards], ['120', 50]);
    assert.equal((await ids()).filter((x) => x.startsWith('d'))[0], 'd119', 'newest finished card on top');
    await evaluate(`document.querySelector('[data-col="done"] .show-more').click()`);
    assert.equal((await column('done')).cards, 100);

    // Words search everything on the card, including input, output and workers.
    await search('gpu');
    assert.deepEqual(await ids(), ['r1']);
    assert.match(await status(), /1 of 123 cards match/);
    assert.equal((await column('done')).count, '0 / 120');
    assert.match((await column('done')).text, /No matches/);
    await search('pgvector');
    assert.deepEqual(await ids(), ['d7'], 'output is searched, beyond the first page of the column');
    await search('worker:gpu-02 goal:schedulers');
    assert.deepEqual(await ids(), ['r1']);
    await search('goal:sbom');
    assert.deepEqual(await ids(), ['q1']);
    await search('status:failed');
    assert.deepEqual(await ids(), ['f1']);
    await search('-title:report -status:failed');
    assert.deepEqual(await ids(), ['q1', 'r1']);

    // Escape clears the box; / focuses it from anywhere on the board.
    await evaluate(`document.getElementById('search').dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape', bubbles: true }))`);
    await evaluate(frame);
    assert.equal((await ids()).length, 53, 'every column back to its first page');
    await evaluate(`document.activeElement.blur(); document.body.dispatchEvent(new KeyboardEvent('keydown', { key: '/', bubbles: true }))`);
    assert.equal(await evaluate(`document.activeElement.id`), 'search');

    // Hide done / failed: the columns stay (as drop targets) but show how many are hidden; remembered on reload.
    await evaluate(`document.getElementById('hide-finished').click()`);
    await evaluate(frame);
    assert.deepEqual((await ids()).sort(), ['q1', 'r1']);
    assert.match((await column('done')).text, /120 hidden/);
    assert.match((await column('stopped')).text, /1 hidden/);
    await search('gpu');
    assert.deepEqual(await ids(), ['r1'], 'filters combine');
    await evaluate('window.beforeReload = true'); // Page.reload returns before the old page goes away
    await call('Page.reload');
    await waitFor(`!window.beforeReload && typeof cards !== 'undefined' && document.querySelectorAll('#columns .card').length > 0`);
    assert.equal(await evaluate(`document.getElementById('hide-finished').checked`), true);
    assert.deepEqual((await ids()).sort(), ['q1', 'r1']);
    await evaluate(`document.getElementById('hide-finished').click()`);

    // The next page loads when its sentinel is scrolled into view, including on narrow screens.
    for (const width of [1440, 390]) {
      await call('Emulation.setDeviceMetricsOverride', { width, height: 800, deviceScaleFactor: 1, mobile: width < 700 });
      await search('');
      await waitFor(`document.querySelector('[data-col="done"] .show-more')`);
      await evaluate(`document.querySelector('[data-col="done"] .show-more').scrollIntoView()`);
      await waitFor(`document.querySelector('[data-col="done"]').querySelectorAll('.card').length >= 100`);
    }

    // The drawer shows the run's input and its execution workers.
    await evaluate(`document.querySelector('[data-id="r1"]').click()`);
    await waitFor(`document.getElementById('drawer').classList.contains('open')`);
    assert.match(await evaluate(`document.getElementById('d-input').textContent`), /compare schedulers/);
    assert.match(await evaluate(`document.getElementById('d-facts').textContent`), /fetch: gpu-02 · pool gpu · cuda/);
    // Closing the drawer opens a new live stream whose snapshot replaces the cards: let it land first.
    await evaluate(`window.snapshots = 0; { const live = onLive; onLive = (message) => { if (message.type === 'cards' && message.reset) window.snapshots++; live(message); }; } document.getElementById('d-close').click()`);
    await waitFor(`window.snapshots >= 1`);
    await evaluate(`onLive({ type: 'cards', reset: true, cards: [
      { id: 'newer', column: 'running', title: 'Newer', task: 't', system: 'hatchet', status: 'RUNNING', created_at: '2026-01-02', started_at: '2026-01-03', moves: {} },
      { id: 'older', column: 'running', title: 'Older', task: 't', system: 'hatchet', status: 'RUNNING', created_at: '2026-01-01', started_at: '2026-01-03', moves: {} }
    ] })`);
    assert.deepEqual(await ids(), ['older', 'newer'], 'equal start times retain the created-time tie break');
    assert.deepEqual(errors, []);
  } finally {
    socket?.close();
    browser.kill();
    server.closeAllConnections();
    await new Promise((resolve) => server.close(resolve));
    setTimeout(() => { try { fs.rmSync(profile, { recursive: true, force: true }); } catch {} }, 500);
  }
});
