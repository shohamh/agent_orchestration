/* Load test for the board UI: the real board server with the synthetic "loadtest" backend
 * (loadtest_adapter.py) and headless Chrome, at several card counts.
 *
 *   node board/loadtest_ui.cjs [counts, default 1000,5000,10000,20000]
 *
 * PYTHON picks the interpreter that has the board's requirements (default agents/.venv);
 * CHROME_PATH overrides the Chrome executable. For each count it reports the size of the first
 * live snapshot, the time until every column has painted, main-thread time spent on live updates
 * while 20 cards move every 2 s, search keystroke and "Hide finished" latency, and the JS heap.
 */
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const net = require('node:net');
const { spawn } = require('node:child_process');

const root = path.resolve(__dirname, '..');
const python = process.env.PYTHON || path.join(root, 'agents/.venv', process.platform === 'win32' ? 'Scripts/python.exe' : 'bin/python');
const chromePath = process.env.CHROME_PATH || 'C:/Program Files/Google/Chrome/Application/chrome.exe';
const counts = (process.argv[2] || '1000,5000,10000,20000').split(',').map(Number);
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const freePort = () => new Promise((resolve) => { const s = net.createServer().listen(0, '127.0.0.1', () => { const { port } = s.address(); s.close(() => resolve(port)); }); });

// A flow host with no flows: without one, every board refresh waits for a refused connection
// (about 2 s on Windows) before labelling cards.
const flowHost = require('node:http').createServer((request, response) => {
  response.setHeader('Content-Type', 'application/json');
  response.end(JSON.stringify(request.url === '/flows' ? { flows: [], broken: {} } : request.url === '/workers' ? { workers: [] } : { steps: {} }));
});

async function startBoard(count) {
  const port = await freePort();
  if (!flowHost.listening) await new Promise((resolve) => flowHost.listen(0, '127.0.0.1', resolve));
  const env = { ...process.env, BOARD_HOST: '127.0.0.1', BOARD_PORT: String(port), BOARD_LOADTEST_CARDS: String(count),
    BOARD_PASSWORD: '', FLOW_HOST_URL: `http://127.0.0.1:${flowHost.address().port}`, HATCHET_CLIENT_TOKEN: '', TRIGGER_SECRET_KEY: '',
    TRIGGER_DEV_SECRET_KEY: '', PREFECT_API_URL: '' };
  const proc = spawn(python, ['-u', path.join(root, 'board/server.py')], { env, stdio: 'ignore', windowsHide: true });
  for (let i = 0; i < 200; i++) {
    try { if ((await fetch(`http://127.0.0.1:${port}/api/systems`)).ok) return { proc, base: `http://127.0.0.1:${port}` }; } catch {}
    await sleep(100);
  }
  proc.kill(); throw new Error('board did not start');
}

// The board's cards message on a cold server: how long the first viewer of a board waits for it
// (the server builds the board), and its size, which is what every browser downloads.
async function snapshot(base) {
  const started = performance.now(), controller = new AbortController();
  const response = await fetch(`${base}/api/live?system=loadtest&board=research`, { signal: controller.signal });
  const reader = response.body.getReader(), decoder = new TextDecoder();
  let bytes = 0, carry = '', cards = false;
  for (;;) {
    const { value } = await reader.read();
    bytes += value.length;
    const text = carry + decoder.decode(value, { stream: true }); // only the new chunk: the snapshot is megabytes
    const at = cards ? 0 : text.indexOf('"type": "cards"');
    if (at >= 0) cards = true;
    if (cards && text.indexOf('\n\n', at) >= 0) { controller.abort(); return { bytes, ms: performance.now() - started }; }
    carry = text.slice(-32);
  }
}

async function chrome() {
  const profile = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-board-load-'));
  const proc = spawn(chromePath, ['--headless=new', '--disable-gpu', '--no-first-run', '--no-default-browser-check',
    '--remote-debugging-port=0', `--user-data-dir=${profile}`, '--enable-precise-memory-info', 'about:blank'], { stdio: 'ignore', windowsHide: true });
  let port;
  for (let i = 0; i < 300 && !port; i++) {
    try { const text = fs.readFileSync(path.join(profile, 'DevToolsActivePort'), 'utf8'); if (text.includes('\n')) port = text.split('\n')[0]; } catch {}
    if (!port) await sleep(100);
  }
  const targets = await (await fetch(`http://127.0.0.1:${port}/json`)).json();
  const socket = new WebSocket(targets.find((x) => x.type === 'page').webSocketDebuggerUrl);
  await new Promise((resolve) => socket.addEventListener('open', resolve, { once: true }));
  let id = 0; const pending = new Map();
  socket.addEventListener('message', (event) => {
    const data = JSON.parse(event.data);
    if (data.id) { const h = pending.get(data.id); pending.delete(data.id); data.error ? h.reject(new Error(data.error.message)) : h.resolve(data.result); }
  });
  const call = (method, params = {}) => new Promise((resolve, reject) => { const key = ++id; pending.set(key, { resolve, reject }); socket.send(JSON.stringify({ id: key, method, params })); });
  // A frozen page never answers: give up after `ms` instead of hanging the whole run.
  const evaluate = async (expression, ms = 60000) => {
    const r = await Promise.race([call('Runtime.evaluate', { expression, awaitPromise: true, returnByValue: true }), sleep(ms).then(() => ({ timeout: true }))]);
    if (r.timeout) return { timeout: true };
    if (r.exceptionDetails) throw new Error(r.exceptionDetails.exception?.description || r.exceptionDetails.text);
    return r.result.value;
  };
  return { call, evaluate, close: () => { socket.close(); proc.kill(); setTimeout(() => { try { fs.rmSync(profile, { recursive: true, force: true }); } catch {} }, 1000); } };
}

// Installed before the page's own scripts: records long main-thread tasks.
const PROBE = `window.__long = []; new PerformanceObserver((list) => { for (const e of list.getEntries()) window.__long.push({ at: e.startTime, ms: e.duration }); }).observe({ type: 'longtask', buffered: true });
localStorage.setItem('system', '"loadtest"'); localStorage.setItem('board', '"research"');`;

// Time from an input change to the next painted frame.
const latency = (setup) => `new Promise((resolve) => { const t = performance.now(); ${setup};
  requestAnimationFrame(() => setTimeout(() => resolve(Math.round(performance.now() - t)), 0)); })`;

async function measure(count) {
  const { proc, base } = await startBoard(count);
  const browser = await chrome();
  const result = { count };
  try {
    Object.assign(result, await snapshot(base).then((s) => ({ snapshotMB: +(s.bytes / 1e6).toFixed(1), snapshotMs: Math.round(s.ms) })));
    await browser.call('Page.enable'); await browser.call('Runtime.enable');
    await browser.call('Emulation.setDeviceMetricsOverride', { width: 1440, height: 1000, deviceScaleFactor: 1, mobile: false });
    await browser.call('Page.addScriptToEvaluateOnNewDocument', { source: PROBE });
    const started = Date.now();
    await browser.call('Page.navigate', { url: base });
    let painted = null;
    while (Date.now() - started < 120000) {
      const n = await browser.evaluate(`typeof cards !== 'undefined' && cards.length >= ${count} && document.querySelectorAll('#columns .card').length > 0 ? performance.now() : 0`, 30000);
      if (n?.timeout) break;
      if (n) { painted = n; break; }
      await sleep(100);
    }
    result.firstPaintMs = painted ? Math.round(painted) : 'never (2 min)';
    if (!painted) return result;
    // Live updates: 20 cards change every 2 s. Busy = main-thread time in long tasks over 10 s.
    const t0 = await browser.evaluate('performance.now()');
    await sleep(10000);
    const busy = await browser.evaluate(`window.__long.filter((e) => e.at > ${t0}).reduce((s, e) => s + e.ms, 0)`, 60000);
    result.liveBusyPct = busy?.timeout ? 'unresponsive' : Math.round(busy / 100);
    const worst = await browser.evaluate(`Math.max(0, ...window.__long.filter((e) => e.at > ${t0}).map((e) => e.ms))`, 60000);
    result.liveWorstTaskMs = worst?.timeout ? 'unresponsive' : Math.round(worst);
    result.domCards = await browser.evaluate(`document.querySelectorAll('#columns .card').length`);
    const search = await browser.evaluate(`!!document.getElementById('search')`);
    if (search === true) {
      const type = (value) => browser.evaluate(latency(`const s = document.getElementById('search'); s.value = ${JSON.stringify(value)}; s.dispatchEvent(new Event('input', { bubbles: true }))`));
      result.searchMs = [];
      for (const value of ['g', 'gp', 'gpu', 'gpu ', 'gpu w', 'gpu worker:gpu-02', '']) result.searchMs.push(await type(value));
      result.searchHits = await browser.evaluate(`(document.getElementById('search').value = 'worker:gpu-02 -status:failed', document.getElementById('search').dispatchEvent(new Event('input')), new Promise((r) => requestAnimationFrame(() => r(document.getElementById('status').textContent))))`);
      await browser.evaluate(latency(`const s = document.getElementById('search'); s.value = ''; s.dispatchEvent(new Event('input'))`));
      const toggle = () => browser.evaluate(latency(`document.getElementById('hide-finished').click()`));
      result.hideFinishedMs = [await toggle(), await toggle()];
    }
    const heap = await browser.evaluate('performance.memory?.usedJSHeapSize');
    result.heapMB = heap ? Math.round(heap / 1e6) : null;
  } finally { browser.close(); proc.kill(); }
  return result;
}

(async () => {
  for (const count of counts) console.log(JSON.stringify(await measure(count)));
  process.exit(0);
})();
