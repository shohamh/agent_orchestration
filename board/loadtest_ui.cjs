/* Load test for the board UI: the real board server with the synthetic "loadtest" backend
 * (loadtest_adapter.py) and headless Chrome, at several card counts.
 *
 *   node board/loadtest_ui.cjs [counts, default 1000,5000,10000,20000] [--compare[=<git ref>]]
 *
 * --compare also runs the board page from another commit (default main) against the same server
 * and card counts, and prints both side by side. PYTHON picks the interpreter that has the board's
 * requirements (default agents/.venv); CHROME_PATH overrides the Chrome executable.
 * --startup-only stops after the first complete snapshot is rendered; --cold skips the warm-up
 * snapshot. --legacy-snapshot measures the original object-per-card wire format.
 *
 * Per run: time until the first card elements exist, long-task time during live updates while
 * 20 cards move every 2 s, the time to open a card, the JS heap, and for pages with the filter box
 * search keystrokes, "Hide done / failed", loading more cards by scrolling, and a search for a card
 * that isn't rendered. Times to "the next frame" run from the input to the frame after it.
 */
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const net = require('node:net');
const { spawn, execFileSync } = require('node:child_process');

const root = path.resolve(__dirname, '..');
const python = process.env.PYTHON || path.join(root, 'agents/.venv', process.platform === 'win32' ? 'Scripts/python.exe' : 'bin/python');
const chromePath = process.env.CHROME_PATH || 'C:/Program Files/Google/Chrome/Application/chrome.exe';
const args = process.argv.slice(2);
const counts = (args.find((a) => !a.startsWith('--')) || '1000,5000,10000,20000').split(',').map(Number);
const compare = args.find((a) => a.startsWith('--compare'));
const startupOnly = args.includes('--startup-only');
const cold = args.includes('--cold');
const legacySnapshot = args.includes('--legacy-snapshot');
const baselineRef = compare?.includes('=') ? compare.split('=')[1] : 'main';
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const median = (xs) => { const s = [...xs].sort((a, b) => a - b); return s[Math.floor(s.length / 2)]; };
const freePort = () => new Promise((resolve) => { const s = net.createServer().listen(0, '127.0.0.1', () => { const { port } = s.address(); s.close(() => resolve(port)); }); });

// A flow host with no flows: without one, every board refresh waits for a refused connection
// (about 2 s on Windows) before labelling cards.
const flowHost = require('node:http').createServer((request, response) => {
  response.setHeader('Content-Type', 'application/json');
  response.end(JSON.stringify(request.url === '/flows' ? { flows: [], broken: {} } : request.url === '/workers' ? { workers: [] } : { steps: {} }));
});

async function startBoard(count) {
  if (!flowHost.listening) await new Promise((resolve) => flowHost.listen(0, '127.0.0.1', resolve));
  for (let attempt = 0; attempt < 3; attempt++) {
    const port = await freePort();
    const env = { ...process.env, BOARD_HOST: '127.0.0.1', BOARD_PORT: String(port), BOARD_LOADTEST_CARDS: String(count),
      BOARD_PASSWORD: '', FLOW_HOST_URL: `http://127.0.0.1:${flowHost.address().port}`, HATCHET_CLIENT_TOKEN: '', TRIGGER_SECRET_KEY: '',
      TRIGGER_DEV_SECRET_KEY: '', PREFECT_API_URL: '' };
    const proc = spawn(python, ['-u', path.join(root, 'board/server.py')], { env, stdio: 'ignore', windowsHide: true });
    for (let i = 0; i < 200; i++) {
      try { if ((await fetch(`http://127.0.0.1:${port}/api/systems`)).ok) return { proc, base: `http://127.0.0.1:${port}` }; } catch {}
      await sleep(100);
    }
    proc.kill(); // the port may have been taken in between: try another
  }
  throw new Error('board did not start');
}

// The board's cards message on a cold server: how long the first viewer of a board waits for it
// (the server builds the board), and its size, which is what every browser downloads.
async function snapshot(base, compact = false) {
  const started = performance.now(), controller = new AbortController();
  const response = await fetch(`${base}/api/live?system=loadtest&board=research${compact ? '&compact=1' : ''}`, { signal: controller.signal });
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
  let id = 0; const pending = new Map(), handlers = new Map();
  socket.addEventListener('message', (event) => {
    const data = JSON.parse(event.data);
    if (data.id) { const h = pending.get(data.id); pending.delete(data.id); data.error ? h.reject(new Error(data.error.message)) : h.resolve(data.result); }
    else handlers.get(data.method)?.(data.params);
  });
  const call = (method, params = {}) => new Promise((resolve, reject) => { const key = ++id; pending.set(key, { resolve, reject }); socket.send(JSON.stringify({ id: key, method, params })); });
  // A frozen page never answers: give up after `ms` instead of hanging the whole run.
  const evaluate = async (expression, ms = 30000) => {
    const r = await Promise.race([call('Runtime.evaluate', { expression, awaitPromise: true, returnByValue: true }), sleep(ms).then(() => ({ timeout: true }))]);
    if (r.timeout) return { timeout: true };
    if (r.exceptionDetails) throw new Error(r.exceptionDetails.exception?.description || r.exceptionDetails.text);
    return r.result.value;
  };
  return { call, evaluate, on: (method, fn) => handlers.set(method, fn),
    close: () => { socket.close(); proc.kill(); setTimeout(() => { try { fs.rmSync(profile, { recursive: true, force: true }); } catch {} }, 1000); } };
}

// Installed before the page's own scripts: records long main-thread tasks.
const PROBE = `window.__long = []; new PerformanceObserver((list) => { for (const e of list.getEntries()) window.__long.push({ at: e.startTime, ms: e.duration }); }).observe({ type: 'longtask', buffered: true });
window.__startup = {}; const OriginalEventSource = window.EventSource;
window.EventSource = class extends OriginalEventSource { constructor(url, ...args) { if (${legacySnapshot}) { const u = new URL(url, location.href); u.searchParams.delete('compact'); url = u.href; } super(url, ...args); window.__startup.connect = performance.now(); this.addEventListener('message', (e) => { if (!window.__startup.feed) window.__startup.feed = performance.now(); if (!window.__startup.message && e.data.includes('"type": "cards"')) window.__startup.message = performance.now(); }); } };
const replace = Element.prototype.replaceChildren; Element.prototype.replaceChildren = function(...children) { const result = replace.apply(this, children); if (this.id === 'columns' && !window.__startup.render && this.querySelector('.card')) { window.__startup.render = performance.now(); window.__startup.renderCount = typeof cards === 'undefined' ? 0 : cards.length; } return result; };
localStorage.setItem('system', '"loadtest"'); localStorage.setItem('board', '"research"');`;

// Time from an input to the next painted frame.
const latency = (setup) => `new Promise((resolve) => { const t = performance.now(); ${setup};
  requestAnimationFrame(() => setTimeout(() => resolve(Math.round(performance.now() - t)), 0)); })`;
const unresponsive = (v) => v?.timeout ? 'unresponsive' : v;

// Main-thread time spent in long tasks over `ms` while the live feed runs. The window is taken
// from this process's clock (timeOrigin: the page's), so a page too busy to answer is still measured.
async function liveLoad(browser, timeOrigin, ms = 10000) {
  const t0 = Date.now() - timeOrigin;
  await sleep(ms);
  const r = await browser.evaluate(`(() => { const l = window.__long.filter((e) => e.at > ${t0} && e.at < ${t0 + ms}); return { busy: l.reduce((s, e) => s + Math.min(e.ms, ${t0 + ms} - e.at), 0), worst: Math.max(0, ...l.map((e) => e.ms)) }; })()`, 90000);
  return r?.timeout ? { busyPct: 'unresponsive', worstMs: 'unresponsive' } : { busyPct: Math.round(r.busy / (ms / 100)), worstMs: Math.round(r.worst) };
}

// How long the page takes to run anything at all (a trivial script) while live updates arrive:
// the delay a click or keystroke would wait before the page even starts handling it.
async function responsiveness(browser, samples = 5) {
  const xs = [];
  for (let i = 0; i < samples; i++) {
    const t = Date.now(), r = await browser.evaluate('1', 20000);
    xs.push(r?.timeout ? Infinity : Date.now() - t);
    await sleep(700);
  }
  const show = (v) => v === Infinity ? '>20 s' : v;
  return { median: show(median(xs)), worst: show(Math.max(...xs)) };
}

async function measure(count, page) {
  const { proc, base } = await startBoard(count);
  const browser = await chrome();
  const result = { count, page: page ? `before (${baselineRef})` : 'now' };
  try {
    if (!cold) Object.assign(result, await snapshot(base, !page && !legacySnapshot).then((s) => ({ snapshotMB: +(s.bytes / 1e6).toFixed(1), coldBoardMs: Math.round(s.ms) })));
    await browser.call('Page.enable'); await browser.call('Runtime.enable');
    if (page) {
      // Serve the old board page in place of the current one; everything else is the same server.
      browser.on('Fetch.requestPaused', (p) => browser.call('Fetch.fulfillRequest', { requestId: p.requestId, responseCode: 200,
        responseHeaders: [{ name: 'Content-Type', value: 'text/html; charset=utf-8' }], body: Buffer.from(page).toString('base64') }));
      await browser.call('Fetch.enable', { patterns: [{ urlPattern: `${base}/`, requestStage: 'Request' }] });
    }
    await browser.call('Emulation.setDeviceMetricsOverride', { width: 1440, height: 1000, deviceScaleFactor: 1, mobile: false });
    await browser.call('Page.addScriptToEvaluateOnNewDocument', { source: PROBE });
    const started = Date.now();
    const navigation = await Promise.race([
      browser.call('Page.navigate', { url: base }),
      sleep(30000).then(() => ({ timeout: true })),
    ]);
    if (navigation.timeout) { result.cardsReadyMs = 'navigation timed out (>30 s)'; return result; }
    let painted = null;
    while (Date.now() - started < 90000) {
      const n = await browser.evaluate(`typeof cards !== 'undefined' && cards.length >= ${count} && document.querySelectorAll('#columns .card').length > 0 ? performance.now() : 0`);
      if (n?.timeout) break;
      if (n) { painted = n; break; }
      await sleep(100);
    }
    result.cardsReadyMs = painted ? Math.round(painted) : 'not observed (timeout)';
    if (!painted) return result;
    result.startup = await browser.evaluate('window.__startup');
    if (result.startup.renderCount >= count) result.cardsReadyMs = Math.round(result.startup.render);
    if (startupOnly) {
      result.resources = await browser.evaluate(`performance.getEntriesByType('resource').map((r) => ({ name: new URL(r.name).pathname, start: Math.round(r.startTime), end: Math.round(r.responseEnd) }))`);
      result.longTasks = await browser.evaluate(`window.__long.map((x) => ({ at: Math.round(x.at), ms: Math.round(x.ms) }))`);
    }
    if (startupOnly) return result;
    const timeOrigin = await browser.evaluate('performance.timeOrigin');
    const live = await liveLoad(browser, timeOrigin);
    result.liveBusyPct = live.busyPct; result.liveWorstTaskMs = live.worstMs;
    result.responseMs = await responsiveness(browser);
    result.domCards = unresponsive(await browser.evaluate(`document.querySelectorAll('#columns .card').length`));
    // Opening a card: click the first one, time to the next frame.
    result.openCardMs = unresponsive(await browser.evaluate(latency(`document.querySelector('#columns .card').click()`)));
    await browser.evaluate(`document.getElementById('d-close').click()`);
    if (await browser.evaluate(`!!document.getElementById('search')`) === true) {
      const type = (value) => browser.evaluate(latency(`const s = document.getElementById('search'); s.value = ${JSON.stringify(value)}; s.dispatchEvent(new Event('input', { bubbles: true }))`));
      const keys = [];
      for (const value of ['g', 'gp', 'gpu', 'gpu ', 'gpu w', 'gpu worker:gpu-02', '']) keys.push(await type(value));
      result.searchMs = { median: median(keys), worst: Math.max(...keys) };
      // Search covers every card, not just the rendered ones: look for one far down a column.
      result.searchFindsUnrendered = await browser.evaluate(`(async () => {
        const rendered = new Set([...document.querySelectorAll('#columns .card')].map((n) => n.dataset.id));
        const target = cards.find((c) => !rendered.has(c.id));
        const s = document.getElementById('search'); s.value = 'id:' + target.id; s.dispatchEvent(new Event('input'));
        await new Promise((r) => requestAnimationFrame(r));
        const found = [...document.querySelectorAll('#columns .card')].map((n) => n.dataset.id);
        s.value = ''; s.dispatchEvent(new Event('input')); await new Promise((r) => requestAnimationFrame(r));
        return found.length === 1 && found[0] === target.id; })()`);
      const toggle = () => browser.evaluate(latency(`document.getElementById('hide-finished').click()`));
      result.hideFinishedMs = [await toggle(), await toggle()];
      // Scrolling: each step jumps to the bottom of the page, which loads the next page of every
      // column that reaches it. Time = scroll to the frame after the new cards are in.
      const loads = [];
      for (let i = 0; i < 20; i++) {
        const r = await browser.evaluate(`new Promise((resolve) => { const before = document.querySelectorAll('#columns .card').length, t = performance.now();
          scrollTo(0, document.documentElement.scrollHeight);
          const check = () => { const now = document.querySelectorAll('#columns .card').length;
            // A live update can add a card or two to a column: a page load adds tens.
            if (now >= before + 10) requestAnimationFrame(() => resolve(Math.round(performance.now() - t))); else if (performance.now() - t > 1000) resolve(null); else requestAnimationFrame(check); };
          requestAnimationFrame(check); })`);
        if (r == null || r.timeout) break;
        loads.push(r);
      }
      result.scrollLoadMs = loads.length ? { loads: loads.length, median: median(loads), worst: Math.max(...loads) } : null;
      result.domCardsAfterScroll = await browser.evaluate(`document.querySelectorAll('#columns .card').length`);
      const after = await liveLoad(browser, timeOrigin);
      result.liveBusyAfterScrollPct = after.busyPct; result.liveWorstAfterScrollMs = after.worstMs;
    }
    const heap = await browser.evaluate('performance.memory?.usedJSHeapSize');
    result.heapMB = typeof heap === 'number' ? Math.round(heap / 1e6) : unresponsive(heap);
  } finally { browser.close(); proc.kill(); }
  return result;
}

const cell = (v, unit = '') => v == null ? '—' : typeof v === 'number' ? `${v}${unit}` : String(v);
function row(r) {
  return `| ${r.count.toLocaleString('en')} | ${r.page} | ${cell(r.cardsReadyMs, ' ms')} | ${cell(r.liveBusyPct, '%')} | ${cell(r.liveWorstTaskMs, ' ms')} | ${r.responseMs ? `${r.responseMs.median}${typeof r.responseMs.median === 'number' ? ' ms' : ''} (${r.responseMs.worst})` : '—'} | ${cell(r.openCardMs, ' ms')} | ${r.searchMs ? `${r.searchMs.median} ms (${r.searchMs.worst})` : '—'} | ${r.scrollLoadMs ? `${r.scrollLoadMs.median} ms (${r.scrollLoadMs.worst})` : '—'} | ${cell(r.heapMB, ' MB')} |`;
}

(async () => {
  const before = compare ? execFileSync('git', ['show', `${baselineRef}:board/static/index.html`], { cwd: root, encoding: 'utf8', maxBuffer: 1 << 26 }) : null;
  const rows = [];
  for (const count of counts) {
    for (const page of compare ? [before, null] : [null]) {
      const r = await measure(count, page);
      console.log(JSON.stringify(r));
      rows.push(r);
    }
  }
  console.log('\n| Cards | Page | Cards ready | Live updates: time in long tasks | Longest task | Page answers in, median (worst) | Open a card | Search keystroke, median (worst) | Scroll loads more, median (worst) | JS heap |');
  console.log('|---|---|---|---|---|---|---|---|---|---|');
  for (const r of rows) console.log(row(r));
  process.exit(0);
})();
