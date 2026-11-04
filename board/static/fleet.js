/* Fleet tab: the registered execution PCs, their live CPU/RAM, capabilities and settings, and
 * the registration form with engine- and OS-specific setup instructions. Pure helpers are
 * exported for node tests. Worker data is only ever placed in text nodes and attributes; the
 * board's fleet API never returns endpoints, token names or tokens, and this page never asks
 * for a token value. */
(function (root) {
  'use strict';
  const ENGINES = ['hatchet', 'prefect', 'trigger'];
  const ENGINE_LABELS = { hatchet: 'Hatchet', prefect: 'Prefect', trigger: 'Trigger.dev' };
  const OS_LABELS = { windows: 'Windows', linux: 'Linux', macos: 'macOS' };
  const AGENT_LABELS = { demo: 'demo', codex: 'codex', claude: 'Claude Code', opencode: 'opencode v1', opencode2: 'opencode v2', omp: 'omp', pi: 'pi' };
  const STATUS = {
    online: 'Online',
    awaiting_connection: 'Awaiting connection',
    unreachable: 'Offline',
    credentials_unavailable: 'Offline: no token on the coordinator',
    unauthorized: 'Offline: the PC rejected the token',
    identity_mismatch: 'Offline: identity mismatch (FLOW_WORKER_ID differs)',
  };
  const RESULT = { installed: ['✓', 'Installed'], absent: ['✗', 'Absent'], unknown: ['?', 'Unknown'], error: ['?', 'Check failed'] };
  const config = { metricsMs: 3000, fullMs: 30000 };

  // ------------------------------------------------------------------ Pure helpers
  const pad = (n) => String(n).padStart(2, '0');
  function formatTime(value) {
    const d = new Date(value);
    if (!value || Number.isNaN(d.getTime())) return 'never';
    return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}`;
  }
  const clock = (d) => `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
  const plural = (n, word) => `${n} ${word}${n === 1 ? '' : 's'}`;

  function tokenEnvFor(id) {
    const base = String(id || '').toUpperCase().replace(/[^A-Z0-9]+/g, '_').replace(/^_+|_+$/g, '');
    return `FLOW_WORKER_TOKEN_${base || 'PC'}`;
  }

  // A new tag or pool, as the registry accepts it.
  const validLabel = (v) => typeof v === 'string' && v.length > 0 && v.length <= 100 && v === v.trim() && !v.includes(',') && !/[\u0000-\u001f\u007f]/.test(v);

  function tagSuggestions(query, known, selected) {
    const text = String(query || '').trim(), lower = text.toLowerCase(), taken = new Set(selected || []);
    const matches = (known || []).filter((t) => !taken.has(t) && t.toLowerCase().includes(lower)).slice(0, 8);
    const create = text && validLabel(text) && !(known || []).includes(text) && !taken.has(text) ? text : null;
    return { matches, create };
  }

  // One CPU or RAM reading as words: the value, the limit and why it is unknown.
  function metricView(capacity, kind, limit, { online, staleSince } = {}) {
    const name = kind === 'cpu' ? 'CPU' : 'RAM';
    let value = 'unknown', detail, level = 'unknown';
    if (!online) detail = 'PC not connected';
    else if (!capacity) detail = 'no measurements reported';
    else if (capacity.state !== 'known') {
      const age = Math.round(capacity.sample_age_seconds ?? 0);
      detail = capacity.reason === 'stale' ? `stale: last sample ${age} s ago`
        : capacity.reason === 'incomplete_window' ? 'collecting a full 10-second window'
        : 'no measurements yet';
    } else {
      const pct = capacity[kind === 'cpu' ? 'cpu_percent' : 'ram_percent'];
      value = `${Math.round(pct)}%`;
      level = pct >= limit ? 'over' : 'ok';
      detail = level === 'over' ? `10-second average · at or above the ${limit}% limit: no new work` : `10-second average · limit ${limit}%`;
    }
    if (staleSince) { detail += ` · not updated since ${staleSince}`; if (level === 'ok' || level === 'over') level = `${level} outdated`; }
    const label = value === 'unknown' ? `${name} unknown, ${detail}` : `${name} ${value}, limit ${limit}%${level.startsWith('over') ? ', at or above the limit' : ''}${staleSince ? `, not updated since ${staleSince}` : ''}`;
    return { value, detail, level, label };
  }

  function engineText(engine, status, worker) {
    const label = ENGINE_LABELS[engine] || engine;
    if (status?.connected) {
      const note = status.coordinator === 'unconfirmed' ? ' (the trigger dev worker on the coordinator is not checked by the flow host)' : '';
      return `${label}: Connected through the shared execution service${note}`;
    }
    if (!worker.online) return `${label}: Not connected: the PC is ${(STATUS[worker.status] || 'offline').replace(/^Offline: /, 'offline: ').toLowerCase()}`;
    if (status?.coordinator === 'not_enabled') return `${label}: Not connected: ${label} is not enabled on the flow host (FLOW_BACKENDS)`;
    if (status?.coordinator === 'stopped') return `${label}: Not connected: the ${label} flow worker is not running on the coordinator`;
    return `${label}: Unknown`;
  }

  function syncText(sync, id) {
    const state = sync?.state;
    if (state === 'applied') return `Limits are applied on ${id} for new work; running work continues.`;
    if (state === 'pending' || !state) return 'Limits apply when the PC connects.';
    if (state === 'unsupported') return `Limits not applied: ${sync.message || 'update the execution service on the PC'}.`;
    return `Limits not applied yet: ${sync.message || 'the PC did not accept them'}.`;
  }

  /* Setup steps for a PC, from how this repository starts workers: every engine places work
   * on a PC through agents/flow_execution_worker.py (the shared execution service); native
   * engine placement on remote PCs is not enabled (docs/workers-and-context.md). */
  function instructions({ engines = [], os = 'windows', id = '', tokenEnv, slots = 1, existing = null } = {}) {
    const win = os === 'windows';
    const worker = id || '<worker ID>';
    const token = tokenEnv || tokenEnvFor(id);
    const py = win ? 'agents\\.venv\\Scripts\\python.exe' : 'agents/.venv/bin/python';
    const sections = [], warnings = [];
    const chosen = ENGINES.filter((e) => engines.includes(e));
    const adding = existing ? chosen.filter((e) => !existing.includes(e)) : chosen;
    if (existing) {
      sections.push({ title: 'This PC is already registered', steps: [{ text: 'Nothing new is needed on the PC: its execution service already serves every engine. Saving records which engines may place work on it.' }] });
    } else {
      sections.push({ title: `1. Prepare the PC (${OS_LABELS[os] || os})`, steps: [
        { text: 'Copy this repository to the PC (offline: unpack source.tar from the offline bundle, or copy a checkout) and open a terminal in it.' },
        { text: `Install the agents environment from a local wheelhouse and npm mirror. The wheelhouse must hold ${OS_LABELS[os]} wheels for agents/requirements.txt: `
            + (os === 'linux' ? 'add agents/requirements.txt to PY_REQUIREMENTS when building the offline bundle (its wheels are Linux x86_64, Python 3.12), '
              : `build it on a connected ${OS_LABELS[os]} PC with the same Python version (pip download -r agents/requirements.txt -d wheelhouse), `)
            + 'and point npm at the bundle\'s npm mirror.',
          code: win
            ? 'python -m venv agents\\.venv\nagents\\.venv\\Scripts\\python.exe -m pip install --no-index --find-links <wheelhouse folder> -r agents\\requirements.txt\nnpm.cmd ci --prefix agents --registry <offline npm mirror URL>'
            : 'python3 -m venv agents/.venv\nagents/.venv/bin/python -m pip install --no-index --find-links <wheelhouse folder> -r agents/requirements.txt\nnpm ci --prefix agents --registry <offline npm mirror URL>' },
        { text: 'Install and sign in to the coding agent CLIs this PC should run (see agents/README.md). The fleet tab shows each as installed, absent or unknown after the PC connects; sign-in is not checked.' },
      ] });
      sections.push({ title: '2. Create the token (protected)', steps: [
        { text: 'On the coordinator, generate a random token with its agents Python:', code: '<coordinator agents Python> -c "import secrets; print(secrets.token_urlsafe(32))"' },
        { text: 'Add it to the coordinator\'s ignored workers.local.env (readable only by the account running the flow host). The flow host reads it from there; the board never sees it.', code: `${token}=<generated token>` },
        { text: 'Copy the same value to a file on the PC that only the account running the service can read, through your protected transfer process. Never paste a token into the board or a shared terminal history.' },
      ] });
      sections.push({ title: '3. Start the execution service on the PC', steps: [
        { code: win
          ? `$env:FLOW_WORKER_ID = '${worker}'\n$env:FLOW_WORKER_BIND = '0.0.0.0'\n$env:FLOW_WORKER_PORT = '8096'\n$env:FLOW_WORKER_SLOTS = '${slots}'\n$env:FLOW_WORKER_TOKEN = (Get-Content '<protected token file>' -TotalCount 1).Trim()\nagents\\.venv\\Scripts\\python.exe -u agents\\flow_execution_worker.py`
          : `export FLOW_WORKER_ID='${worker}'\nexport FLOW_WORKER_BIND='0.0.0.0'\nexport FLOW_WORKER_PORT='8096'\nexport FLOW_WORKER_SLOTS='${slots}'\nexport FLOW_WORKER_TOKEN="$(head -n 1 '<protected token file>')"\nagents/.venv/bin/python -u agents/flow_execution_worker.py` },
        { text: `FLOW_WORKER_ID must equal the worker ID; the coordinator checks it when the PC connects. The service refuses a network bind without FLOW_WORKER_TOKEN. Allow inbound TCP 8096 only from the coordinator, over a private network or HTTPS termination. FLOW_WORKER_SLOTS caps one service process; keep it at least the fleet slot count. Pools, tags and limits come from this fleet tab. All services on one PC share one capacity store (FLOW_CAPACITY_DB, default ${win ? '%PROGRAMDATA%\\agent-orchestration\\capacity.db' : '/var/tmp/agent-orchestration/capacity.db'}); never put it on a network drive.` },
      ] });
      sections.push({ title: '4. Register it here', steps: [
        { text: `Enter the PC's endpoint (http(s)://<pc host>:8096) and the token variable ${token} below. Leave the endpoint blank to save the PC as awaiting connection and add it later. When it connects, the coordinator verifies its identity, then applies the slots and CPU/RAM limits set here; they affect new work only.` },
      ] });
      if (os === 'macos') warnings.push('macOS: the execution service starts, but its CPU/RAM sampler supports only Windows and Linux. A macOS PC always reports unknown capacity, so its gate never admits work; new work for it waits with "missing metrics".');
    }
    const notes = {
      hatchet: 'Hatchet tasks reach this PC through its execution service, so the PC needs no Hatchet token or connection. On the coordinator the flow host runs the Hatchet flow worker (FLOW_BACKENDS includes hatchet); standalone coding-agent cards with a worker selection run through agents/hatchet_worker.py there. Native Hatchet placement on remote workers is not enabled (docs/workers-and-context.md).',
      prefect: 'Prefect flow runs stay in the coordinator\'s serve process (the flow host runs flow_worker_prefect.py; FLOW_BACKENDS includes prefect) and send each step to this PC\'s execution service. Cards with a worker selection go through agents/prefect_worker.py on the coordinator. The PC needs no Prefect API access.',
      trigger: 'Only the local development worker is supported: trigger dev runs on the coordinator (trigger.dev/example-project/scripts/dev-worker.ps1) and sends flow steps to this PC\'s execution service. Standalone Trigger.dev cards always run on the coordinator, never on this PC. Trigger.dev production deployment to other PCs is not supported.',
    };
    const listed = existing ? (adding.length ? adding : chosen) : chosen;
    if (listed.length) sections.push({ title: existing && adding.length ? 'Engines you are adding' : 'Engines', steps: listed.map((e) => ({ engine: e, text: `${ENGINE_LABELS[e]}: ${notes[e]}` })) });
    return { sections, warnings };
  }

  // ------------------------------------------------------------------ DOM helpers
  const PROPS = new Set(['value', 'checked', 'disabled', 'hidden', 'readOnly']);
  function h(tag, attrs, ...children) {
    const node = document.createElement(tag);
    for (const [k, v] of Object.entries(attrs || {})) {
      if (v == null || PROPS.has(k) || (v === false && !k.startsWith('aria-'))) continue;
      if (k.startsWith('on') && typeof v === 'function') node.addEventListener(k.slice(2), v);
      else node.setAttribute(k, v === true ? '' : String(v));
    }
    for (const child of children.flat()) if (child != null && child !== false) node.append(child);
    for (const k of PROPS) if (attrs && attrs[k] != null) node[k] = attrs[k];
    return node;
  }
  async function fetchJson(method, path, body) {
    try {
      const response = await fetch(path, { method, headers: body ? { 'Content-Type': 'application/json' } : {}, body: body ? JSON.stringify(body) : undefined });
      const data = await response.json().catch(() => ({}));
      return { ok: response.ok, status: response.status, data };
    } catch (error) { return { ok: false, status: 0, data: { error: `${error.message || 'Network error'} (is the board reachable?)` } }; }
  }
  const errorText = (r) => r.data?.error || `Request failed (HTTP ${r.status})`;
  const enc = encodeURIComponent;

  // ------------------------------------------------------------------ The tab
  function mount(container, options = {}) {
    const request = options.request || fetchJson;
    const S = { workers: [], tags: [], pools: [], open: false, timer: null, lastFull: 0, staleSince: null, states: new Map(), form: null, busy: false };

    const status = h('p', { class: 'fl-status', role: 'status' });
    // One polite live region for action results and connection changes; metric ticks are not announced.
    const live = h('p', { class: 'fl-live fl-sr', 'aria-live': 'polite' });
    const stale = h('p', { class: 'fl-stale', hidden: true });
    const list = h('div', { class: 'fl-list', role: 'list', 'aria-label': 'Registered PCs' });
    const formPanel = h('section', { class: 'fl-form', hidden: true, 'aria-labelledby': 'fl-form-title' });
    const top = h('div', { class: 'fl-top' },
      h('div', { class: 'fl-heading' }, h('h2', { class: 'fl-title', id: 'fl-title' }, 'Fleet'),
        h('p', { class: 'fl-hint' }, 'One entry per PC. A PC serves every engine selected for it through its execution service.')),
      h('div', { class: 'fl-actions' },
        h('button', { type: 'button', class: 'primary', onclick: () => openForm(null) }, 'Register PC'),
        h('button', { type: 'button', class: 'secondary', onclick: () => load(true) }, 'Reload'),
        h('button', { type: 'button', class: 'secondary', 'aria-label': 'Close fleet', onclick: () => options.onClose?.() }, '×')),
      status);
    container.replaceChildren(h('div', { class: 'fl' }, top, stale, h('div', { class: 'fl-body' }, formPanel, list), live));

    const say = (text, error = false) => { status.textContent = text; status.classList.toggle('error', error); };
    const announce = (text) => { live.textContent = ''; setTimeout(() => { live.textContent = text; }, 30); };

    // ---- worker list
    function resultRow(kind, name, entry) {
      const [glyph, word] = RESULT[entry?.status] || RESULT.unknown;
      const label = kind === 'agent' ? AGENT_LABELS[name] || name : name;
      return h('li', { class: `fl-result fl-${entry?.status || 'unknown'}`, [`data-${kind}`]: name },
        h('span', { class: 'fl-glyph', 'aria-hidden': 'true' }, glyph), ' ',
        h('span', { class: 'fl-result-name' }, label), ' ',
        h('span', { class: 'fl-result-word' }, word),
        entry?.checked_at ? h('span', { class: 'fl-when' }, ` · checked ${formatTime(entry.checked_at)}`) : null);
    }

    function metricNode(worker, kind) {
      const limit = kind === 'cpu' ? worker.cpu_limit : worker.ram_limit;
      const view = metricView(worker.capacity, kind, limit, { online: worker.online, staleSince: S.staleSince });
      const pct = worker.capacity?.state === 'known' && worker.online ? Math.max(0, Math.min(100, worker.capacity[kind === 'cpu' ? 'cpu_percent' : 'ram_percent'])) : 0;
      return h('div', { class: `fl-metric ${view.level.split(' ').map((l) => `fl-${l}`).join(' ')}`, 'data-metric': kind, role: 'group', 'aria-label': view.label },
        h('span', { class: 'fl-metric-name' }, kind === 'cpu' ? 'CPU' : 'RAM'), ' ',
        h('span', { class: 'fl-metric-value' }, view.value), ' ',
        h('span', { class: 'fl-bar', 'aria-hidden': 'true' }, h('span', { class: 'fl-fill', style: `width:${pct}%` }), h('span', { class: 'fl-mark', style: `left:${limit}%` })),
        h('span', { class: 'fl-metric-detail' }, view.detail));
    }

    function slotsText(worker) {
      const used = worker.online ? (worker.capacity?.leases ?? worker.active ?? 0) : 0;
      return worker.online ? `${used} of ${plural(worker.slots, 'slot')} in use` : `${plural(worker.slots, 'slot')}; usage is shown while the PC is connected`;
    }

    function liveParts(worker) {
      return [
        h('p', { class: 'fl-state', 'data-status': worker.status }, STATUS[worker.status] || worker.status || 'Unknown',
          worker.hostname ? h('span', { class: 'fl-host' }, ` · ${worker.hostname}`) : null),
        h('div', { class: 'fl-metrics' }, metricNode(worker, 'cpu'), metricNode(worker, 'ram'),
          h('p', { class: 'fl-basis' }, 'RAM is physical memory in use (total minus available). New work waits while either average is at or above its limit; running work is never stopped.')),
        h('p', { class: 'fl-slots' }, slotsText(worker)),
        h('ul', { class: 'fl-engines', 'aria-label': 'Engines' }, worker.engines.map((e) => h('li', { 'data-engine': e, class: worker.engine_status?.[e]?.connected ? 'fl-connected' : 'fl-disconnected' }, engineText(e, worker.engine_status?.[e], worker)))),
      ];
    }

    function workerNode(worker) {
      const inv = worker.inventory;
      const agents = Object.entries(inv?.agents || {}), commands = Object.entries(inv?.commands || {});
      const offlineNote = !worker.online ? h('p', { class: 'fl-hint' }, 'Capabilities are checked when it connects.') : null;
      return h('article', { class: 'fl-worker', role: 'listitem', 'data-worker': worker.id, 'aria-labelledby': `fl-w-${worker.id}` },
        h('header', { class: 'fl-worker-head' }, h('h3', { id: `fl-w-${worker.id}` }, worker.id),
          h('div', { class: 'fl-worker-actions' },
            h('button', { type: 'button', class: 'secondary', 'data-action': 'edit', onclick: () => openForm(worker) }, 'Edit settings'),
            h('button', { type: 'button', class: 'secondary', 'data-action': 'refresh', disabled: !worker.online, title: worker.online ? 'Check installed agents and commands now' : 'The PC must be connected',
              onclick: (e) => refresh(worker.id, e.currentTarget) }, 'Refresh capabilities'))),
        h('div', { class: 'fl-live-parts' }, liveParts(worker)),
        h('dl', { class: 'fl-facts' },
          h('dt', {}, 'Pools'), h('dd', {}, worker.pools.map((p) => h('span', { class: 'fl-chip' }, p))),
          h('dt', {}, 'Tags'), h('dd', {}, worker.tags.length ? worker.tags.map((t) => h('span', { class: 'fl-chip' }, t)) : h('span', { class: 'fl-hint' }, 'none')),
          h('dt', {}, 'Limits'), h('dd', { class: 'fl-limits' }, `CPU limit ${worker.cpu_limit}% · RAM limit ${worker.ram_limit}% · ${plural(worker.slots, 'slot')}`,
            h('span', { class: 'fl-sync' }, ` · ${syncText(worker.settings_sync, worker.id)}`))),
        h('section', { class: 'fl-capabilities', 'aria-label': `Capabilities of ${worker.id}` },
          h('p', { class: 'fl-refreshed' }, inv ? `Last refreshed ${formatTime(inv.refreshed_at)}` : 'Capabilities unknown',
            inv?.cached ? h('span', { class: 'fl-hint' }, ' · Last known results; the PC is not connected') : null),
          offlineNote,
          agents.length ? h('ul', { class: 'fl-results', 'aria-label': 'Agents' }, agents.map(([n, r]) => resultRow('agent', n, r))) : null,
          commands.length ? h('ul', { class: 'fl-results', 'aria-label': 'Commands' }, commands.map(([n, r]) => resultRow('command', n, r))) : null));
    }

    function render() {
      const focused = document.activeElement?.closest?.('.fl-worker') && document.activeElement.dataset.action
        ? [document.activeElement.closest('.fl-worker').dataset.worker, document.activeElement.dataset.action] : null;
      list.replaceChildren(...(S.workers.length ? S.workers.map(workerNode) : [h('p', { class: 'fl-empty' }, 'No PCs are registered yet.')]));
      if (focused) list.querySelector(`.fl-worker[data-worker="${CSS.escape(focused[0])}"] [data-action="${focused[1]}"]`)?.focus();
    }

    function noteStates(workers, quiet) {
      for (const w of workers) {
        if (!quiet && S.states.has(w.id) && S.states.get(w.id) !== w.online) announce(`${w.id} is now ${w.online ? 'online' : 'offline'}`);
        S.states.set(w.id, w.online);
      }
    }

    // ---- loading and polling
    function markStale(r) {
      if (!S.staleSince) S.staleSince = clock(new Date(S.lastMetrics || S.lastFull || Date.now()));
      stale.textContent = `Live metrics are stale: not updated since ${S.staleSince} (${errorText(r)}).`;
      stale.hidden = false;
    }

    async function load(announceIt = false) {
      const r = await request('GET', '/api/fleet');
      if (!r.ok) { say(`Could not load the fleet: ${errorText(r)}`, true); if (S.workers.length) { markStale(r); render(); } return false; }
      const first = !S.states.size;
      S.workers = r.data.workers || []; S.tags = r.data.tags || []; S.pools = r.data.pools || [];
      S.lastFull = S.lastMetrics = Date.now(); S.staleSince = null; stale.hidden = true;
      noteStates(S.workers, first);
      render();
      if (announceIt) say(`Fleet reloaded at ${clock(new Date())}.`);
      return true;
    }

    async function poll() {
      if (!S.open) return;
      if (document.hidden) return schedule();
      if (Date.now() - S.lastFull >= config.fullMs && !S.form) await load();
      else {
        const r = await request('GET', '/api/fleet/metrics');
        if (!S.open) return;
        if (!r.ok) markStale(r);
        else {
          S.lastMetrics = Date.now(); S.staleSince = null; stale.hidden = true;
          const fresh = new Map((r.data.workers || []).map((w) => [w.id, w]));
          const known = new Set(S.workers.map((w) => w.id));
          if ([...fresh.keys()].some((id) => !known.has(id)) || fresh.size !== known.size) { await load(); return schedule(); }
          S.workers = S.workers.map((w) => ({ ...fresh.get(w.id), inventory: w.inventory }));  // the metrics listing has every field but inventory
          noteStates(S.workers, false);
        }
        for (const w of S.workers) {
          const node = list.querySelector(`.fl-worker[data-worker="${CSS.escape(w.id)}"]`);
          if (!node) continue;
          node.querySelector('.fl-live-parts').replaceChildren(...liveParts(w));
          const refresh = node.querySelector('[data-action="refresh"]');
          refresh.disabled = !w.online;
        }
      }
      schedule();
    }
    function schedule() { clearTimeout(S.timer); if (S.open) S.timer = setTimeout(poll, config.metricsMs); }

    async function refresh(id, button) {
      button.disabled = true;
      say(`Checking the capabilities of ${id}…`);
      const r = await request('POST', `/api/fleet/workers/${enc(id)}/refresh`, {});
      if (!r.ok) { say(`Could not refresh ${id}: ${errorText(r)}`, true); button.disabled = false; return; }
      S.workers = S.workers.map((w) => (w.id === id ? { ...w, inventory: r.data.inventory } : w));
      render();
      say(`Capabilities of ${id} refreshed at ${clock(new Date())}.`);
      announce(`Capabilities of ${id} refreshed`);
    }

    // ---- registration and settings form
    function openForm(worker, preset = {}) {
      const editing = !!worker;
      S.form = { worker, engines: preset.engines || (worker ? [...worker.engines] : ['hatchet']), os: 'windows',
        pools: worker ? [...worker.pools] : ['default'], extraPools: [], tags: worker ? [...worker.tags] : [], active: -1, options: [] };
      const f = S.form;
      const idInput = h('input', { name: 'id', required: true, maxlength: 100, autocomplete: 'off', spellcheck: 'false', readOnly: editing, value: worker?.id || preset.id || '',
        'aria-describedby': 'fl-id-help' });
      const tokenInput = h('input', { name: 'token_env', maxlength: 100, autocomplete: 'off', spellcheck: 'false', value: editing ? '' : tokenEnvFor(preset.id || ''),
        placeholder: editing ? 'unchanged' : '', 'aria-describedby': 'fl-token-help' });
      let tokenTouched = editing;
      const urlInput = h('input', { name: 'url', type: 'url', autocomplete: 'off', spellcheck: 'false', placeholder: 'https://pc-host:8096', 'aria-describedby': 'fl-url-help' });
      const number = (name, value, min, max) => h('input', { name, type: 'number', min, max, step: 1, required: true, value: String(value) });
      const slots = number('slots', worker?.slots ?? 1, 1, 100), cpu = number('cpu_limit', worker?.cpu_limit ?? 90, 1, 100), ram = number('ram_limit', worker?.ram_limit ?? 90, 1, 100);
      const steps = h('div', { class: 'fl-instructions', 'aria-label': 'Setup instructions' });
      const errorBox = h('div', { class: 'fl-form-error', role: 'alert' });

      const drawSteps = () => {
        const doc = instructions({ engines: f.engines, os: f.os, id: idInput.value.trim(), tokenEnv: tokenInput.value.trim() || undefined, slots: Number(slots.value) || 1,
          existing: editing ? worker.engines : null });
        steps.replaceChildren(
          ...doc.warnings.map((w) => h('p', { class: 'fl-warning' }, w)),
          ...doc.sections.map((s) => h('section', { class: 'fl-step' }, h('h4', {}, s.title),
            ...s.steps.map((x) => [x.text ? h('p', x.engine ? { 'data-engine-note': x.engine } : {}, x.text) : null, x.code ? h('pre', { class: 'fl-code' }, h('code', {}, x.code)) : null]))));
      };

      const engineBoxes = h('fieldset', { class: 'fl-engines-choice' }, h('legend', {}, 'Engines this PC serves'),
        ENGINES.map((e) => h('label', { class: 'fl-check' }, h('input', { type: 'checkbox', name: 'engine', value: e, checked: f.engines.includes(e),
          onchange: (ev) => { f.engines = ENGINES.filter((x) => (x === e ? ev.target.checked : f.engines.includes(x))); drawSteps(); } }), ` ${ENGINE_LABELS[e]}`)));
      const osBoxes = h('fieldset', { class: 'fl-os-choice' }, h('legend', {}, 'Operating system of the PC'),
        Object.entries(OS_LABELS).map(([value, label]) => h('label', { class: 'fl-check' }, h('input', { type: 'radio', name: 'os', value, checked: value === f.os,
          onchange: () => { f.os = value; drawSteps(); } }), ` ${label}`)));

      // Pools: several may be chosen; a new pool is added inline and saved with the PC.
      const poolList = h('div', { class: 'fl-pool-list' });
      const drawPools = () => {
        const names = [...new Set([...S.pools, 'default', ...f.pools, ...f.extraPools])].sort();
        poolList.replaceChildren(...names.map((p) => h('label', { class: 'fl-check' }, h('input', { type: 'checkbox', value: p, checked: f.pools.includes(p),
          onchange: (ev) => { f.pools = ev.target.checked ? [...f.pools, p] : f.pools.filter((x) => x !== p); } }), ` ${p}`)));
      };
      const newPool = h('input', { name: 'new_pool', maxlength: 100, autocomplete: 'off', 'aria-label': 'New pool name', placeholder: 'New pool name' });
      const addPool = () => {
        const name = newPool.value.trim();
        if (!validLabel(name)) { errorBox.textContent = 'A pool name is 1 to 100 characters without commas.'; return; }
        if (!f.extraPools.includes(name)) f.extraPools.push(name);
        if (!f.pools.includes(name)) f.pools.push(name);
        newPool.value = ''; errorBox.textContent = ''; drawPools();
      };
      newPool.addEventListener('keydown', (e) => { if (e.key === 'Enter') { e.preventDefault(); addPool(); } });

      // Tags: a combobox with suggestions from existing tags and an inline "create" option.
      const chosen = h('div', { class: 'fl-chosen-tags', 'aria-label': 'Chosen tags' });
      const listbox = h('ul', { class: 'fl-tag-options', id: 'fl-tag-options', role: 'listbox', 'aria-label': 'Tag suggestions', hidden: true });
      const tagInput = h('input', { name: 'tag', class: 'fl-tag-input', role: 'combobox', 'aria-autocomplete': 'list', 'aria-expanded': 'false', 'aria-controls': 'fl-tag-options',
        autocomplete: 'off', spellcheck: 'false', maxlength: 100, placeholder: 'Type to find or create a tag', 'aria-describedby': 'fl-tag-help' });
      const drawChosen = () => chosen.replaceChildren(...f.tags.map((t) => h('span', { class: 'fl-chip' }, t,
        h('button', { type: 'button', class: 'fl-x', 'aria-label': `Remove tag ${t}`, onclick: () => { f.tags = f.tags.filter((x) => x !== t); drawChosen(); tagInput.focus(); } }, '×'))));
      const closeList = () => { listbox.hidden = true; tagInput.setAttribute('aria-expanded', 'false'); tagInput.removeAttribute('aria-activedescendant'); f.active = -1; };
      const drawOptions = () => {
        const { matches, create } = tagSuggestions(tagInput.value, S.tags, f.tags);
        f.options = [...matches.map((t) => ({ tag: t })), ...(create ? [{ tag: create, create: true }] : [])];
        if (!tagInput.value.trim() || !f.options.length) return closeList();
        f.active = Math.min(f.active, f.options.length - 1);
        listbox.replaceChildren(...f.options.map((o, i) => h('li', { id: `fl-tag-opt-${i}`, role: 'option', 'aria-selected': String(i === f.active), class: o.create ? 'fl-create' : '',
          onmousedown: (e) => e.preventDefault(), onclick: () => pick(o) }, o.create ? `Create tag “${o.tag}”` : o.tag)));
        listbox.hidden = false; tagInput.setAttribute('aria-expanded', 'true');
        if (f.active >= 0) tagInput.setAttribute('aria-activedescendant', `fl-tag-opt-${f.active}`); else tagInput.removeAttribute('aria-activedescendant');
      };
      async function pick(option) {
        if (option.create) {
          const r = await request('POST', '/api/fleet/tags', { name: option.tag });
          if (!r.ok) { errorBox.textContent = `Could not create the tag: ${errorText(r)}`; return; }
          S.tags = r.data.tags || [...S.tags, option.tag];
          announce(`Tag ${option.tag} created`);
        }
        if (!f.tags.includes(option.tag)) f.tags.push(option.tag);
        tagInput.value = ''; closeList(); drawChosen(); tagInput.focus();
      }
      tagInput.addEventListener('input', () => { f.active = -1; drawOptions(); });
      tagInput.addEventListener('blur', () => setTimeout(() => { if (document.activeElement !== tagInput) closeList(); }, 150));
      tagInput.addEventListener('keydown', (e) => {
        if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
          e.preventDefault();
          if (listbox.hidden) { drawOptions(); if (listbox.hidden) return; }
          f.active = (f.active + (e.key === 'ArrowDown' ? 1 : -1) + f.options.length) % f.options.length;
          drawOptions();
        } else if (e.key === 'Enter') {
          e.preventDefault();
          const option = f.options[f.active] || f.options.find((o) => o.tag === tagInput.value.trim()) || f.options.find((o) => o.create);
          if (option && !listbox.hidden) pick(option);
        } else if (e.key === 'Escape' && !listbox.hidden) { e.preventDefault(); e.stopPropagation(); closeList(); }
      });

      idInput.addEventListener('input', () => { if (!tokenTouched) tokenInput.value = tokenEnvFor(idInput.value.trim()); drawSteps(); });
      tokenInput.addEventListener('input', () => { tokenTouched = true; drawSteps(); });
      slots.addEventListener('input', drawSteps);

      const field = (label, input, help, helpId) => h('label', { class: 'fl-field' }, h('span', {}, label), input, help ? h('small', { id: helpId }, help) : null);
      const form = h('form', { class: 'fl-form-body', novalidate: true },
        h('h3', { id: 'fl-form-title' }, editing ? `Edit ${worker.id}` : 'Register a PC'),
        h('p', { class: 'fl-hint' }, editing ? 'Pools, tags, engines and limits are stored in the fleet registry on the coordinator. Limit changes affect new work only.'
          : 'Choose the engines and the PC\'s operating system for setup steps, then enter its details. You can save it before it is reachable.'),
        engineBoxes, osBoxes, steps,
        h('fieldset', { class: 'fl-details' }, h('legend', {}, 'Worker details'),
          field('Worker ID', idInput, editing ? 'The ID cannot change. Register a new PC instead.' : 'Letters, digits, dots, dashes and underscores. The PC\'s FLOW_WORKER_ID must be the same.', 'fl-id-help'),
          field('Endpoint (write-only)', urlInput, editing && worker.connection === 'configured' ? 'An endpoint is configured; it is never shown. Leave blank to keep it.'
            : 'Optional. Leave blank to save as awaiting connection.', 'fl-url-help'),
          field('Token variable on the coordinator', tokenInput, editing ? 'Leave blank to keep the current variable. The token value itself is never entered here.'
            : 'The name of the workers.local.env entry that holds this PC\'s token. The token value itself is never entered here.', 'fl-token-help')),
        h('fieldset', { class: 'fl-pools' }, h('legend', {}, 'Pools'), poolList,
          h('div', { class: 'fl-row' }, newPool, h('button', { type: 'button', class: 'secondary', onclick: addPool }, 'Add pool'))),
        h('fieldset', { class: 'fl-tags' }, h('legend', {}, 'Tags'), chosen,
          h('div', { class: 'fl-combo' }, tagInput, listbox),
          h('small', { id: 'fl-tag-help' }, 'Existing tags are suggested as you type; a new one is created when you choose “Create tag”.')),
        h('fieldset', { class: 'fl-settings' }, h('legend', {}, 'Capacity'),
          field('Slots (jobs at once, all engines)', slots), field('CPU limit (%)', cpu), field('RAM limit (%)', ram),
          h('small', {}, 'Admission thresholds for new work, defaults 90% and 90%: a 10-second average at or above a limit holds new work. Running work is never stopped.')),
        errorBox,
        h('div', { class: 'fl-row fl-form-actions' },
          h('button', { type: 'button', class: 'secondary', onclick: closeForm }, 'Cancel'),
          h('button', { type: 'submit', class: 'primary' }, 'Save')));

      form.addEventListener('submit', async (e) => {
        e.preventDefault();
        if (S.busy) return;
        errorBox.replaceChildren();
        const problem = (text) => { errorBox.textContent = text; return null; };
        const id = idInput.value.trim();
        const whole = (input, name, max) => {
          const n = Number(input.value);
          return Number.isInteger(n) && n >= 1 && n <= max ? n : problem(`${name} must be a whole number from 1 to ${max}.`);
        };
        if (!editing && !/^[A-Za-z0-9][A-Za-z0-9._-]{0,99}$/.test(id)) return problem('Worker ID: letters, digits, dots, dashes or underscores, starting with a letter or digit.');
        if (!f.engines.length) return problem('Choose at least one engine.');
        if (!f.pools.length) return problem('Choose at least one pool.');
        const s = whole(slots, 'Slots', 100); if (s == null) return;
        const c = whole(cpu, 'CPU limit', 100); if (c == null) return;
        const m = whole(ram, 'RAM limit', 100); if (m == null) return;
        const body = { ...(editing ? {} : { id }), engines: f.engines, pools: [...f.pools], tags: [...f.tags], slots: s, cpu_limit: c, ram_limit: m };
        if (urlInput.value.trim()) body.url = urlInput.value.trim();
        if (tokenInput.value.trim()) body.token_env = tokenInput.value.trim();
        S.busy = true;
        const r = editing ? await request('PUT', `/api/fleet/workers/${enc(worker.id)}`, body) : await request('POST', '/api/fleet/workers', body);
        S.busy = false;
        if (!r.ok) {
          const existing = r.status === 409 && S.workers.find((w) => w.id === id);
          errorBox.replaceChildren(h('p', {}, errorText(r)));
          if (existing) {
            const merged = ENGINES.filter((x) => existing.engines.includes(x) || f.engines.includes(x));
            const added = merged.filter((x) => !existing.engines.includes(x)).map((x) => ENGINE_LABELS[x]);
            errorBox.append(h('button', { type: 'button', class: 'secondary', onclick: () => openForm(existing, { engines: merged }) },
              added.length ? `Edit ${existing.id} and add ${added.join(' and ')}` : `Edit ${existing.id}`));
          }
          return;
        }
        const saved = r.data.worker?.id || worker?.id || id;
        urlInput.value = ''; // never keep an endpoint in the page
        say(`Saved ${saved}${r.data.worker?.connection === 'awaiting' ? ' (awaiting connection)' : ''}. ${syncText(r.data.settings_sync, saved)}`);
        announce(`Saved ${saved}`);
        closeForm();
        await load();
      });

      drawSteps(); drawPools(); drawChosen();
      formPanel.replaceChildren(form);
      formPanel.hidden = false;
      list.hidden = true;
      formPanel.parentElement.scrollTop = 0;
      engineBoxes.querySelector('input').focus();  // start at the top: engines and OS choose the setup steps
    }

    function closeForm() {
      S.form = null;
      formPanel.replaceChildren();
      formPanel.hidden = true;
      list.hidden = false;
    }

    return {
      async open() {
        S.open = true;
        closeForm();
        say('Loading the fleet…');
        if (await load()) say(`${plural(S.workers.length, 'PC')} registered.`);
        schedule();
      },
      // Escape inside the form closes the form first; returns true when the tab may close.
      back() { if (S.form) { closeForm(); return false; } return true; },
      stop() { S.open = false; clearTimeout(S.timer); },
    };
  }

  const api = { ENGINES, ENGINE_LABELS, config, instructions, tagSuggestions, metricView, engineText, syncText, tokenEnvFor, formatTime, validLabel, mount };
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  else root.Fleet = api;
})(typeof window !== 'undefined' ? window : globalThis);
