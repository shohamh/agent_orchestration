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
  const config = { metricsMs: 3000, fullMs: 30000, installMs: 2000 };
  const MAX_SLOTS = 100;  // the worker capacity gate's maximum (agents/flowkit/capacity.py MAX_SLOTS)

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
    if (status?.state === 'unconfirmed' || (worker.online && status?.coordinator === 'unconfirmed')) {
      return `${label}: Unconfirmed: the PC is online, but the flow host cannot check the ${engine === 'trigger' ? 'trigger dev worker' : `${label} worker`} on the coordinator`;
    }
    if (status?.connected) return `${label}: Connected through the shared execution service`;
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
            + 'and install the agents ACP adapters from the bundle\'s offline npm mirror. The mirror holds only the projects listed in NPM_PROJECTS '
            + 'when the offline bundle is built (default trigger.dev/example-project): add agents to it (NPM_PROJECTS="trigger.dev/example-project agents") '
            + 'or npm ci fails offline.',
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


  // ------------------------------------------------------------------ One-line install (pure helpers)
  const INSTALL_STEPS = { check: 'Checking the PC', download: 'Downloading the worker', python: 'Installing Python packages', npm: 'Installing the agent adapters',
    token: 'Creating its token', service: 'Starting the worker', register: 'Registering', connect: 'Waiting for the connection', boot: 'Setting up start at boot (approve the administrator prompt on the PC)' };
  const LOOPBACK = /^(localhost|127\.\d+\.\d+\.\d+|0\.0\.0\.0|\[?::1\]?)$/i;

  /* The server address as the server will accept it: scheme://host[:port] and never a loopback name. */
  function serverAddress(value) {
    const text = String(value || '').trim();
    if (!text) return { error: 'Enter the address this PC uses to reach this server' };
    let url;
    try { url = new URL(text); } catch { return { error: 'The server address must start with http:// or https:// and name a host' }; }
    if (!['http:', 'https:'].includes(url.protocol) || !url.hostname) return { error: 'The server address must start with http:// or https:// and name a host' };
    if (url.username || url.password || url.search || url.hash || !['', '/'].includes(url.pathname)) return { error: 'Use only scheme, host and port, for example http://192.168.1.10:8090' };
    if (LOOPBACK.test(url.hostname) || url.hostname.toLowerCase().endsWith('.localhost')) return { error: 'The PC cannot reach this server at localhost; use the name or address the PC can reach' };
    return { value: url.origin };
  }

  const AUTOSTARTS = ['logon', 'boot', 'none'];

  /* What each way of starting the worker means, with its consequences, for the operating system chosen in the form. */
  function autostartChoices(os) {
    const windows = os === 'windows';
    return [
      { value: 'logon', label: 'When the user logs in',
        text: 'The worker starts when the user logs in and runs with that user’s own environment, so tools and agents that rely on the user’s login, credentials and PATH work. '
          + 'The consequence: after the PC restarts the worker is not running until someone logs in, and the PC shows as offline in the fleet until then.' },
      { value: 'boot', label: 'When the PC starts, before anyone logs in',
        text: windows
          ? 'The worker starts with the PC, even at the logon screen. Windows asks for administrator approval once (a UAC prompt during the install). '
            + 'The consequence: it runs without the user’s credentials: no saved Windows or network logins, mapped drives, desktop or logged-in sessions, and not the user’s logon environment (the PATH and folders seen at install time are used instead). '
            + 'Agents and tools that need the user to be signed in can fail until the user logs in.'
          : 'The worker starts with the PC, before anyone logs in (systemd lingering; it may ask for sudo once). '
            + 'The consequence: it runs without a login session: no desktop, keyring or agent sessions that depend on the user being logged in, so tools that need those can fail until the user logs in.' },
      { value: 'none', label: 'Do not start automatically',
        text: 'The worker runs now but is not set up to start by itself. After the PC restarts it stays off, and shows as offline in the fleet, until you run agent-worker start on the PC.' },
    ];
  }

  /* Check the install form the way the server will; returns {errors: {field: message}, body}. */
  function validateInstall(v, { taken = [] } = {}) {
    const errors = {};
    const id = String(v.id || '').trim();
    if (!/^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$/.test(id)) errors.id = 'A name is 1 to 64 letters, digits, dashes or underscores, starting with a letter or digit';
    else if (taken.includes(id)) errors.id = `${id} is already registered; edit it instead`;
    if (!OS_LABELS[v.os]) errors.os = "Choose the PC's operating system";
    if (!(v.engines || []).length) errors.engines = 'Choose at least one engine';
    if (!validLabel(String(v.pool || '').trim())) errors.pool = 'A pool is 1 to 100 characters without commas';
    const tags = (v.tags || []).map((t) => String(t).trim());
    if (tags.some((t) => !validLabel(t))) errors.tags = 'A tag is 1 to 100 characters without commas';
    const whole = (value, max) => /^\d+$/.test(String(value).trim()) && Number(value) >= 1 && Number(value) <= max;
    if (!whole(v.slots, MAX_SLOTS)) errors.slots = `Slots must be a whole number from 1 to ${MAX_SLOTS}`;
    const percent = (value) => /^\d+(\.\d+)?$/.test(String(value).trim()) && Number(value) > 0 && Number(value) <= 100;
    if (!percent(v.cpu_limit)) errors.cpu_limit = 'The CPU limit is above 0 and at most 100';
    if (!percent(v.ram_limit)) errors.ram_limit = 'The RAM limit is above 0 and at most 100';
    const server = serverAddress(v.server);
    if (server.error) errors.server = server.error;
    const autostart = v.autostart || 'logon';
    if (!AUTOSTARTS.includes(autostart)) errors.autostart = 'Choose when the worker starts';
    if (Object.keys(errors).length) return { errors, body: null };
    return { errors, body: { id, os: v.os, engines: ENGINES.filter((e) => v.engines.includes(e)), pool: String(v.pool).trim(), tags: [...new Set(tags)],
      slots: Number(v.slots), cpu_limit: Number(v.cpu_limit), ram_limit: Number(v.ram_limit), server: server.value, autostart } };
  }

  /* The commands to show for the chosen operating system, with the elevation note. */
  function installCommands(os, commands) {
    const windows = os === 'windows';
    return windows
      ? [{ id: 'powershell', label: 'Windows PowerShell', hint: 'Open PowerShell as a normal user (not as Administrator) and paste:', text: commands.powershell },
         { id: 'cmd', label: 'Command Prompt (cmd)', hint: 'Or in a normal Command Prompt:', text: commands.cmd }]
      : [{ id: 'bash', label: os === 'macos' ? 'macOS Terminal' : 'Linux terminal', hint: 'Open a terminal as your normal user (no sudo) and paste:', text: commands.bash }];
  }

  /* One sentence for an install record: what it is waiting for, where it is, or why it failed. */
  function installProgress(record, now = Date.now()) {
    if (!record) return { text: 'This install link is no longer listed.', level: 'warn' };
    const stepText = INSTALL_STEPS[record.step] || null;
    if (record.state === 'cancelled') return { text: 'Cancelled.', level: 'muted' };
    if (record.state === 'expired' || (record.state === 'pending' && record.expires_at * 1000 <= now)) return { text: 'The link expired. Create a new one.', level: 'warn' };
    if (record.state === 'registered') {
      return record.connected ? { text: `Connected${record.hostname ? ` from ${record.hostname}` : ''}.`, level: 'ok' }
        : { text: 'Registered. Waiting for the worker to connect…', level: 'wait' };
    }
    if (record.error) return { text: `Failed${stepText ? ` while ${stepText.toLowerCase()}` : ''}: ${record.error}`, level: 'error' };
    if (!record.step) return { text: 'Waiting for you to run the command on the PC…', level: 'wait' };
    return { text: `${stepText || record.step}…`, level: 'wait' };
  }

  /* The words for one PC's update state; `link` when its user can use an update command. */
  function updateStatusText(item) {
    const states = {
      up_to_date: { text: 'Up to date', level: 'ok', link: false },
      updating: { text: 'Updating…', level: 'wait', link: false },
      waiting: { text: 'Offline: updates when it connects', level: 'wait', link: true },
      offline: { text: 'Offline', level: 'warn', link: true },
      outdated: { text: 'Update available', level: 'warn', link: true },
      failed: { text: `Update failed${item.error ? `: ${item.error}` : ''}`, level: 'error', link: true },
    };
    return states[item.status] || { text: 'Unknown', level: 'warn', link: false };
  }

  function minutesLeft(expiresAt, now = Date.now()) {
    return Math.max(0, Math.ceil((expiresAt * 1000 - now) / 60000));
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
    const S = { workers: [], tags: [], pools: [], open: false, timer: null, lastFull: 0, staleSince: null, states: new Map(), form: null, busy: false, installs: [], installTimer: null, updates: null };

    const status = h('p', { class: 'fl-status', role: 'status' });
    // One polite live region for action results and connection changes; metric ticks are not announced.
    const live = h('p', { class: 'fl-live fl-sr', 'aria-live': 'polite' });
    const stale = h('p', { class: 'fl-stale', hidden: true });
    const list = h('div', { class: 'fl-list', role: 'list', 'aria-label': 'Registered PCs' });
    const pending = h('section', { class: 'fl-installs', 'aria-label': 'PCs being installed', hidden: true });
    const formPanel = h('section', { class: 'fl-form', hidden: true, 'aria-labelledby': 'fl-form-title' });
    const top = h('div', { class: 'fl-top' },
      h('div', { class: 'fl-heading' }, h('h2', { class: 'fl-title', id: 'fl-title' }, 'Fleet'),
        h('p', { class: 'fl-hint' }, 'One entry per PC. A PC serves every engine selected for it through its execution service.')),
      h('div', { class: 'fl-actions' },
        h('button', { type: 'button', class: 'primary', onclick: () => openInstall() }, 'Register PC'),
        h('button', { type: 'button', class: 'secondary', 'data-action': 'advanced', onclick: () => openForm(null) }, 'Register by URL (advanced)'),
        h('button', { type: 'button', class: 'secondary', 'data-action': 'updates', onclick: () => openUpdates() }, 'Update workers'),
        h('button', { type: 'button', class: 'secondary', onclick: () => load(true) }, 'Reload'),
        h('button', { type: 'button', class: 'secondary', 'aria-label': 'Close fleet', onclick: () => options.onClose?.() }, '×')),
      status);
    container.replaceChildren(h('div', { class: 'fl' }, top, stale, h('div', { class: 'fl-body' }, formPanel, pending, list), live));

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
        h('ul', { class: 'fl-engines', 'aria-label': 'Engines' }, worker.engines.map((e) => {
          const st = worker.engine_status?.[e], state = st?.state || (st?.connected ? 'connected' : 'not_connected');
          return h('li', { 'data-engine': e, class: `fl-${state.replace('_', '-')}` }, engineText(e, st, worker));
        })),
      ];
    }

    function versionLine(worker) {
      const item = S.updates?.workers?.find((w) => w.id === worker.id);
      if (!item) return null;
      const words = updateStatusText(item);
      return h('p', { class: `fl-version fl-${words.level}`, 'data-version': item.version || '' }, `Worker version ${shortVersion(item.version)} · ${words.text}`);
    }

    function workerNode(worker) {
      const inv = worker.inventory;
      const agents = Object.entries(inv?.agents || {}), commands = Object.entries(inv?.commands || {});
      const offlineNote = !worker.online ? h('p', { class: 'fl-hint' }, 'Capabilities are checked when it connects.') : null;
      return h('article', { class: 'fl-worker', role: 'listitem', 'data-worker': worker.id, 'aria-labelledby': `fl-w-${worker.id}` },
        h('header', { class: 'fl-worker-head' }, h('h3', { id: `fl-w-${worker.id}` }, worker.id), worker.uuid ? h('small', { class: 'fl-uuid', 'data-uuid': worker.uuid, title: 'Unique id (generated for this PC)' }, worker.uuid) : null,
          h('div', { class: 'fl-worker-actions' },
            h('button', { type: 'button', class: 'secondary', 'data-action': 'edit', onclick: () => openForm(worker) }, 'Edit settings'),
            h('button', { type: 'button', class: 'secondary', 'data-action': 'refresh', disabled: !worker.online, title: worker.online ? 'Check installed agents and commands now' : 'The PC must be connected',
              onclick: (e) => refresh(worker.id, e.currentTarget) }, 'Refresh capabilities'))),
        (worker.warnings || []).map((w) => h('p', { class: 'fl-warning' }, `Warning: ${w}`)),
        h('div', { class: 'fl-live-parts' }, liveParts(worker)),
        versionLine(worker),
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
      loadInstalls();
      loadUpdates().then(() => render());
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


    // ---- one-line install: form, command, live progress
    async function loadInstalls() {
      const r = await request('GET', '/api/fleet/installs');
      if (r.ok) S.installs = r.data.installs || [];
      drawInstalls();
      return r.ok;
    }

    function drawInstalls() {
      const open = S.installs.filter((i) => !(i.state === 'registered' && i.connected) && i.state !== 'cancelled' && i.state !== 'expired');
      pending.hidden = !open.length || !!S.form;
      pending.replaceChildren(...(open.length ? [h('h3', { class: 'fl-installs-title' }, 'Waiting to be installed')] : []),
        ...open.map((record) => {
          const progress = installProgress(record);
          return h('article', { class: 'fl-install-card', 'data-install': record.id },
            h('div', { class: 'fl-install-head' }, h('strong', {}, record.id), h('span', { class: `fl-install-state fl-${progress.level}` }, progress.text)),
            h('div', { class: 'fl-row' },
              record.commands ? h('button', { type: 'button', class: 'secondary', 'data-action': 'show-command', onclick: () => showInstall(record) }, 'Show command') : null,
              record.state === 'pending' ? h('button', { type: 'button', class: 'secondary', 'data-action': 'cancel-install', onclick: () => cancelInstall(record) }, 'Cancel') : null));
        }));
    }

    async function cancelInstall(record) {
      const r = await request('DELETE', `/api/fleet/installs/${enc(record.code)}`);
      if (!r.ok) { say(`Could not cancel: ${errorText(r)}`, true); return false; }
      say(`Cancelled the install link for ${record.id}.`);
      announce(`Install link for ${record.id} cancelled`);
      await loadInstalls();
      return true;
    }

    function copyText(text) {
      if (navigator.clipboard?.writeText && window.isSecureContext) return navigator.clipboard.writeText(text).then(() => true, () => false);
      const area = h('textarea', { 'aria-hidden': 'true', tabindex: '-1', style: 'position:fixed;opacity:0' });
      area.value = text; document.body.append(area); area.select();
      let ok = false;
      try { ok = document.execCommand('copy'); } catch { ok = false; }
      area.remove();
      return Promise.resolve(ok);
    }

    /* The result of a valid form: the command for the PC's OS, its status and a way out. */
    function showInstall(record, commands = record.commands) {
      clearTimeout(S.installTimer);
      S.form = { install: true };
      const status = h('p', { class: 'fl-install-status', role: 'status' });
      const note = h('p', { class: 'fl-hint fl-install-expiry' });
      const done = h('button', { type: 'button', class: 'primary', 'data-action': 'install-done', hidden: true, onclick: async () => { closeForm(); await load(); } }, 'Done');
      const cancel = h('button', { type: 'button', class: 'secondary', 'data-action': 'cancel-install', onclick: async () => { if (await cancelInstall(record)) closeForm(); } }, 'Cancel this install');
      const blocks = installCommands(record.os, commands).map((c) => {
        const pre = h('pre', { class: 'fl-code fl-command', 'data-command': c.id, tabindex: '0' }, h('code', {}, c.text));
        const button = h('button', { type: 'button', class: 'secondary', 'data-action': `copy-${c.id}`, onclick: async () => {
          const ok = await copyText(c.text);
          button.textContent = ok ? 'Copied' : 'Select the command and copy it';
          announce(ok ? `${c.label} command copied` : 'Copy failed: select the command and copy it');
          setTimeout(() => { button.textContent = 'Copy'; }, 2500);
        } }, 'Copy');
        return h('section', { class: 'fl-command-block', 'aria-label': c.label }, h('h4', {}, c.label), h('p', { class: 'fl-hint' }, c.hint), pre, h('div', { class: 'fl-row' }, button));
      });
      formPanel.replaceChildren(h('div', { class: 'fl-form-body fl-install-result' },
        h('h3', { id: 'fl-form-title' }, `Install ${record.id}`),
        h('p', { class: 'fl-hint' }, 'Run one command on the PC. It downloads the worker from this server, installs it for that user and connects back; nothing on the PC needs to be opened.'),
        ...blocks,
        h('p', { class: 'fl-elevation' }, h('strong', {}, 'Run as a normal user. '), 'Do not use Administrator or sudo: everything installs for your own user.'),
        record.autostart === 'boot' ? h('p', { class: 'fl-hint fl-boot-note' }, record.os === 'windows' ? 'Starting before anyone logs in needs administrator approval: Windows shows a UAC prompt near the end of the install. Approve it on the PC. If you decline, the worker starts at logon instead and the install says so.' : 'Starting before anyone logs in may ask for your sudo password once (systemd lingering). If it cannot, the worker starts at login instead and the install prints the command for an administrator.') : null,
        h('p', { class: 'fl-hint' }, 'The PC needs nothing installed first: the command downloads one program from this server.'),
        note, status,
        h('div', { class: 'fl-row fl-form-actions' }, cancel, done)));
      formPanel.hidden = false; list.hidden = true; pending.hidden = true;
      formPanel.parentElement.scrollTop = 0;
      const tick = async () => {
        if (!S.form?.install || !S.open) return;
        await loadInstalls();
        pending.hidden = true;
        const current = S.installs.find((i) => i.code === record.code);
        const progress = installProgress(current);
        status.textContent = progress.text; status.className = `fl-install-status fl-${progress.level}`;
        note.textContent = current?.state === 'pending' ? `The link works once and expires in about ${minutesLeft(record.expires_at)} min.` : '';
        const finished = current?.state === 'registered' && current.connected;
        done.hidden = !finished;
        if (finished) { announce(`${record.id} is connected`); say(`${record.id} is connected.`); return; }
        if (!current || ['cancelled', 'expired'].includes(current.state)) return;
        S.installTimer = setTimeout(tick, config.installMs);
      };
      tick();
    }

    /* The register form: details first, validated before any install link exists. */
    function openInstall(preset = {}) {
      clearTimeout(S.installTimer);
      S.form = { install: true, engines: preset.engines || ['hatchet'] };
      const f = S.form;
      const taken = () => S.workers.map((w) => w.id);
      const state = { os: preset.os || 'windows', pool: preset.pool || (S.pools.includes('default') ? 'default' : S.pools[0]) || 'default', newPool: false, tags: [...(preset.tags || [])], autostart: preset.autostart || 'logon' };
      const errorBox = h('div', { class: 'fl-form-error', role: 'alert' });
      const fieldError = (name) => h('small', { class: 'fl-field-error', 'data-error': name, id: `fl-err-${name}` });
      const errors = { id: fieldError('id'), os: fieldError('os'), engines: fieldError('engines'), pool: fieldError('pool'), tags: fieldError('tags'),
        slots: fieldError('slots'), cpu_limit: fieldError('cpu_limit'), ram_limit: fieldError('ram_limit'), server: fieldError('server'), autostart: fieldError('autostart') };

      const idInput = h('input', { name: 'id', maxlength: 64, autocomplete: 'off', spellcheck: 'false', value: preset.id || '', placeholder: 'for example build-pc-1', 'aria-describedby': 'fl-id-help fl-err-id' });
      const osBoxes = h('fieldset', { class: 'fl-os-choice' }, h('legend', {}, 'Operating system of the PC'),
        Object.entries(OS_LABELS).map(([value, label]) => h('label', { class: 'fl-check' }, h('input', { type: 'radio', name: 'os', value, checked: value === state.os, onchange: () => { state.os = value; drawAutostart(); } }), ` ${label}`)), errors.os);
      const autostartBox = h('fieldset', { class: 'fl-autostart-choice' }, h('legend', {}, 'When should the worker start on the PC?'));
      const drawAutostart = () => autostartBox.replaceChildren(h('legend', {}, 'When should the worker start on the PC?'),
        ...autostartChoices(state.os).map((c) => h('label', { class: 'fl-check fl-autostart', 'data-autostart': c.value },
          h('input', { type: 'radio', name: 'autostart', value: c.value, checked: c.value === state.autostart, onchange: () => { state.autostart = c.value; } }),
          h('span', {}, ` ${c.label}`, h('small', { class: 'fl-hint' }, c.text)))), errors.autostart);
      const engineBoxes = h('fieldset', { class: 'fl-engines-choice' }, h('legend', {}, 'Engines this PC serves'),
        ENGINES.map((e) => h('label', { class: 'fl-check' }, h('input', { type: 'checkbox', name: 'engine', value: e, checked: f.engines.includes(e),
          onchange: (ev) => { f.engines = ENGINES.filter((x) => (x === e ? ev.target.checked : f.engines.includes(x))); } }), ` ${ENGINE_LABELS[e]}`)), errors.engines);

      const poolSelect = h('select', { name: 'pool', 'aria-describedby': 'fl-err-pool' });
      const newPoolInput = h('input', { name: 'new_pool', maxlength: 100, autocomplete: 'off', 'aria-label': 'New pool name', placeholder: 'New pool name', hidden: true });
      const drawPoolOptions = () => {
        const names = [...new Set([...S.pools, 'default'])].sort();
        poolSelect.replaceChildren(...names.map((p) => h('option', { value: p }, p)), h('option', { value: '__new__' }, 'New pool…'));
        poolSelect.value = names.includes(state.pool) ? state.pool : names[0];
        state.pool = poolSelect.value;
      };
      poolSelect.addEventListener('change', () => { state.newPool = poolSelect.value === '__new__'; newPoolInput.hidden = !state.newPool; if (!state.newPool) state.pool = poolSelect.value; else newPoolInput.focus(); });

      const tagInput = h('input', { name: 'tag', class: 'fl-tag-input', list: 'fl-install-tags', autocomplete: 'off', spellcheck: 'false', maxlength: 100, placeholder: 'Type a tag and press Enter', 'aria-describedby': 'fl-err-tags' });
      const tagList = h('datalist', { id: 'fl-install-tags' }, S.tags.map((t) => h('option', { value: t })));
      const chosen = h('div', { class: 'fl-chosen-tags', 'aria-label': 'Chosen tags' });
      const drawChosen = () => chosen.replaceChildren(...state.tags.map((t) => h('span', { class: 'fl-chip' }, t,
        h('button', { type: 'button', class: 'fl-x', 'aria-label': `Remove tag ${t}`, onclick: () => { state.tags = state.tags.filter((x) => x !== t); drawChosen(); tagInput.focus(); } }, '×'))));
      const addTag = () => {
        const name = tagInput.value.trim().replace(/,$/, '');
        if (!name) return;
        if (!validLabel(name)) { errors.tags.textContent = 'A tag is 1 to 100 characters without commas'; return; }
        errors.tags.textContent = '';
        if (!state.tags.includes(name)) state.tags.push(name);
        tagInput.value = ''; drawChosen();
      };
      tagInput.addEventListener('keydown', (e) => { if (e.key === 'Enter' || e.key === ',') { e.preventDefault(); addTag(); } });
      tagInput.addEventListener('blur', addTag);

      const number = (name, value, min, max, step = 1) => h('input', { name, type: 'number', inputmode: 'decimal', min, max, step, value: String(value), 'aria-describedby': `fl-err-${name}` });
      const slots = number('slots', preset.slots ?? 1, 1, MAX_SLOTS), cpu = number('cpu_limit', preset.cpu_limit ?? 90, 1, 100, 'any'), ram = number('ram_limit', preset.ram_limit ?? 90, 1, 100, 'any');
      const origin = typeof location !== 'undefined' && location.origin && location.origin !== 'null' ? location.origin : '';
      const serverInput = h('input', { name: 'server', type: 'url', autocomplete: 'off', spellcheck: 'false', value: preset.server || origin, placeholder: 'http://192.168.1.10:8090', 'aria-describedby': 'fl-server-help fl-err-server' });
      const serverWarning = h('small', { class: 'fl-warning', 'data-warning': 'server' });
      const drawServerWarning = () => {
        const check = serverAddress(serverInput.value);
        const plain = check.value && check.value.startsWith('http://');
        const private_ = plain && (/^http:\/\/(10\.|192\.168\.|172\.(1[6-9]|2\d|3[01])\.|100\.(6[4-9]|[7-9]\d|1[01]\d|12[0-7])\.)/.test(check.value) || /^http:\/\/[^./:]+(:\d+)?$/.test(check.value) || /\.ts\.net/i.test(check.value));
        serverWarning.textContent = plain && !private_ ? 'This address uses plain http outside a private network or tailnet: the install command and the PC\'s token would cross it unencrypted. Prefer https.' : '';
      };
      serverInput.addEventListener('input', drawServerWarning);

      const field = (label, input, helpText, helpId, name) => h('label', { class: 'fl-field' }, h('span', {}, label), input, helpText ? h('small', { id: helpId }, helpText) : null, name ? errors[name] : null);
      const submit = h('button', { type: 'submit', class: 'primary' }, 'Create install command');
      const form = h('form', { class: 'fl-form-body', novalidate: true },
        h('h3', { id: 'fl-form-title' }, 'Register a PC'),
        h('p', { class: 'fl-hint' }, 'Fill in the details first. When they are valid you get one command to run on the PC; nothing is created before that.'),
        field('Name', idInput, 'Letters, digits, dashes and underscores. This is how the PC appears in the fleet.', 'fl-id-help', 'id'),
        osBoxes, autostartBox, engineBoxes,
        h('fieldset', { class: 'fl-pools' }, h('legend', {}, 'Pool'), poolSelect, newPoolInput, errors.pool),
        h('fieldset', { class: 'fl-tags' }, h('legend', {}, 'Tags (optional)'), chosen, tagInput, tagList, errors.tags),
        h('fieldset', { class: 'fl-settings' }, h('legend', {}, 'Capacity'),
          field('Slots (jobs at once, all engines)', slots, null, null, 'slots'), field('CPU limit (%)', cpu, null, null, 'cpu_limit'), field('RAM limit (%)', ram, null, null, 'ram_limit'),
          h('small', {}, 'New work waits while a 10-second average is at or above a limit. Running work is never stopped.')),
        field('Address the PC uses to reach this server', serverInput, 'Name or IP and port the PC can open, for example http://192.168.1.10:8090. Not localhost.', 'fl-server-help', 'server'),
        serverWarning, errorBox,
        h('div', { class: 'fl-row fl-form-actions' }, h('button', { type: 'button', class: 'secondary', onclick: closeForm }, 'Cancel'), submit));

      form.addEventListener('submit', async (e) => {
        e.preventDefault();
        if (S.busy) return;
        addTag();
        errorBox.replaceChildren();
        for (const node of Object.values(errors)) node.textContent = '';
        for (const input of form.querySelectorAll('[aria-invalid]')) input.removeAttribute('aria-invalid');
        const pool = state.newPool ? newPoolInput.value : poolSelect.value;
        const result = validateInstall({ id: idInput.value, os: state.os, engines: f.engines, pool, tags: state.tags, slots: slots.value, cpu_limit: cpu.value, ram_limit: ram.value, server: serverInput.value, autostart: state.autostart }, { taken: taken() });
        if (!result.body) {
          const names = Object.keys(result.errors);
          for (const name of names) { errors[name].textContent = result.errors[name]; form.querySelector(`[name="${name}"]`)?.setAttribute('aria-invalid', 'true'); }
          errorBox.textContent = `Fix ${names.length === 1 ? 'this' : 'these'} first: ${names.map((n) => result.errors[n]).join('; ')}.`;
          form.querySelector('[aria-invalid="true"]')?.focus?.();
          return;
        }
        S.busy = true; submit.disabled = true;
        const r = await request('POST', '/api/fleet/installs', result.body);
        S.busy = false; submit.disabled = false;
        if (!r.ok) { errorBox.textContent = errorText(r); return; }
        say(`Install command created for ${result.body.id}.`);
        announce(`Install command for ${result.body.id} created`);
        showInstall({ ...r.data.install, commands: r.data.commands }, r.data.commands);
      });

      drawPoolOptions(); drawChosen(); drawServerWarning(); drawAutostart();
      formPanel.replaceChildren(form);
      formPanel.hidden = false; list.hidden = true; pending.hidden = true;
      formPanel.parentElement.scrollTop = 0;
      idInput.focus();
    }


    // ---- worker updates: versions, "update all" and a command for a PC that was down
    async function loadUpdates() {
      const r = await request('GET', '/api/fleet/updates');
      if (r.ok) S.updates = r.data;
      return r.ok;
    }

    const shortVersion = (v) => (v ? String(v).slice(0, 8) : 'unknown');

    function updateRow(item, onLink) {
      const words = updateStatusText(item);
      return h('li', { class: `fl-update-row fl-${words.level}`, 'data-update': item.id },
        h('span', { class: 'fl-update-name' }, item.id), ' ',
        h('span', { class: 'fl-update-version' }, `version ${shortVersion(item.version)}`), ' ',
        h('span', { class: 'fl-update-state' }, words.text),
        words.link ? h('button', { type: 'button', class: 'secondary', 'data-action': 'update-link', onclick: () => onLink(item) }, 'Get update command') : null);
    }

    /* The update view: what each PC runs, update them all, and a command for any PC that cannot update itself. */
    function openUpdates() {
      clearTimeout(S.installTimer);
      S.form = { install: true, updates: true };
      const summary = h('p', { class: 'fl-hint fl-update-summary', role: 'status' });
      const listNode = h('ul', { class: 'fl-update-list', 'aria-label': 'Worker versions' });
      const linkBox = h('div', { class: 'fl-update-link' });
      const error = h('div', { class: 'fl-form-error', role: 'alert' });
      const originInput = h('input', { name: 'server', type: 'url', value: typeof location !== 'undefined' && location.origin !== 'null' ? location.origin : '', 'aria-label': 'Address the PC uses to reach this server' });

      const draw = () => {
        const data = S.updates || { workers: [], latest: {} };
        const counts = { up_to_date: 0, updating: 0, waiting: 0, failed: 0, outdated: 0, offline: 0 };
        for (const w of data.workers) counts[w.status] = (counts[w.status] || 0) + 1;
        summary.textContent = data.workers.length
          ? `${counts.up_to_date || 0} up to date · ${(counts.updating || 0)} updating · ${(counts.waiting || 0) + (counts.offline || 0)} waiting for the PC to connect · ${(counts.outdated || 0)} outdated · ${(counts.failed || 0)} failed`
          : 'No PC installed with the one-line command is registered yet.';
        listNode.replaceChildren(...data.workers.map((w) => updateRow(w, showLink)));
      };

      async function showLink(item) {
        error.textContent = '';
        const os = item.os || (await promptOs());
        const r = await request('POST', '/api/fleet/updates/link', { worker: item.id, os, server: originInput.value.trim() });
        if (!r.ok) { error.textContent = errorText(r); return; }
        const commands = installCommands(os, r.data.commands);
        linkBox.replaceChildren(h('h4', {}, `Update ${item.id}`), h('p', { class: 'fl-hint' }, `Run on the PC as a normal user. It works once and expires in about ${minutesLeft(r.data.link.expires_at)} min. If the worker is installed and healthy, \`agent-worker update\` does the same.`),
          ...commands.map((c) => h('section', { class: 'fl-command-block', 'aria-label': c.label }, h('h4', {}, c.label), h('pre', { class: 'fl-code fl-command', 'data-command': c.id, tabindex: '0' }, h('code', {}, c.text)),
            h('div', { class: 'fl-row' }, h('button', { type: 'button', class: 'secondary', onclick: async (e) => { const ok = await copyText(c.text); e.currentTarget.textContent = ok ? 'Copied' : 'Select the command and copy it'; } }, 'Copy')))));
      }
      const promptOs = async () => (typeof window.prompt === 'function' ? (window.prompt('Operating system of this PC (windows or linux)', 'windows') || 'windows').trim().toLowerCase() : 'windows');

      const updateAll = h('button', { type: 'button', class: 'primary', 'data-action': 'update-all', onclick: async () => {
        error.textContent = '';
        const r = await request('POST', '/api/fleet/updates');
        if (!r.ok) { error.textContent = errorText(r); return; }
        S.updates = r.data; draw(); announce('Fleet update started');
      } }, 'Update all workers now');
      const stop = h('button', { type: 'button', class: 'secondary', 'data-action': 'update-stop', onclick: async () => { const r = await request('DELETE', '/api/fleet/updates'); if (r.ok) { S.updates = r.data; draw(); } } }, 'Stop updating');
      formPanel.replaceChildren(h('div', { class: 'fl-form-body fl-updates' },
        h('h3', { id: 'fl-form-title' }, 'Worker updates'),
        h('p', { class: 'fl-hint' }, 'Connected PCs update themselves within seconds: each waits for its running jobs to finish, swaps in the new worker and restarts, and goes back to the previous one if the new one fails. A PC that is down updates when it connects, or run the command below on it.'),
        summary, listNode, error,
        h('label', { class: 'fl-field' }, h('span', {}, 'Address the PC uses to reach this server (for the update command)'), originInput),
        linkBox,
        h('div', { class: 'fl-row fl-form-actions' }, h('button', { type: 'button', class: 'secondary', onclick: closeForm }, 'Close'), stop, updateAll)));
      formPanel.hidden = false; list.hidden = true; pending.hidden = true;
      draw();
      const tick = async () => {
        if (!S.form?.updates || !S.open) return;
        await loadUpdates(); draw();
        S.installTimer = setTimeout(tick, config.installMs);
      };
      tick();
    }

    // ---- direct-URL registration and settings form
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
      const slots = number('slots', worker?.slots ?? 1, 1, MAX_SLOTS), cpu = number('cpu_limit', worker?.cpu_limit ?? 90, 1, 100), ram = number('ram_limit', worker?.ram_limit ?? 90, 1, 100);
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
        const s = whole(slots, 'Slots', MAX_SLOTS); if (s == null) return;
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
      clearTimeout(S.installTimer);
      formPanel.replaceChildren();
      formPanel.hidden = true;
      list.hidden = false;
      drawInstalls();
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

  const api = { autostartChoices, updateStatusText, MAX_SLOTS, ENGINES, ENGINE_LABELS, config, instructions, serverAddress, validateInstall, installCommands, installProgress, minutesLeft, tagSuggestions, metricView, engineText, syncText, tokenEnvFor, formatTime, validLabel, mount };
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  else root.Fleet = api;
})(typeof window !== 'undefined' ? window : globalThis);
