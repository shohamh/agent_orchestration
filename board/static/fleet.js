/* Fleet tab: the registered execution PCs, their live CPU/RAM, capabilities and settings, and
 * the registration form with engine- and OS-specific setup instructions. Pure helpers are
 * exported for node tests. Worker data is only ever placed in text nodes and attributes; the
 * board's fleet API never returns endpoints, token names or tokens. The Board provisions a
 * dedicated Hatchet worker token server-side and delivers it only to the installing PC.
 */
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
    const sharedBridge = status?.route === 'shared_service' || engine === 'trigger';
    const nativeWorker = ['native', 'pc_native'].includes(status?.route);
    if (status?.state === 'unconfirmed' || (worker.online && status?.coordinator === 'unconfirmed')) {
      return sharedBridge
        ? `${label}: Unconfirmed: the PC is online, but the flow host cannot check the trigger dev worker on the coordinator`
        : `${label}: Unconfirmed: the PC is online, but the backend cannot confirm its ${nativeWorker ? `native ${label}` : label} worker`;
    }
    if (status?.connected) return sharedBridge
      ? `${label}: Connected through the shared Fleet execution bridge`
      : nativeWorker ? `${label}: Connected as a native worker on this PC` : `${label}: Connected`;
    if (!worker.online) return `${label}: Not connected: the PC is ${(STATUS[worker.status] || 'offline').replace(/^Offline: /, 'offline: ').toLowerCase()}`;
    if (status?.coordinator === 'not_enabled') return `${label}: Not connected: ${label} is not enabled on the flow host (FLOW_BACKENDS)`;
    if (status?.coordinator === 'stopped') return sharedBridge
      ? `${label}: Not connected: the ${label} flow worker is not running on the coordinator`
      : `${label}: Not connected: the ${nativeWorker ? `native ${label} worker` : `${label} worker`} is not running on this PC`;
    return `${label}: Unknown`;
  }

  function syncText(sync, id) {
    const state = sync?.state;
    if (state === 'applied') return `Limits are applied on ${id} for new work; running work continues.`;
    if (state === 'pending' || !state) return 'Limits apply when the PC connects.';
    if (state === 'unsupported') return `Limits not applied: ${sync.message || 'update the execution service on the PC'}.`;
    return `Limits not applied yet: ${sync.message || 'the PC did not accept them'}.`;
  }

  /* Setup guidance for the selected engine workers. The backend's status is authoritative about
   * runtime readiness; these instructions describe the intended per-PC setup. */
  function instructions({ engines = [], os = 'windows', existing = null } = {}) {
    const sections = [], warnings = [];
    const chosen = ENGINES.filter((e) => engines.includes(e));
    const adding = existing ? chosen.filter((e) => !existing.includes(e)) : chosen;
    if (existing) {
      sections.push({ title: 'This PC is already registered', steps: [{ text: 'Nothing new is needed on the PC; saving updates the engines enabled for this registration. Engine status reflects what the backend has actually confirmed.' }] });
    } else {
      sections.push({ title: `1. Prepare the PC (${OS_LABELS[os] || os})`, steps: [
        { text: 'Run the one-line install command below on the PC. It downloads and installs the Fleet worker from this server; these instructions do not assert that an engine runtime is ready.' },
      ] });
      if (os === 'macos') warnings.push('macOS: its CPU/RAM sampler supports only Windows and Linux. The PC reports unknown capacity, so capacity-gated new work waits with "missing metrics".');
    }
    const notes = {
      hatchet: 'Hatchet is intended to run as a native worker on this PC. The Board creates a dedicated worker-capable token for this install using its server-side Hatchet ADMIN/OWNER session; do not enter or reuse the Board API token. Its configured worker endpoint must be reachable from this PC and use the deployment TLS strategy. Readiness is shown only after the backend confirms the worker.',
      prefect: 'Prefect is intended to run as a native worker on this PC. Its configured API URL must be reachable from this PC. Readiness is shown only after the backend confirms the worker.',
      trigger: 'Trigger.dev runs through the shared Fleet execution bridge; only the bridge’s backend availability is reported.',
    };
    const listed = existing ? (adding.length ? adding : chosen) : chosen;
    if (listed.length) sections.push({ title: existing && adding.length ? 'Engines you are adding' : 'Engines', steps: listed.map((e) => ({ engine: e, text: `${ENGINE_LABELS[e]}: ${notes[e]}` })) });
    return { sections, warnings };
  }


  // ------------------------------------------------------------------ One-line install (pure helpers)
  const INSTALL_STEPS = { check: 'Checking the PC', download: 'Downloading the worker', python: 'Installing Python packages', npm: 'Installing the agent adapters',
    token: 'Creating its token', service: 'Starting the worker', register: 'Registering', connect: 'Waiting for the connection', boot: 'Setting up start at boot (approve the administrator prompt on the PC)' };
  const LOOPBACK = /^(localhost|127\.\d+\.\d+\.\d+|0\.0\.0\.0|\[?::1\]?)$/i;

  function privateNetworkHost(hostname) {
    const host = String(hostname || '').toLowerCase().replace(/^\[|\]$/g, '');
    if (host.endsWith('.ts.net')) return true;
    if (/^(fc|fd)[0-9a-f]{2}:/i.test(host) || /^fe[89ab][0-9a-f]:/i.test(host)) return true;
    const octets = host.split('.');
    if (octets.length !== 4 || octets.some((part) => !/^\d{1,3}$/.test(part) || Number(part) > 255)) return false;
    const [a, b] = octets.map(Number);
    return a === 10 || a === 192 && b === 168 || a === 172 && b >= 16 && b <= 31 || a === 169 && b === 254;
  }

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

  /* The auto restart choice and what it means. It only applies when the worker starts by itself (logon or boot). */
  function autoRestartText(os, autostart) {
    const windows = os === 'windows';
    if (autostart === 'none') return 'Not used here: without a startup entry nothing restarts the worker.';
    return 'Restart the worker automatically if it crashes or is killed. '
      + (windows ? 'A Scheduled Task checks every minute and starts it again, so a crash costs up to a minute. ' : 'systemd starts it again after a few seconds. ')
      + 'Turn it off to leave a stopped worker down (for example while you debug it): it then starts again only at the next '
      + (autostart === 'boot' ? 'PC start-up' : 'logon') + ' or with agent-worker start. You can change this later with agent-worker autorestart on|off.';
  }

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
    const autoRestart = v.auto_restart === undefined ? true : v.auto_restart;
    if (typeof autoRestart !== 'boolean') errors.auto_restart = 'Auto restart is on or off';
    if (!AUTOSTARTS.includes(autostart)) errors.autostart = 'Choose when the worker starts';
    if (Object.keys(errors).length) return { errors, body: null };
    const body = { id, os: v.os, engines: ENGINES.filter((e) => v.engines.includes(e)), pool: String(v.pool).trim(), tags: [...new Set(tags)],
      slots: Number(v.slots), cpu_limit: Number(v.cpu_limit), ram_limit: Number(v.ram_limit), server: server.value, autostart, auto_restart: autoRestart };
    return { errors, body };
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
    const unregisterDialog = h('dialog', { class: 'fl-unregister', 'aria-labelledby': 'fl-unregister-title', 'aria-describedby': 'fl-unregister-message' });
    const filterInput = h('input', { type: 'search', class: 'fl-filter-input', placeholder: 'Filter PCs', 'aria-label': 'Filter PCs', autocomplete: 'off', spellcheck: 'false',
      title: 'Words match a PC\'s name, pools, tags, engines, state and installed software.\nid: pool: tag: engine: status: agent: command: narrow to one, for example tag:gpu or agent:codex.\n-word excludes.',
      oninput: (e) => { S.filter = e.target.value; render(); } });
    const filterCount = h('span', { class: 'fl-filter-count', role: 'status' });
    const filterClear = h('button', { type: 'button', class: 'secondary', hidden: true, onclick: () => { setFilter(''); filterInput.focus(); } }, 'Clear');
    const filterBar = h('div', { class: 'fl-filter', role: 'search' }, filterInput, filterCount, filterClear);
    const pending = h('section', { class: 'fl-installs', 'aria-label': 'PCs being installed', hidden: true });
    const formPanel = h('section', { class: 'fl-form', hidden: true, 'aria-labelledby': 'fl-form-title' });
    // Icons say more on hover; their words stay for screen readers. Field help sits behind an "i".
    const FL_ICONS = { history: 'M8 4.500V8l2.500 1.500M2.500 8a5.500 5.500 0 1 0 1.700-4M2.500 2.500V5H5', link: 'M6.500 9.500 9.500 6.500M7 4.500l1-1a3 3 0 0 1 4.500 4.500l-1 1M9 11.500l-1 1A3 3 0 0 1 3.500 8l1-1', update: 'M8 2v8M4.500 6.500 8 10l3.500-3.500M2.500 13.500h11', reload: 'M13 8a5 5 0 1 1-1.500-3.500M13 2v3h-3', edit: 'M10.500 3 13 5.500 6 12.500H3.500V10z', refresh: 'M3 8a5 5 0 0 1 8.500-3.500L13 6M13 2.500V6H9.500M13 8a5 5 0 0 1-8.500 3.500L3 10M3 13.500V10h3.500' };
    const flIcon = (name) => {
      const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg'), path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
      for (const [k, v] of Object.entries({ width: 15, height: 15, viewBox: '0 0 16 16', fill: 'none', stroke: 'currentColor', 'stroke-width': 1.4, 'stroke-linecap': 'round', 'stroke-linejoin': 'round', 'aria-hidden': 'true' })) svg.setAttribute(k, v);
      path.setAttribute('d', FL_ICONS[name]); svg.append(path);
      return svg;
    };
    const iconButton = (name, label, attrs = {}) => h('button', { type: 'button', class: 'secondary fl-icon', title: label, ...attrs }, flIcon(name), h('span', { class: 'fl-sr' }, label));
    const info = (text) => h('span', { class: 'fl-info', title: text });
    const top = h('div', { class: 'fl-top' },
      h('div', { class: 'fl-heading', title: 'One entry per PC. A PC serves every engine selected for it through its execution service.' }, h('h2', { class: 'fl-title', id: 'fl-title' }, 'Fleet'),
        h('p', { class: 'fl-hint fl-intro' }, 'One entry per PC. A PC serves every engine selected for it through its execution service.'), status),
      h('div', { class: 'fl-actions' },
        h('button', { type: 'button', class: 'primary', onclick: () => openInstall() }, 'Register PC'),
        iconButton('link', 'Register by URL (advanced)', { 'data-action': 'advanced', onclick: () => openForm(null) }),
        iconButton('update', 'Update workers', { 'data-action': 'updates', onclick: () => openUpdates() }),
        iconButton('reload', 'Reload', { onclick: () => load(true) }),
        h('button', { type: 'button', class: 'close', 'aria-label': 'Close fleet', onclick: () => options.onClose?.() }, '×')));
    container.replaceChildren(h('div', { class: 'fl' }, top, stale, h('div', { class: 'fl-body' }, formPanel, pending, filterBar, list), live, unregisterDialog));

    const say = (text, error = false) => { status.textContent = text; status.classList.toggle('error', error); };
    const announce = (text) => { live.textContent = ''; setTimeout(() => { live.textContent = text; }, 30); };

    function confirmUnregister(id) {
      return new Promise((resolve) => {
        let done = false;
        const finish = (confirmed) => {
          if (done) return;
          done = true;
          unregisterDialog.removeEventListener('cancel', onCancel);
          unregisterDialog.removeEventListener('click', onBackdrop);
          unregisterDialog.close();
          resolve(confirmed);
        };
        const onCancel = (e) => { e.preventDefault(); finish(false); };
        const onBackdrop = (event) => {
          if (event.target === unregisterDialog) unregisterDialog.dispatchEvent(new Event('cancel', { cancelable: true }));
        };
        unregisterDialog.addEventListener('click', onBackdrop);
        unregisterDialog.addEventListener('cancel', onCancel);
        unregisterDialog.replaceChildren(
          h('h3', { id: 'fl-unregister-title' }, `Unregister ${id}?`),
          h('p', { id: 'fl-unregister-message' }, `This removes ${id} from Fleet, revokes its server connection, and frees its name for a new worker. Already-running work is not canceled synchronously.`),
          h('div', { class: 'fl-row fl-unregister-actions' },
            h('button', { type: 'button', class: 'secondary', onclick: () => finish(false) }, 'Cancel'),
            h('button', { type: 'button', class: 'danger', onclick: () => finish(true) }, `Unregister ${id}`)));
        unregisterDialog.showModal();
        unregisterDialog.querySelector('button').focus();
      });
    }

    async function unregisterWorker(id) {
      if (!await confirmUnregister(id)) return;
      const r = await request('DELETE', `/api/fleet/workers/${enc(id)}`);
      if (!r.ok) {
        if (r.status === 404) {
          const refreshed = await load();
          S.workers = S.workers.filter((w) => w.id !== id);
          if (S.form?.detail === id) closeForm();
          else render();
          say(refreshed ? `${id} was already unregistered from Fleet; the list was refreshed.` : `${id} was already unregistered from Fleet; its card was removed, but the list could not be refreshed.`, true);
        } else say(`Could not unregister ${id}: ${errorText(r)}`, true);
        return;
      }
      S.workers = S.workers.filter((w) => w.id !== id);
      if (S.form?.detail === id) closeForm();
      else render();
      await load();
      say(`${id} was removed from Fleet.`);
      announce(`${id} was removed from Fleet`);
    }

    // ---- worker list
    function resultRow(kind, name, entry) {
      const [glyph, word] = RESULT[entry?.status] || RESULT.unknown;
      const label = kind === 'agent' ? AGENT_LABELS[name] || name : name;
      return h('li', { class: `fl-result fl-${entry?.status || 'unknown'}`, [`data-${kind}`]: name, title: entry?.checked_at ? `Checked ${formatTime(entry.checked_at)}` : word },
        h('span', { class: 'fl-glyph', 'aria-hidden': 'true' }, glyph), ' ',
        h('span', { class: 'fl-result-name' }, label), ' ',
        h('span', { class: 'fl-result-word' }, word),
        entry?.checked_at ? h('span', { class: 'fl-when' }, ` · checked ${formatTime(entry.checked_at)}`) : null);
    }

    function metricNode(worker, kind) {
      const limit = kind === 'cpu' ? worker.cpu_limit : worker.ram_limit;
      const view = metricView(worker.capacity, kind, limit, { online: worker.online, staleSince: S.staleSince });
      const ramNote = `${kind === 'ram' ? '\nPhysical memory in use (total minus available).' : ''}\nNew work waits at or above the limit; running work is never stopped.`;
      const pct = worker.capacity?.state === 'known' && worker.online ? Math.max(0, Math.min(100, worker.capacity[kind === 'cpu' ? 'cpu_percent' : 'ram_percent'])) : 0;
      return h('div', { class: `fl-metric ${view.level.split(' ').map((l) => `fl-${l}`).join(' ')}`, 'data-metric': kind, role: 'group', 'aria-label': view.label, title: `${view.detail}${ramNote}` },
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
          h('details', { class: 'fl-basis' }, h('summary', {}, 'How limits work'), 'RAM is physical memory in use (total minus available). New work waits while either average is at or above its limit; running work is never stopped.')),
        h('p', { class: 'fl-slots' }, slotsText(worker)),
        h('ul', { class: 'fl-engines', 'aria-label': 'Engines' }, worker.engines.map((e) => {
          const st = worker.engine_status?.[e], state = st?.state || (st?.connected ? 'connected' : 'not_connected');
          const words = engineText(e, st, worker), split = words.indexOf(': ');
          // The engine's name and a coloured dot; the explanation shows on hover.
          return h('li', { 'data-engine': e, class: `fl-${state.replace('_', '-')}`, title: words.slice(split + 2) }, h('span', { class: 'fl-engine-name' }, words.slice(0, split)), h('span', { class: 'fl-engine-why' }, words.slice(split)));
        })),
      ];
    }

    function versionLine(worker) {
      const item = S.updates?.workers?.find((w) => w.id === worker.id);
      if (!item) return null;
      const words = updateStatusText(item);
      return h('p', { class: `fl-version fl-${words.level}`, 'data-version': item.version || '' }, `Worker version ${shortVersion(item.version)} · ${words.text}`);
    }

    // ---- one PC: what it is running now, and what it has run
    const UNDERWAY = new Set(['queued', 'waiting', 'running']);
    const isCurrent = (job) => UNDERWAY.has(job.state) && !['done', 'stopped'].includes(job.column);
    function jobRow(job) {
      const name = job.title || `Run ${job.run.slice(0, 12)}`, step = job.step === 'card' ? '' : job.step;
      return h('li', {}, h('button', { type: 'button', class: `fl-job fl-job-${isCurrent(job) ? 'current' : job.state || 'unknown'}`, 'data-job': job.run, disabled: !job.system,
        title: job.system ? `${job.system} / ${job.board}` : 'Not on a board anyone has open, so it can\'t be opened from here', onclick: () => options.onOpenJob?.(job) },
      h('span', { class: 'fl-job-name' }, name), step ? h('span', { class: 'fl-job-step' }, step) : null,
      h('span', { class: 'fl-job-state' }, String(job.state || 'unknown').replaceAll('_', ' ')), h('time', { class: 'fl-job-when' }, formatTime(new Date(job.at * 1000).toISOString()))));
    }
    async function openWorker(id) {
      const worker = S.workers.find((w) => w.id === id);
      S.form = { detail: id };
      const current = h('ul', { class: 'fl-jobs', 'data-jobs': 'current' }), past = h('ul', { class: 'fl-jobs', 'data-jobs': 'past' });
      const note = h('p', { class: 'fl-hint', role: 'status' }, 'Loading…');
      formPanel.replaceChildren(h('div', { class: 'fl-worker-detail' },
        h('button', { type: 'button', class: 'secondary fl-back', onclick: closeForm }, '← All PCs'),
        worker ? workerNode(worker) : h('p', { class: 'fl-warning' }, `${id} isn't registered in the fleet any more.`),
        h('section', {}, h('h3', {}, 'Running now'), current), h('section', {}, h('h3', {}, 'History'), note, past)));
      formPanel.hidden = false; list.hidden = true; pending.hidden = true;
      formPanel.parentElement.scrollTop = 0;
      const r = await request('GET', `/api/fleet/workers/${enc(id)}/jobs`);
      if (S.form?.detail !== id) return;
      if (!r.ok) { note.textContent = `Couldn't load its jobs: ${errorText(r)}`; return; }
      const jobs = r.data.jobs || [];
      note.textContent = jobs.some((j) => !isCurrent(j)) ? '' : 'Nothing has finished on this PC yet.';
      current.replaceChildren(...(jobs.some(isCurrent) ? jobs.filter(isCurrent).map(jobRow) : [h('li', { class: 'fl-hint' }, 'Nothing right now.')]));
      past.replaceChildren(...jobs.filter((j) => !isCurrent(j)).map(jobRow));
    }

    function workerMenu(e, worker) {
      if (!window.Overlays || e.pointerType === 'touch' || e.target.closest('a, input, textarea, select')) return;
      e.preventDefault();
      const act = (name) => () => list.querySelector(`.fl-worker[data-worker="${CSS.escape(worker.id)}"] [data-action="${name}"]`)?.click();
      window.Overlays.menu(e, [{ label: 'Jobs and history', onSelect: () => openWorker(worker.id) }, { label: 'Edit settings', onSelect: act('edit') },
        { label: 'Refresh capabilities', disabled: !worker.online, title: worker.online ? '' : 'The PC must be connected', onSelect: act('refresh') }, null,
        { label: 'Unregister from Fleet', onSelect: () => unregisterWorker(worker.id) }, null,
        { label: 'Copy name', disabled: !navigator.clipboard, onSelect: () => navigator.clipboard.writeText(worker.id) }]);
    }

    const filterChip = (field, value) => h('button', { type: 'button', class: 'fl-chip', title: `Show every PC with ${field} ${value}`, onclick: () => setFilter(searchTerm(field, value)) }, value);

    function workerNode(worker) {
      const inv = worker.inventory;
      const agents = Object.entries(inv?.agents || {}), commands = Object.entries(inv?.commands || {});
      const offlineNote = !worker.online ? h('p', { class: 'fl-hint' }, 'Capabilities are checked when it connects.') : null;
      return h('article', { class: 'fl-worker', role: 'listitem', 'data-worker': worker.id, 'aria-labelledby': `fl-w-${worker.id}`, oncontextmenu: (e) => workerMenu(e, worker) },
        h('header', { class: 'fl-worker-head' }, h('h3', { id: `fl-w-${worker.id}` }, worker.id), worker.uuid ? h('small', { class: 'fl-uuid', 'data-uuid': worker.uuid, title: 'Unique id (generated for this PC)' }, worker.uuid) : null,
          h('div', { class: 'fl-worker-actions' },
            iconButton('history', 'Jobs and history', { 'data-action': 'jobs', onclick: () => openWorker(worker.id) }),
            iconButton('edit', 'Edit settings', { 'data-action': 'edit', onclick: () => openForm(worker) }),
            h('button', { type: 'button', class: 'secondary fl-icon', 'data-action': 'refresh', disabled: !worker.online, title: worker.online ? 'Refresh capabilities: check installed agents and commands now' : 'Refresh capabilities: the PC must be connected',
              onclick: (e) => refresh(worker.id, e.currentTarget) }, flIcon('refresh'), h('span', { class: 'fl-sr' }, 'Refresh capabilities')),
            h('button', { type: 'button', class: 'secondary fl-unregister-button', 'data-action': 'unregister', onclick: () => unregisterWorker(worker.id) }, 'Unregister'))),
        (worker.warnings || []).map((w) => h('p', { class: 'fl-warning' }, `Warning: ${w}`)),
        h('div', { class: 'fl-live-parts' }, liveParts(worker)),
        versionLine(worker),
        h('dl', { class: 'fl-facts' },
          h('dt', {}, 'Pools'), h('dd', {}, worker.pools.map((p) => filterChip('pool', p))),
          h('dt', {}, 'Tags'), h('dd', {}, worker.tags.length ? worker.tags.map((t) => filterChip('tag', t)) : h('span', { class: 'fl-hint' }, 'none')),
          h('dt', {}, 'Limits'), h('dd', { class: 'fl-limits', title: syncText(worker.settings_sync, worker.id) }, `CPU ${worker.cpu_limit}% · RAM ${worker.ram_limit}% · ${plural(worker.slots, 'slot')}`,
            h('span', { class: 'fl-sync' }, ` · ${syncText(worker.settings_sync, worker.id)}`))),
        h('section', { class: 'fl-capabilities', 'aria-label': `Capabilities of ${worker.id}` },
          h('p', { class: 'fl-refreshed' }, inv ? `Last refreshed ${formatTime(inv.refreshed_at)}` : 'Capabilities unknown',
            inv?.cached ? h('span', { class: 'fl-hint' }, ' · Last known results; the PC is not connected') : null),
          offlineNote,
          agents.length ? h('ul', { class: 'fl-results', 'aria-label': 'Agents' }, agents.map(([n, r]) => resultRow('agent', n, r))) : null,
          commands.length ? h('ul', { class: 'fl-results', 'aria-label': 'Commands' }, commands.map(([n, r]) => resultRow('command', n, r))) : null));
    }

    function setFilter(query) { S.filter = query || ''; filterInput.value = S.filter; render(); }
    function render() {
      const focused = document.activeElement?.closest?.('.fl-worker') && document.activeElement.dataset.action
        ? [document.activeElement.closest('.fl-worker').dataset.worker, document.activeElement.dataset.action] : null;
      const shown = S.workers.filter((w) => workerMatches(w, S.filter));
      filterCount.textContent = S.filter?.trim() ? `${shown.length} of ${plural(S.workers.length, 'PC')}` : '';
      filterClear.hidden = !S.filter?.trim();
      list.replaceChildren(...(!S.workers.length ? [h('p', { class: 'fl-empty' }, 'No PCs are registered yet.')]
        : shown.length ? shown.map(workerNode) : [h('p', { class: 'fl-empty' }, 'No PCs match this filter.')]));
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
      const open = S.installs.filter((i) => !(i.state === 'registered' && i.connected) && !['cancelled', 'expired', 'unregistered'].includes(i.state));
      pending.hidden = !open.length || !!S.form;
      pending.replaceChildren(...(open.length ? [h('h3', { class: 'fl-installs-title' }, 'Waiting to be installed')] : []),
        ...open.map((record) => {
          const progress = installProgress(record);
          return h('article', { class: 'fl-install-card', 'data-install': record.id },
            h('div', { class: 'fl-install-head' }, h('strong', {}, record.id), h('span', { class: `fl-install-state fl-${progress.level}` }, progress.text)),
            h('div', { class: 'fl-row' },
              record.commands ? h('button', { type: 'button', class: 'secondary', 'data-action': 'show-command', onclick: () => showInstall(record) }, 'Show command') : null,
              record.state === 'pending' ? h('button', { type: 'button', class: 'secondary', 'data-action': 'cancel-install', onclick: () => cancelInstall(record) }, 'Cancel')
                : record.state === 'registered' && !record.connected ? h('button', { type: 'button', class: 'secondary', 'data-action': 'unregister-install', onclick: () => unregisterInstall(record) }, 'Unregister') : null));
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

    async function unregisterInstall(record) {
      await unregisterWorker(record.id);
      await loadInstalls();
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
      const state = { os: preset.os || 'windows', pool: preset.pool || (S.pools.includes('default') ? 'default' : S.pools[0]) || 'default', newPool: false, tags: [...(preset.tags || [])], autostart: preset.autostart || 'logon', autoRestart: preset.auto_restart !== false };
      const errorBox = h('div', { class: 'fl-form-error', role: 'alert' });
      const fieldError = (name) => h('small', { class: 'fl-field-error', 'data-error': name, id: `fl-err-${name}` });
      const errors = { id: fieldError('id'), os: fieldError('os'), engines: fieldError('engines'), pool: fieldError('pool'), tags: fieldError('tags'),
        slots: fieldError('slots'), cpu_limit: fieldError('cpu_limit'), ram_limit: fieldError('ram_limit'), server: fieldError('server'), autostart: fieldError('autostart'), auto_restart: fieldError('auto_restart') };

      const idInput = h('input', { name: 'id', maxlength: 64, autocomplete: 'off', spellcheck: 'false', value: preset.id || '', placeholder: 'for example build-pc-1', 'aria-describedby': 'fl-id-help fl-err-id' });
      const osBoxes = h('fieldset', { class: 'fl-os-choice' }, h('legend', {}, 'Operating system'),
        h('div', { class: 'fl-choices' }, Object.entries(OS_LABELS).map(([value, label]) => h('label', { class: 'fl-check' }, h('input', { type: 'radio', name: 'os', value, checked: value === state.os, onchange: () => { state.os = value; drawAutostart(); drawRestart(); } }), ` ${label}`))), errors.os);
      const autostartBox = h('fieldset', { class: 'fl-autostart-choice' }, h('legend', {}, 'Starts'));
      const drawAutostart = () => autostartBox.replaceChildren(h('legend', {}, 'Starts'),
        h('div', { class: 'fl-choices' }, autostartChoices(state.os).map((c) => h('label', { class: 'fl-check fl-autostart', 'data-autostart': c.value, title: c.text },
          h('input', { type: 'radio', name: 'autostart', value: c.value, checked: c.value === state.autostart, onchange: () => { state.autostart = c.value; drawRestart(); } }),
          h('span', {}, ` ${c.label}`, h('small', { class: 'fl-hint' }, c.text))))), errors.autostart);
      const restartHelp = h('small', { class: 'fl-hint' });
      const restartBox = h('input', { type: 'checkbox', name: 'auto_restart', checked: state.autoRestart, onchange: (ev) => { state.autoRestart = ev.target.checked; } });
      const restartLabel = h('label', { class: 'fl-check fl-auto-restart' }, restartBox, h('span', {}, ' Restart automatically if the worker crashes or is killed', restartHelp), errors.auto_restart);
      const drawRestart = () => { restartHelp.textContent = autoRestartText(state.os, state.autostart); restartLabel.title = restartHelp.textContent; restartBox.disabled = state.autostart === 'none'; };
      const engineBoxes = h('fieldset', { class: 'fl-engines-choice' }, h('legend', {}, 'Engines'),
        h('div', { class: 'fl-choices' }, ENGINES.map((e) => h('label', { class: 'fl-check' }, h('input', { type: 'checkbox', name: 'engine', value: e, checked: f.engines.includes(e),
          onchange: (ev) => { f.engines = ENGINES.filter((x) => (x === e ? ev.target.checked : f.engines.includes(x))); } }), ` ${ENGINE_LABELS[e]}`))), errors.engines);

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
        serverWarning.textContent = plain && !privateNetworkHost(new URL(check.value).hostname) ? 'This address uses plain http outside a private network or tailnet: the registration request and per-PC credential would cross it unencrypted. Prefer https.' : '';
      };
      serverInput.addEventListener('input', drawServerWarning);

      const field = (label, input, helpText, helpId, name) => h('label', { class: 'fl-field' }, h('span', { class: 'fl-label' }, label, helpText ? info(helpText) : null), input, helpText ? h('small', { id: helpId, class: 'fl-help' }, helpText) : null, name ? errors[name] : null);
      const submit = h('button', { type: 'submit', class: 'primary' }, 'Create install command');
      const form = h('form', { class: 'fl-form-body', novalidate: true },
        h('div', { class: 'fl-form-head' }, h('h3', { id: 'fl-form-title' }, 'Register a PC'), info('Fill in the details first. When they are valid you get one command to run on the PC; nothing is created before that.')),
        h('p', { class: 'fl-hint fl-help' }, 'Fill in the details first. When they are valid you get one command to run on the PC; nothing is created before that.'),
        field('Name', idInput, 'Letters, digits, dashes and underscores. This is how the PC appears in the fleet.', 'fl-id-help', 'id'),
        osBoxes, autostartBox, restartLabel, engineBoxes,
        h('fieldset', { class: 'fl-pools' }, h('legend', {}, 'Pool'), poolSelect, newPoolInput, errors.pool),
        h('fieldset', { class: 'fl-tags' }, h('legend', {}, 'Tags'), chosen, tagInput, tagList, errors.tags),
        h('fieldset', { class: 'fl-settings' }, h('legend', {}, 'Capacity', info('Slots: jobs at once, across all engines.\nNew work waits while a 10-second average is at or above a limit. Running work is never stopped.')),
          field('Slots', slots, null, null, 'slots'), field('CPU limit %', cpu, null, null, 'cpu_limit'), field('RAM limit %', ram, null, null, 'ram_limit'),
          h('small', { class: 'fl-help' }, 'New work waits while a 10-second average is at or above a limit. Running work is never stopped.')),
        field('Server address', serverInput, 'The address this PC uses to reach this server: a name or IP and port it can open, for example http://192.168.1.10:8090. Not localhost.', 'fl-server-help', 'server'),
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
        const result = validateInstall({ id: idInput.value, os: state.os, engines: f.engines, pool, tags: state.tags, slots: slots.value, cpu_limit: cpu.value, ram_limit: ram.value, server: serverInput.value, autostart: state.autostart, auto_restart: state.autostart === 'none' ? true : state.autoRestart }, { taken: taken() });
        if (!result.body) {
          const names = Object.keys(result.errors);
          for (const name of names) { errors[name].textContent = result.errors[name]; form.querySelector(`[name="${name}"]`)?.setAttribute('aria-invalid', 'true'); }
          errorBox.replaceChildren(...(names.length === 1 ? [h('p', {}, `Fix this first: ${result.errors[names[0]]}.`)]
            : [h('p', {}, 'Fix these first:'), h('ul', {}, names.map((n) => h('li', {}, `${result.errors[n]}.`)))]));
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

      drawPoolOptions(); drawChosen(); drawServerWarning(); drawAutostart(); drawRestart();
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
        const doc = instructions({ engines: f.engines, os: f.os, existing: editing ? worker.engines : null });
        steps.replaceChildren(
          ...doc.warnings.map((w) => h('p', { class: 'fl-warning' }, w)),
          ...doc.sections.map((s) => h('section', { class: 'fl-step' }, h('h4', {}, s.title),
            ...s.steps.map((x) => [x.text ? h('p', x.engine ? { 'data-engine-note': x.engine } : {}, x.text) : null, x.code ? h('pre', { class: 'fl-code' }, h('code', {}, x.code)) : null]))));
      };

      const engineBoxes = h('fieldset', { class: 'fl-engines-choice' }, h('legend', {}, 'Engines'),
        h('div', { class: 'fl-choices' }, ENGINES.map((e) => h('label', { class: 'fl-check' }, h('input', { type: 'checkbox', name: 'engine', value: e, checked: f.engines.includes(e),
          onchange: (ev) => { f.engines = ENGINES.filter((x) => (x === e ? ev.target.checked : f.engines.includes(x))); drawSteps(); } }), ` ${ENGINE_LABELS[e]}`))));
      const osBoxes = h('fieldset', { class: 'fl-os-choice' }, h('legend', {}, 'Operating system'),
        h('div', { class: 'fl-choices' }, Object.entries(OS_LABELS).map(([value, label]) => h('label', { class: 'fl-check' }, h('input', { type: 'radio', name: 'os', value, checked: value === f.os,
          onchange: () => { f.os = value; drawSteps(); } }), ` ${label}`))));

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

      const field = (label, input, help, helpId) => h('label', { class: 'fl-field' }, h('span', { class: 'fl-label' }, label, help ? info(help) : null), input, help ? h('small', { id: helpId, class: 'fl-help' }, help) : null);
      const intro = editing ? 'Pools, tags, engines and limits are stored in the fleet registry on the coordinator. Limit changes affect new work only.'
        : 'Choose the engines and the PC\'s operating system for setup steps, then enter its details. You can save it before it is reachable.';
      // Setup steps matter when adding a PC. An existing PC needs nothing new, so they fold away.
      const setup = h('details', { class: 'fl-setup' }, h('summary', {}, editing ? 'Setup notes' : 'Setup steps'), osBoxes, steps);
      setup.open = !editing;
      const idField = field('Worker ID', idInput, editing ? 'The ID cannot change. Register a new PC instead.' : 'Letters, digits, dots, dashes and underscores. The PC\'s FLOW_WORKER_ID must be the same.', 'fl-id-help');
      idField.hidden = editing;
      const connection = [field('Endpoint', urlInput, editing && worker.connection === 'configured' ? 'An endpoint is configured; it is never shown. Leave blank to keep it.'
        : 'Optional. Leave blank to save as awaiting connection.', 'fl-url-help'),
      field('Token variable', tokenInput, editing ? 'The name of the variable on the coordinator. Leave blank to keep the current one. The token value itself is never entered here.'
        : 'The name of the workers.local.env entry that holds this PC\'s token. The token value itself is never entered here.', 'fl-token-help')];
      const connectionFold = h('details', { class: 'fl-setup' }, h('summary', {}, 'Connection'), h('div', { class: 'fl-details' }, connection));
      connectionFold.open = !editing || worker.connection !== 'configured';
      const form = h('form', { class: 'fl-form-body', novalidate: true },
        h('div', { class: 'fl-form-head' }, h('h3', { id: 'fl-form-title' }, editing ? `Edit ${worker.id}` : 'Register a PC'), info(intro)),
        h('p', { class: 'fl-hint fl-help' }, intro),
        idField, engineBoxes,
        h('fieldset', { class: 'fl-pools' }, h('legend', {}, 'Pools'), poolList,
          h('div', { class: 'fl-row' }, newPool, h('button', { type: 'button', class: 'secondary', onclick: addPool }, 'Add pool'))),
        h('fieldset', { class: 'fl-tags' }, h('legend', {}, 'Tags'), chosen,
          h('div', { class: 'fl-combo' }, tagInput, listbox),
          h('small', { id: 'fl-tag-help', class: 'fl-help' }, 'Existing tags are suggested as you type; a new one is created when you choose “Create tag”.')),
        h('fieldset', { class: 'fl-settings' }, h('legend', {}, 'Capacity', info('Slots: jobs at once, across all engines.\nAdmission thresholds for new work, defaults 90% and 90%: a 10-second average at or above a limit holds new work. Running work is never stopped.')),
          field('Slots', slots), field('CPU limit %', cpu), field('RAM limit %', ram),
          h('small', { class: 'fl-help' }, 'Admission thresholds for new work, defaults 90% and 90%: a 10-second average at or above a limit holds new work. Running work is never stopped.')),
        connectionFold, setup, errorBox,
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
      // filter: a query to start from, as when a flow step shows the PCs that can run it.
      async open({ filter = '', worker = null } = {}) {
        S.open = true;
        closeForm();
        S.filter = filter; filterInput.value = filter;
        say('Loading the fleet…');
        if (await load()) say(`${plural(S.workers.length, 'PC')} registered.`);
        if (worker) await openWorker(worker);
        schedule();
      },
      // Escape inside the form closes the form first; returns true when the tab may close.
      back() { if (S.form) { closeForm(); return false; } return true; },
      stop() { S.open = false; clearTimeout(S.timer); },
    };
  }

  // What the fleet can be filtered by. Same query language as the board's cards. Agents and commands
  // count only where they're installed: "agent:codex" finds PCs that can run Codex.
  const Search = () => (typeof module !== 'undefined' && module.exports ? require('./card-search.js') : root.CardSearch);
  const searchTerm = (field, value) => Search().term(field, value);
  function workerMatches(worker, query) {
    if (!query?.trim()) return true;
    const installed = (found) => Object.entries(found || {}).filter(([, r]) => r?.status === 'installed').map(([name]) => name);
    return Search().matchFields(query, { id: [worker.id, worker.hostname], pool: worker.pools, tag: worker.tags, engine: [...(worker.engines || []), ...(worker.engines || []).map((e) => ENGINE_LABELS[e])],
      status: [worker.status, worker.online ? 'online' : 'offline'], agent: installed(worker.inventory?.agents), command: installed(worker.inventory?.commands) });
  }

  const api = { workerMatches, autoRestartText, autostartChoices, updateStatusText, MAX_SLOTS, ENGINES, ENGINE_LABELS, config, instructions, serverAddress, validateInstall, installCommands, installProgress, minutesLeft, tagSuggestions, metricView, engineText, syncText, tokenEnvFor, formatTime, validLabel, mount };
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  else root.Fleet = api;
})(typeof window !== 'undefined' ? window : globalThis);
