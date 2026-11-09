/* Admin engine settings. Credentials are never requested or rendered here. */
(function (root) {
  'use strict';

  const ENGINES = [
    { id: 'hatchet', label: 'Hatchet' },
    { id: 'trigger', label: 'Trigger.dev · production' },
    { id: 'trigger-dev', label: 'Trigger.dev · local' },
    { id: 'prefect', label: 'Prefect' },
  ];
  const config = { pollMs: 10000 };
  const el = (tag, attrs = {}, ...children) => {
    const node = root.document.createElement(tag);
    for (const [key, value] of Object.entries(attrs)) {
      if (key === 'class') node.className = value;
      else if (key === 'text') node.textContent = value;
      else if (key.startsWith('on') && typeof value === 'function') node.addEventListener(key.slice(2).toLowerCase(), value);
      else if (value !== undefined && value !== null) node.setAttribute(key, String(value));
    }
    for (const child of children.flat(Infinity)) if (child != null) node.append(child.nodeType ? child : root.document.createTextNode(String(child)));
    return node;
  };

  function statusView(record) {
    if (!record || record.enabled === false || record.status === 'disabled') return { label: 'Disabled', state: 'disabled' };
    if (record.status === 'reachable') return { label: 'Reachable', state: 'reachable' };
    if (record.status === 'unauthorized') return { label: 'Authentication failed', state: 'unauthorized' };
    if (record.status === 'unreachable') return { label: 'Unreachable', state: 'unreachable' };
    return { label: 'Unknown', state: 'unknown' };
  }


  function checkedText(value) {
    if (!value) return 'Not checked yet';
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? String(value) : date.toLocaleString();
  }

  function mount(dialog, { onClose } = {}) {
    const rows = new Map();
    let timer = null;
    let loading = false;
    let mounted = true;

    const status = el('p', { class: 'engine-settings-status', role: 'status', 'aria-live': 'polite' }, 'Loading engine settings…');
    const list = el('div', { class: 'engine-card-list', 'aria-label': 'Workflow engines' });
    const heading = el('div', { class: 'engine-settings-heading' },
      el('h2', { id: 'engine-settings-title', class: 'engine-settings-title' }, 'Engine settings'),
      el('p', { class: 'engine-settings-hint' }, 'Configure engine and PC-worker endpoints. Hatchet worker tokens are supplied separately in Fleet registration.'));
    const reload = el('button', { type: 'button', class: 'secondary', onclick: () => refresh() }, 'Refresh');
    const close = el('button', { type: 'button', class: 'close', 'aria-label': 'Close Admin', onclick: () => onClose ? onClose() : dialog.close() }, '×');
    const top = el('header', { class: 'engine-settings-top' }, heading, el('div', { class: 'engine-settings-actions' }, reload, close), status);

    function createRow(engine) {
      const state = el('span', { class: 'engine-state', 'data-status': 'unknown' }, 'Loading…');
      const enabled = el('span', { class: 'engine-enabled' }, '');
      const apiUrl = el('input', { type: 'url', required: 'required', autocomplete: 'url', spellcheck: 'false', 'aria-label': `${engine.label} API URL`, placeholder: 'https://engine.example.com' });
      const dashboardUrl = el('input', { type: 'url', autocomplete: 'url', spellcheck: 'false', 'aria-label': `${engine.label} dashboard URL`, placeholder: 'https://dashboard.example.com' });
      const apiField = el('label', { class: 'engine-field' }, el('span', {}, 'API URL'), apiUrl);
      const dashboardField = el('label', { class: 'engine-field' }, el('span', {}, 'Browser dashboard URL'), dashboardUrl);
      const workerGrpcHostPort = engine.id === 'hatchet'
        ? el('input', { type: 'text', autocomplete: 'off', spellcheck: 'false', 'aria-label': 'Hatchet worker gRPC host and port', placeholder: 'hatchet.example.com:7077' })
        : null;
      const hatchetTlsStrategy = engine.id === 'hatchet'
        ? el('select', { 'aria-label': 'Hatchet worker TLS strategy' },
          el('option', { value: 'none' }, 'No TLS'),
          el('option', { value: 'tls' }, 'TLS'))
        : null;
      const prefectWorkerApiUrl = engine.id === 'prefect'
        ? el('input', { type: 'url', autocomplete: 'url', spellcheck: 'false', 'aria-label': 'Prefect worker API URL', placeholder: 'https://prefect.example.com/api' })
        : null;
      const fields = [apiField, dashboardField];
      let workerHint = '';
      if (engine.id === 'hatchet') {
        fields.push(
          el('label', { class: 'engine-field' }, el('span', {}, 'Worker gRPC host:port'), workerGrpcHostPort),
          el('label', { class: 'engine-field' }, el('span', {}, 'Worker TLS'), hatchetTlsStrategy));
        workerHint = 'Must be reachable from the PC. Fleet registration asks for a dedicated Hatchet worker token.';
      } else if (engine.id === 'prefect') {
        fields.push(el('label', { class: 'engine-field' }, el('span', {}, 'PC worker API URL'), prefectWorkerApiUrl));
        workerHint = 'Must be reachable from the PC running the Prefect process worker.';
      } else if (engine.id.startsWith('trigger')) {
        workerHint = 'Trigger.dev runs on its deployment workers; Fleet PCs remain execution bridges.';
      }
      const workerStatus = el('p', { class: 'engine-worker-status', role: 'status' }, workerHint);
      const checked = el('p', { class: 'engine-checked' }, 'Not checked yet');
      const error = el('p', { class: 'engine-error', hidden: true });
      const save = el('button', { type: 'button', class: 'primary', 'data-action': 'save' }, 'Save settings');
      const check = el('button', { type: 'button', class: 'secondary', 'data-action': 'check' }, 'Check now');
      const actions = el('div', { class: 'engine-row-actions' }, check, save);
      const card = el('section', { class: 'engine-card', 'data-engine': engine.id, 'aria-labelledby': `engine-card-${engine.id}` },
        el('div', { class: 'engine-card-head' }, el('h3', { id: `engine-card-${engine.id}`, class: 'engine-card-title' }, engine.label), state, enabled),
        el('div', { class: 'engine-fields' }, fields, workerStatus),
        el('div', { class: 'engine-meta' }, checked, error), actions);
      const row = { ...engine, card, state, enabled, apiUrl, dashboardUrl, workerGrpcHostPort, hatchetTlsStrategy,
        prefectWorkerApiUrl, workerHint, workerStatus, checked, error, save, check,
        enabledValue: false, dirty: { api: false, dashboard: false, grpc: false, tls: false, prefectWorker: false }, saving: false, checking: false };
      apiUrl.addEventListener('input', () => { row.dirty.api = true; });
      dashboardUrl.addEventListener('input', () => { row.dirty.dashboard = true; });
      workerGrpcHostPort?.addEventListener('input', () => { row.dirty.grpc = true; });
      hatchetTlsStrategy?.addEventListener('change', () => { row.dirty.tls = true; });
      prefectWorkerApiUrl?.addEventListener('input', () => { row.dirty.prefectWorker = true; });
      save.addEventListener('click', () => saveRow(row));
      check.addEventListener('click', () => checkRow(row));
      rows.set(engine.id, row);
      return card;
    }

    list.replaceChildren(...ENGINES.map(createRow));
    dialog.replaceChildren(el('div', { class: 'engine-settings-content' }, top, el('div', { class: 'engine-settings-body' }, list)));

    function applyRecord(row, record, { saved = false } = {}) {
      row.enabledValue = record.enabled === true;
      const view = statusView(record);
      row.state.textContent = view.label;
      row.state.dataset.status = view.state;
      row.enabled.textContent = row.enabledValue ? 'Enabled' : 'Disabled';
      if (saved || !row.dirty.api) row.apiUrl.value = record.api_url || '';
      if (saved || !row.dirty.dashboard) row.dashboardUrl.value = record.dashboard_url || '';
      if (row.workerGrpcHostPort && (saved || !row.dirty.grpc)) row.workerGrpcHostPort.value = record.hatchet_worker_grpc_host_port || '';
      if (row.hatchetTlsStrategy && (saved || !row.dirty.tls)) row.hatchetTlsStrategy.value = record.hatchet_worker_tls_strategy || 'none';
      if (row.prefectWorkerApiUrl && (saved || !row.dirty.prefectWorker)) row.prefectWorkerApiUrl.value = record.prefect_worker_api_url || '';
      if (saved) row.dirty = { api: false, dashboard: false, grpc: false, tls: false, prefectWorker: false };
      if (record.worker_endpoint_configured === true) row.workerStatus.textContent = 'PC worker endpoint configured.';
      else if (record.worker_endpoint_configured === false) row.workerStatus.textContent = 'PC worker endpoint not configured.';
      else row.workerStatus.textContent = row.workerHint;
      row.checked.textContent = `Last checked: ${checkedText(record.checked_at)}`;
      row.checked.title = record.checked_at || '';
      row.error.textContent = record.error ? `Error: ${record.error}` : '';
      row.error.hidden = !record.error;
      row.card.dataset.status = view.state;
      updateButtons(row);
    }

    function updateButtons(row) {
      row.save.disabled = row.saving;
      row.check.disabled = !row.enabledValue || row.checking;
      row.check.title = row.enabledValue ? 'Check the engine connection now' : 'This engine is disabled';
    }

    function setStatus(message, isError = false) {
      status.textContent = message;
      status.classList.toggle('error', isError);
    }

    async function request(path, method = 'GET', body) {
      const response = await root.fetch(path, {
        method,
        headers: body ? { 'Content-Type': 'application/json' } : {},
        body: body ? JSON.stringify(body) : undefined,
      });
      const data = await response.json().catch(() => ({}));
      if (!response.ok) throw new Error(data.error || response.statusText || `Request failed (${response.status})`);
      return data;
    }

    async function refresh() {
      if (!mounted || !dialog.open || root.document.hidden || loading) return;
      loading = true;
      try {
        const data = await request('/api/admin/engines');
        if (!mounted || !dialog.open || root.document.hidden) return;
        const byId = new Map((Array.isArray(data.engines) ? data.engines : []).map((record) => [record.id, record]));
        for (const [id, row] of rows) {
          const record = byId.get(id);
          if (record) applyRecord(row, record);
        }
        setStatus('Engine settings refreshed.');
      } catch (error) {
        if (mounted && dialog.open && !root.document.hidden) setStatus(`Could not load engine settings: ${error.message}`, true);
      } finally {
        loading = false;
      }
    }

    async function saveRow(row) {
      if (row.saving) return;
      if (!row.apiUrl.checkValidity() || (row.dashboardUrl.value && !row.dashboardUrl.checkValidity()) ||
          (row.prefectWorkerApiUrl?.value && !row.prefectWorkerApiUrl.checkValidity())) {
        const invalid = !row.apiUrl.checkValidity() ? row.apiUrl
          : row.dashboardUrl.value && !row.dashboardUrl.checkValidity() ? row.dashboardUrl : row.prefectWorkerApiUrl;
        invalid.reportValidity();
        return;
      }
      row.saving = true;
      updateButtons(row);
      try {
        const settings = { api_url: row.apiUrl.value.trim(), dashboard_url: row.dashboardUrl.value.trim() };
        if (row.workerGrpcHostPort) {
          settings.hatchet_worker_grpc_host_port = row.workerGrpcHostPort.value.trim();
          settings.hatchet_worker_tls_strategy = row.hatchetTlsStrategy.value;
        }
        if (row.prefectWorkerApiUrl) settings.prefect_worker_api_url = row.prefectWorkerApiUrl.value.trim();
        const record = await request(`/api/admin/engines/${encodeURIComponent(row.id)}`, 'PUT', settings);
        if (!mounted) return;
        applyRecord(row, record, { saved: true });
        setStatus(`${row.label} settings saved.`);
      } finally {
        row.saving = false;
        updateButtons(row);
      }
    }

    async function checkRow(row) {
      if (!row.enabledValue || row.checking) return;
      row.checking = true;
      updateButtons(row);
      try {
        const record = await request(`/api/admin/engines/${encodeURIComponent(row.id)}/check`, 'POST');
        if (!mounted) return;
        applyRecord(row, record);
        setStatus(`${row.label} check complete.`);
      } catch (error) {
        if (mounted) setStatus(`Could not check ${row.label}: ${error.message}`, true);
      } finally {
        row.checking = false;
        updateButtons(row);
      }
    }

    function stop() {
      if (timer !== null) root.clearInterval(timer);
      timer = null;
    }

    function start() {
      stop();
      if (!mounted || !dialog.open || root.document.hidden) return;
      refresh();
      timer = root.setInterval(refresh, Math.max(100, config.pollMs));
    }

    function visibilityChanged() {
      if (root.document.hidden) stop();
      else if (dialog.open) start();
    }
    root.document.addEventListener('visibilitychange', visibilityChanged);
    dialog.addEventListener('close', stop);

    return {
      open() { start(); },
      refresh,
      stop() { stop(); },
      destroy() {
        stop();
        mounted = false;
        root.document.removeEventListener('visibilitychange', visibilityChanged);
      },
    };
  }

  const api = { ENGINES, config, statusView, checkedText, mount };
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  else root.Admin = api;
})(typeof window !== 'undefined' ? window : globalThis);
