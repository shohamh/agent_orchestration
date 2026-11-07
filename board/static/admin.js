/* Admin settings for the board's workflow engines. Only URL settings are editable here; credentials
 * stay in the server environment and are never requested or rendered by this page. */
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

    const status = el('p', { class: 'ad-status', role: 'status', 'aria-live': 'polite' }, 'Loading engine settings…');
    const list = el('div', { class: 'ad-list', 'aria-label': 'Workflow engines' });
    const heading = el('div', { class: 'ad-heading' },
      el('h2', { id: 'ad-title', class: 'ad-title' }, 'Engine settings'),
      el('p', { class: 'ad-hint' }, 'Configure service and dashboard URLs. Credentials remain in the board server environment.'));
    const reload = el('button', { type: 'button', class: 'secondary', onclick: () => refresh() }, 'Refresh');
    const close = el('button', { type: 'button', class: 'close', 'aria-label': 'Close Admin', onclick: () => onClose ? onClose() : dialog.close() }, '×');
    const top = el('header', { class: 'ad-top' }, heading, el('div', { class: 'ad-actions' }, reload, close), status);

    function createRow(engine) {
      const state = el('span', { class: 'ad-state', 'data-status': 'unknown' }, 'Loading…');
      const enabled = el('span', { class: 'ad-enabled' }, '');
      const apiUrl = el('input', { type: 'url', required: 'required', autocomplete: 'url', spellcheck: 'false', 'aria-label': `${engine.label} API URL`, placeholder: 'https://engine.example.com' });
      const dashboardUrl = el('input', { type: 'url', autocomplete: 'url', spellcheck: 'false', 'aria-label': `${engine.label} dashboard URL`, placeholder: 'https://dashboard.example.com' });
      const apiField = el('label', { class: 'ad-field' }, el('span', {}, 'API URL'), apiUrl);
      const dashboardField = el('label', { class: 'ad-field' }, el('span', {}, 'Browser dashboard URL'), dashboardUrl);
      const checked = el('p', { class: 'ad-checked' }, 'Not checked yet');
      const error = el('p', { class: 'ad-error', hidden: true });
      const save = el('button', { type: 'button', class: 'primary', 'data-action': 'save' }, 'Save URLs');
      const check = el('button', { type: 'button', class: 'secondary', 'data-action': 'check' }, 'Check now');
      const actions = el('div', { class: 'ad-row-actions' }, check, save);
      const card = el('section', { class: 'ad-engine', 'data-engine': engine.id, 'aria-labelledby': `ad-engine-${engine.id}` },
        el('div', { class: 'ad-engine-head' }, el('h3', { id: `ad-engine-${engine.id}`, class: 'ad-engine-title' }, engine.label), state, enabled),
        el('div', { class: 'ad-fields' }, apiField, dashboardField),
        el('div', { class: 'ad-meta' }, checked, error), actions);
      const row = { ...engine, card, state, enabled, apiUrl, dashboardUrl, checked, error, save, check,
        enabledValue: false, dirty: { api: false, dashboard: false }, saving: false, checking: false };
      apiUrl.addEventListener('input', () => { row.dirty.api = true; });
      dashboardUrl.addEventListener('input', () => { row.dirty.dashboard = true; });
      save.addEventListener('click', () => saveRow(row));
      check.addEventListener('click', () => checkRow(row));
      rows.set(engine.id, row);
      return card;
    }

    list.replaceChildren(...ENGINES.map(createRow));
    dialog.replaceChildren(el('div', { class: 'ad' }, top, el('div', { class: 'ad-body' }, list)));

    function applyRecord(row, record, { saved = false } = {}) {
      row.enabledValue = record.enabled === true;
      const view = statusView(record);
      row.state.textContent = view.label;
      row.state.dataset.status = view.state;
      row.enabled.textContent = row.enabledValue ? 'Enabled' : 'Disabled';
      if (saved || !row.dirty.api) row.apiUrl.value = record.api_url || '';
      if (saved || !row.dirty.dashboard) row.dashboardUrl.value = record.dashboard_url || '';
      if (saved) row.dirty = { api: false, dashboard: false };
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
      if (!row.apiUrl.checkValidity() || (row.dashboardUrl.value && !row.dashboardUrl.checkValidity())) {
        (row.apiUrl.checkValidity() ? row.dashboardUrl : row.apiUrl).reportValidity();
        return;
      }
      row.saving = true;
      updateButtons(row);
      try {
        const record = await request(`/api/admin/engines/${encodeURIComponent(row.id)}`, 'PUT', {
          api_url: row.apiUrl.value.trim(),
          dashboard_url: row.dashboardUrl.value.trim(),
        });
        if (!mounted) return;
        applyRecord(row, record, { saved: true });
        setStatus(`${row.label} URLs saved.`);
      } catch (error) {
        if (mounted) setStatus(`Could not save ${row.label}: ${error.message}`, true);
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
