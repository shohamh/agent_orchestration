/* Shared transcript/form model and the dependency-free session drawer. */
(function (root) {
  'use strict';
  function commandMatches(commands, value) {
    const match = /^\/([^\s/]*)$/.exec(value);
    if (!match) return [];
    const prefix = match[1].toLowerCase();
    return (commands || []).filter(command => typeof command.name === 'string' && command.name.toLowerCase().startsWith(prefix));
  }
  function transcript(events) {
    const rows = [], messages = new Map(), tools = new Map(), requests = new Map();
    let commands = null, acpState = null;
    for (const event of events || []) {
      if (event.type === 'commands') commands = { ...event };
      else if (event.type === 'acp_state') acpState = { ...event };
      else if (event.type === 'agent' && event.message_id) {
        if (messages.has(event.message_id)) messages.get(event.message_id).text += event.text || '';
        else { const row = { ...event, text: event.text || '' }; rows.push(row); messages.set(event.message_id, row); }
      } else if (event.type === 'tool') {
        // Agents send a tool's name/summary once and later updates without them (opencode's updates
        // say only "tool"), so an update only replaces the fields it actually carries.
        if (tools.has(event.id)) {
          const row = tools.get(event.id);
          for (const [key, value] of Object.entries(event)) {
            const generic = key === 'name' && (!value || value === 'tool') && row.name;
            if (value != null && value !== '' && !generic) row[key] = value;
          }
        } else { const row = { ...event }; rows.push(row); tools.set(event.id, row); }
      } else if (event.type === 'approval' || event.type === 'question') {
        const row = { ...event }; rows.push(row); requests.set(`${event.type}:${event.id}`, row);
      } else if (event.type === 'approval_resolved' || event.type === 'answer') {
        const row = requests.get(`${event.type === 'answer' ? 'question' : 'approval'}:${event.id}`);
        if (row) row.resolved = event;
      } else if (['user', 'agent', 'error', 'plan'].includes(event.type)) rows.push({ ...event });
    }
    if (commands) rows.unshift(commands);
    if (acpState) rows.unshift(acpState);
    return rows;
  }

  // One-line label for a collapsed tool row: file paths shrink to their file name.
  function toolSummary(summary) {
    const text = String(summary).split('\n')[0];
    const path = text.match(/^[A-Za-z]?:?[\\/]?(?:[^\\/\s]+[\\/])+([^\\/]+)$/);
    const short = path ? path[1] : text;
    return short.length > 60 ? `${short.slice(0, 59)}…` : short;
  }

  // ---- Rich rendering. Everything is built from DOM nodes (never innerHTML), so agent output
  // cannot inject markup; links are limited to http(s).

  function parseJsonish(value) {
    if (value && typeof value === 'object') return value;
    if (typeof value !== 'string') return undefined;
    const text = value.trim();
    if (!/^[[{]/.test(text)) return undefined;
    try { return JSON.parse(text); } catch { return undefined; }
  }

  const JSON_TOKEN = /("(?:\\u[a-fA-F0-9]{4}|\\[^u]|[^\\"])*")(\s*:)?|\b(true|false)\b|\b(null)\b|(-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)/g;
  function jsonView(value, h) {
    const text = JSON.stringify(value, null, 2), nodes = [];
    let last = 0, match;
    JSON_TOKEN.lastIndex = 0;
    while ((match = JSON_TOKEN.exec(text))) {
      if (match.index > last) nodes.push(text.slice(last, match.index));
      const kind = match[1] ? (match[2] ? 'key' : 'string') : match[3] ? 'boolean' : match[4] ? 'null' : 'number';
      nodes.push(h('span', { class: `json-${kind}` }, match[1] || match[0]));
      if (match[2]) nodes.push(match[2]);
      last = JSON_TOKEN.lastIndex;
    }
    if (last < text.length) nodes.push(text.slice(last));
    return h('pre', { class: 'json-view' }, ...nodes);
  }

  // Text that may be JSON: highlighted when it parses, plain otherwise.
  function codeView(value, h) {
    const parsed = parseJsonish(value);
    return parsed !== undefined ? jsonView(parsed, h) : h('pre', {}, typeof value === 'string' ? value : JSON.stringify(value, null, 2));
  }

  // Line diff (LCS) for edit requests; large inputs fall back to old/new blocks.
  function diffView(before, after, h) {
    const a = String(before ?? '').replace(/\r\n/g, '\n').split('\n'), b = String(after ?? '').replace(/\r\n/g, '\n').split('\n');
    const rows = [];
    if (a.length * b.length > 250000) {
      a.forEach(line => rows.push(['del', line])); b.forEach(line => rows.push(['add', line]));
    } else {
      const lcs = Array.from({ length: a.length + 1 }, () => new Uint16Array(b.length + 1));
      for (let i = a.length - 1; i >= 0; i--) for (let j = b.length - 1; j >= 0; j--)
        lcs[i][j] = a[i] === b[j] ? lcs[i + 1][j + 1] + 1 : Math.max(lcs[i + 1][j], lcs[i][j + 1]);
      let i = 0, j = 0;
      while (i < a.length || j < b.length) {
        if (i < a.length && j < b.length && a[i] === b[j]) { rows.push(['ctx', a[i]]); i++; j++; }
        // Removed lines before added ones, as unified diffs show a replaced line.
        else if (i < a.length && (j >= b.length || lcs[i + 1][j] >= lcs[i][j + 1])) rows.push(['del', a[i++]]);
        else rows.push(['add', b[j++]]);
      }
    }
    const sign = { add: '+', del: '-', ctx: ' ' };
    return h('pre', { class: 'diff-view' }, ...rows.map(([kind, line]) => h('div', { class: `diff-${kind}` }, `${sign[kind]} ${line}`)));
  }

  function editOf(input) {
    if (!input || typeof input !== 'object') return null;
    const before = input.oldText ?? input.old_string ?? input.oldString, after = input.newText ?? input.new_string ?? input.newString;
    return typeof before === 'string' && typeof after === 'string' ? { path: input.path || input.filePath || input.file_path, before, after } : null;
  }

  // The parameter that says what a tool call is about: shown on its collapsed row.
  const PREVIEW_KEYS = ['filePath', 'file_path', 'path', 'command', 'cmd', 'pattern', 'query', 'url', 'glob', 'description', 'prompt'];
  function toolPreview(event) {
    const name = String(event.name || '').toLowerCase();
    if (event.summary && String(event.summary).toLowerCase() !== name) return toolSummary(event.summary);
    const input = parseJsonish(event.detail);
    if (input && typeof input === 'object') {
      for (const key of PREVIEW_KEYS) {
        const value = input[key];
        if (typeof value === 'string' && value.trim()) return toolSummary(value);
        if (Array.isArray(value) && value.length) return toolSummary(value.join(' '));
      }
    }
    return '';
  }

  // Small Markdown subset for agent prose: code fences, headings, lists, paragraphs,
  // `code`, **bold**, *italic* and [links](https://...).
  const INLINE = /(`[^`\n]+`)|(\*\*[^*\n]+\*\*)|(\[[^\]\n]+\]\((https?:\/\/[^)\s]+)\))|(\*[^*\s][^*\n]*\*)/g;
  function inlineMarkdown(text, h) {
    const nodes = [];
    let last = 0, match;
    INLINE.lastIndex = 0;
    while ((match = INLINE.exec(text))) {
      if (match.index > last) nodes.push(text.slice(last, match.index));
      if (match[1]) nodes.push(h('code', { class: 'md-code' }, match[1].slice(1, -1)));
      else if (match[2]) nodes.push(h('strong', {}, match[2].slice(2, -2)));
      else if (match[3]) nodes.push(h('a', { href: match[4], target: '_blank', rel: 'noopener noreferrer' }, match[3].slice(1, match[3].indexOf(']('))));
      else nodes.push(h('em', {}, match[5].slice(1, -1)));
      last = INLINE.lastIndex;
    }
    if (last < text.length) nodes.push(text.slice(last));
    return nodes;
  }

  function markdown(text, h) {
    const lines = String(text || '').replace(/\r\n/g, '\n').split('\n'), out = [];
    const isItem = line => /^\s*([-*+]|\d+[.)])\s+/.test(line);
    for (let i = 0; i < lines.length;) {
      const line = lines[i];
      const fence = line.match(/^\s*```\s*([\w+-]*)\s*$/);
      if (fence) {
        const body = [];
        for (i++; i < lines.length && !/^\s*```\s*$/.test(lines[i]); i++) body.push(lines[i]);
        i++;
        out.push(fence[1] === 'diff' ? h('pre', { class: 'diff-view' }, ...body.map(l => h('div', { class: l.startsWith('+') ? 'diff-add' : l.startsWith('-') ? 'diff-del' : 'diff-ctx' }, l))) : codeView(body.join('\n'), h));
      } else if (!line.trim()) i++;
      else if (/^#{1,4}\s/.test(line)) { out.push(h('div', { class: 'md-heading' }, ...inlineMarkdown(line.replace(/^#{1,4}\s+/, ''), h))); i++; }
      else if (isItem(line)) {
        const ordered = /^\s*\d/.test(line), items = [];
        for (; i < lines.length && isItem(lines[i]); i++) items.push(h('li', {}, ...inlineMarkdown(lines[i].replace(/^\s*([-*+]|\d+[.)])\s+/, ''), h)));
        out.push(h(ordered ? 'ol' : 'ul', { class: 'md-list' }, ...items));
      } else {
        const para = [];
        for (; i < lines.length && lines[i].trim() && !/^\s*```/.test(lines[i]) && !/^#{1,4}\s/.test(lines[i]) && !isItem(lines[i]); i++) para.push(lines[i]);
        out.push(h('p', { class: 'md-p' }, ...inlineMarkdown(para.join('\n'), h)));
      }
    }
    return out;
  }

  // Engines report UTC ISO times (Hatchet and Prefect with microseconds); show them in the viewer's
  // own timezone and locale. Some browsers reject more than 3 fractional digits, and an ISO time
  // without an offset would otherwise be read as local time.
  function localTime(iso, { locale, timeZone } = {}) {
    if (!iso) return '';
    const text = String(iso).replace(/(\.\d{3})\d+/, '$1');
    const date = new Date(/[zZ]|[+-]\d\d:?\d\d$/.test(text) ? text : `${text}Z`);
    if (Number.isNaN(date.getTime())) return String(iso);
    return date.toLocaleString(locale, { timeZone, year: 'numeric', month: 'short', day: 'numeric',
      hour: '2-digit', minute: '2-digit', second: '2-digit', timeZoneName: 'short' });
  }
  function choices(schema = {}) {
    const variants = schema.oneOf || schema.anyOf;
    if (variants) return variants.filter(x => Object.hasOwn(x, 'const')).map(x => ({ value: x.const, label: x.title || String(x.const), description: x.description || '' }));
    return (schema.enum || []).map(value => ({ value, label: String(value), description: '' }));
  }

  function answers(schema, values) {
    const result = {};
    for (const [key, property] of Object.entries(schema.properties || {})) {
      let value = values[key];
      const label = property.title || key, required = (schema.required || []).includes(key);
      if (value === undefined || value === '') {
        if (required) throw new Error(`${label} is required.`);
        continue;
      }
      const type = property.type;
      if (type === 'integer' || type === 'number') {
        value = Number(value);
        if (!Number.isFinite(value) || (type === 'integer' && !Number.isInteger(value))) throw new Error(`${label} needs a ${type === 'integer' ? 'whole number' : 'number'}.`);
        if (property.minimum != null && value < property.minimum) throw new Error(`${label} must be at least ${property.minimum}.`);
        if (property.maximum != null && value > property.maximum) throw new Error(`${label} must be at most ${property.maximum}.`);
      } else if ((type === 'object' || type === 'array') && typeof value === 'string') {
        try { value = JSON.parse(value); } catch { throw new Error(`${label} needs valid JSON.`); }
      }
      if (type === 'boolean' && typeof value !== 'boolean') throw new Error(`${label} needs Yes or No.`);
      if (type === 'string' && typeof value !== 'string') throw new Error(`${label} needs text.`);
      if (type === 'object' && (value === null || Array.isArray(value) || typeof value !== 'object')) throw new Error(`${label} needs a JSON object.`);
      if (type === 'array') {
        if (!Array.isArray(value)) throw new Error(`${label} needs a list.`);
        if (property.minItems != null && value.length < property.minItems) throw new Error(`${label}: select at least ${property.minItems}.`);
        if (property.maxItems != null && value.length > property.maxItems) throw new Error(`${label}: select at most ${property.maxItems}.`);
        const options = choices(property.items);
        if (options.length && value.some(item => !options.some(option => JSON.stringify(option.value) === JSON.stringify(item)))) throw new Error(`${label} contains an invalid choice.`);
      }
      const options = choices(property);
      if (options.length && !options.some(option => JSON.stringify(option.value) === JSON.stringify(value))) throw new Error(`${label}: select an option.`);
      if (typeof value === 'string' && property.minLength != null && value.length < property.minLength) throw new Error(`${label} is too short.`);
      result[key] = value;
    }
    return result;
  }

  const STATES = {
    starting: ['Starting session', 'Connecting to the agent in its workspace.'],
    working: ['Agent is working', 'Send guidance now; it will be queued for the next turn.'],
    retrying: ['Retrying', 'The agent is recovering from an error.'],
    approval: ['Permission needed', 'Review the request below to let the agent continue.'],
    question: ['Your answer is needed', 'Answer the question below to continue this turn.'],
    awaiting_review: ['Ready for your review', 'Continue the conversation, or mark this session done.'],
    awaiting_input: ['Waiting for your direction', 'The turn was interrupted. Send a message to pick up from here.'],
    stuck: ['Session needs a hand', 'The retry limit was reached. Continue to try again, or send a new direction.'],
    done: ['Session complete', 'You marked this session done.'],
    stopped: ['Session stopped', 'The run ended. Start a new session to continue.'],
  };
  function stateView(card) {
    // Its worker can't be reached: nothing here would get through, so don't pretend the session is live.
    if (card.offline_worker && !['done', 'stopped'].includes(card.column)) {
      return { state: 'offline', title: `${card.offline_worker} is offline`, hint: "This session can't respond until that PC reconnects.", terminal: true, offline: true, queued: false, actions: [] };
    }
    const state = card.column === 'stopped' ? 'stopped' : card.column === 'done' ? 'done' : card.session_state || 'starting';
    const [title, hint] = STATES[state] || [state.replaceAll('_', ' '), ''];
    const terminal = ['done', 'stopped'].includes(state);
    const queued = ['starting', 'working', 'retrying', 'approval', 'question'].includes(state);
    const actions = terminal ? [] : queued ? ['interrupt'] : ['continue', 'done'];
    return { state, title, hint: card.error || hint, terminal, queued, actions };
  }

  let commandMenuId = 0;
  function mount(container, config) {
    const h = config.el;
    const sessions = new Map();
    let currentKey = null, card = null, view = null, signature = null;
    const banner = h('div', { class: 'session-banner', role: 'status' });
    const log = h('div', { class: 'chat-log', role: 'log', 'aria-label': 'Session conversation', tabindex: '0' });
    const error = h('div', { class: 'chat-error', role: 'alert', hidden: '' });
    const menuId = `slash-commands-${++commandMenuId}`;
    const text = h('textarea', { rows: '1', placeholder: 'Give the agent a direction…', 'aria-label': 'Message to agent', 'data-chat-focus': 'composer', role: 'combobox', 'aria-autocomplete': 'list', 'aria-controls': menuId, 'aria-expanded': 'false' });
    const SVG_NS = 'http://www.w3.org/2000/svg';
    const icon = (d) => {
      const svg = document.createElementNS(SVG_NS, 'svg'), path = document.createElementNS(SVG_NS, 'path');
      for (const [k, v] of Object.entries({ width: 16, height: 16, viewBox: '0 0 16 16', fill: 'none', stroke: 'currentColor', 'stroke-width': 1.6, 'stroke-linecap': 'round', 'stroke-linejoin': 'round', 'aria-hidden': 'true' })) svg.setAttribute(k, v);
      path.setAttribute('d', d); svg.append(path);
      return svg;
    };
    const ICONS = { attach: 'M8 3v10M3 8h10', send: 'M8 13V3M3.500 7.500 8 3l4.500 4.500', interrupt: 'M4.500 4.500h7v7h-7z', continue: 'M5 3.500v9l7-4.500z', done: 'M3 8.500 6.500 12 13 4.500', info: 'M8 7.500V11M8 5v.01M14 8A6 6 0 1 1 2 8a6 6 0 0 1 12 0Z' };
    const send = h('button', { type: 'submit', class: 'primary composer-send', 'aria-label': 'Send message', title: 'Send · Ctrl+Enter' }, icon(ICONS.send));
    const actions = h('div', { class: 'session-actions' });
    const settings = h('div', { class: 'agent-settings', 'aria-label': 'Agent settings' });
    const attachmentInput = h('input', { type: 'file', multiple: '', 'aria-label': 'Attach images or files' });
    const attachments = h('div', { class: 'chat-attachments' });
    const attachLabel = h('label', { class: 'composer-icon composer-attach', title: 'Attach images or files · up to 5, 96 KiB total' }, icon(ICONS.attach), attachmentInput);
    const meta = h('div', { class: 'composer-meta' });
    const suggestions = h('div', { id: menuId, class: 'command-suggestions', role: 'listbox', 'aria-label': 'Slash commands', hidden: '' });
    let selected = 0, matches = [], dismissed = false;
    const form = h('form', { class: 'chat-composer', onsubmit: async event => {
      event.preventDefault();
      const value = text.value.trim(), key = currentKey, session = sessions.get(key), files = session.files;
      if ((!value && !files.length) || !view || view.terminal || session.sending) return;
      try {
        const encoded = await Promise.all(files.map(file => new Promise((resolve, reject) => {
          const reader = new FileReader(); reader.onerror = () => reject(new Error(`Could not read ${file.name}`));
          reader.onload = () => resolve({ name: file.name, mimeType: file.type || 'application/octet-stream', data: String(reader.result).split(',')[1] });
          reader.readAsDataURL(file);
        })));
        if (key !== currentKey) return;
        const message = encoded.length ? { type: 'message', text: value, attachments: encoded } : { type: 'message', text: value };
        if (await post(message) && key === currentKey && text.value.trim() === value) { text.value = ''; session.draft = ''; session.files = []; attachmentInput.value = ''; renderAttachments(); renderCommands(); fit(); }
      } catch (failure) { error.textContent = failure.message; error.hidden = false; }
    } }, suggestions, error, h('div', { class: 'composer-box' }, attachments, text, h('div', { class: 'composer-bar' }, settings, h('span', { class: 'composer-gap' }), meta, actions, send)));
    // Grow with the text (up to the CSS max-height) instead of reserving rows on small screens.
    const fit = () => { text.style.height = 'auto'; text.style.height = `${text.scrollHeight + 2}px`; };
    function chooseCommand(command) {
      text.value = `/${command.name} `; sessions.get(currentKey).draft = text.value;
      dismissed = true; renderCommands(); fit(); text.focus();
    }
    function renderCommands() {
      const session = currentKey && sessions.get(currentKey);
      matches = !dismissed && session && !view?.terminal ? commandMatches(session.commands, text.value) : [];
      selected = Math.min(selected, Math.max(0, matches.length - 1));
      suggestions.hidden = !matches.length;
      text.setAttribute('aria-expanded', String(!!matches.length));
      if (matches.length) text.setAttribute('aria-activedescendant', `${menuId}-${selected}`);
      else text.removeAttribute('aria-activedescendant');
      suggestions.replaceChildren(...matches.map((command, index) => h('button', {
        id: `${menuId}-${index}`, type: 'button', role: 'option', 'aria-selected': String(index === selected),
        onmousedown: event => event.preventDefault(), onclick: () => chooseCommand(command),
      }, h('strong', {}, `/${command.name}`), command.input?.hint ? h('span', { class: 'command-hint' }, command.input.hint) : null,
      h('small', {}, command.description || ''))));
    }
    text.addEventListener('input', () => { fit(); dismissed = false; selected = 0; if (currentKey) sessions.get(currentKey).draft = text.value; renderCommands(); });
    // When the keyboard opens, keep the latest messages in view above the composer.
    text.addEventListener('focus', () => setTimeout(() => { log.scrollTop = log.scrollHeight; }, 300));
    text.addEventListener('keydown', event => {
      if (matches.length && !event.ctrlKey && !event.metaKey && !event.shiftKey) {
        if (event.key === 'ArrowDown' || event.key === 'ArrowUp') { event.preventDefault(); selected = (selected + (event.key === 'ArrowDown' ? 1 : -1) + matches.length) % matches.length; renderCommands(); return; }
        if (event.key === 'Enter' || event.key === 'Tab') { event.preventDefault(); chooseCommand(matches[selected]); return; }
        if (event.key === 'Escape') { event.preventDefault(); dismissed = true; renderCommands(); return; }
      }
      if (event.key === 'Enter' && (event.ctrlKey || event.metaKey)) { event.preventDefault(); form.requestSubmit(); }
    });
    container.append(banner, log, form);
    // Scrolling near the top of the conversation loads the previous page of rows.
    log.addEventListener('scroll', () => { if (log.scrollTop < 120) loadOlder(); });

    function newSession() {
      return { draft: '', files: [], commands: [], acpState: {}, scroll: 0, seen: false, answers: {}, expanded: new Set(), details: {}, pending: new Set(), sending: false,
        rows: [], loaded: false, more: false, truncated: false, loading: false, olderError: null, version: 0 };
    }

    async function loadOlder() {
      const key = currentKey, session = key && sessions.get(key);
      if (!session || !session.loaded || !session.more || session.loading || !session.rows.length || !config.loadOlder) return;
      session.loading = true; session.olderError = null; renderRows();
      const before = session.rows[0].key;
      try {
        const page = await config.loadOlder(before);
        if (key !== currentKey || session.rows[0]?.key !== before) return;
        const known = new Set(session.rows.map(row => row.key));
        // Keep the rows on screen where they are while older ones are added above them.
        const height = log.scrollHeight, top = log.scrollTop;
        session.rows = [...page.rows.filter(row => !known.has(row.key)), ...session.rows];
        session.more = page.more; session.loading = false; session.version++;
        renderRows();
        log.scrollTop = top + (log.scrollHeight - height);
        session.scroll = log.scrollTop;
      } catch (failure) {
        session.olderError = failure.message;
      } finally {
        session.loading = false;
        if (key === currentKey) { const height = log.scrollHeight, top = log.scrollTop; renderRows(); log.scrollTop = top + (log.scrollHeight - height); }
      }
    }

    function olderIndicator(session) {
      if (session.loading) return h('div', { class: 'chat-older', role: 'status' }, h('span', { class: 'spinner', 'aria-hidden': 'true' }), 'Loading earlier messages…');
      if (session.olderError) return h('div', { class: 'chat-older' }, `Couldn't load earlier messages: ${session.olderError}`, h('button', { type: 'button', class: 'secondary', onclick: loadOlder }, 'Retry'));
      if (session.more) return h('div', { class: 'chat-older' }, h('button', { type: 'button', class: 'secondary', onclick: loadOlder }, 'Load earlier messages'));
      if (session.truncated && session.rows.length) return h('div', { class: 'chat-older' }, 'Earlier messages are no longer kept by this backend.');
      return null;
    }

    async function post(input, requestKey) {
      const key = currentKey, session = sessions.get(key);
      if (session.sending) return false;
      session.sending = true; send.disabled = true; error.hidden = true;
      actions.querySelectorAll('button').forEach(button => { button.disabled = true; });
      if (requestKey) { session.pending.add(requestKey); renderRows(); }
      try {
        await config.input(input);
        return true;
      } catch (failure) {
        if (key === currentKey) { error.textContent = failure.message; error.hidden = false; }
        return false;
      } finally {
        session.sending = false;
        if (requestKey) session.pending.delete(requestKey);
        if (key === currentKey) { send.disabled = view.terminal; actions.querySelectorAll('button').forEach(button => { button.disabled = view.terminal; }); renderRows(); }
      }
    }

    function renderSettings(session) {
      const state = session.acpState || {}, controls = [], marks = [];
      const options = state.configOptions || [];
      for (const option of options) {
        if (!option.id || !['select', 'boolean'].includes(option.type)) continue;
        const select = h('select', { 'aria-label': option.name || option.id });
        const values = option.type === 'boolean' ? [{ value: 'true', name: 'On' }, { value: 'false', name: 'Off' }]
          : (option.options || []).flatMap(item => item.options || [item]);
        for (const item of values) select.append(h('option', { value: String(item.value) }, item.name || String(item.value)));
        select.value = String(option.currentValue); select.disabled = view.terminal || session.sending;
        select.addEventListener('change', () => post({ type: 'config', id: option.id, value: option.type === 'boolean' ? select.value === 'true' : select.value }));
        select.title = option.name || option.id;
        controls.push(select);
      }
      if (!options.some(option => option.category === 'mode') && state.modes?.availableModes?.length) {
        const select = h('select', { 'aria-label': 'Agent mode' });
        for (const mode of state.modes.availableModes) select.append(h('option', { value: mode.id }, mode.name || mode.id));
        select.value = state.modes.currentModeId; select.disabled = view.terminal || session.sending;
        select.addEventListener('change', () => post({ type: 'mode', id: select.value }));
        select.title = 'Agent mode';
        controls.push(select);
      }
      if (state.usage && state.usage.size > 0) {
        const usage = state.usage, percentage = Math.min(100, Math.round(100 * usage.used / usage.size));
        // A small ring that fills as the context does; the numbers are on hover.
        const ring = document.createElementNS(SVG_NS, 'svg');
        ring.setAttribute('viewBox', '0 0 16 16'); ring.setAttribute('width', 16); ring.setAttribute('height', 16); ring.setAttribute('aria-hidden', 'true');
        for (const [cls, dash] of [['ring-track', '100 100'], ['ring-used', `${percentage} 100`]]) {
          const circle = document.createElementNS(SVG_NS, 'circle');
          for (const [k, v] of Object.entries({ class: cls, cx: 8, cy: 8, r: 6, pathLength: 100, 'stroke-dasharray': dash, transform: 'rotate(-90 8 8)' })) circle.setAttribute(k, v);
          ring.append(circle);
        }
        const cost = usage.cost ? ` · ${usage.cost.amount} ${usage.cost.currency}` : '';
        marks.push(h('span', { class: `context-usage${percentage >= 80 ? ' high' : ''}`, title: `Context ${percentage}% used\n${usage.used.toLocaleString()} of ${usage.size.toLocaleString()} tokens${cost}` }, h('span', { class: 'sr-only' }, `Context ${percentage}%`), ring));
      }
      if (state.capabilities?.loadSession && state.sessionId) {
        marks.push(h('button', { type: 'button', class: 'composer-icon', 'aria-label': 'Copy session ID', title: `Session ${state.sessionId}\nUse this ID with the same agent and workspace to resume. Click to copy.`,
          onclick: () => navigator.clipboard?.writeText(state.sessionId) }, icon(ICONS.info)));
      }
      const promptCaps = state.capabilities?.promptCapabilities || {};
      if (promptCaps.image || promptCaps.embeddedContext) {
        attachmentInput.accept = promptCaps.embeddedContext ? '' : 'image/*';
        attachmentInput.disabled = view.terminal || session.sending;
        attachLabel.classList.toggle('disabled', attachmentInput.disabled);
        controls.unshift(attachLabel);
      }
      settings.replaceChildren(...controls);
      meta.replaceChildren(...marks);
    }

    function renderAttachments() {
      const session = currentKey && sessions.get(currentKey);
      attachments.replaceChildren(...(session?.files || []).map((file, i) => h('span', { class: 'attachment-chip' }, file.name,
        h('button', { type: 'button', 'aria-label': `Remove ${file.name}`, onclick: () => { session.files.splice(i, 1); renderAttachments(); } }, '×'))));
    }
    attachmentInput.addEventListener('change', () => {
      const session = sessions.get(currentKey), files = [...attachmentInput.files], caps = session.acpState.capabilities?.promptCapabilities || {};
      if (files.length > 5 || files.reduce((sum, file) => sum + file.size, 0) > 96 * 1024 || files.some(file => file.type.startsWith('image/') ? !caps.image : !caps.embeddedContext)) {
        error.textContent = 'Choose up to five supported files, with a combined size of at most 96 KiB.'; error.hidden = false;
      } else { session.files = files; error.hidden = true; renderAttachments(); }
      attachmentInput.value = '';
    });

    function questionSchema(event) {
      if (event.schema) return event.schema;
      return { type: 'object', required: (event.questions || []).map(q => q.id), properties: Object.fromEntries((event.questions || []).map(q => [q.id, {
        title: q.header || q.question, description: q.question, type: q.multi ? 'array' : 'string',
        ...(q.multi ? { items: { oneOf: (q.options || []).map(x => ({ const: x.label, title: x.label, description: x.description })) } } : { oneOf: (q.options || []).map(x => ({ const: x.label, title: x.label, description: x.description })) }),
      }])) };
    }

    function renderQuestion(event, session) {
      const schema = questionSchema(event), requestKey = `question:${event.id}`;
      const resolved = event.resolved;
      const disabled = !!resolved || view.terminal || session.pending.has(requestKey);
      const draft = session.answers[event.id] ||= { ...(event.resolved?.answers || {}) };
      const fields = h('fieldset', { class: 'question-fields' }); fields.disabled = disabled;
      for (const [key, property] of Object.entries(schema.properties || {})) {
        const isArray = property.type === 'array', options = choices(isArray ? property.items : property);
        const field = h('div', { class: 'question-field' });
        const title = property.title || key;
        field.append(h('div', { class: 'question-label' }, title, (schema.required || []).includes(key) ? h('span', { class: 'required-mark', 'aria-label': 'required' }, ' *') : null));
        if (property.description && property.description !== title) field.append(h('p', { class: 'field-description' }, property.description));
        if (options.length) {
          const optionGroup = h('div', { class: 'question-options', role: 'group', 'aria-label': title });
          options.forEach((option, index) => {
            const input = h('input', { type: isArray ? 'checkbox' : 'radio', name: `${currentKey}:${event.id}:${key}`, 'data-chat-focus': `${event.id}:${key}:${index}` });
            input.checked = isArray ? (draft[key] || []).some(x => JSON.stringify(x) === JSON.stringify(option.value)) : JSON.stringify(draft[key]) === JSON.stringify(option.value);
            input.addEventListener('change', () => {
              if (isArray) { const selected = draft[key] || []; draft[key] = input.checked ? [...selected, option.value] : selected.filter(x => JSON.stringify(x) !== JSON.stringify(option.value)); }
              else draft[key] = option.value;
            });
            optionGroup.append(h('label', { class: 'question-option' }, input, h('span', {}, option.label, option.description ? h('small', {}, option.description) : null)));
          });
          field.append(optionGroup);
          if (isArray && draft[key] === undefined) draft[key] = [];
        } else if (property.type === 'boolean') {
          const input = h('input', { type: 'checkbox', 'data-chat-focus': `${event.id}:${key}` });
          input.checked = draft[key] ?? property.default ?? false;
          draft[key] = input.checked;
          input.addEventListener('change', () => { draft[key] = input.checked; });
          field.append(h('label', { class: 'question-option' }, input, 'Yes'));
        } else {
          const structured = ['array', 'object'].includes(property.type);
          const input = h(structured || property.format === 'textarea' ? 'textarea' : 'input', {
            ...(structured ? { rows: '3', placeholder: property.type === 'array' ? '[…]' : '{…}' } : { type: ['number', 'integer'].includes(property.type) ? 'number' : 'text' }),
            'aria-label': title, 'data-chat-focus': `${event.id}:${key}`,
          });
          input.value = draft[key] ?? (property.default == null ? '' : typeof property.default === 'object' ? JSON.stringify(property.default) : property.default);
          draft[key] = input.value;
          if (property.type === 'integer') input.step = '1';
          if (property.minimum != null) input.min = property.minimum;
          if (property.maximum != null) input.max = property.maximum;
          input.addEventListener('input', () => { draft[key] = input.value; });
          field.append(input);
        }
        fields.append(field);
      }
      const validation = h('div', { class: 'chat-error', role: 'alert', hidden: '' });
      const submit = h('button', { type: 'submit', class: 'primary' }, session.pending.has(requestKey) ? 'Waiting for agent…' : 'Submit answer'); submit.disabled = disabled;
      const question = h('form', { class: 'request-card question-card', onsubmit: eventSubmit => {
        eventSubmit.preventDefault();
        try { const response = answers(schema, draft); validation.hidden = true; post({ type: 'answer', id: event.id, answers: response }, requestKey); }
        catch (failure) { validation.textContent = failure.message; validation.hidden = false; }
      } }, h('div', { class: 'request-heading' }, h('span', { class: 'request-icon', 'aria-hidden': 'true' }, '?'), h('strong', {}, resolved ? 'Question answered' : 'The agent has a question')),
      event.message ? h('p', { class: 'request-summary' }, event.message) : null, fields, validation,
      resolved ? h('div', { class: 'request-resolution' }, event.resolved?.cancelled ? 'Cancelled · the turn was interrupted' : 'Answer sent') : submit);
      return question;
    }

    function renderRows() {
      if (!card || !currentKey) return;
      const session = sessions.get(currentKey);
      const atBottom = log.scrollTop + log.clientHeight >= log.scrollHeight - 50;
      const top = log.scrollTop, active = document.activeElement;
      const focus = log.contains(active) ? { key: active.dataset.chatFocus, start: active.selectionStart, end: active.selectionEnd } : null;
      const rows = session.rows.filter(row => row.type !== 'commands');
      log.replaceChildren(...(rows.length ? [olderIndicator(session), ...rows.map((event, index) => {
        if (event.type === 'user' || event.type === 'agent') return h('article', { class: `chat-message ${event.type}` },
          h('div', { class: 'message-label' }, event.type === 'user' ? 'You' : card.agent === 'demo' ? 'Demo agent · simulated' : card.agent || 'Agent', event.queued ? h('span', { class: 'queued-label' }, 'Queued for next turn') : null),
          event.type === 'agent' ? h('div', { class: 'message-text markdown' }, ...markdown(event.text || '…', h)) : h('div', { class: 'message-text' }, event.text || '',
            ...(event.attachments || []).map(file => h('div', { class: 'message-attachment' }, `📎 ${file.name}`))));
        if (event.type === 'tool') {
          const details = h('details', { class: `tool-row tool-${event.status || 'running'}` });
          details.open = session.expanded.has(event.id);
          details.addEventListener('toggle', () => { if (details.isConnected) { if (details.open) session.expanded.add(event.id); else session.expanded.delete(event.id); } });
          const preview = toolPreview(event), input = parseJsonish(event.detail), edit = editOf(input);
          details.append(...[h('summary', { 'data-chat-focus': `tool:${event.id}` }, h('span', { class: 'tool-mark', 'aria-hidden': 'true' }, event.status === 'ok' ? '✓' : event.status === 'error' ? '!' : '↻'), h('span', { class: 'tool-name' }, event.name || 'Tool'), preview ? h('span', { class: 'tool-summary' }, preview) : null, h('span', { class: 'tool-status' }, event.status || 'running')),
            event.summary && String(event.summary).toLowerCase() !== String(event.name || '').toLowerCase() && String(event.summary) !== preview ? h('p', { class: 'tool-full-summary' }, event.summary) : null,
            edit ? diffView(edit.before, edit.after, h) : event.detail ? h('div', { class: 'tool-section' }, h('div', { class: 'tool-section-label' }, 'Input'), codeView(event.detail, h)) : null,
            event.output ? h('div', { class: 'tool-section' }, h('div', { class: 'tool-section-label' }, 'Output'), codeView(event.output, h)) : null,
            ...(event.content || []).map(item => item.type === 'diff'
              ? h('div', { class: 'tool-section' }, h('div', { class: 'tool-section-label' }, item.path || 'Changes'), diffView(item.oldText || '', item.newText || '', h))
              : item.type === 'terminal' ? h('div', { class: 'tool-section' }, h('div', { class: 'tool-section-label' }, 'Terminal output'),
                  codeView(session.rows.find(row => row.id === `terminal:${item.terminalId}`)?.output || 'Waiting for output…', h))
              : item.content?.type === 'text' ? h('div', { class: 'tool-section' }, codeView(item.content.text, h)) : null)].filter(Boolean));
          return details;
        }
        if (event.type === 'question') return renderQuestion(event, session);
        if (event.type === 'approval') {
          const requestKey = `approval:${event.id}`, resolved = event.resolved;
          const controls = h('div', { class: 'approval-actions' });
          for (const option of event.options || []) {
            const button = h('button', { type: 'button', class: option.kind?.startsWith('reject') ? 'danger' : option.kind === 'allow_once' ? 'primary' : 'secondary', onclick: () => post({ type: 'approval', id: event.id, optionId: option.id }, requestKey) }, option.label);
            button.disabled = !!resolved || view.terminal || session.pending.has(requestKey); controls.append(button);
          }
          let detail = null;
          const requested = parseJsonish(event.detail), edit = editOf(requested);
          if (edit && !resolved) {
            // An edit is what is being approved: show the change itself, not its JSON.
            detail = diffView(edit.before, edit.after, h);
          } else if (event.detail) {
            detail = h('details', { class: 'permission-detail' }, h('summary', { 'data-chat-focus': `detail:${event.id}` }, edit ? 'View change' : 'View request details'),
              edit ? diffView(edit.before, edit.after, h) : codeView(event.detail, h));
            detail.open = !!session.details[event.id];
            detail.addEventListener('toggle', () => { if (detail.isConnected) session.details[event.id] = detail.open; });
          }
          const chosen = (event.options || []).find(option => option.id === resolved?.optionId);
          const outcome = resolved?.decision === 'cancelled' ? 'Cancelled · the turn was interrupted'
            : chosen ? `${chosen.kind?.startsWith('reject') ? '✕' : '✓'} ${chosen.label}` : resolved?.decision || 'Response sent';
          return h('article', { class: `request-card approval-card${resolved ? ' resolved' : ''}` }, h('div', { class: 'request-heading' }, h('span', { class: 'request-icon', 'aria-hidden': 'true' }, '◇'), h('strong', {}, resolved ? 'Permission' : 'Permission to proceed')),
            h('p', { class: 'request-summary' }, event.summary || event.tool || 'The agent is requesting permission.'),
            detail,
            resolved ? h('div', { class: 'request-resolution' }, outcome) : controls);
        }
        if (event.type === 'error') return h('div', { class: 'transcript-error', role: 'note' }, h('strong', {}, event.will_retry ? 'Retrying after an error' : 'Agent error'), h('div', {}, event.message));
        return h('details', { class: 'tool-row' }, h('summary', {}, 'Agent plan'), codeView(event.entries || event.plan || event, h));
      })].filter(Boolean) : !session.loaded ? [h('div', { class: 'chat-empty', role: 'status' }, session.loadError ? null : h('span', { class: 'spinner large', 'aria-hidden': 'true' }),
          h('strong', {}, session.loadError ? "Couldn't load this conversation" : 'Loading conversation…'),
          session.loadError ? h('p', {}, session.loadError) : session.slow ? h('p', {}, 'Still waiting for the engine. It may be busy or unreachable.') : null)]
        : [h('div', { class: 'chat-empty' }, h('div', { class: 'empty-orbit', 'aria-hidden': 'true' }, '✦'), h('strong', {}, 'A new conversation is starting'), h('p', {}, 'Agent messages, tool activity, and requests will appear here.'))]));
      if (focus?.key) {
        const next = Array.from(log.querySelectorAll('[data-chat-focus]')).find(node => node.dataset.chatFocus === focus.key);
        if (next && !next.disabled) { next.focus({ preventScroll: true }); if (typeof focus.start === 'number' && next.setSelectionRange && next.type !== 'number') next.setSelectionRange(focus.start, focus.end); }
      }
      log.scrollTop = atBottom ? log.scrollHeight : top;
      session.scroll = log.scrollTop;
    }

    return { render(nextCard, key) {
      const changed = key !== currentKey;
      if (changed && currentKey) { const previous = sessions.get(currentKey); previous.draft = text.value; previous.scroll = log.scrollTop; }
      currentKey = key; card = nextCard; view = stateView(card);
      if (!sessions.has(key)) sessions.set(key, newSession());
      const session = sessions.get(key);
      if (changed) { text.value = session.draft; dismissed = false; selected = 0; fit(); error.hidden = true; }
      banner.className = `session-banner state-${view.state}`;
      // replaceChildren() would render a null child as the text "null".
      banner.replaceChildren(...[h('span', { class: 'state-dot', 'aria-hidden': 'true' }), h('div', { title: view.hint }, h('strong', {}, view.title), h('p', {}, view.hint)), card.agent === 'demo' ? h('span', { class: 'simulation-badge' }, 'SIMULATED') : null].filter(Boolean));
      text.disabled = view.terminal; send.disabled = view.terminal || session.sending;
      renderCommands();
      renderSettings(session);
      renderAttachments();
      text.placeholder = view.offline ? `Waiting for ${card.offline_worker} to reconnect` : view.terminal ? 'This session has ended.' : view.queued ? 'Queued for the next turn. Ctrl+Enter to send' : 'Give the agent a direction. Ctrl+Enter to send';
      actions.replaceChildren(...view.actions.map(action => {
        const name = action === 'done' ? 'Mark done' : action === 'continue' ? 'Continue' : 'Interrupt turn';
        const button = h('button', { type: 'button', class: `composer-icon${action === 'done' ? ' finish-session' : ''}`, 'aria-label': name, title: name, onclick: () => post(action === 'continue' ? { type: 'message', text: 'continue' } : { type: action }) }, icon(ICONS[action] || ICONS.interrupt));
        button.disabled = session.sending; return button;
      }));
      // A conversation that doesn't arrive says so, rather than spinning forever.
      if (!session.loaded && !session.slowTimer) {
        session.slowTimer = setTimeout(() => { session.slow = true; if (sessions.get(currentKey) === session && !session.loaded) renderRows(); }, 6000);
      }
      const nextSignature = `${session.version}|${session.loaded}|${view.state}`;
      if (changed || signature !== nextSignature) {
        signature = nextSignature;
        const savedScroll = session.scroll;
        renderRows();
        if (changed) log.scrollTop = session.seen ? savedScroll : log.scrollHeight;
        session.seen = true;
      }
    },
    // Rows pushed by the server: a reset (the newest page, on open or reconnect) or changed/new rows.
    rows(key, message) {
      if (!sessions.has(key)) sessions.set(key, newSession());
      const session = sessions.get(key);
      const commandRow = [...(message.rows || []), ...(message.patches || [])].find(row => row.type === 'commands');
      const stateRow = [...(message.rows || []), ...(message.patches || [])].find(row => row.type === 'acp_state');
      if (commandRow) session.commands = commandRow.commands || [];
      else if (message.reset) session.commands = [];
      if (stateRow) session.acpState = stateRow;
      else if (message.reset) session.acpState = {};
      message = { ...message, rows: (message.rows || []).filter(row => !['commands', 'acp_state'].includes(row.type)), patches: (message.patches || []).filter(row => !['commands', 'acp_state'].includes(row.type)) };
      if (message.reset) {
        // On a reconnect, keep older rows the user already scrolled back to.
        const first = message.rows[0]?.key, at = first ? session.rows.findIndex(row => row.key === first) : -1;
        session.rows = at > 0 ? [...session.rows.slice(0, at), ...message.rows] : message.rows;
        if (at <= 0) session.more = message.more;
        session.truncated = !!message.truncated; session.loaded = true; session.loadError = null;
      } else {
        const index = new Map(session.rows.map((row, i) => [row.key, i]));
        for (const row of message.rows) {
          if (index.has(row.key)) session.rows[index.get(row.key)] = row;
          else { index.set(row.key, session.rows.length); session.rows.push(row); }
        }
        // Older rows can change after paging (a long-running tool or unresolved permission).
        // Patch them if loaded; never append unseen older rows at the bottom of the transcript.
        for (const row of message.patches || []) if (index.has(row.key)) session.rows[index.get(row.key)] = row;
      }
      session.version++;
      if (key === currentKey && card) this.render(card, key);
    },
    // The engine couldn't give us this conversation.
    fail(message) {
      const session = currentKey && sessions.get(currentKey);
      if (!session || session.loaded) return;
      session.loadError = message; renderRows();
    },
    focus() { if (!text.disabled) text.focus(); } };
  }

  const api = { transcript, commandMatches, choices, answers, stateView, mount, markdown, jsonView, diffView, toolPreview, parseJsonish, localTime };
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  else root.AgentChat = api;
})(typeof window !== 'undefined' ? window : globalThis);
