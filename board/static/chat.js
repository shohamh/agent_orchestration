/* Shared transcript/form model and the dependency-free session drawer. */
(function (root) {
  'use strict';
  function transcript(events) {
    const rows = [], messages = new Map(), tools = new Map(), requests = new Map();
    for (const event of events || []) {
      if (event.type === 'agent' && event.message_id) {
        if (messages.has(event.message_id)) messages.get(event.message_id).text += event.text || '';
        else { const row = { ...event, text: event.text || '' }; rows.push(row); messages.set(event.message_id, row); }
      } else if (event.type === 'tool') {
        if (tools.has(event.id)) Object.assign(tools.get(event.id), event);
        else { const row = { ...event }; rows.push(row); tools.set(event.id, row); }
      } else if (event.type === 'approval' || event.type === 'question') {
        const row = { ...event }; rows.push(row); requests.set(`${event.type}:${event.id}`, row);
      } else if (event.type === 'approval_resolved' || event.type === 'answer') {
        const row = requests.get(`${event.type === 'answer' ? 'question' : 'approval'}:${event.id}`);
        if (row) row.resolved = event;
      } else if (['user', 'agent', 'error', 'plan'].includes(event.type)) rows.push({ ...event });
    }
    return rows;
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
    const state = card.column === 'stopped' ? 'stopped' : card.column === 'done' ? 'done' : card.session_state || 'starting';
    const [title, hint] = STATES[state] || [state.replaceAll('_', ' '), ''];
    const terminal = ['done', 'stopped'].includes(state);
    const queued = ['starting', 'working', 'retrying', 'approval', 'question'].includes(state);
    const actions = terminal ? [] : queued ? ['interrupt'] : ['continue', 'done'];
    return { state, title, hint: card.error || hint, terminal, queued, actions };
  }

  function mount(container, config) {
    const h = config.el;
    const sessions = new Map();
    let currentKey = null, card = null, view = null, signature = null;
    const banner = h('div', { class: 'session-banner', role: 'status' });
    const log = h('div', { class: 'chat-log', role: 'log', 'aria-label': 'Session conversation', tabindex: '0' });
    const error = h('div', { class: 'chat-error', role: 'alert', hidden: '' });
    const text = h('textarea', { rows: '3', placeholder: 'Give the agent a direction…', 'aria-label': 'Message to agent', 'data-chat-focus': 'composer' });
    const send = h('button', { type: 'submit', class: 'primary' }, 'Send message');
    const actions = h('div', { class: 'session-actions' });
    const hint = h('span', { class: 'composer-hint' });
    const form = h('form', { class: 'chat-composer', onsubmit: async event => {
      event.preventDefault();
      const value = text.value.trim(), key = currentKey;
      if (!value || !view || view.terminal || sessions.get(key).sending) return;
      if (await post({ type: 'message', text: value }) && key === currentKey && text.value.trim() === value) { text.value = ''; sessions.get(key).draft = ''; }
    } }, text, error, h('div', { class: 'composer-bottom' }, hint, send), actions);
    text.addEventListener('input', () => { if (currentKey) sessions.get(currentKey).draft = text.value; });
    text.addEventListener('keydown', event => { if (event.key === 'Enter' && (event.ctrlKey || event.metaKey)) { event.preventDefault(); form.requestSubmit(); } });
    container.append(banner, log, form);

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
      const rows = transcript(card.events);
      log.replaceChildren(...(rows.length ? rows.map((event, index) => {
        if (event.type === 'user' || event.type === 'agent') return h('article', { class: `chat-message ${event.type}` },
          h('div', { class: 'message-label' }, event.type === 'user' ? 'You' : card.agent === 'demo' ? 'Demo agent · simulated' : card.agent || 'Agent', event.queued ? h('span', { class: 'queued-label' }, 'Queued for next turn') : null),
          h('div', { class: 'message-text' }, event.text || '…'));
        if (event.type === 'tool') {
          const details = h('details', { class: `tool-row tool-${event.status || 'running'}` });
          details.open = session.expanded.has(event.id);
          details.addEventListener('toggle', () => { if (details.isConnected) { if (details.open) session.expanded.add(event.id); else session.expanded.delete(event.id); } });
          details.append(h('summary', { 'data-chat-focus': `tool:${event.id}` }, h('span', { class: 'tool-mark', 'aria-hidden': 'true' }, event.status === 'ok' ? '✓' : event.status === 'error' ? '!' : '↻'), h('span', { class: 'tool-name' }, event.name || 'Tool'), h('span', { class: 'tool-status' }, event.status || 'running')),
            event.summary ? h('p', {}, event.summary) : null, event.output ? h('pre', {}, typeof event.output === 'string' ? event.output : JSON.stringify(event.output, null, 2)) : null);
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
          if (event.detail) {
            detail = h('details', { class: 'permission-detail' }, h('summary', { 'data-chat-focus': `detail:${event.id}` }, 'View request details'), h('pre', {}, typeof event.detail === 'string' ? event.detail : JSON.stringify(event.detail, null, 2)));
            detail.open = !!session.details[event.id];
            detail.addEventListener('toggle', () => { if (detail.isConnected) session.details[event.id] = detail.open; });
          }
          return h('article', { class: 'request-card approval-card' }, h('div', { class: 'request-heading' }, h('span', { class: 'request-icon', 'aria-hidden': 'true' }, '◇'), h('strong', {}, resolved ? 'Permission resolved' : 'Permission to proceed')),
            h('p', { class: 'request-summary' }, event.summary || event.tool || 'The agent is requesting permission.'),
            detail,
            controls, resolved ? h('div', { class: 'request-resolution' }, event.resolved?.decision === 'cancelled' ? 'Cancelled · the turn was interrupted' : event.resolved?.decision || 'Response sent') : null);
        }
        if (event.type === 'error') return h('div', { class: 'transcript-error', role: 'note' }, h('strong', {}, event.will_retry ? 'Retrying after an error' : 'Agent error'), h('div', {}, event.message));
        return h('details', { class: 'tool-row' }, h('summary', {}, 'Agent plan'), h('pre', {}, typeof event.plan === 'string' ? event.plan : JSON.stringify(event.entries || event.plan || event, null, 2)));
      }) : [h('div', { class: 'chat-empty' }, h('div', { class: 'empty-orbit', 'aria-hidden': 'true' }, '✦'), h('strong', {}, 'A new conversation is starting'), h('p', {}, 'Agent messages, tool activity, and requests will appear here.'))]));
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
      if (!sessions.has(key)) sessions.set(key, { draft: '', scroll: 0, seen: false, answers: {}, expanded: new Set(), details: {}, pending: new Set(), sending: false });
      const session = sessions.get(key);
      if (changed) { text.value = session.draft; error.hidden = true; }
      banner.className = `session-banner state-${view.state}`;
      banner.replaceChildren(h('span', { class: 'state-dot', 'aria-hidden': 'true' }), h('div', {}, h('strong', {}, view.title), h('p', {}, view.hint)), card.agent === 'demo' ? h('span', { class: 'simulation-badge' }, 'SIMULATED') : null);
      text.disabled = view.terminal; send.disabled = view.terminal || session.sending;
      text.placeholder = view.terminal ? 'This session has ended.' : 'Give the agent a direction…';
      hint.textContent = view.terminal ? 'Session closed' : view.queued ? 'Queued for next turn · Ctrl/⌘ Enter to send' : 'Ctrl/⌘ Enter to send';
      actions.replaceChildren(...view.actions.map(action => {
        const button = h('button', { type: 'button', class: action === 'done' ? 'secondary finish-session' : 'secondary', onclick: () => post(action === 'continue' ? { type: 'message', text: 'continue' } : { type: action }) }, action === 'done' ? '✓ Mark done' : action === 'continue' ? 'Continue' : 'Interrupt turn');
        button.disabled = session.sending; return button;
      }));
      const nextSignature = JSON.stringify([card.events, view.state]);
      if (changed || signature !== nextSignature) {
        signature = nextSignature;
        const savedScroll = session.scroll;
        renderRows();
        if (changed) log.scrollTop = session.seen ? savedScroll : log.scrollHeight;
        session.seen = true;
      }
    }, focus() { if (!text.disabled) text.focus(); } };
  }

  const api = { transcript, choices, answers, stateView, mount };
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  else root.AgentChat = api;
})(typeof window !== 'undefined' ? window : globalThis);
