/* Graphical flow builder: pure document edits (tested under node) and the dependency-free editor
 * behind the board's Flows button. Flow data is only ever placed in text nodes and attributes. */
(function (root) {
  'use strict';
  const AGENTS = ['demo', 'codex', 'claude', 'opencode', 'opencode2', 'omp', 'pi'];
  const AGENT_LABELS = { demo: 'Demo · simulated, no model', codex: 'Codex', claude: 'Claude Code', opencode: 'opencode v1', opencode2: 'opencode v2', omp: 'omp', pi: 'pi' };
  const STEP_ID = /^[a-z][a-z0-9_]{0,47}$/;
  const INPUT_NAME = STEP_ID;
  const FLOW_NAME = /^[a-z][a-z0-9-]{0,47}$/;
  const TEMPLATED = ['run', 'prompt'];
  const NODE_W = 184, NODE_H = 56, GAP_X = 36, GAP_Y = 64, PAD = 24;
  // Keys the forms edit. Anything else (environment, setup, keys added later) stays in the
  // document untouched and is shown as "edit in YAML".
  const FORM_STEP_KEYS = new Set(['kind', 'description', 'needs', 'agent', 'review', 'worker', 'retries', 'timeout_minutes', 'run', 'requires_command', 'prompt']);
  const FORM_FLOW_KEYS = new Set(['name', 'description', 'input', 'worker', 'steps']);
  const STATES = {
    active: ['Active', 'Active: new runs use this draft.'],
    changed: ['Changed', 'Saved draft differs from the active version. Activate it to use it for new runs.'],
    inactive: ['Not active', 'Saved but never activated: it cannot be launched yet.'],
    broken: ['Errors', 'The saved draft has errors. An older active version, if any, keeps serving new runs.'],
    removed: ['Removed', 'The draft was deleted. The active version keeps serving new runs until the removal is activated.'],
  };
  const ENGINE_LABELS = { hatchet: 'Hatchet', prefect: 'Prefect', trigger: 'Trigger.dev' };

  // ------------------------------------------------------------------ Pure document helpers
  const clone = (value) => (value === undefined ? undefined : JSON.parse(JSON.stringify(value)));
  const needsOf = (step) => (Array.isArray(step?.needs) ? step.needs : []);
  const stepIds = (doc) => Object.keys(doc?.steps || {});
  const ok = (doc, extra) => ({ ok: true, doc, ...extra });
  const fail = (doc, error) => ({ ok: false, doc, error });

  function checkStepId(doc, id) {
    if (typeof id !== 'string' || !STEP_ID.test(id)) return 'A step ID starts with a lowercase letter and uses only lowercase letters, digits and _ (at most 48 characters).';
    if (doc?.steps && Object.prototype.hasOwnProperty.call(doc.steps, id)) return `A step named ${id} already exists.`;
    if (id === 'setup_environment' && doc?.environment) return 'setup_environment is reserved for the environment setup step.';
    return null;
  }

  function uniqueStepId(doc, base) {
    for (let i = 1; ; i++) if (!checkStepId(doc, `${base}_${i}`)) return `${base}_${i}`;
  }

  function newStep(kind) {
    return kind === 'agent'
      ? { kind: 'agent', agent: 'demo', review: false, prompt: 'Describe what this agent should do.\n' }
      : { kind: 'program', run: `python -c "print('hello')"` };
  }

  function addStep(doc, options = {}) {
    const kind = options.kind === 'agent' ? 'agent' : 'program';
    const id = options.id ?? uniqueStepId(doc, kind);
    const problem = checkStepId(doc, id);
    if (problem) return fail(doc, problem);
    const next = clone(doc);
    next.steps = next.steps || {};
    const step = newStep(kind);
    const needs = (options.needs || []).filter((n) => next.steps[n]);
    next.steps[id] = { kind: step.kind, needs, ...step };
    return ok(next, { id });
  }

  // Steps whose command or prompt refers to `{{ steps.<id>.… }}`.
  function referencesTo(doc, id) {
    const pattern = new RegExp(`\\{\\{\\s*steps\\.${id}(?![A-Za-z0-9_])`);
    return stepIds(doc).filter((sid) => TEMPLATED.some((key) => typeof doc.steps[sid]?.[key] === 'string' && pattern.test(doc.steps[sid][key])));
  }

  function removeStep(doc, id) {
    if (!doc?.steps?.[id]) return fail(doc, `There is no step ${id}.`);
    const next = clone(doc);
    delete next.steps[id];
    const unlinked = [];
    for (const [sid, step] of Object.entries(next.steps)) {
      if (!needsOf(step).includes(id)) continue;
      unlinked.push(sid);
      step.needs = step.needs.filter((n) => n !== id);
    }
    // References in prompts and commands are left as typed; validation then points at them.
    return ok(next, { unlinked, dangling: referencesTo(next, id) });
  }

  const replaceRefs = (text, prefix, from, to) => text.replace(new RegExp(`(\\{\\{\\s*${prefix}\\.)${from}(?![A-Za-z0-9_])`, 'g'), `$1${to}`);

  // Renames a step and, in one edit, every `needs` entry and `{{ steps.<old>.… }}` reference.
  function renameStep(doc, from, to) {
    if (!doc?.steps?.[from]) return fail(doc, `There is no step ${from}.`);
    if (from === to) return ok(doc, { unchanged: true });
    const problem = checkStepId(doc, to);
    if (problem) return fail(doc, problem);
    const next = clone(doc), steps = {};
    for (const [sid, step] of Object.entries(next.steps)) {
      if (Array.isArray(step.needs)) step.needs = step.needs.map((n) => (n === from ? to : n));
      for (const key of TEMPLATED) if (typeof step[key] === 'string') step[key] = replaceRefs(step[key], 'steps', from, to);
      steps[sid === from ? to : sid] = step;
    }
    next.steps = steps;
    return ok(next);
  }

  // Renames a flow input, keeping input order, and updates `{{ input.<old> }}` references.
  function renameInput(doc, from, to) {
    const input = doc?.input || {};
    if (!Object.prototype.hasOwnProperty.call(input, from)) return fail(doc, `There is no input ${from}.`);
    if (from === to) return ok(doc, { unchanged: true });
    if (!INPUT_NAME.test(to)) return fail(doc, 'An input name starts with a lowercase letter and uses only lowercase letters, digits and _.');
    if (Object.prototype.hasOwnProperty.call(input, to)) return fail(doc, `An input named ${to} already exists.`);
    const next = clone(doc);
    next.input = Object.fromEntries(Object.entries(next.input).map(([k, v]) => [k === from ? to : k, v]));
    for (const step of Object.values(next.steps || {}))
      for (const key of TEMPLATED) if (typeof step[key] === 'string') step[key] = replaceRefs(step[key], 'input', from, to);
    return ok(next);
  }

  // True when step `a` (transitively) needs step `b`.
  function dependsOn(doc, a, b) {
    const seen = new Set(), stack = [...needsOf(doc?.steps?.[a])];
    while (stack.length) {
      const id = stack.pop();
      if (id === b) return true;
      if (seen.has(id)) continue;
      seen.add(id);
      stack.push(...needsOf(doc.steps?.[id]));
    }
    return false;
  }

  // `to` needs `from`: an edge from -> to. Refuses self-dependencies and cycles.
  function addDependency(doc, from, to) {
    if (!doc?.steps?.[from] || !doc?.steps?.[to]) return fail(doc, 'Both steps must exist.');
    if (from === to) return fail(doc, `${to} cannot depend on itself.`);
    if (needsOf(doc.steps[to]).includes(from)) return ok(doc, { unchanged: true });
    if (dependsOn(doc, from, to)) return fail(doc, `That would create a cycle: ${from} already runs after ${to}.`);
    const next = clone(doc);
    next.steps[to].needs = [...needsOf(next.steps[to]), from];
    return ok(next);
  }

  function removeDependency(doc, from, to) {
    if (!needsOf(doc?.steps?.[to]).includes(from)) return ok(doc, { unchanged: true });
    const next = clone(doc);
    next.steps[to].needs = next.steps[to].needs.filter((n) => n !== from);
    return ok(next);
  }

  // Candidate prerequisites for `to`, with the ones that would close a cycle disabled.
  function dependencyOptions(doc, to) {
    const needs = needsOf(doc?.steps?.[to]);
    return stepIds(doc).filter((id) => id !== to && !needs.includes(id))
      .map((id) => ({ id, disabled: dependsOn(doc, id, to), reason: dependsOn(doc, id, to) ? 'would create a cycle' : null }));
  }

  // Layer of each step: 1 + the deepest prerequisite (cycles and unknown needs are tolerated).
  function layers(doc) {
    const ids = stepIds(doc), depth = {}, visiting = new Set();
    const preds = Object.fromEntries(ids.map((id) => [id, needsOf(doc.steps[id]).filter((n) => n !== id && doc.steps[n])]));
    const rowOf = (id) => {
      if (id in depth) return depth[id];
      if (visiting.has(id)) return 0;
      visiting.add(id);
      const d = preds[id].length ? Math.max(...preds[id].map(rowOf)) + 1 : 0;
      visiting.delete(id);
      return (depth[id] = d);
    };
    ids.forEach(rowOf);
    const rows = [];
    for (const id of ids) (rows[depth[id]] ??= []).push(id);
    return { row: depth, rows: rows.filter(Boolean), preds };
  }

  // Deterministic layered layout, top to bottom, like the run view's layoutDag: each step wants to
  // sit under the average of its prerequisites; a row is pushed apart just enough not to overlap.
  function autoLayout(doc) {
    const { row, rows, preds } = layers(doc);
    const step = NODE_W + GAP_X, x = {};
    rows.forEach((ids) => {
      ids = [...ids];
      const want = Object.fromEntries(ids.map((id, i) => {
        const above = preds[id].filter((p) => row[p] < row[id] && p in x);
        return [id, above.length ? above.reduce((s, p) => s + x[p], 0) / above.length : i * step];
      }));
      ids.sort((a, b) => want[a] - want[b]);
      ids.forEach((id, i) => { x[id] = i === 0 ? want[id] : Math.max(want[id], x[ids[i - 1]] + step); });
      const drift = ids.reduce((s, id) => s + x[id] - want[id], 0) / ids.length;
      ids.forEach((id) => { x[id] -= drift; });
    });
    const xs = Object.values(x), minX = xs.length ? Math.min(...xs) : 0;
    const positions = {};
    for (const id of stepIds(doc)) positions[id] = { x: Math.round(x[id] - minX + PAD), y: PAD + row[id] * (NODE_H + GAP_Y) };
    return { positions, ...bounds(positions) };
  }

  // Saved positions where present, auto-layout for the rest; stale IDs are ignored.
  function layout(doc, saved = {}) {
    const auto = autoLayout(doc).positions, positions = {};
    for (const id of stepIds(doc)) {
      const p = saved?.[id];
      positions[id] = p && Number.isFinite(p.x) && Number.isFinite(p.y) ? { x: p.x, y: p.y } : auto[id];
    }
    return { positions, ...bounds(positions) };
  }

  function bounds(positions) {
    const all = Object.values(positions);
    if (!all.length) return { minX: 0, minY: 0, width: 0, height: 0 };
    const minX = Math.min(...all.map((p) => p.x)), minY = Math.min(...all.map((p) => p.y));
    const maxX = Math.max(...all.map((p) => p.x)) + NODE_W, maxY = Math.max(...all.map((p) => p.y)) + NODE_H;
    return { minX, minY, width: maxX - minX, height: maxY - minY };
  }

  // What a step's prompt or command may reference: flow inputs and its direct prerequisites'
  // outputs (plus output fields other steps already use, and stdout for program steps).
  function referenceCandidates(doc, stepId) {
    const out = [];
    for (const name of Object.keys(doc?.input || {})) out.push({ group: 'Flow inputs', label: name, text: `{{ input.${name} }}` });
    const used = {};
    for (const step of Object.values(doc?.steps || {}))
      for (const key of TEMPLATED) for (const m of String(step[key] ?? '').matchAll(/\{\{\s*steps\.([a-z][a-z0-9_]*)\.output\.([A-Za-z0-9_]+)\s*\}\}/g))
        (used[m[1]] ??= new Set()).add(m[2]);
    for (const need of needsOf(doc?.steps?.[stepId])) {
      const step = doc.steps[need];
      if (!step) continue;
      out.push({ group: 'Prerequisite outputs', label: `${need} · whole output`, text: `{{ steps.${need}.output }}` });
      const fields = new Set(step.kind === 'program' ? ['stdout'] : []);
      for (const f of used[need] || []) fields.add(f);
      for (const f of fields) out.push({ group: 'Prerequisite outputs', label: `${need} · ${f}`, text: `{{ steps.${need}.output.${f} }}` });
    }
    return out;
  }

  function insertText(value, start, end, text) {
    const a = Math.max(0, Math.min(start ?? value.length, value.length)), b = Math.max(a, Math.min(end ?? a, value.length));
    return { value: value.slice(0, a) + text + value.slice(b), caret: a + text.length };
  }

  const canonical = (value) => JSON.stringify(value, (key, v) => (v && typeof v === 'object' && !Array.isArray(v) ? Object.fromEntries(Object.keys(v).sort().map((k) => [k, v[k]])) : v));
  // Differences between my document and the saved one, for the conflict dialog. A missing
  // needs and an empty one mean the same.
  function compareDocuments(mine, theirs) {
    const norm = (steps) => Object.fromEntries(Object.entries(steps || {}).map(([id, st]) => [id, { ...st, needs: needsOf(st) }]));
    const a = norm(mine?.steps), b = norm(theirs?.steps);
    return {
      onlyMine: Object.keys(a).filter((id) => !(id in b)),
      onlyTheirs: Object.keys(b).filter((id) => !(id in a)),
      changed: Object.keys(a).filter((id) => id in b && canonical(a[id]) !== canonical(b[id])),
      flow: ['name', 'description', 'input', 'worker', 'environment'].filter((k) => canonical(mine?.[k] ?? null) !== canonical(theirs?.[k] ?? null)),
    };
  }

  const TEMPLATES = {
    starter: { label: 'Starter: a program step, then an agent step', make: (name) => ({ name, description: 'What this flow does', input: { topic: 'something' }, steps: {
      gather: { kind: 'program', run: `python -c "print('gathering {{ input.topic }}')"` },
      think: { kind: 'agent', needs: ['gather'], agent: 'demo', review: false, prompt: 'Work on {{ input.topic }} using:\n{{ steps.gather.output.stdout }}\n' } } }) },
    parallel: { label: 'Parallel review: a brief, two agents in parallel, then a join', make: (name) => ({ name, description: 'Two agents work independently, then their results are combined.', input: { feature: 'A feature to design' }, steps: {
      brief: { kind: 'program', run: `python -c "print('Design a practical feature with a clear scope.')"` },
      design: { kind: 'agent', needs: ['brief'], agent: 'codex', review: false, prompt: 'Design {{ input.feature }}.\nBrief: {{ steps.brief.output.stdout }}\n' },
      review: { kind: 'agent', needs: ['brief'], agent: 'opencode2', review: false, prompt: 'Review the risks of {{ input.feature }}.\nBrief: {{ steps.brief.output.stdout }}\n' },
      plan: { kind: 'agent', needs: ['design', 'review'], agent: 'codex', review: true, prompt: 'Combine the design and the review into a plan.\nDesign: {{ steps.design.output }}\nReview: {{ steps.review.output }}\n' } } }) },
    empty: { label: 'Empty: a single program step', make: (name) => ({ name, description: '', input: {}, steps: { first_step: { kind: 'program', run: `python -c "print('hello')"` } } }) },
  };

  // ------------------------------------------------------------------ DOM helpers
  const SVG_NS = 'http://www.w3.org/2000/svg';
  const PROPS = new Set(['value', 'checked', 'disabled', 'hidden', 'selected', 'readOnly', 'multiple']);
  function build(node, attrs, children) {
    for (const [k, v] of Object.entries(attrs || {})) {
      if (v == null || PROPS.has(k) || (v === false && !k.startsWith('aria-'))) continue;
      if (k.startsWith('on') && typeof v === 'function') node.addEventListener(k.slice(2), v);
      else node.setAttribute(k, v === true ? '' : String(v));
    }
    for (const child of children.flat()) if (child != null && child !== false) node.append(child);
    // Properties last, so a <select>'s value can pick one of its options.
    for (const k of PROPS) if (attrs && attrs[k] != null) node[k] = attrs[k];
    return node;
  }
  const h = (tag, attrs, ...children) => build(document.createElement(tag), attrs, children);
  const s = (tag, attrs, ...children) => build(document.createElementNS(SVG_NS, tag), attrs, children);
  const short = (text, n) => (text.length > n ? `${text.slice(0, n - 1)}…` : text);
  const shortRev = (rev) => (rev ? String(rev).replace(/^sha256:/, '').slice(0, 10) : 'none');
  const plural = (n, word) => `${n} ${word}${n === 1 ? '' : 's'}`;

  async function fetchJson(method, path, body) {
    try {
      const response = await fetch(path, { method, headers: body ? { 'Content-Type': 'application/json' } : {}, body: body ? JSON.stringify(body) : undefined });
      const data = await response.json().catch(() => ({}));
      return { ok: response.ok, status: response.status, data };
    } catch (error) { return { ok: false, status: 0, data: { error: `${error.message || 'Network error'} (is the board reachable?)` } }; }
  }
  const errorText = (r) => r.data?.error || (r.data?.errors || []).map((e) => e.message).join(' ') || `Request failed (HTTP ${r.status})`;

  // ------------------------------------------------------------------ Fleet placement
  // Words for the flow host's launch check (POST /api/launch/validate: ready | wait | blocked with
  // per-worker reasons) and for execution records (state, waiting_reason). Shared with the New
  // card dialog and the card drawer in index.html, so every view explains placement the same way.
  const WAITING_WORDS = {
    offline: 'a matching worker is offline',
    credentials_unavailable: 'the coordinator has no usable credentials for a matching worker',
    full_slots: 'every matching worker is busy (no free slot)',
    high_cpu: 'CPU on the matching workers is at or above their limit',
    high_ram: 'RAM on the matching workers is at or above their limit',
    missing_metrics: 'waiting for a complete 10-second CPU and RAM measurement',
    cooldown: 'the worker waits for a fresh 10-second measurement after its last start',
    software_unknown: 'the required software has not been checked on a matching worker yet',
    software_stale: 'the required software was last checked over a day ago and is checked again',
    software_missing: 'the required software was missing on the chosen worker; looking for another one',
    intervention_needed: 'no registered worker can run it now',
    awaiting_authorization: 'the worker admitted it and waits for the coordinator to confirm the start',
    coordinator_unreachable: 'the worker cannot reach the coordinator',
    starting: 'starting',
  };
  const waitingWords = (code) => WAITING_WORDS[code] || String(code || 'unknown reason').replaceAll('_', ' ');
  const PLACEMENT_LABELS = { ready: 'Ready', wait: 'Will wait', blocked: 'Blocked', local: 'Coordinator', unknown: 'Unknown' };
  const PLACEMENT_CHIPS = { ready: 'Ready', wait: 'Waits', blocked: 'Blocked', local: 'Coordinator' };
  const MATCH_WORDS = { compatible: 'Compatible', unknown: 'Might match, waiting', incompatible: 'Not compatible' };
  const SOFTWARE_WORDS = { installed: 'installed', absent: 'not installed', unknown: 'unknown', error: 'check failed' };

  function placementStatus(decision) { return !decision ? 'unknown' : decision.fleet === false ? 'local' : decision.status; }
  function placementHeadline(decision) {
    const status = placementStatus(decision);
    if (status === 'local') return 'Runs on the coordinator: no worker is selected anywhere, so the fleet is not used.';
    if (status === 'ready') return `Can start now on ${(decision.candidates || []).join(', ')}.`;
    if (status === 'wait') return `Waits for a worker: ${waitingWords(decision.waiting_reason)}. It starts by itself once a matching worker can take it.`;
    if (status === 'blocked') return decision.message || 'No registered worker can run this.';
    return 'Not checked yet.';
  }
  // A step's or card's record: queued, waiting, running, done, failed, cancelled, timed_out, interrupted.
  function executionText(record) {
    if (!record) return 'Not placed yet';
    const reason = record.waiting_reason && record.waiting_reason !== 'intervention_needed' ? `: ${waitingWords(record.waiting_reason)}` : '';
    const on = record.worker?.id ? ` on ${record.worker.id}${record.worker.hostname && record.worker.hostname !== record.worker.id ? ` (${record.worker.hostname})` : ''}` : '';
    switch (record.state) {
      case 'queued': return reason ? `Queued on a worker${reason}` : 'Queued: choosing a worker';
      case 'waiting': return record.intervention_needed || record.waiting_reason === 'intervention_needed'
        ? `Intervention needed: ${record.message || WAITING_WORDS.intervention_needed}` : `Waiting for a worker${reason}`;
      case 'running': return `Running${on}`;
      case 'done': return `Done${on}`;
      case 'failed': return `Failed${on}`;
      case 'cancelled': return `Cancelled${reason ? ` while waiting${reason}` : ''}`;
      case 'timed_out': return `Timed out after ${record.timeout_minutes ?? '?'} minutes${reason ? ` while waiting${reason}` : ''}`;
      case 'interrupted': return `Interrupted by the engine${reason ? ` while waiting${reason}` : ''}`;
      default: return String(record.state || 'unknown').replaceAll('_', ' ');
    }
  }
  const localTime = (iso) => { const d = new Date(iso); return Number.isNaN(d.getTime()) ? String(iso) : d.toLocaleString(); };
  function detectedNodes(list) {
    return (list || []).flatMap((d) => [` · ${d.name} ${SOFTWARE_WORDS[d.status] || d.status}`,
      d.checked_at ? ', checked ' : ', never checked', d.checked_at ? h('time', { datetime: d.checked_at }, localTime(d.checked_at)) : null,
      d.cached ? ' (last known: the PC is offline)' : null]);
  }
  function placementView(decision, { title } = {}) {
    const status = placementStatus(decision);
    const workers = (decision?.workers || []).map((m) => h('li', { 'data-worker': m.worker, 'data-match': m.status },
      h('strong', {}, m.worker), ` · ${MATCH_WORDS[m.status] || m.status}`, m.reasons?.length ? `: ${m.reasons.map((r) => r.message).join('; ')}` : '', ...detectedNodes(m.detected)));
    return h('div', { class: `fb-placement fb-placement-${status}`, 'data-status': status },
      title ? h('h4', {}, title) : null,
      h('p', { class: 'fb-placement-head' }, h('strong', {}, PLACEMENT_LABELS[status] || status), ' ', placementHeadline(decision)),
      status === 'blocked' && decision.resolution ? h('p', { class: 'fb-placement-fix' }, `How to fix: ${decision.resolution}`) : null,
      workers.length ? h('ul', { class: 'fb-placement-workers', 'aria-label': 'Registered workers' }, ...workers) : null);
  }
  function executionView(record, { label } = {}) {
    const state = record?.state || 'none';
    const unfinished = ['queued', 'waiting', 'cancelled', 'timed_out', 'interrupted'].includes(state);
    const reasons = unfinished ? (record?.waiting || []).map((r) => r.message).filter(Boolean) : [];
    return h('div', { class: `fb-exec fb-exec-${state}`, 'data-state': state },
      h('p', {}, label ? h('strong', {}, `${label} `) : null, executionText(record), record?.engine ? h('span', { class: 'fb-hint' }, ` · ${ENGINE_LABELS[record.engine] || record.engine}`) : null),
      reasons.length ? h('ul', { class: 'fb-placement-workers' }, ...reasons.map((m) => h('li', {}, m))) : null,
      state === 'waiting' && record.resolution ? h('p', { class: 'fb-placement-fix' }, `How to fix: ${record.resolution}`) : null);
  }

  // ------------------------------------------------------------------ Editor
  function mount(container, options = {}) {
    const win = container.ownerDocument.defaultView;
    const request = options.request || fetchJson;
    const narrow = () => !!win.matchMedia?.('(max-width: 700px)').matches;
    const S = { docs: [], cur: null, sel: null, view: narrow() ? 'steps' : 'graph', vp: { x: 0, y: 0, k: 1 }, inspecting: false,
      drafts: {}, picker: null, caret: {}, lastName: null, visible: false, losslessAsk: null,
      placement: { key: null, serial: 0, result: null, error: null } };

    // ---- skeleton
    const title = h('h2', { class: 'fb-title', id: 'fb-title' }, 'Flows');
    const stateLine = h('span', { class: 'fb-state' });
    const statusLine = h('p', { class: 'fb-status', role: 'status', 'aria-live': 'polite' });
    const saveBtn = h('button', { type: 'button', class: 'primary', onclick: () => save() }, 'Save draft');
    const activateBtn = h('button', { type: 'button', class: 'secondary', onclick: () => activate() }, 'Activate…');
    const runBtn = h('button', { type: 'button', class: 'secondary', onclick: () => run() }, 'Run…');
    const deleteBtn = h('button', { type: 'button', class: 'danger', onclick: () => removeDraft() }, 'Delete');
    const closeBtn = h('button', { type: 'button', class: 'close fb-close', 'aria-label': 'Close flows', onclick: () => options.onClose?.() }, '×');
    const top = h('div', { class: 'fb-top' }, h('div', { class: 'fb-heading' }, title, stateLine), h('div', { class: 'fb-actions' }, saveBtn, activateBtn, runBtn, deleteBtn), closeBtn, statusLine);

    const flowList = h('ul', { class: 'fb-flow-list' });
    const nav = h('nav', { class: 'fb-flows', 'aria-label': 'Flow list' },
      h('button', { type: 'button', class: 'secondary fb-new', onclick: () => newFlow() }, '+ New flow'), flowList);

    const tabs = ['graph', 'steps', 'yaml'].map((v) => h('button', { type: 'button', role: 'tab', class: 'fb-tab', id: `fb-tab-${v}`, 'aria-controls': `fb-view-${v}`, 'data-view': v,
      onclick: () => setView(v), onkeydown: tabKey }, { graph: 'Graph', steps: 'Steps', yaml: 'YAML' }[v]));
    const tabList = h('div', { class: 'fb-tabs', role: 'tablist', 'aria-label': 'Editor view' }, ...tabs);
    const banner = h('div', { class: 'fb-banner', hidden: true });

    const svgEl = s('svg', { class: 'fb-svg', tabindex: '0', role: 'group', 'aria-label': 'Flow graph. Tab to a step; Enter selects it, Delete removes it and arrow keys move it.' });
    const graphEmpty = h('p', { class: 'fb-empty', hidden: true });
    const zoomLabel = h('span', { class: 'fb-zoom', 'aria-live': 'polite' }, '100%');
    const tool = (label, onclick, attrs = {}) => h('button', { type: 'button', class: 'secondary', onclick, ...attrs }, label);
    const toolbar = h('div', { class: 'fb-toolbar', role: 'toolbar', 'aria-label': 'Graph tools' },
      tool('+ Program step', () => addNewStep('program')), tool('+ Agent step', () => addNewStep('agent')),
      tool('Flow settings', () => select({ type: 'flow' }, { inspect: true })),
      h('span', { class: 'fb-spacer' }),
      tool('−', () => zoomBy(1 / 1.25), { 'aria-label': 'Zoom out', title: 'Zoom out' }), zoomLabel,
      tool('+', () => zoomBy(1.25), { 'aria-label': 'Zoom in', title: 'Zoom in' }),
      tool('Fit', () => fit(), { title: 'Fit the whole flow on screen' }),
      tool('Auto-layout', () => autoArrange(), { title: 'Arrange steps by dependency depth' }));
    const canvas = h('div', { class: 'fb-canvas' }, svgEl, graphEmpty);
    const graphView = h('div', { class: 'fb-view fb-graph-view', id: 'fb-view-graph', role: 'tabpanel', 'aria-labelledby': 'fb-tab-graph' }, toolbar, canvas,
      h('p', { class: 'fb-hint fb-graph-hint' }, 'Drag a step to move it. Drag from the round handle under a step onto another step to make that step wait for it. Drag the background to pan; pinch or Ctrl+scroll to zoom.'));
    const stepsView = h('div', { class: 'fb-view fb-steps-view', id: 'fb-view-steps', role: 'tabpanel', 'aria-labelledby': 'fb-tab-steps' });
    const yamlArea = h('textarea', { class: 'fb-yaml fb-code', spellcheck: 'false', autocapitalize: 'off', autocomplete: 'off', 'aria-label': 'Flow YAML', oninput: onYamlInput });
    const yamlView = h('div', { class: 'fb-view fb-yaml-view', id: 'fb-view-yaml', role: 'tabpanel', 'aria-labelledby': 'fb-tab-yaml' }, yamlArea,
      h('p', { class: 'fb-hint' }, 'YAML edits are kept exactly as typed. Settings the forms do not cover, such as environment and setup, are edited here; graph edits keep them.'));
    const problems = h('section', { class: 'fb-problems', 'aria-label': 'Problems', hidden: true });
    const inspector = h('section', { class: 'fb-inspector', 'aria-label': 'Properties' });
    const main = h('section', { class: 'fb-main', 'aria-label': 'Editor' }, tabList, banner, graphView, stepsView, yamlView, problems);
    const askDialog = h('dialog', { class: 'fb-ask', 'aria-labelledby': 'fb-ask-title' });
    container.classList.add('fb');
    container.setAttribute('aria-labelledby', 'fb-title');
    container.replaceChildren(top, h('div', { class: 'fb-body' }, nav, main, inspector), askDialog);

    win.addEventListener('beforeunload', (e) => { if (S.visible && isDirty()) { e.preventDefault(); e.returnValue = ''; } });

    // ---- small helpers
    const cur = () => S.cur;
    const isDirty = () => !!S.cur && (S.cur.isNew || S.cur.pendingEdit || S.cur.yamlEdited || S.cur.yaml !== S.cur.savedYaml);
    const errorsFor = (step, field) => (S.cur?.errors || []).filter((e) => (step === undefined || (e.step ?? null) === step) && (field === undefined || (e.field ?? null) === field));
    const docName = () => S.cur?.doc?.name || S.cur?.name || 'new flow';
    function notify(text, error = false) { statusLine.textContent = text || ''; statusLine.classList.toggle('error', !!error); statusLine.hidden = !text; }
    notify('');

    let askChain = Promise.resolve();
    // An in-page confirmation (a nested modal dialog), so it works the same with touch, mouse and keys.
    // The first button is the safe choice and also what Escape picks.
    function ask({ title: heading, body = [], buttons }) {
      const shown = askChain.then(() => new Promise((resolve) => {
        let done = false;
        const finish = (value) => {
          if (done || value === undefined) return;
          done = true; askDialog.removeEventListener('cancel', onCancel); askDialog.close(); resolve(value);
        };
        const onCancel = (e) => { e.preventDefault(); finish(buttons[0].value); };
        askDialog.addEventListener('cancel', onCancel);
        const buttonEls = buttons.map((b) => h('button', { type: 'button', class: b.kind || 'secondary', onclick: () => finish(typeof b.value === 'function' ? b.value() : b.value) }, b.label));
        askDialog.replaceChildren(h('h3', { class: 'fb-ask-title', id: 'fb-ask-title' }, heading), h('div', { class: 'fb-ask-body' }, ...body), h('div', { class: 'fb-ask-actions' }, ...buttonEls));
        askDialog.showModal();
        (askDialog.querySelector('.fb-ask-body input, .fb-ask-body select') || buttonEls[0]).focus();
      }));
      askChain = shown.catch(() => {});
      return shown;
    }
    const p = (...text) => h('p', {}, ...text);

    // ---- loading
    function fromDoc(d) {
      return { name: d.name, revision: d.revision ?? null, savedYaml: d.yaml ?? '', yaml: d.yaml ?? '', goodYaml: d.document ? d.yaml : null,
        doc: d.document ? clone(d.document) : null, errors: d.errors || [], lossless: d.lossless !== false, losslessAck: false,
        yamlInvalid: !d.document && !!d.yaml, positions: { ...(d.layout?.positions || {}) }, state: d.state || null,
        activeRevision: d.activeRevision ?? null, pendingEdit: false, yamlEdited: false, serial: 0, isNew: false, fitPending: true, stash: {} };
    }
    async function refreshList() {
      const r = await request('GET', '/api/flow-documents');
      if (r.ok) S.docs = r.data.documents || [];
      else notify(`Couldn't load flows: ${errorText(r)}`, true);
      renderList();
      return r.ok;
    }
    async function load(name) {
      const r = await request('GET', `/api/flow-documents/${encodeURIComponent(name)}`);
      if (!r.ok) { notify(`Couldn't open ${name}: ${errorText(r)}`, true); return false; }
      setCurrent(fromDoc(r.data));
      const summary = S.docs.findIndex((d) => d.name === r.data.name);
      if (summary >= 0) S.docs[summary] = { ...S.docs[summary], ...r.data };
      notify('');
      return true;
    }
    function setCurrent(next) {
      S.cur = next; S.sel = null; S.drafts = {}; S.picker = null; S.inspecting = false; S.lastName = next?.name || S.lastName;
      if (next?.yamlInvalid && !next.doc) S.view = 'yaml';
      yamlArea.value = next?.yaml ?? '';
      renderAll();
      schedulePlacement();
    }
    async function openDoc(name) {
      if (S.cur && S.cur.name === name && !S.cur.isNew) return;
      if (!(await guard())) return;
      await load(name);
    }

    // Asks before throwing away unsaved edits; resolves true when it is fine to continue.
    async function guard() {
      await flushParse();
      if (!isDirty()) return true;
      return ask({ title: 'Discard unsaved changes?', body: [p(`${docName()} has changes that are not saved as a draft.`)],
        buttons: [{ label: 'Keep editing', value: false }, { label: 'Discard changes', value: true, kind: 'danger' }] });
    }

    // ---- editing pipeline: graph/form edit -> local document -> debounced serialize -> YAML
    async function edit(change, { inspector: rebuild = false } = {}) {
      const c = S.cur;
      if (!c?.doc) return null;
      if (c.yamlEdited) await flushParse();
      if (S.cur !== c) return null;
      if (c.yamlInvalid) {
        notify('The YAML has errors. Fix it, or discard the YAML changes, before editing in the graph or forms.', true);
        renderInspector(); return null;
      }
      if (!c.lossless && !c.losslessAck) {
        S.losslessAsk ||= ask({ title: 'Rewrite this YAML?', body: [
          p('This flow’s YAML has comments or formatting that the graph editor cannot keep.'),
          p('Editing in the graph or forms replaces the text with canonical YAML. All settings are kept, but comments and formatting are not. You can cancel and edit in the YAML tab instead, where text is kept as typed.')],
        buttons: [{ label: 'Cancel', value: false }, { label: 'Edit and rewrite YAML', value: true, kind: 'primary' }] }).finally(() => { S.losslessAsk = null; });
        const accepted = await S.losslessAsk;
        if (S.cur !== c) return null;
        if (!accepted) { renderAll(); return null; }
        c.losslessAck = true;
      }
      const result = change(c.doc);
      if (!result) return null;
      if (!result.ok) { notify(result.error, true); if (rebuild) renderInspector(); return result; }
      if (result.unchanged) return result;
      c.doc = result.doc; c.pendingEdit = true;
      notify('');
      scheduleSerialize(); schedulePlacement();
      renderList(); renderTop(); renderBanner(); renderGraph(); renderSteps();
      if (rebuild) renderInspector();
      return result;
    }
    const mutate = (fn) => (doc) => { const next = clone(doc); fn(next); return ok(next); };
    const setKey = (obj, key, value) => { if (value === undefined || value === '' || value === null) delete obj[key]; else obj[key] = value; };

    let serializeTimer = null, parseTimer = null;
    function scheduleSerialize() { clearTimeout(serializeTimer); serializeTimer = setTimeout(flushSerialize, 250); yamlArea.readOnly = true; }
    async function flushSerialize() {
      clearTimeout(serializeTimer); serializeTimer = null;
      const c = S.cur;
      if (!c?.pendingEdit) { yamlArea.readOnly = false; return; }
      const serial = ++c.serial, doc = c.doc;
      const r = await request('POST', '/api/flow-documents/serialize', { document: doc });
      if (S.cur !== c || serial !== c.serial) return;
      yamlArea.readOnly = false;
      if (!r.ok) { notify(`Couldn't update the YAML: ${errorText(r)}`, true); return; }
      c.yaml = r.data.yaml; c.goodYaml = r.data.yaml; c.errors = r.data.errors || []; c.lossless = true; c.pendingEdit = false; c.yamlInvalid = false;
      if (yamlArea.value !== c.yaml) yamlArea.value = c.yaml;
      renderList(); renderTop(); renderBanner(); renderGraph(); renderSteps(); renderProblems(); updateInspectorErrors();
    }
    function onYamlInput() {
      const c = S.cur;
      if (!c) return;
      c.yaml = yamlArea.value; c.yamlEdited = true;
      clearTimeout(parseTimer); parseTimer = setTimeout(flushParse, 350);
      renderTop(); renderList();
    }
    async function flushParse() {
      clearTimeout(parseTimer); parseTimer = null;
      const c = S.cur;
      if (!c?.yamlEdited) return;
      const serial = ++c.serial, text = c.yaml;
      const r = await request('POST', '/api/flow-documents/parse', { yaml: text });
      if (S.cur !== c || serial !== c.serial) return;
      if (!r.ok) { notify(`Couldn't check the YAML: ${errorText(r)}`, true); return; }
      if (c.yaml === text) c.yamlEdited = false;
      c.errors = r.data.errors || [];
      if (r.data.document) {
        if (!r.data.lossless && c.lossless) c.losslessAck = false;
        c.doc = r.data.document; c.yamlInvalid = false; c.lossless = r.data.lossless !== false; c.goodYaml = text;
        if (S.sel?.type === 'step' && !c.doc.steps?.[S.sel.id]) S.sel = null;
        if (S.sel?.type === 'edge' && !needsOf(c.doc.steps?.[S.sel.to]).includes(S.sel.from)) S.sel = null;
        S.drafts = {};
        notify('');
        schedulePlacement();
      } else c.yamlInvalid = true;   // the last valid document stays as it was
      renderAll({ keepYaml: true });
    }
    async function flushPending() { await flushParse(); await flushSerialize(); }
    async function discardYaml() {
      const c = S.cur;
      if (!c) return;
      if (c.goodYaml != null) { c.yaml = c.goodYaml; yamlArea.value = c.yaml; c.yamlEdited = true; await flushParse(); }
      else if (c.doc) { c.yamlInvalid = false; c.pendingEdit = true; await flushSerialize(); }
    }

    // ---- fleet placement: the current document is checked the way the New card dialog launches
    // it (with a run-level selection, so every step is placed). Saving is never blocked by it.
    let placementTimer = null;
    function schedulePlacement() { clearTimeout(placementTimer); placementTimer = setTimeout(checkPlacement, 400); }
    async function checkPlacement() {
      const c = S.cur, P = S.placement;
      if (!S.visible || !c?.doc) return;
      const doc = c.doc, key = canonical(doc);
      if (key === P.key) return;
      Object.assign(P, { key, result: null, error: null });
      const serial = ++P.serial;
      const r = await request('POST', '/api/launch/validate', { document: doc, execution: {} });
      if (serial !== P.serial || S.cur !== c) return;
      Object.assign(P, r.ok ? { result: r.data, error: null } : { result: null, error: errorText(r) });
      renderSteps(); renderPlacement();
    }
    // The check of the document as it is now, or null while it is not back yet.
    const placementFor = () => (S.cur?.doc && S.placement.key === canonical(S.cur.doc) && S.placement.result ? S.placement.result : null);
    const decisionFor = (id) => placementFor()?.steps?.[id] || null;
    const ambiguous = (id) => !!decisionFor(id)?.reasons?.some((r) => r.code === 'ambiguous_command');
    function placementChip(id) {
      const d = decisionFor(id), status = d ? placementStatus(d) : null;
      return status ? h('span', { class: `fb-chip fb-placement-chip fb-chip-${status}`, title: placementHeadline(d) }, PLACEMENT_CHIPS[status] || status) : null;
    }
    function placementSource(id) {
      const doc = S.cur.doc, own = doc.steps[id]?.worker;
      const source = own?.id ? `Uses this step's own worker setting: worker ${own.id}, which overrides the flow default.`
        : own ? "Uses this step's own worker setting, on top of the default worker in Flow settings."
        : doc.worker ? 'Uses the default worker in Flow settings.' : 'No worker is selected: any registered worker can run it, or choose one when you start a run.';
      const blocked = decisionFor(id)?.status === 'blocked' ? ' You can save this draft; launch is blocked until this is resolved.' : '';
      return h('p', { class: 'fb-hint fb-placement-source' }, source, blocked);
    }
    function placementBox(id) {
      const result = placementFor();
      if (S.placement.error) return [h('p', { class: 'fb-hint' }, `Couldn't check the fleet: ${S.placement.error}. Saving still works.`)];
      if (!result) return [h('p', { class: 'fb-hint', role: 'status' }, 'Checking the fleet…')];
      if (result.fleet === false) return [placementView(result)];
      if (id === null) return [h('ul', { class: 'fb-placement-summary' }, ...stepIds(S.cur.doc).map((sid) => h('li', {}, h('strong', {}, sid), ' ', placementChip(sid), ' ',
        decisionFor(sid) ? placementHeadline(decisionFor(sid)) : '')))];
      const d = decisionFor(id);
      return d ? [placementView(d), placementSource(id)] : [h('p', { class: 'fb-hint' }, 'Not checked.')];
    }
    // Updates the open form in place, so typing is never interrupted by a check coming back.
    function renderPlacement() {
      const slot = inspector.querySelector('.fb-placement-slot');
      if (slot) slot.replaceChildren(h('h4', {}, slot.dataset.title), ...placementBox(slot.dataset.step || null));
      const wrap = inspector.querySelector('.fb-field[data-field="requires_command"]');
      if (wrap) {
        const needed = ambiguous(wrap.dataset.step), input = wrap.querySelector('input');
        wrap.dataset.extra = needed ? REQUIRES_COMMAND_ERROR : '';
        setFieldErrors(wrap, errorsFor(wrap.dataset.step, 'requires_command'));
        input.required = needed; input.setAttribute('aria-required', String(needed));
      }
    }
    const REQUIRES_COMMAND_ERROR = 'The executable cannot be inferred from this command, so the fleet cannot check it: declare it here, for example bash. Launch is blocked until you do.';

    // ---- saving
    async function save(overwrite) {
      const c = S.cur;
      if (!c) return;
      await flushPending();
      if (S.cur !== c) return;
      if (c.pendingEdit) { notify('Not saved: the YAML could not be updated from the graph. Try again.', true); return; }
      const name = c.yamlInvalid ? c.name : c.doc?.name;
      if (!name || !FLOW_NAME.test(name)) { notify('Not saved: give the flow a valid name (lowercase letters, digits and -).', true); select({ type: 'flow' }, { inspect: true }); return; }
      const renamed = !c.isNew && name !== c.name;
      const expected = overwrite !== undefined ? overwrite : c.isNew || renamed ? null : c.revision;
      const sent = c.yaml;
      saveBtn.disabled = true; notify('Saving draft…');
      const r = await request('PUT', `/api/flow-documents/${encodeURIComponent(name)}`, { yaml: sent, expectedRevision: expected });
      saveBtn.disabled = false;
      if (S.cur !== c) return;
      if (r.status === 409 && r.data?.current) { notify(''); await conflict(r.data.current, name); return; }
      if (r.status === 409) { notify(`Not saved: ${errorText(r)}. A flow named ${name} may already exist.`, true); return; }
      if (r.status === 400 && r.data?.errors) { c.errors = r.data.errors; notify(`Not saved: ${errorText(r)}`, true); renderAll({ keepYaml: true }); return; }
      if (!r.ok) { notify(`Save failed: ${errorText(r)}. Your changes are still here.`, true); renderTop(); return; }
      const d = r.data, old = c.name;
      Object.assign(c, { name: d.name, revision: d.revision, savedYaml: d.yaml ?? sent, state: d.state, activeRevision: d.activeRevision ?? null, isNew: false,
        errors: d.errors || c.errors, lossless: d.lossless !== false });
      if (c.yaml === sent && d.yaml != null && d.yaml !== sent) { c.yaml = d.yaml; c.savedYaml = d.yaml; yamlArea.value = d.yaml; }
      S.lastName = d.name;
      if (Object.keys(c.positions).length) saveLayout();
      await refreshList();
      notify(`Saved draft ${d.name}. ${d.state === 'active' ? 'It matches the active version.' : 'Nothing changes for new runs until you activate it.'}`);
      renderAll({ keepYaml: true });
      if (renamed && old && S.docs.some((x) => x.name === old)) {
        const del = await ask({ title: `Delete the old draft ${old}?`, body: [p(`The flow was saved under its new name, ${d.name}. The draft ${old} still exists.`)],
          buttons: [{ label: `Keep ${old}`, value: false }, { label: `Delete ${old}`, value: true, kind: 'danger' }] });
        if (del) {
          const meta = S.docs.find((x) => x.name === old);
          const x = await request('DELETE', `/api/flow-documents/${encodeURIComponent(old)}?revision=${encodeURIComponent(meta?.revision || '')}`);
          if (!x.ok) notify(`Couldn't delete ${old}: ${errorText(x)}`, true);
          await refreshList();
        }
      }
    }

    // A stale save: show both versions and let the user pick; never overwrite silently.
    async function conflict(current, name) {
      const c = S.cur, diff = compareDocuments(c.doc, current.document);
      const summary = (doc) => (doc ? `${plural(stepIds(doc).length, 'step')}: ${stepIds(doc).join(', ')}` : 'unreadable YAML');
      const list = (label, ids) => (ids.length ? h('li', {}, `${label}: ${ids.join(', ')}`) : null);
      const choice = await ask({ title: `${name} changed since you opened it`, body: [
        p('Someone (another tab, another person, or a file edit) saved a newer draft. Choose which version to keep.'),
        h('div', { class: 'fb-compare' },
          h('section', {}, h('h4', {}, 'Saved version'), p(`Revision ${shortRev(current.revision)}${current.state ? ` · ${(STATES[current.state] || [current.state])[0]}` : ''}`), p(summary(current.document))),
          h('section', {}, h('h4', {}, 'Your version'), p(`Based on revision ${shortRev(c.revision)}`), p(summary(c.doc)))),
        h('ul', { class: 'fb-diff' }, list('Only in yours', diff.onlyMine), list('Only in the saved version', diff.onlyTheirs), list('Different', diff.changed),
          list('Different flow settings', diff.flow))],
      buttons: [{ label: 'Keep editing', value: 'keep' }, { label: 'Load the saved version', value: 'reload', kind: 'secondary' },
        { label: 'Overwrite with mine', value: 'overwrite', kind: 'danger' }] });
      if (S.cur !== c) return;
      if (choice === 'reload') { setCurrent(fromDoc(current)); await refreshList(); notify(`Loaded the saved version of ${name}.`); }
      else if (choice === 'overwrite') await save(current.revision ?? null);
    }

    let layoutTimer = null;
    function scheduleLayoutSave() { clearTimeout(layoutTimer); layoutTimer = setTimeout(saveLayout, 600); }
    // Positions are view metadata: saved on their own, never dirty the draft or activate anything.
    async function saveLayout() {
      clearTimeout(layoutTimer);
      const c = S.cur;
      if (!c || c.isNew || !c.name || !c.revision || !c.doc) return; // only an existing draft has a layout
      const ids = new Set(stepIds(c.doc));
      const positions = Object.fromEntries(Object.entries(c.positions).filter(([id]) => ids.has(id)));
      const r = await request('PUT', `/api/flow-documents/${encodeURIComponent(c.name)}/layout`, { layout: { version: 1, positions } });
      if (!r.ok && S.cur === c) notify(`Couldn't save step positions: ${errorText(r)}`, true);
    }

    async function removeDraft() {
      const c = S.cur;
      if (!c?.name || c.isNew) return;
      const yes = await ask({ title: `Delete the draft ${c.name}?`, body: [p('The active version, if any, keeps serving new runs until you activate the removal. Past runs stay in each backend.')],
        buttons: [{ label: 'Keep it', value: false }, { label: 'Delete draft', value: true, kind: 'danger' }] });
      if (!yes || S.cur !== c) return;
      const r = await request('DELETE', `/api/flow-documents/${encodeURIComponent(c.name)}?revision=${encodeURIComponent(c.revision || '')}`);
      if (r.status === 409) { notify('Not deleted: the draft changed since you opened it. Reload it first.', true); return; }
      if (!r.ok) { notify(`Couldn't delete: ${errorText(r)}`, true); return; }
      const name = c.name;
      S.cur = null;
      await refreshList();
      const next = S.docs.find((d) => d.name === name)?.name || S.docs[0]?.name;
      if (next) await load(next); else setCurrent(null);
      notify(`Deleted the draft ${name}.`);
    }

    async function newFlow() {
      if (!(await guard())) return;
      const nameInput = h('input', { value: '', placeholder: 'my-flow', autocomplete: 'off', autocapitalize: 'off', spellcheck: 'false', 'aria-describedby': 'fb-new-hint' });
      const hint = h('small', { id: 'fb-new-hint', class: 'fb-hint' }, 'Lowercase letters, digits and -.');
      const template = h('select', {}, ...Object.entries(TEMPLATES).map(([k, t]) => h('option', { value: k }, t.label)),
        ...S.docs.filter((d) => d.document || d.state !== 'removed').map((d) => h('option', { value: `copy:${d.name}` }, `Copy of ${d.name}`)));
      const choice = await ask({ title: 'New flow', body: [h('label', {}, 'Name', nameInput, hint), h('label', {}, 'Start from', template)],
        buttons: [{ label: 'Cancel', value: null }, { label: 'Create', kind: 'primary', value: () => {
          const name = nameInput.value.trim();
          if (!FLOW_NAME.test(name) || S.docs.some((d) => d.name === name)) {
            hint.textContent = FLOW_NAME.test(name) ? `A flow named ${name} already exists.` : 'Use lowercase letters, digits and -, starting with a letter.';
            hint.classList.add('error'); nameInput.setAttribute('aria-invalid', 'true'); nameInput.focus(); return undefined;
          }
          return { name, template: template.value };
        } }] });
      if (!choice) return;
      let doc;
      if (choice.template.startsWith('copy:')) {
        const source = choice.template.slice(5), r = await request('GET', `/api/flow-documents/${encodeURIComponent(source)}`);
        if (!r.ok || !r.data.document) { notify(`Couldn't copy ${source}: ${errorText(r)}`, true); return; }
        doc = { ...clone(r.data.document), name: choice.name };
      } else doc = TEMPLATES[choice.template].make(choice.name);
      setCurrent({ name: null, revision: null, savedYaml: null, yaml: '', goodYaml: null, doc, errors: [], lossless: true, losslessAck: true, yamlInvalid: false,
        positions: {}, state: null, activeRevision: null, pendingEdit: true, yamlEdited: false, serial: 0, isNew: true, fitPending: true, stash: {} });
      await flushSerialize();
      notify(`New flow ${choice.name}: not saved yet.`);
    }

    // ---- activation
    async function activate() {
      const c = S.cur;
      if (!c?.name || c.isNew) return;
      if (isDirty()) { notify('Save the draft before activating it.', true); return; }
      notify('Checking what activation affects…');
      const r = await request('GET', `/api/flow-documents/${encodeURIComponent(c.name)}/activation`);
      if (S.cur !== c) return;
      if (!r.ok) { notify(`Couldn't prepare activation: ${errorText(r)}`, true); return; }
      notify('');
      await preview(r.data);
    }
    function engineItems(engines) {
      return (engines || []).map((e) => {
        const label = ENGINE_LABELS[e.engine] || e.engine;
        const what = e.action === 'none' ? 'no change' : e.action === 'reload worker' ? 'reloads its flow worker' : e.action === 'regenerate tasks' ? 'regenerates its tasks' : e.action;
        const runs = e.action === 'none' ? null : e.activeRuns === null || e.activeRuns === undefined
          ? h('p', { class: 'fb-unknown' }, 'Active runs: unknown. The engine could not be checked, so running work may be interrupted.')
          : e.activeRuns.length ? h('div', {}, p(`${plural(e.activeRuns.length, 'active run')} will be interrupted:`),
            h('ul', { class: 'fb-runs' }, ...e.activeRuns.map((run) => h('li', {}, h('strong', {}, run.title || run.id), ` · ${run.status || 'running'} · ${run.board || ''} · `, h('code', {}, run.id)))))
            : p('No active runs.');
        return h('li', { class: 'fb-engine' }, h('strong', {}, label), ` ${what}${e.available === false ? ' · engine unavailable' : ''}`, runs);
      });
    }
    async function preview(pv, note) {
      const c = S.cur, affected = (pv.engines || []).filter((e) => e.action !== 'none');
      const known = affected.reduce((n, e) => n + (e.activeRuns?.length || 0), 0), unknown = affected.some((e) => e.activeRuns == null);
      const change = { add: 'adds this flow', update: 'replaces the active version', remove: 'removes the active version', none: 'changes nothing' }[pv.change] || pv.change;
      const body = [note ? h('p', { class: 'fb-note' }, note) : null,
        p(`Activating ${pv.name} ${change}${pv.change === 'update' ? ` (${shortRev(pv.activeRevision)} → ${shortRev(pv.revision)})` : ''}. New runs use the activated version; runs keep the version they started with.`),
        h('ul', { class: 'fb-engines' }, ...engineItems(pv.engines)),
        pv.disruptive ? h('p', { class: 'fb-warning' }, 'Reloading these workers interrupts the work they are running. Nothing is cancelled for you; choose Not now to activate later.') : null];
      const label = !pv.disruptive ? 'Activate' : `Activate and interrupt ${plural(known, 'run')}${unknown ? ' (and any unknown)' : ''}`;
      if (pv.errors?.length) body.push(h('p', { class: 'fb-warning' }, `The draft has ${plural(pv.errors.length, 'problem')}, so it cannot be activated. Fix and save it first.`));
      const choice = await ask({ title: `Activate ${pv.name}?`, body, buttons: pv.change === 'none' || pv.errors?.length ? [{ label: 'Close', value: false }]
        : [{ label: 'Not now', value: false }, { label, value: true, kind: pv.disruptive ? 'danger' : 'primary' }] });
      if (!choice || S.cur !== c) return;
      notify('Activating…');
      const r = await request('POST', `/api/flow-documents/${encodeURIComponent(pv.name)}/activate`, { revision: pv.revision, impact: pv.impact, confirm: !!pv.disruptive });
      if (S.cur !== c) return;
      if (r.status === 409 && r.data?.preview) {
        notify('');
        const why = { 'impact changed': 'What activation affects changed since you looked. Review it again.', 'confirmation required': 'This activation interrupts work and needs your confirmation.',
          conflict: 'The draft changed since the preview. Review the current version.' }[r.data.error] || r.data.error;
        await preview(r.data.preview, why); return;
      }
      if (!r.ok) { notify(`Activation failed: ${errorText(r)}`, true); return; }
      notify('');
      await results(r.data);
      await refreshList();
      if (S.cur === c && !isDirty()) await load(c.name);
      options.onActivated?.(r.data);
    }
    function results(result) {
      const label = { active: 'active', pending: 'pending: will apply when the engine is ready', failed: 'failed: the previous version stays in force', unavailable: 'unavailable: the engine could not be reached' };
      return ask({ title: `Activation of ${result.name}`, body: [
        h('ul', { class: 'fb-engines' }, ...(result.engines || []).map((e) => h('li', { class: `fb-engine fb-result-${e.state}` }, h('strong', {}, ENGINE_LABELS[e.engine] || e.engine), ` ${label[e.state] || e.state}`, e.error ? h('p', { class: 'fb-unknown' }, e.error) : null))),
        (result.engines || []).some((e) => e.state !== 'active') ? p('Activating again retries the engines that did not finish; it is safe to repeat.') : null],
      buttons: [{ label: 'Close', value: true }] });
    }
    function run() {
      const c = S.cur;
      if (!c?.activeRevision || !c.name) return;
      options.onRun?.(c.name);
    }

    // ---- selection and views
    function select(sel, { inspect = false } = {}) {
      S.sel = sel; S.drafts = {}; S.picker = null;
      if (inspect && narrow()) S.inspecting = true;
      renderGraph(); renderSteps(); renderInspector(); renderLayout();
      if (inspect && narrow()) inspector.querySelector('h3')?.focus();
    }
    function closeInspector() {
      const sel = S.sel;
      S.inspecting = false; renderLayout();
      const row = sel?.type === 'step' && stepsView.querySelector(`[data-row="${CSS.escape(sel.id)}"] .fb-step-open`);
      (row || tabs.find((t) => t.dataset.view === S.view)).focus();
    }
    async function setView(view) {
      if (view === 'yaml') await flushSerialize(); else await flushParse();
      S.view = view; S.inspecting = false;
      renderLayout();
      if (view === 'graph') { renderGraph(); if (S.cur?.fitPending) fit(); }
      if (view === 'steps') renderSteps();
    }
    function tabKey(e) {
      const i = tabs.indexOf(e.currentTarget);
      if (e.key !== 'ArrowRight' && e.key !== 'ArrowLeft') return;
      e.preventDefault();
      const next = tabs[(i + (e.key === 'ArrowRight' ? 1 : tabs.length - 1)) % tabs.length];
      next.focus(); setView(next.dataset.view);
    }

    // ---- graph edits
    function positionsNow() { return layout(S.cur.doc, S.cur.positions).positions; }
    // Manual positions are kept once the user places a step; until then the graph follows auto-layout.
    function freeze() { const c = S.cur; if (!Object.keys(c.positions).length) c.positions = positionsNow(); }
    function freeSpot(want) {
      const taken = Object.values(positionsNow());
      const spot = { x: Math.round(want.x), y: Math.round(want.y) };
      while (taken.some((q) => Math.abs(q.x - spot.x) < NODE_W + 8 && Math.abs(q.y - spot.y) < NODE_H + 8)) spot.x += NODE_W + GAP_X;
      return spot;
    }
    async function addNewStep(kind, after) {
      const c = S.cur;
      if (!c?.doc) return;
      const frozen = Object.keys(c.positions).length > 0;
      let spot = null;
      if (frozen) {
        const pos = positionsNow();
        if (after && pos[after]) spot = freeSpot({ x: pos[after].x, y: pos[after].y + NODE_H + GAP_Y });
        else {
          const rect = svgEl.getBoundingClientRect();
          spot = rect.width ? freeSpot({ x: (rect.width / 2 - S.vp.x) / S.vp.k - NODE_W / 2, y: (rect.height / 2 - S.vp.y) / S.vp.k - NODE_H / 2 })
            : freeSpot({ x: PAD, y: bounds(pos).minY + bounds(pos).height + GAP_Y });
        }
      }
      const result = await edit((doc) => addStep(doc, { kind, needs: after ? [after] : [] }));
      if (!result?.ok) return;
      if (spot) { c.positions[result.id] = spot; scheduleLayoutSave(); }
      notify(`Added ${result.id}.`);
      select({ type: 'step', id: result.id }, { inspect: true });
      if (S.view === 'graph') reveal(result.id);
      if (!narrow()) inspector.querySelector('[data-focus="step:id"]')?.focus();
    }
    async function connect(from, to) {
      const result = await edit((doc) => addDependency(doc, from, to), { inspector: true });
      if (result?.ok && !result.unchanged) notify(`${to} now waits for ${from}.`);
      else if (result?.unchanged) notify(`${to} already waits for ${from}.`);
    }
    async function disconnect(from, to, { confirm = false } = {}) {
      if (confirm) {
        const yes = await ask({ title: 'Remove this dependency?', body: [p(`${to} will no longer wait for ${from}, and can no longer use its output.`)],
          buttons: [{ label: 'Keep it', value: false }, { label: 'Remove dependency', value: true, kind: 'danger' }] });
        if (!yes) { focusGraphItem(`edge:${from}>${to}`); return; }
      }
      const result = await edit((doc) => removeDependency(doc, from, to), { inspector: true });
      if (result?.ok) {
        if (S.sel?.type === 'edge' && S.sel.from === from && S.sel.to === to) select({ type: 'step', id: to });
        notify(`${to} no longer waits for ${from}.`);
      }
    }
    async function confirmRemoveStep(id) {
      const c = S.cur, doc = c?.doc;
      if (!doc?.steps?.[id]) return;
      const dependents = stepIds(doc).filter((sid) => needsOf(doc.steps[sid]).includes(id));
      const refs = referencesTo(doc, id).filter((sid) => sid !== id);
      const yes = await ask({ title: `Delete step ${id}?`, body: [
        dependents.length ? p(`It is also removed from what these steps wait for: ${dependents.join(', ')}.`) : null,
        refs.length ? p(`These prompts or commands still use its output and will show errors until you edit them: ${refs.join(', ')}.`) : null,
        p('You can undo this by not saving the draft.')],
      buttons: [{ label: 'Keep step', value: false }, { label: 'Delete step', value: true, kind: 'danger' }] });
      if (!yes) { focusGraphItem(`step:${id}`); return; }
      const result = await edit((d) => removeStep(d, id));
      if (!result?.ok) return;
      delete c.positions[id];
      S.sel = null;
      notify(`Deleted ${id}.`);
      renderAll({ keepYaml: true });
      if (S.view === 'graph') svgEl.focus();
    }
    function nudge(id, key, by) {
      freeze();
      const at = S.cur.positions[id];
      S.cur.positions[id] = { x: at.x + (key === 'ArrowLeft' ? -by : key === 'ArrowRight' ? by : 0), y: at.y + (key === 'ArrowUp' ? -by : key === 'ArrowDown' ? by : 0) };
      renderGraph(); scheduleLayoutSave();
    }
    function autoArrange() {
      const c = S.cur;
      if (!c?.doc) return;
      c.positions = {};
      renderGraph(); fit(); scheduleLayoutSave();
      notify('Arranged by dependency depth.');
    }

    // ---- viewport
    function applyViewport() {
      svgEl.querySelector('.fb-viewport')?.setAttribute('transform', `translate(${S.vp.x},${S.vp.y}) scale(${S.vp.k})`);
      zoomLabel.textContent = `${Math.round(S.vp.k * 100)}%`;
    }
    function zoomAt(k, cx, cy) {
      const next = Math.min(2.5, Math.max(0.25, k));
      S.vp.x = cx - ((cx - S.vp.x) / S.vp.k) * next; S.vp.y = cy - ((cy - S.vp.y) / S.vp.k) * next; S.vp.k = next;
    }
    function zoomBy(f) { const r = svgEl.getBoundingClientRect(); zoomAt(S.vp.k * f, r.width / 2, r.height / 2); renderGraph(); }
    // Pan just enough to show a step whole. The zoom stays unless the step can't fit at it.
    function reveal(id) {
      const at = positionsNow()[id], rect = svgEl.getBoundingClientRect();
      if (!at || !rect.width || !rect.height) return;
      const margin = 16, fits = Math.min((rect.width - 2 * margin) / NODE_W, (rect.height - 2 * margin) / NODE_H);
      if (S.vp.k > fits) zoomAt(fits, rect.width / 2, rect.height / 2);
      const w = NODE_W * S.vp.k, h = NODE_H * S.vp.k;
      const left = at.x * S.vp.k + S.vp.x, top = at.y * S.vp.k + S.vp.y;
      const shift = (start, size, room) => size + 2 * margin > room ? (room - size) / 2 - start
        : start < margin ? margin - start : start + size > room - margin ? room - margin - (start + size) : 0;
      S.vp.x += shift(left, w, rect.width); S.vp.y += shift(top, h, rect.height);
      applyViewport();
    }
    function fit() {
      const c = S.cur, rect = svgEl.getBoundingClientRect();
      if (!c?.doc || !rect.width || !rect.height) return;
      c.fitPending = false;
      const b = bounds(positionsNow());
      if (!b.width) { S.vp = { x: 0, y: 0, k: 1 }; renderGraph(); return; }
      const k = Math.min(1, Math.max(0.25, Math.min((rect.width - 32) / b.width, (rect.height - 32) / b.height)));
      S.vp = { k, x: (rect.width - b.width * k) / 2 - b.minX * k, y: Math.max(16, (rect.height - b.height * k) / 2) - b.minY * k };
      renderGraph();
    }

    // ---- pointer gestures on the canvas: move, connect, pan, pinch; taps select.
    const pointers = new Map();
    let gesture = null;
    const local = (e) => { const r = svgEl.getBoundingClientRect(); return { x: e.clientX - r.left, y: e.clientY - r.top }; };
    const toGraph = (pt) => ({ x: (pt.x - S.vp.x) / S.vp.k, y: (pt.y - S.vp.y) / S.vp.k });
    svgEl.addEventListener('pointerdown', (e) => {
      if (e.pointerType === 'mouse' && e.button !== 0) return;
      if (!S.cur?.doc) return;
      try { svgEl.setPointerCapture(e.pointerId); } catch {}
      pointers.set(e.pointerId, local(e));
      if (pointers.size === 2) {
        const [a, b] = [...pointers.values()];
        gesture = { mode: 'pinch', dist: Math.hypot(a.x - b.x, a.y - b.y) || 1, mid: { x: (a.x + b.x) / 2, y: (a.y + b.y) / 2 }, vp: { ...S.vp } };
        clearPreview(); return;
      }
      if (pointers.size > 2) return;
      const handle = e.target.closest?.('[data-handle]'), node = e.target.closest?.('[data-step]'), edge = e.target.closest?.('[data-edge]');
      const start = local(e);
      if (handle) { e.preventDefault(); gesture = { mode: 'connect', from: handle.dataset.handle, start, moved: false }; }
      else if (node) gesture = { mode: 'move', id: node.dataset.step, start, origin: { ...positionsNow()[node.dataset.step] }, moved: false };
      else if (edge) gesture = { mode: 'edge', from: edge.dataset.from, to: edge.dataset.to, start, moved: false };
      else { gesture = { mode: 'pan', start, vp: { ...S.vp }, moved: false }; e.preventDefault(); svgEl.focus({ preventScroll: true }); }
    });
    svgEl.addEventListener('pointermove', (e) => {
      if (!pointers.has(e.pointerId) || !gesture) return;
      const pt = local(e);
      pointers.set(e.pointerId, pt);
      if (gesture.mode === 'pinch') {
        if (pointers.size < 2) return;
        const [a, b] = [...pointers.values()], mid = { x: (a.x + b.x) / 2, y: (a.y + b.y) / 2 };
        S.vp = { ...gesture.vp, x: gesture.vp.x + mid.x - gesture.mid.x, y: gesture.vp.y + mid.y - gesture.mid.y };
        zoomAt(gesture.vp.k * (Math.hypot(a.x - b.x, a.y - b.y) / gesture.dist), mid.x, mid.y);
        applyViewport(); return;
      }
      const dx = pt.x - gesture.start.x, dy = pt.y - gesture.start.y;
      if (!gesture.moved && Math.hypot(dx, dy) < 5) return;
      gesture.moved = true;
      if (gesture.mode === 'pan') { S.vp = { ...gesture.vp, x: gesture.vp.x + dx, y: gesture.vp.y + dy }; applyViewport(); }
      else if (gesture.mode === 'move') {
        freeze();
        S.cur.positions[gesture.id] = { x: Math.round(gesture.origin.x + dx / S.vp.k), y: Math.round(gesture.origin.y + dy / S.vp.k) };
        renderGraph();
      } else if (gesture.mode === 'connect') {
        const from = positionsNow()[gesture.from], end = toGraph(pt);
        const x1 = from.x + NODE_W / 2, y1 = from.y + NODE_H;
        svgEl.querySelector('.fb-preview')?.setAttribute('d', `M${x1},${y1} C${x1},${y1 + 40} ${end.x},${end.y - 40} ${end.x},${end.y}`);
        const over = dropTarget(e);
        svgEl.querySelectorAll('.fb-node.drop').forEach((n) => n.classList.remove('drop'));
        if (over && over !== gesture.from) svgEl.querySelector(`[data-step="${CSS.escape(over)}"]`)?.classList.add('drop');
      }
    });
    const dropTarget = (e) => container.ownerDocument.elementFromPoint(e.clientX, e.clientY)?.closest?.('[data-step]')?.dataset.step;
    function clearPreview() { svgEl.querySelector('.fb-preview')?.setAttribute('d', ''); svgEl.querySelectorAll('.fb-node.drop').forEach((n) => n.classList.remove('drop')); }
    function endGesture(e) {
      if (!pointers.has(e.pointerId)) return;
      pointers.delete(e.pointerId);
      const g = gesture;
      if (!g) return;
      if (g.mode === 'pinch') { if (!pointers.size) { gesture = null; renderGraph(); } return; }
      gesture = null;
      if (e.type === 'pointercancel') { clearPreview(); return; }
      if (g.mode === 'connect') {
        clearPreview();
        const to = g.moved && dropTarget(e);
        if (to && to !== g.from) connect(g.from, to);
        else if (g.moved && to === g.from) notify(`${g.from} cannot depend on itself.`, true);
        else if (!g.moved) select({ type: 'step', id: g.from }, { inspect: true });
      } else if (g.mode === 'move') { if (g.moved) scheduleLayoutSave(); else select({ type: 'step', id: g.id }, { inspect: true }); }
      else if (g.mode === 'edge') { if (!g.moved) select({ type: 'edge', from: g.from, to: g.to }, { inspect: true }); }
      else if (g.mode === 'pan' && !g.moved) select({ type: 'flow' });
    }
    svgEl.addEventListener('pointerup', endGesture);
    svgEl.addEventListener('pointercancel', endGesture);
    svgEl.addEventListener('wheel', (e) => {
      if (!S.cur?.doc) return;
      e.preventDefault();
      if (e.ctrlKey || e.metaKey) { const pt = local(e); zoomAt(S.vp.k * Math.exp(-e.deltaY / 300), pt.x, pt.y); renderGraph(); }
      else { S.vp.x -= e.deltaX; S.vp.y -= e.deltaY; applyViewport(); }
    }, { passive: false });
    svgEl.addEventListener('keydown', (e) => {
      if (e.target !== svgEl) return;
      const step = 40;
      const moves = { ArrowLeft: [step, 0], ArrowRight: [-step, 0], ArrowUp: [0, step], ArrowDown: [0, -step] };
      if (moves[e.key]) { e.preventDefault(); S.vp.x += moves[e.key][0]; S.vp.y += moves[e.key][1]; applyViewport(); }
      else if (e.key === '+' || e.key === '=') { e.preventDefault(); zoomBy(1.25); }
      else if (e.key === '-') { e.preventDefault(); zoomBy(1 / 1.25); }
    });

    function focusGraphItem(key) {
      const [type, rest] = [key.slice(0, key.indexOf(':')), key.slice(key.indexOf(':') + 1)];
      let node = null;
      if (type === 'step') node = svgEl.querySelector(`[data-step="${CSS.escape(rest)}"]`);
      else if (type === 'edge') { const [from, to] = rest.split('>'); node = svgEl.querySelector(`[data-edge][data-from="${CSS.escape(from)}"][data-to="${CSS.escape(to)}"]`); }
      if (S.view === 'graph' && node) node.focus({ preventScroll: true });
    }

    // ---- rendering
    function renderAll({ keepYaml = false } = {}) {
      if (!keepYaml && S.cur && yamlArea.value !== S.cur.yaml) yamlArea.value = S.cur.yaml;
      renderList(); renderTop(); renderLayout(); renderBanner(); renderGraph(); renderSteps(); renderInspector(); renderProblems();
      if (S.view === 'graph' && S.cur?.fitPending) fit();
    }
    function renderLayout() {
      container.classList.toggle('fb-inspecting', S.inspecting);
      for (const t of tabs) { const on = t.dataset.view === S.view; t.setAttribute('aria-selected', String(on)); t.tabIndex = on ? 0 : -1; }
      graphView.hidden = S.view !== 'graph'; stepsView.hidden = S.view !== 'steps'; yamlView.hidden = S.view !== 'yaml';
    }
    function renderList() {
      const c = S.cur;
      const item = (name, state, current) => h('li', {}, h('button', { type: 'button', class: 'fb-flow-item', 'aria-current': current ? 'true' : null,
        title: STATES[state]?.[1] || '', onclick: () => (name ? openDoc(name) : null) },
      h('span', { class: 'fb-flow-name' }, name || c.doc?.name || 'new flow'),
      current && isDirty() ? h('span', { class: 'fb-dirty', title: 'Unsaved changes' }, h('span', { 'aria-hidden': 'true' }, '●'), h('span', { class: 'fb-sr' }, ' unsaved changes')) : null,
      state ? h('span', { class: `fb-chip fb-state-${state}` }, (STATES[state] || [state])[0]) : h('span', { class: 'fb-chip' }, 'Unsaved')));
      flowList.replaceChildren(...(c?.isNew ? [item(null, null, true)] : []), ...S.docs.map((d) => item(d.name, d.state, !!c && !c.isNew && c.name === d.name)));
    }
    function renderTop() {
      const c = S.cur, dirty = isDirty();
      title.textContent = c ? (c.doc?.name || c.name || 'New flow') : 'Flows';
      stateLine.replaceChildren(...(c ? [
        c.state ? h('span', { class: `fb-chip fb-state-${c.state}`, title: STATES[c.state]?.[1] || '' }, STATES[c.state]?.[0] || c.state) : null,
        h('span', { class: `fb-state-text${dirty ? ' fb-dirty-text' : ''}` }, dirty ? (c.isNew ? 'Not saved yet' : 'Unsaved changes') : c.state ? STATES[c.state]?.[1] || '' : '')].filter(Boolean) : [h('span', {}, 'Pick a flow or create one.')]));
      saveBtn.disabled = !c || (!dirty && !c.isNew);
      const blocked = !c || c.isNew ? 'Save the new flow first' : dirty ? 'Save the draft before activating it' : c.state === 'active' ? 'This draft is already active'
        : c.state === 'broken' ? 'Fix the errors and save before activating' : null;
      activateBtn.disabled = !!blocked; activateBtn.title = blocked || 'Review what activation affects, then activate';
      runBtn.disabled = !c?.activeRevision || c.isNew; runBtn.title = runBtn.disabled ? 'Activate the flow before running it' : 'Start a run of the active version';
      deleteBtn.hidden = !c || c.isNew || !c.revision;
    }
    function renderBanner() {
      const c = S.cur, parts = [];
      if (c?.yamlInvalid && c.doc) parts.push(h('p', { class: 'fb-warning' }, 'The YAML has errors. The graph shows the last valid version and editing it is paused until the YAML is fixed. ',
        h('button', { type: 'button', class: 'secondary', onclick: () => discardYaml() }, 'Discard YAML changes')));
      else if (c?.yamlInvalid) parts.push(h('p', { class: 'fb-warning' }, 'This draft’s YAML cannot be read. Fix it in the YAML tab.'));
      if (c && !c.lossless && !c.losslessAck && !c.yamlInvalid) parts.push(h('p', {}, 'This YAML has comments or formatting the graph editor cannot keep. The first graph or form edit rewrites it as canonical YAML (you will be asked first); YAML-tab edits are kept as typed.'));
      if (c?.state === 'removed') parts.push(h('p', { class: 'fb-warning' }, `${STATES.removed[1]} `, c.doc || c.yaml ? 'Saving restores it as a draft. ' : '',
        c.doc || c.yaml ? h('button', { type: 'button', class: 'secondary', onclick: () => save() }, 'Restore as draft') : null));
      banner.hidden = !parts.length; banner.replaceChildren(...parts);
    }
    function renderProblems() {
      const errors = S.cur?.errors || [];
      problems.hidden = !errors.length;
      const where = (e) => (e.step ? `${e.step}${e.field ? ` · ${e.field}` : ''}` : e.field ? `flow · ${e.field}` : 'YAML');
      problems.replaceChildren(h('h3', {}, plural(errors.length, 'problem')),
        h('ul', {}, ...errors.map((e) => h('li', {}, h('button', { type: 'button', class: 'fb-problem', onclick: () => goToError(e) }, h('strong', {}, where(e)), ' ', e.message)))));
    }
    function goToError(e) {
      const c = S.cur;
      if (e.step && c?.doc?.steps?.[e.step]) select({ type: 'step', id: e.step }, { inspect: true });
      else if (!e.step && e.field && c?.doc) select({ type: 'flow' }, { inspect: true });
      else { setView('yaml'); yamlArea.focus(); return; }
      const field = e.field && inspector.querySelector(`[data-field="${CSS.escape(e.field)}"] :is(input, textarea, select, button)`);
      (field || inspector.querySelector('h3'))?.focus();
    }

    function renderGraph() {
      const c = S.cur;
      if (!c?.doc || !stepIds(c.doc).length) {
        svgEl.replaceChildren();
        graphEmpty.hidden = false;
        graphEmpty.textContent = !c ? 'Pick a flow or create one.' : !c.doc ? 'The YAML cannot be read, so there is no graph to show. Fix it in the YAML tab.' : 'No steps yet. Add a program or agent step.';
        return;
      }
      graphEmpty.hidden = true;
      const doc = c.doc, pos = positionsNow(), ids = stepIds(doc), k = S.vp.k;
      const active = container.ownerDocument.activeElement;
      const focusKey = active?.dataset?.step ? `step:${active.dataset.step}` : active?.dataset?.edge ? `edge:${active.dataset.from}>${active.dataset.to}` : null;
      const edges = [];
      for (const to of ids) for (const from of needsOf(doc.steps[to])) if (pos[from] && from !== to) edges.push({ from, to });
      const anchor = (end, other) => {
        const groups = {}, at = new Map();
        for (const e of edges) (groups[e[end]] ??= []).push(e);
        for (const list of Object.values(groups)) {
          list.sort((a, b) => pos[a[other]].x - pos[b[other]].x);
          list.forEach((e, i) => at.set(e, (NODE_W * (i + 1)) / (list.length + 1)));
        }
        return at;
      };
      const outX = anchor('from', 'to'), inX = anchor('to', 'from');
      const edgeEls = edges.map((e) => {
        const a = pos[e.from], b = pos[e.to];
        const x1 = a.x + outX.get(e), y1 = a.y + NODE_H, x2 = b.x + inX.get(e), y2 = b.y;
        const bend = Math.max(30, Math.abs(y2 - y1) / 2);
        // A dependency that points upwards (a cycle from YAML, or a manual layout) goes round the side.
        const side = Math.max(a.x, b.x) + NODE_W + 28;
        const d = y2 >= y1 + 12 ? `M${x1},${y1} C${x1},${y1 + bend} ${x2},${y2 - bend} ${x2},${y2 - 3}`
          : `M${x1},${y1} C${x1},${y1 + 50} ${side},${y1 + 50} ${side},${(y1 + y2) / 2} S${x2},${y2 - 50} ${x2},${y2 - 3}`;
        const selected = S.sel?.type === 'edge' && S.sel.from === e.from && S.sel.to === e.to;
        // Not in the tab order (Tab walks the steps); keyboard users edit dependencies in the step form.
        return s('g', { class: `fb-edge${selected ? ' selected' : ''}`, 'data-edge': '1', 'data-from': e.from, 'data-to': e.to, tabindex: '-1', role: 'button',
          'aria-pressed': String(selected), 'aria-label': `Dependency: ${e.to} waits for ${e.from}. Delete removes it.`,
          onkeydown: (ev) => edgeKey(ev, e.from, e.to) },
        s('title', {}, `${e.to} waits for ${e.from}`),
        s('path', { class: 'hit', d, 'stroke-width': Math.max(16, 24 / k) }), s('path', { class: 'line', d, 'marker-end': 'url(#fb-arrow)' }));
      });
      const nodeEls = ids.map((id) => {
        const step = doc.steps[id], p0 = pos[id], errs = errorsFor(id), selected = S.sel?.type === 'step' && S.sel.id === id;
        const kind = step?.kind === 'agent' ? 'agent' : step?.kind === 'program' ? 'program' : 'unknown';
        const sub = kind === 'agent' ? `agent · ${step.agent || 'demo'}${step.review ? ' · review' : ''}` : kind === 'program' ? 'program' : String(step?.kind ?? 'unknown kind');
        const needs = needsOf(step);
        const hit = Math.max(23, 23 / k); // at least a 44px touch target at any zoom
        return s('g', { class: `fb-node ${kind}${selected ? ' selected' : ''}${errs.length ? ' has-error' : ''}`, 'data-step': id, transform: `translate(${p0.x},${p0.y})`,
          tabindex: '0', role: 'button', 'aria-pressed': String(selected),
          'aria-label': `${id}, ${kind} step${needs.length ? `, waits for ${needs.join(', ')}` : ', starts first'}${errs.length ? `, ${plural(errs.length, 'problem')}` : ''}`,
          onkeydown: (ev) => nodeKey(ev, id) },
        s('title', {}, `${id}${step?.description ? ` — ${step.description}` : ''}${errs.length ? `\n${errs.map((x) => x.message).join('\n')}` : ''}`),
        s('rect', { class: 'box', width: NODE_W, height: NODE_H, rx: 10 }),
        s('rect', { class: 'stripe', x: 5, y: 6, width: 5, height: NODE_H - 12, rx: 2 }),
        s('text', { class: 'label', x: 18, y: 24 }, short(id, 20)),
        s('text', { class: 'sub', x: 18, y: 42 }, short(sub, 26)),
        errs.length ? s('g', { class: 'badge', 'aria-hidden': 'true' }, s('circle', { cx: NODE_W - 14, cy: 14, r: 9 }), s('text', { x: NODE_W - 14, y: 18, 'text-anchor': 'middle' }, '!')) : null,
        s('circle', { class: 'in', cx: NODE_W / 2, cy: 0, r: 4 }),
        s('g', { class: 'handle', 'data-handle': id, 'aria-hidden': 'true' }, s('circle', { class: 'hit', cx: NODE_W / 2, cy: NODE_H, r: hit }), s('circle', { class: 'dot', cx: NODE_W / 2, cy: NODE_H, r: 7 })));
      });
      const marker = s('marker', { id: 'fb-arrow', viewBox: '0 0 10 10', refX: 8, refY: 5, markerWidth: 7, markerHeight: 7, orient: 'auto-start-reverse' }, s('path', { d: 'M0,0 L10,5 L0,10 z', class: 'fb-arrowhead' }));
      svgEl.replaceChildren(s('defs', {}, marker), s('g', { class: 'fb-viewport' }, ...edgeEls, ...nodeEls, s('path', { class: 'fb-preview', d: '' })));
      svgEl.setAttribute('aria-label', `Flow graph with ${plural(ids.length, 'step')}. Tab to a step; Enter selects it, Delete removes it and arrow keys move it.`);
      applyViewport();
      if (focusKey) focusGraphItem(focusKey);
    }
    function nodeKey(e, id) {
      if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); select({ type: 'step', id }, { inspect: true }); }
      else if (e.key === 'Delete' || e.key === 'Backspace') { e.preventDefault(); confirmRemoveStep(id); }
      else if (e.key.startsWith('Arrow')) { e.preventDefault(); nudge(id, e.key, e.shiftKey ? 40 : 8); }
    }
    function edgeKey(e, from, to) {
      if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); select({ type: 'edge', from, to }, { inspect: true }); }
      else if (e.key === 'Delete' || e.key === 'Backspace') { e.preventDefault(); disconnect(from, to, { confirm: true }); }
    }

    function needChip(from, to) {
      return h('button', { type: 'button', class: 'fb-need', 'aria-label': `Remove dependency: ${to} waits for ${from}`, title: `Remove: ${to} waits for ${from}`, onclick: () => disconnect(from, to) },
        h('span', {}, from), h('span', { 'aria-hidden': 'true', class: 'fb-x' }, '×'));
    }
    function addDependencySelect(to, focusKey) {
      const optionsList = dependencyOptions(S.cur.doc, to);
      if (!optionsList.length) return null;
      return h('select', { class: 'fb-add-dep', 'aria-label': `Add dependency: ${to} waits for…`, 'data-focus': focusKey, value: '',
        onchange: async (e) => { const from = e.target.value; if (from) await connect(from, to); } },
      h('option', { value: '' }, '+ Add dependency…'),
      ...optionsList.map((o) => h('option', { value: o.id, disabled: o.disabled }, o.disabled ? `${o.id} (would create a cycle)` : o.id)));
    }
    function renderSteps() {
      const c = S.cur;
      if (!c?.doc) { stepsView.replaceChildren(h('p', { class: 'fb-empty' }, !c ? 'Pick a flow or create one.' : 'The YAML cannot be read. Fix it in the YAML tab.')); return; }
      const doc = c.doc, { rows } = layers(doc);
      const focusKey = stepsView.contains(container.ownerDocument.activeElement) ? container.ownerDocument.activeElement.dataset.focus : null;
      const stage = (ids, i) => h('li', { class: 'fb-stage' },
        h('h3', { class: 'fb-stage-title' }, `Stage ${i + 1}`, i === 0 ? ' · starts first' : '', ids.length > 1 ? ` · ${ids.length} run in parallel` : ''),
        h('ol', { class: 'fb-step-list' }, ...ids.map((id) => {
          const step = doc.steps[id], errs = errorsFor(id), needs = needsOf(step), selected = S.sel?.type === 'step' && S.sel.id === id;
          return h('li', { class: `fb-step-row${errs.length ? ' has-error' : ''}${selected ? ' selected' : ''}`, 'data-row': id },
            h('button', { type: 'button', class: 'fb-step-open', 'data-focus': `row:${id}`, 'aria-label': `Edit step ${id}${errs.length ? `, ${plural(errs.length, 'problem')}` : ''}`,
              onclick: () => select({ type: 'step', id }, { inspect: true }) },
            h('span', { class: 'fb-step-id' }, id), h('span', { class: `fb-chip fb-kind-${step?.kind}` }, step?.kind === 'agent' ? `agent · ${step.agent || 'demo'}` : String(step?.kind ?? '?')),
            placementChip(id),
            step?.review ? h('span', { class: 'fb-chip' }, 'review') : null,
            step?.description ? h('span', { class: 'fb-step-desc' }, step.description) : null,
            errs.length ? h('span', { class: 'fb-err-count' }, errs.map((x) => x.message).join(' ')) : null),
            h('div', { class: 'fb-needs' }, h('span', { class: 'fb-needs-label' }, needs.length ? 'Waits for' : 'Starts first'), ...needs.map((n) => needChip(n, id)),
              addDependencySelect(id, `adddep:${id}`)));
        })));
      stepsView.replaceChildren(
        h('div', { class: 'fb-steps-head' }, h('button', { type: 'button', class: 'secondary', 'data-focus': 'flow-settings', onclick: () => select({ type: 'flow' }, { inspect: true }) }, 'Flow settings'),
          h('span', { class: 'fb-hint' }, `${plural(stepIds(doc).length, 'step')} · name, inputs and default worker are in Flow settings`)),
        h('ol', { class: 'fb-stages' }, ...rows.map(stage)),
        h('div', { class: 'fb-row' }, h('button', { type: 'button', class: 'secondary', 'data-focus': 'add-program', onclick: () => addNewStep('program') }, '+ Program step'),
          h('button', { type: 'button', class: 'secondary', 'data-focus': 'add-agent', onclick: () => addNewStep('agent') }, '+ Agent step')));
      if (focusKey) stepsView.querySelector(`[data-focus="${CSS.escape(focusKey)}"]`)?.focus({ preventScroll: true });
    }

    // ---- inspector
    let fieldSerial = 0;
    function field(label, control, { key, step = null, errors, hint, extraError } = {}) {
      const id = control.id || `fb-field-${++fieldSerial}`;
      control.id = id;
      const wrap = h('div', { class: 'fb-field', 'data-field': key, 'data-step': step ?? '' },
        h('label', { for: id }, label), control, hint ? h('small', { class: 'fb-hint' }, hint) : null,
        h('p', { class: 'fb-field-error', id: `${id}-error`, hidden: true }));
      wrap.dataset.extra = extraError || '';
      setFieldErrors(wrap, errors);
      return wrap;
    }
    function setFieldErrors(wrap, errors) {
      const messages = [...(errors || []).map((e) => e.message), wrap.dataset.extra].filter(Boolean);
      const slot = wrap.querySelector('.fb-field-error'), control = wrap.querySelector('#' + CSS.escape(slot.id.replace(/-error$/, '')));
      slot.textContent = messages.join(' '); slot.hidden = !messages.length;
      wrap.classList.toggle('invalid', !!messages.length);
      if (control) { if (messages.length) { control.setAttribute('aria-invalid', 'true'); control.setAttribute('aria-describedby', slot.id); } else { control.removeAttribute('aria-invalid'); control.removeAttribute('aria-describedby'); } }
    }
    function updateInspectorErrors() {
      for (const wrap of inspector.querySelectorAll('.fb-field[data-field]')) setFieldErrors(wrap, errorsFor(wrap.dataset.step || null, wrap.dataset.field));
      const head = inspector.querySelector('.fb-inspector-errors');
      if (head) renderInspectorSummary(head);
    }
    function renderInspectorSummary(slot) {
      const sel = S.sel, errs = sel?.type === 'step' ? errorsFor(sel.id).filter((e) => !e.field || !inspector.querySelector(`[data-field="${CSS.escape(e.field)}"]`)) : sel?.type === 'flow' ? errorsFor(null).filter((e) => !e.field || !inspector.querySelector(`[data-field="${CSS.escape(e.field)}"]`)) : [];
      slot.replaceChildren(...errs.map((e) => h('p', { class: 'fb-field-error' }, `${e.field ? `${e.field}: ` : ''}${e.message}`)));
      slot.hidden = !errs.length;
    }
    // Text the user is typing that the document stores differently (tags, numbers, pending renames).
    const draft = (key, value) => (key in S.drafts ? S.drafts[key] : value);
    const remember = (key) => (e) => { S.drafts[key] = e.target.value; };
    const forget = (key) => () => { delete S.drafts[key]; };

    function renderInspector() {
      const doc = container.ownerDocument, active = doc.activeElement;
      const focusKey = inspector.contains(active) ? active.dataset.focus : null;
      let range = null;
      try { if (focusKey && typeof active.selectionStart === 'number') range = [active.selectionStart, active.selectionEnd]; } catch {}
      const scroll = inspector.scrollTop;
      const c = S.cur, sel = S.sel;
      const back = h('button', { type: 'button', class: 'secondary fb-back', onclick: closeInspector }, S.view === 'graph' ? '← Back to graph' : S.view === 'yaml' ? '← Back to YAML' : '← Back to steps');
      let content;
      if (!c?.doc) content = [h('p', { class: 'fb-empty' }, !c ? 'Pick a flow or create one.' : 'Fix the YAML to edit settings here.')];
      else if (sel?.type === 'step' && c.doc.steps?.[sel.id]) content = stepInspector(sel.id);
      else if (sel?.type === 'edge' && needsOf(c.doc.steps?.[sel.to]).includes(sel.from)) content = edgeInspector(sel.from, sel.to);
      else content = flowInspector();
      inspector.replaceChildren(back, ...content.filter(Boolean));
      inspector.scrollTop = scroll;
      updateInspectorErrors();
      if (focusKey) {
        const node = inspector.querySelector(`[data-focus="${CSS.escape(focusKey)}"]`);
        if (node) { node.focus({ preventScroll: true }); try { if (range) node.setSelectionRange(...range); } catch {} }
      }
    }

    function workerFields(prefix, current, apply) {
      const sel = current || {};
      const read = () => {
        const next = { ...sel };
        for (const [k, input] of [['id', idInput], ['pool', poolInput]]) setKey(next, k, input.value.trim() || undefined);
        const tags = tagsInput.value.split(',').map((t) => t.trim()).filter(Boolean);
        setKey(next, 'tags', tags.length ? tags : undefined);
        return Object.keys(next).length ? next : undefined;
      };
      const text = (key, value, placeholder) => h('input', { 'data-focus': `${prefix}:worker-${key}`, value: draft(`${prefix}:worker-${key}`, value ?? ''), placeholder, autocomplete: 'off', spellcheck: 'false',
        oninput: (e) => { remember(`${prefix}:worker-${key}`)(e); apply(read()); }, onchange: forget(`${prefix}:worker-${key}`) });
      const idInput = text('id', sel.id, 'Any worker'), poolInput = text('pool', sel.pool, 'Any pool');
      const tagsInput = text('tags', (sel.tags || []).join(', '), 'For example: linux, gpu');
      return h('fieldset', { class: 'fb-fieldset' }, h('legend', {}, prefix === 'flow' ? 'Default worker (optional)' : 'Run on worker (optional)'),
        field('Worker ID', idInput, { key: 'worker', step: prefix === 'flow' ? null : prefix.slice(5), hint: 'A specific PC; overrides pool and tags.' }),
        field('Pool', poolInput, {}), field('Required tags', tagsInput, { hint: 'Separate with commas.' }));
    }
    function extras(obj, known) {
      const keys = Object.keys(obj || {}).filter((k) => !known.has(k));
      if (!keys.length) return null;
      return h('div', { class: 'fb-extra' }, h('h4', {}, 'Other settings (edit in the YAML tab)'),
        p('Kept exactly as they are when you edit with the forms.'),
        ...keys.map((k) => h('details', {}, h('summary', {}, k), h('pre', {}, JSON.stringify(obj[k], null, 2)))));
    }

    function templateField(id, key, label) {
      const step = S.cur.doc.steps[id];
      const focusKey = `step:${key}`;
      const area = h('textarea', { 'data-focus': focusKey, rows: key === 'prompt' ? 9 : 3, class: key === 'run' ? 'fb-code' : null, spellcheck: key === 'prompt' ? 'true' : 'false',
        autocapitalize: 'off', value: step[key] ?? '',
        oninput: (e) => { const value = e.target.value; S.caret[key] = [e.target.selectionStart, e.target.selectionEnd]; edit(mutate((d) => { d.steps[id][key] = value; })); } });
      const track = () => { S.caret[key] = [area.selectionStart, area.selectionEnd]; };
      for (const ev of ['keyup', 'pointerup', 'select', 'blur']) area.addEventListener(ev, track);
      const open = S.picker === key, listId = `fb-refs-${key}`;
      const candidates = open ? referenceCandidates(S.cur.doc, id) : [];
      const groups = [...new Set(candidates.map((x) => x.group))];
      const picker = open ? h('div', { class: 'fb-refs', id: listId, role: 'group', 'aria-label': 'References you can insert',
        onkeydown: (e) => { if (e.key === 'Escape') { e.preventDefault(); e.stopPropagation(); S.picker = null; renderInspector(); inspector.querySelector(`[data-focus="ref:${key}"]`)?.focus(); } } },
      ...(candidates.length ? groups.map((g) => h('div', { class: 'fb-ref-group' }, h('h5', {}, g),
        ...candidates.filter((x) => x.group === g).map((x) => h('button', { type: 'button', class: 'fb-ref', 'data-ref': x.text, onclick: () => insertRef(id, key, x.text) },
          h('code', {}, x.text), h('span', {}, x.label)))))
        : [p('Nothing to reference yet. Add a flow input, or make this step wait for another step to use its output.')])) : null;
      const toggle = h('button', { type: 'button', class: 'secondary fb-ref-btn', 'data-focus': `ref:${key}`, 'aria-expanded': String(open), 'aria-controls': open ? listId : null,
        onclick: () => {
          S.caret[key] ??= [area.value.length, area.value.length];
          S.picker = open ? null : key; renderInspector();
          if (!open) inspector.querySelector(`#${listId} .fb-ref`)?.focus();
        } }, open ? 'Close references' : 'Insert reference');
      const wrap = field(label, area, { key, step: id, hint: 'Use {{ input.name }} for flow inputs and {{ steps.step.output }} for a prerequisite’s result.' });
      wrap.append(toggle);
      if (picker) wrap.append(picker);
      return wrap;
    }
    async function insertRef(id, key, text) {
      const current = S.cur.doc.steps[id][key] ?? '';
      const [a, b] = S.caret[key] || [current.length, current.length];
      const { value, caret } = insertText(current, a, b, text);
      const result = await edit(mutate((d) => { d.steps[id][key] = value; }));
      S.picker = null;
      if (!result?.ok) { renderInspector(); return; }
      S.caret[key] = [caret, caret];
      renderInspector();
      const area = inspector.querySelector(`[data-focus="step:${key}"]`);
      if (area) { area.focus(); area.setSelectionRange(caret, caret); }
      notify(`Inserted ${text}.`);
    }

    function setKind(id, kind) {
      return edit((doc) => {
        const next = clone(doc), step = next.steps[id], stash = (S.cur.stash[id] ||= {});
        if (step.kind === kind) return ok(doc, { unchanged: true });
        const own = step.kind === 'agent' ? ['agent', 'prompt', 'review', 'setup'] : ['run', 'requires_command'];
        for (const k of own) if (k in step) { stash[k] = step[k]; delete step[k]; }
        step.kind = kind;
        const defaults = newStep(kind);
        for (const k of kind === 'agent' ? ['agent', 'review', 'prompt', 'setup'] : ['run', 'requires_command']) if (k in stash) step[k] = stash[k]; else if (k in defaults) step[k] = defaults[k];
        return ok(next);
      }, { inspector: true });
    }
    async function commitRename(id, value) {
      value = value.trim();
      if (!S.cur?.doc?.steps?.[id]) return; // Enter and the change event can both commit
      const key = 'step:id';
      if (value === id) { delete S.drafts[key]; delete S.drafts[`${key}:error`]; renderInspector(); return; }
      const result = await edit((doc) => renameStep(doc, id, value));
      if (!result) return;
      if (!result.ok) { S.drafts[key] = value; S.drafts[`${key}:error`] = result.error; renderInspector(); return; }
      const c = S.cur;
      if (c.positions[id]) { c.positions[value] = c.positions[id]; delete c.positions[id]; scheduleLayoutSave(); }
      if (c.stash[id]) { c.stash[value] = c.stash[id]; delete c.stash[id]; }
      S.sel = { type: 'step', id: value }; S.drafts = {};
      notify(`Renamed ${id} to ${value}; dependencies and references were updated.`);
      renderGraph(); renderSteps(); renderInspector();
    }
    function numberField(id, key, label, min, max, placeholder, hint) {
      const step = S.cur.doc.steps[id], focusKey = `step:${key}`;
      const input = h('input', { type: 'number', inputmode: 'numeric', min, max, step: 1, placeholder, 'data-focus': focusKey, value: draft(focusKey, step[key] ?? ''),
        oninput: (e) => { remember(focusKey)(e); const raw = e.target.value; edit(mutate((d) => setKey(d.steps[id], key, raw === '' ? undefined : Number(raw)))); }, onchange: forget(focusKey) });
      return field(label, input, { key, step: id, hint });
    }
    function stepInspector(id) {
      const step = S.cur.doc.steps[id], kind = step.kind;
      const idInput = h('input', { 'data-focus': 'step:id', value: draft('step:id', id), autocomplete: 'off', autocapitalize: 'off', spellcheck: 'false', maxlength: 48,
        oninput: remember('step:id'), onchange: (e) => commitRename(id, e.target.value), onkeydown: (e) => { if (e.key === 'Enter') { e.preventDefault(); commitRename(id, e.target.value); } } });
      const kindSelect = h('select', { 'data-focus': 'step:kind', value: kind, onchange: (e) => setKind(id, e.target.value) },
        h('option', { value: 'program' }, 'Program: runs a command'), h('option', { value: 'agent' }, 'Agent: a coding agent you can talk to'),
        kind !== 'program' && kind !== 'agent' ? h('option', { value: String(kind) }, String(kind)) : null);
      const description = h('input', { 'data-focus': 'step:description', value: step.description ?? '', placeholder: 'Optional',
        oninput: (e) => { const v = e.target.value; edit(mutate((d) => setKey(d.steps[id], 'description', v))); } });
      const needs = needsOf(step);
      const needsBox = h('div', { class: 'fb-needs', 'data-field': 'needs' },
        ...(needs.length ? needs.map((n) => needChip(n, id)) : [h('span', { class: 'fb-hint' }, 'Starts as soon as the flow starts.')]), addDependencySelect(id, 'step:adddep'));
      const needsField = h('div', { class: 'fb-field', 'data-field': 'needs', 'data-step': id }, h('span', { class: 'fb-label' }, 'Waits for (needs)'), needsBox,
        h('p', { class: 'fb-field-error', id: 'fb-needs-error', hidden: true }));
      const agentFields = kind === 'agent' ? [
        field('Agent', h('select', { 'data-focus': 'step:agent', value: step.agent ?? 'demo', onchange: (e) => { const v = e.target.value; edit(mutate((d) => { d.steps[id].agent = v; })); } },
          ...AGENTS.map((a) => h('option', { value: a }, AGENT_LABELS[a])), step.agent && !AGENTS.includes(step.agent) ? h('option', { value: step.agent }, step.agent) : null), { key: 'agent', step: id }),
        h('div', { class: 'fb-field fb-check', 'data-field': 'review', 'data-step': id },
          h('label', {}, h('input', { type: 'checkbox', 'data-focus': 'step:review', checked: !!step.review, onchange: (e) => { const v = e.target.checked; edit(mutate((d) => { d.steps[id].review = v; })); } }),
            ' Wait for me to mark this step done (review)'), h('p', { class: 'fb-field-error', id: 'fb-review-error', hidden: true })),
        templateField(id, 'prompt', 'Prompt')] : [];
      const needed = kind === 'program' && ambiguous(id);
      const commandInput = kind === 'program' ? h('input', { 'data-focus': 'step:requires_command', value: draft('step:requires_command', step.requires_command ?? ''), placeholder: 'Inferred from the command',
        autocomplete: 'off', autocapitalize: 'off', spellcheck: 'false', 'aria-required': String(needed),
        oninput: (e) => { remember('step:requires_command')(e); const v = e.target.value.trim(); edit(mutate((d) => setKey(d.steps[id], 'requires_command', v || undefined))); },
        onchange: forget('step:requires_command') }) : null;
      if (commandInput) commandInput.required = needed;
      const programFields = kind === 'program' ? [templateField(id, 'run', 'Command'),
        field('Executable (requires_command)', commandInput, { key: 'requires_command', step: id, extraError: needed ? REQUIRES_COMMAND_ERROR : '',
          hint: 'The program this step runs, checked on the worker before it starts. Leave blank when it is the first word of a simple command; declare it for pipes, variables and scripts.' })] : [];
      const idError = S.drafts['step:id:error'];
      return [
        h('h3', { tabindex: '-1' }, `Step ${id}`),
        h('div', { class: 'fb-inspector-errors', hidden: true }),
        field('Step ID', idInput, { key: 'id', step: id, hint: 'Renaming updates dependencies and {{ steps.… }} references.', extraError: idError }),
        field('Kind', kindSelect, { key: 'kind', step: id }),
        field('Description', description, { key: 'description', step: id }),
        needsField, ...agentFields, ...programFields,
        h('div', { class: 'fb-pair' }, numberField(id, 'retries', 'Retries', 0, 10, '0', '0–10'), numberField(id, 'timeout_minutes', 'Timeout (minutes)', 1, 1440, '60', '1–1440')),
        workerFields(`step:${id}`, step.worker, (value) => edit(mutate((d) => setKey(d.steps[id], 'worker', value)))),
        h('section', { class: 'fb-placement-slot', 'data-step': id, 'data-title': 'Where this step can run', 'aria-live': 'polite' }, h('h4', {}, 'Where this step can run'), ...placementBox(id)),
        extras(step, FORM_STEP_KEYS),
        h('div', { class: 'fb-row' }, h('button', { type: 'button', class: 'secondary', 'data-focus': 'step:after', onclick: () => addNewStep('program', id) }, '+ Program step after this'),
          h('button', { type: 'button', class: 'secondary', 'data-focus': 'step:after-agent', onclick: () => addNewStep('agent', id) }, '+ Agent step after this')),
        h('button', { type: 'button', class: 'danger fb-delete-step', onclick: () => confirmRemoveStep(id) }, `Delete step ${id}`),
      ];
    }
    function edgeInspector(from, to) {
      return [h('h3', { tabindex: '-1' }, 'Dependency'),
        p(h('strong', {}, to), ' waits for ', h('strong', {}, from), ' and can use its output, for example ', h('code', {}, `{{ steps.${from}.output }}`), '.'),
        h('div', { class: 'fb-row' }, h('button', { type: 'button', class: 'secondary', onclick: () => select({ type: 'step', id: from }, { inspect: true }) }, `Edit ${from}`),
          h('button', { type: 'button', class: 'secondary', onclick: () => select({ type: 'step', id: to }, { inspect: true }) }, `Edit ${to}`)),
        h('button', { type: 'button', class: 'danger', onclick: () => disconnect(from, to, { confirm: true }) }, 'Remove dependency')];
    }
    function inputRow(name, value, index) {
      const kindOf = (v) => (v === null || v === undefined ? 'empty' : typeof v === 'boolean' ? 'boolean' : typeof v === 'number' ? 'number' : 'text');
      const type = kindOf(value), nameKey = `input:${index}:name`, valueKey = `input:${index}:value`;
      const nameInput = h('input', { 'data-focus': nameKey, value: draft(nameKey, name), 'aria-label': `Input ${index + 1} name`, autocomplete: 'off', autocapitalize: 'off', spellcheck: 'false',
        oninput: remember(nameKey),
        onchange: async (e) => {
          const to = e.target.value.trim(), result = await edit((d) => renameInput(d, name, to), { inspector: true });
          if (result && !result.ok) { S.drafts[nameKey] = to; S.drafts[`${nameKey}:error`] = result.error; }
          else { delete S.drafts[nameKey]; delete S.drafts[`${nameKey}:error`]; }
          renderInspector();
        } });
      const typeSelect = h('select', { 'data-focus': `input:${index}:type`, 'aria-label': `${name} type`, value: type,
        onchange: (e) => {
          const t = e.target.value;
          edit(mutate((d) => { const v = d.input[name]; d.input[name] = t === 'empty' ? null : t === 'boolean' ? v === true || v === 'true' : t === 'number' ? (Number.isFinite(Number(v)) ? Number(v) : 0) : String(v ?? ''); }), { inspector: true });
        } }, h('option', { value: 'text' }, 'Text'), h('option', { value: 'number' }, 'Number'), h('option', { value: 'boolean' }, 'True/false'), h('option', { value: 'empty' }, 'No default'));
      const setValue = (v) => edit(mutate((d) => { d.input[name] = v; }));
      const valueInput = type === 'boolean'
        ? h('select', { 'data-focus': valueKey, 'aria-label': `${name} default`, value: String(value), onchange: (e) => setValue(e.target.value === 'true') }, h('option', { value: 'true' }, 'true'), h('option', { value: 'false' }, 'false'))
        : type === 'empty' ? h('input', { 'aria-label': `${name} default`, disabled: true, value: '', placeholder: 'No default' })
          : h('input', { 'data-focus': valueKey, 'aria-label': `${name} default`, type: type === 'number' ? 'number' : 'text', value: draft(valueKey, String(value)),
            oninput: (e) => { remember(valueKey)(e); const raw = e.target.value; if (type === 'number' && raw !== '' && Number.isFinite(Number(raw))) setValue(Number(raw)); else if (type !== 'number') setValue(raw); },
            onchange: forget(valueKey) });
      const error = S.drafts[`${nameKey}:error`];
      return h('li', { class: 'fb-input-row' }, nameInput, typeSelect, valueInput,
        h('button', { type: 'button', class: 'secondary fb-remove', 'aria-label': `Remove input ${name}`, onclick: () => edit(mutate((d) => { delete d.input[name]; }), { inspector: true }) }, '×'),
        error ? h('p', { class: 'fb-field-error' }, error) : null);
    }
    function flowInspector() {
      const doc = S.cur.doc, name = h('input', { 'data-focus': 'flow:name', value: doc.name ?? '', autocomplete: 'off', autocapitalize: 'off', spellcheck: 'false', maxlength: 48,
        oninput: (e) => { const v = e.target.value.trim(); edit(mutate((d) => { d.name = v; })); } });
      const nameError = doc.name && !FLOW_NAME.test(doc.name) ? 'Use lowercase letters, digits and -, starting with a letter.' : '';
      const description = h('textarea', { 'data-focus': 'flow:description', rows: 3, value: doc.description ?? '',
        oninput: (e) => { const v = e.target.value; edit(mutate((d) => setKey(d, 'description', v))); } });
      const inputs = Object.entries(doc.input || {});
      return [
        h('h3', { tabindex: '-1' }, 'Flow settings'),
        h('div', { class: 'fb-inspector-errors', hidden: true }),
        field('Name', name, { key: 'name', hint: S.cur.isNew ? 'Lowercase letters, digits and -.' : 'Saving under a new name creates a new flow; the old one stays until you delete it.', extraError: nameError }),
        field('Description', description, { key: 'description' }),
        h('div', { class: 'fb-field', 'data-field': 'input', 'data-step': '' }, h('span', { class: 'fb-label' }, 'Inputs'),
          h('small', { class: 'fb-hint' }, 'Values chosen when a run starts. Use them in prompts and commands as {{ input.name }}.'),
          inputs.length ? h('ul', { class: 'fb-inputs' }, ...inputs.map(([k, v], i) => inputRow(k, v, i))) : h('p', { class: 'fb-hint' }, 'No inputs.'),
          h('button', { type: 'button', class: 'secondary', 'data-focus': 'flow:add-input', onclick: () => edit(mutate((d) => { d.input = d.input || {}; let i = 1; while (`input_${i}` in d.input) i++; d.input[`input_${i}`] = ''; }), { inspector: true }) }, '+ Add input'),
          h('p', { class: 'fb-field-error', id: 'fb-input-error', hidden: true })),
        workerFields('flow', doc.worker, (value) => edit(mutate((d) => setKey(d, 'worker', value)))),
        h('section', { class: 'fb-placement-slot', 'data-title': 'Where each step can run', 'aria-live': 'polite' }, h('h4', {}, 'Where each step can run'), ...placementBox(null)),
        'environment' in doc ? h('div', { class: 'fb-extra' }, h('h4', {}, 'Environment setup'),
          p('This flow prepares an environment (a setup_environment step runs first). Edit it in the YAML tab; graph edits keep it.'),
          h('details', {}, h('summary', {}, 'environment'), h('pre', {}, JSON.stringify(doc.environment, null, 2)))) : null,
        extras(doc, new Set([...FORM_FLOW_KEYS, 'environment'])),
      ];
    }

    // ---- public interface
    renderAll();
    return {
      async open() {
        S.visible = true; S.placement.key = null;  // the fleet may have changed since the last look
        await refreshList();
        schedulePlacement();
        if (S.cur && (isDirty() || S.cur.isNew)) { renderAll({ keepYaml: true }); return; }
        const name = S.cur?.name || S.lastName;
        const target = S.docs.some((d) => d.name === name) ? name : S.docs[0]?.name;
        if (target) await load(target); else setCurrent(null);
      },
      // Resolves false when the user wants to keep editing.
      async confirmLeave() {
        if (!(await guard())) return false;
        if (isDirty()) { S.lastName = S.cur?.name || S.lastName; S.cur = null; }
        S.visible = false;
        return true;
      },
      hide() { S.visible = false; },
      isDirty,
    };
  }

  const api = { AGENTS, NODE_W, NODE_H, checkStepId, uniqueStepId, addStep, removeStep, renameStep, renameInput, referencesTo, addDependency, removeDependency,
    dependsOn, dependencyOptions, layers, autoLayout, layout, bounds, referenceCandidates, insertText, compareDocuments, TEMPLATES, mount,
    waitingWords, placementHeadline, executionText, placementView, executionView };
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  else root.FlowBuilder = api;
})(typeof window !== 'undefined' ? window : globalThis);
