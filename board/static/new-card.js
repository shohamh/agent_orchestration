/* New card: what runs, its input, where it runs and when. Mounted by index.html into #new-dialog.
 *
 * The form holds no engine knowledge: it builds one request for POST /api/boards/.../cards. Pure
 * helpers are exported for the Node tests (new-card.test.cjs).
 */
(function (root, factory) {
  const api = factory();
  if (typeof module === 'object' && module.exports) module.exports = api;
  else root.NewCard = api;
})(typeof self !== 'undefined' ? self : this, function () {
  'use strict';

  const AGENTS = [['codex', 'Codex'], ['claude', 'Claude Code'], ['opencode', 'opencode v1'], ['opencode2', 'opencode v2'], ['omp', 'omp'], ['pi', 'pi'], ['demo', 'Demo agent · simulated, no model']];
  const BUILT_IN = [
    { key: 'research-report', title: 'Research report', description: 'Built-in demo: a multi-step graph on a topic.', label: 'Topic' },
    { key: 'steerable-agent', title: 'Steerable agent', description: 'Built-in demo: a long-running task you can steer.', label: 'Goal' },
  ];
  const MODES = [
    ['agent', 'Agent', 'A coding agent works on a prompt', 'M12 3l1.9 5.1L19 10l-5.1 1.9L12 17l-1.9-5.1L5 10l5.1-1.9L12 3zM19 16l.8 2.2L22 19l-2.2.8L19 22l-.8-2.2L16 19l2.2-.8L19 16z'],
    ['program', 'Program', 'Run a command on a worker', 'M4 5h16a1 1 0 0 1 1 1v12a1 1 0 0 1-1 1H4a1 1 0 0 1-1-1V6a1 1 0 0 1 1-1zm3 4l3 3-3 3m5 0h5'],
    ['flow', 'Flow', 'Start a saved multi-step flow', 'M5 4a2 2 0 1 0 0 4 2 2 0 0 0 0-4zm14 6a2 2 0 1 0 0 4 2 2 0 0 0 0-4zM5 16a2 2 0 1 0 0 4 2 2 0 0 0 0-4zM7 6h4a2 2 0 0 1 2 2v6a2 2 0 0 0 2 2h0M7 18h4a2 2 0 0 0 2-2M13 12h4'],
  ];
  const REPEATS = [['none', "Doesn't repeat"], ['hourly', 'Every hour'], ['daily', 'Every day'], ['weekdays', 'Every weekday'], ['weekly', 'Every week'], ['custom', 'Custom (cron)']];
  const DAYS = [[1, 'Mon'], [2, 'Tue'], [3, 'Wed'], [4, 'Thu'], [5, 'Fri'], [6, 'Sat'], [0, 'Sun']];
  const MAX_ATTACHED_BYTES = 64 * 1024 * 1024; // the transfer limit of a run
  const FLOW_SEARCH_FROM = 7;

  // ---------------------------------------------------------------- pure helpers
  const poolsOf = (worker) => worker.pools || [worker.pool].filter(Boolean);

  /** The fleet requirement for what is selected. No pools means all pools. */
  function selection({ pools = [], tags = [], workerId = '' }) {
    if (workerId) return { id: workerId, ...(tags.length ? { tags } : {}) };
    return { ...(pools.length ? { pools } : {}), ...(tags.length ? { tags } : {}) };
  }

  function matchingWorkers(workers, wanted) {
    return (workers || []).filter((w) => (!wanted.id || w.id === wanted.id) &&
      (!wanted.pools?.length || wanted.pools.some((p) => poolsOf(w).includes(p))) &&
      (wanted.tags || []).every((t) => (w.tags || []).includes(t)));
  }

  function fitText(workers, wanted) {
    const total = (workers || []).length;
    if (!total) return 'No workers are registered yet. Add one in the Fleet tab.';
    const found = matchingWorkers(workers, wanted), online = found.filter((w) => w.online).length;
    if (!found.length) return `None of the ${total} registered ${total === 1 ? 'worker matches' : 'workers match'}.`;
    return `${found.length} of ${total} ${total === 1 ? 'worker matches' : 'workers match'}, ${online} online.`;
  }

  const pad = (n) => String(n).padStart(2, '0');
  const splitTime = (time) => { const [h, m] = String(time || '09:00').split(':').map(Number); return { hour: h || 0, minute: m || 0 }; };

  /** The cron of a repeat choice, in the user's time zone; null when it doesn't repeat. */
  function cronFor({ repeat, time, days = [], cron = '' }) {
    const { hour, minute } = splitTime(time);
    switch (repeat) {
      case 'hourly': return `${minute} * * * *`;
      case 'daily': return `${minute} ${hour} * * *`;
      case 'weekdays': return `${minute} ${hour} * * 1-5`;
      case 'weekly': return `${minute} ${hour} * * ${[...days].sort((a, b) => a - b).join(',') || 1}`;
      case 'custom': return cron.trim();
      default: return null;
    }
  }

  function repeatSummary({ repeat, time, days = [], cron = '' }) {
    const { hour, minute } = splitTime(time);
    const at = `${pad(hour)}:${pad(minute)}`;
    switch (repeat) {
      case 'hourly': return `Every hour at :${pad(minute)}`;
      case 'daily': return `Every day at ${at}`;
      case 'weekdays': return `Every weekday at ${at}`;
      case 'weekly': return `Every ${DAYS.filter(([d]) => days.includes(d)).map(([, n]) => n).join(', ') || 'Mon'} at ${at}`;
      case 'custom': return `Cron ${cron.trim()}`;
      default: return '';
    }
  }

  function startLabel({ mode, when, repeat, session = true }) {
    if (when === 'later') return 'Schedule';
    if (repeat !== 'none') return 'Start and repeat';
    if (mode === 'flow') return session ? 'Start flow' : 'Create card';
    return mode === 'program' ? 'Run program' : 'Start agent';
  }

  function repoProvider(url) {
    if (/(^|[/@.])github\b/i.test(url)) return 'GitHub';
    if (/(^|[/@.])gitlab\b/i.test(url)) return 'GitLab';
    return 'Git';
  }
  const isGitUrl = (url) => /^(?:https?|ssh):\/\/\S+$|^[\w.-]+@[\w.-]+:\S+$/.test(url.trim());
  const isConfluenceUrl = (url) => /^https?:\/\/\S+$/i.test(url.trim()) && /\/spaces\/[^/]+\/pages\/\d+|[?&]pageId=\d+/.test(url);

  function formatBytes(size) {
    if (size < 1024) return `${size} B`;
    if (size < 1024 * 1024) return `${Math.round(size / 1024)} KB`;
    return `${(size / (1024 * 1024)).toFixed(size < 10 * 1024 * 1024 ? 1 : 0)} MB`;
  }

  const localInputValue = (date) => `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}T${pad(date.getHours())}:${pad(date.getMinutes())}`;

  /** The next round hour at least 15 minutes away: a time you'd probably pick anyway. */
  function defaultLater(now = new Date()) {
    const date = new Date(now.getTime() + 15 * 60000);
    date.setHours(date.getHours() + 1, 0, 0, 0);
    return date;
  }

  /** A flow's named inputs as form fields. The first text input is the paragraph. */
  function flowFields(input) {
    let paragraph = false;
    return Object.entries(input || {}).map(([key, value]) => {
      const type = typeof value === 'boolean' ? 'boolean' : typeof value === 'number' ? 'number' : 'text';
      const long = type === 'text' && (!paragraph || String(value ?? '').length > 80);
      if (type === 'text') paragraph = true;
      return { key, value, type, long };
    });
  }

  function flowFacts(flow) {
    const steps = Object.values(flow.steps || {});
    const agents = steps.filter((s) => s.kind === 'agent').length;
    return `${steps.length} ${steps.length === 1 ? 'step' : 'steps'}${agents ? ` · ${agents} ${agents === 1 ? 'agent' : 'agents'}` : ''}`;
  }

  function readableTime(iso) {
    const date = new Date(iso);
    if (Number.isNaN(date.getTime())) return String(iso);
    return date.toLocaleString(undefined, { weekday: 'short', day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit' });
  }

  // ---------------------------------------------------------------- DOM
  function h(tag, attrs, ...children) {
    const node = document.createElement(tag);
    for (const [key, value] of Object.entries(attrs || {})) {
      if (value === null || value === undefined || value === false) continue;
      if (key.startsWith('on')) node.addEventListener(key.slice(2), value);
      else if (['value', 'checked', 'disabled', 'hidden', 'required', 'open', 'multiple', 'selected'].includes(key)) node[key] = value;
      else node.setAttribute(key, value === true ? '' : value);
    }
    for (const child of children.flat()) if (child !== null && child !== undefined && child !== false) node.append(child);
    return node;
  }
  function icon(path) {
    const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
    svg.setAttribute('viewBox', '0 0 24 24'); svg.setAttribute('width', '20'); svg.setAttribute('height', '20'); svg.setAttribute('aria-hidden', 'true');
    svg.setAttribute('fill', 'none'); svg.setAttribute('stroke', 'currentColor'); svg.setAttribute('stroke-width', '1.6'); svg.setAttribute('stroke-linecap', 'round'); svg.setAttribute('stroke-linejoin', 'round');
    const line = document.createElementNS('http://www.w3.org/2000/svg', 'path');
    line.setAttribute('d', path);
    svg.append(line);
    return svg;
  }

  function fresh() {
    return { mode: 'agent', flowKey: null, flowQuery: '', files: [], repositories: [], confluence: [], pools: [], tags: [], workerId: '', steps: {},
      when: 'now', at: '', repeat: 'none', time: '', days: [new Date().getDay()], cron: '', overlap: 'skip', preview: null, previewError: null };
  }

  function mount(dialog, deps) {
    const { api, getSystem, getBoard, getFlows, getWorkers, loadFleet, workerPicker, placementView, onCreated, onScheduled, onClose } = deps;
    let S = fresh();
    let tagCatalog = [];
    const launch = { key: null, serial: 0, result: null, blocked: false, timer: null };
    let previewTimer = null, previewSerial = 0;

    const flowOf = () => (S.flowKey?.startsWith('flow:') ? getFlows().find((f) => f.name === S.flowKey.slice(5)) : null);
    const builtIn = () => BUILT_IN.find((b) => b.key === S.flowKey) || null;
    const placeable = () => !builtIn();

    // ---- skeleton
    const modeGroup = h('div', { class: 'nc-modes', role: 'radiogroup', 'aria-label': 'What to run' },
      MODES.map(([value, title, text, path]) => h('label', { class: 'nc-mode' },
        h('input', { type: 'radio', name: 'n-mode', value, checked: value === 'agent', onchange: () => setMode(value) }),
        h('span', { class: 'nc-mode-icon' }, icon(path)),
        h('span', { class: 'nc-mode-text' }, h('strong', {}, title), h('small', {}, text)))));

    const flowSearch = h('input', { type: 'search', class: 'nc-flow-search', placeholder: 'Search flows', 'aria-label': 'Search flows', hidden: true,
      oninput: () => { S.flowQuery = flowSearch.value; renderFlowCards(); } });
    const flowCards = h('div', { class: 'nc-flow-cards', role: 'radiogroup', 'aria-label': 'Flow to start', id: 'n-flow-cards' });
    const flowPicker = h('div', { id: 'n-flow-picker', class: 'nc-flow-picker', hidden: true }, flowSearch, flowCards);
    const kind = h('input', { type: 'hidden', id: 'n-kind', value: 'agent-session' }); // what is selected, for tests and deep links

    const agentSelect = h('select', { id: 'n-agent', onchange: () => refresh() }, AGENTS.map(([value, label]) => h('option', { value }, label)));
    const agentRow = h('label', { class: 'nc-field', id: 'n-agent-wrap' }, 'Agent', agentSelect);
    const goalLabel = h('span', {}, 'Prompt');
    const goal = h('textarea', { id: 'n-goal', rows: 4, placeholder: 'What should the agent do?', oninput: clearError });
    const goalRow = h('label', { class: 'nc-field', id: 'n-goal-wrap' }, goalLabel, goal);
    const demoNote = h('p', { class: 'nc-hint', id: 'n-demo-note', hidden: true }, 'The demo agent is simulated. It shows the chat, approvals, questions and retries without running a model.');

    const command = h('textarea', { id: 'n-command', class: 'nc-mono', rows: 2, spellcheck: 'false', placeholder: 'python train.py --epochs 5',
      oninput: () => { clearError(); scheduleCheck(); } });
    const programInput = h('textarea', { id: 'n-program-input', rows: 2, placeholder: 'Optional' });
    const programFields = h('div', { id: 'n-program-fields', class: 'nc-stack', hidden: true },
      h('label', { class: 'nc-field' }, 'Program', command, h('small', {}, 'Runs through the worker’s shell, in a new folder that holds what you attach below.')),
      h('label', { class: 'nc-field' }, 'Input', programInput, h('small', {}, 'Passed as JSON on stdin and in FLOW_CONTEXT, under input.text.')));

    const flowDesc = h('p', { class: 'nc-hint', id: 'n-flow-desc', hidden: true });
    const flowInputs = h('div', { id: 'n-flow-fields', class: 'nc-stack', hidden: true });
    const turns = h('input', { id: 'n-turns', type: 'number', min: 1, max: 200, value: '8' });
    const stepSeconds = h('input', { id: 'n-step', type: 'number', min: 1, max: 3600, value: '5' });
    const builtInFields = h('div', { class: 'nc-row', id: 'n-steer-fields', hidden: true },
      h('label', { class: 'nc-field' }, 'Steps', turns), h('label', { class: 'nc-field' }, 'Seconds per step', stepSeconds));

    // Attachments
    const filePicker = h('input', { type: 'file', multiple: true, hidden: true, id: 'n-file-input', onchange: () => { addFiles(filePicker.files); filePicker.value = ''; } });
    const fileList = h('ul', { class: 'nc-attached', id: 'n-files', 'aria-label': 'Attached files' });
    const repoList = h('div', { class: 'nc-stack', id: 'n-repos' });
    const confluenceList = h('div', { class: 'nc-stack', id: 'n-confluence' });
    const contextBox = h('div', { class: 'nc-context', id: 'n-context',
      ondragover: (e) => { e.preventDefault(); contextBox.classList.add('nc-dropping'); },
      ondragleave: (e) => { if (!contextBox.contains(e.relatedTarget)) contextBox.classList.remove('nc-dropping'); },
      ondrop: (e) => { e.preventDefault(); contextBox.classList.remove('nc-dropping'); addFiles(e.dataTransfer.files); } },
      h('div', { class: 'nc-context-head' },
        h('span', { class: 'nc-label' }, 'Attach', h('small', {}, ' · optional, or drop files here')),
        h('div', { class: 'nc-add-buttons' },
          h('button', { type: 'button', class: 'secondary', id: 'n-add-file', onclick: () => filePicker.click() }, '+ Files'),
          h('button', { type: 'button', class: 'secondary', id: 'n-add-repo', onclick: () => { S.repositories.push({ url: '', ref: '' }); renderContext(true); } }, '+ Repository'),
          h('button', { type: 'button', class: 'secondary', id: 'n-add-confluence', onclick: () => { S.confluence.push({ url: '', include_descendants: true }); renderContext(true); } }, '+ Confluence'))),
      filePicker, fileList, repoList, confluenceList,
      h('p', { class: 'nc-hint', id: 'n-context-hint', hidden: true }));

    // Advanced agent options
    const workspace = h('input', { id: 'n-workspace', placeholder: 'A folder on the machine that coordinates the run', oninput: () => refresh() });
    const resume = h('input', { id: 'n-resume', placeholder: 'Optional', oninput: () => refresh() });
    const failures = h('input', { id: 'n-failures', type: 'number', min: 0, max: 20, value: '0' });
    const advanced = h('details', { class: 'nc-more', id: 'n-advanced', hidden: true }, h('summary', {}, 'Advanced'),
      h('div', { class: 'nc-stack' },
        h('label', { class: 'nc-field' }, 'Existing workspace', workspace, h('small', {}, 'Blank: a new folder on the worker. With nothing attached, that folder is a small demo project.')),
        h('label', { class: 'nc-field' }, 'Resume ACP session ID', resume, h('small', {}, 'Use the agent and workspace of the original session.')),
        h('label', { class: 'nc-field' }, 'Simulated failures', failures, h('small', {}, 'Exercises retry and recovery.'))));

    // Where
    const poolChips = h('div', { class: 'nc-chips', id: 'n-pools', role: 'group', 'aria-label': 'Worker pools' });
    const tagChips = h('div', { class: 'nc-chips', id: 'n-tags', role: 'group', 'aria-label': 'Required tags' });
    const tagInput = h('input', { id: 'n-tag-input', placeholder: 'Add a tag', 'aria-label': 'Add a required tag', list: 'n-tag-options',
      onkeydown: (e) => { if (e.key === 'Enter' || e.key === ',') { e.preventDefault(); addTag(); } }, onblur: () => addTag() });
    const tagOptions = h('datalist', { id: 'n-tag-options' });
    const fit = h('p', { class: 'nc-fit', id: 'n-fit', role: 'status' });
    const whereNote = h('p', { class: 'nc-hint nc-warning', id: 'n-where-note', hidden: true });
    const specific = h('select', { id: 'n-worker-id', 'aria-label': 'Run on one worker', onchange: () => { S.workerId = specific.value; renderWhere(); scheduleCheck(); } });
    const specificField = h('label', { class: 'nc-field' }, 'Run on one specific worker', specific);
    const stepWorkers = h('div', { class: 'nc-stack', id: 'n-worker-fields' });
    const whereMore = h('details', { class: 'nc-more', id: 'n-where-more' }, h('summary', {}, 'More placement options'), h('div', { class: 'nc-stack' }, specificField, stepWorkers));
    const whereFields = h('div', { class: 'nc-stack', id: 'n-where-fields' },
      h('div', { class: 'nc-field' }, h('span', { class: 'nc-label' }, 'Worker pools'), poolChips),
      h('div', { class: 'nc-field' }, h('span', { class: 'nc-label' }, 'Required tags'), h('div', { class: 'nc-tag-line' }, tagChips, tagInput, tagOptions)),
      fit, whereMore);
    const placement = h('div', { id: 'n-placement', hidden: true, 'aria-live': 'polite' });

    // When
    const whenButtons = ['now', 'later'].map((value) => h('label', { class: 'nc-segment' },
      h('input', { type: 'radio', name: 'n-when', value, checked: value === 'now', onchange: () => { S.when = value; ensureLater(); renderWhen(); } }),
      h('span', {}, value === 'now' ? 'Right now' : 'Later')));
    const at = h('input', { type: 'datetime-local', id: 'n-at', 'aria-label': 'Start time', oninput: () => { S.at = at.value; syncTimeFromAt(); clearError(); renderWhen(); } });
    const atWrap = h('label', { class: 'nc-field', id: 'n-at-wrap', hidden: true }, 'Starts', at);
    const repeat = h('select', { id: 'n-repeat', onchange: () => { S.repeat = repeat.value; ensureTime(); renderWhen(); } }, REPEATS.map(([value, label]) => h('option', { value }, label)));
    const time = h('input', { type: 'time', id: 'n-time', 'aria-label': 'Time of day', oninput: () => { S.time = time.value; renderWhen(); } });
    const timeWrap = h('label', { class: 'nc-field', id: 'n-time-wrap', hidden: true }, 'At', time);
    const dayChips = h('div', { class: 'nc-chips', id: 'n-days', role: 'group', 'aria-label': 'Days of the week', hidden: true });
    const cron = h('input', { id: 'n-cron', class: 'nc-mono', spellcheck: 'false', placeholder: '0 9 * * 1-5', 'aria-label': 'Cron expression', oninput: () => { S.cron = cron.value; renderWhen(); } });
    const cronWrap = h('label', { class: 'nc-field', id: 'n-cron-wrap', hidden: true }, 'Cron', cron, h('small', {}, 'minute hour day month weekday, in your time zone'));
    const overlap = h('select', { id: 'n-overlap', onchange: () => { S.overlap = overlap.value; } },
      h('option', { value: 'skip' }, 'Skip this run'), h('option', { value: 'start' }, 'Start another run'));
    const overlapRow = h('div', { class: 'nc-when', id: 'n-overlap-row', hidden: true },
      h('label', { class: 'nc-field' }, 'If the previous run is still active', overlap, h('small', {}, 'An agent waiting for your reply counts as active.')));
    const preview = h('p', { class: 'nc-hint', id: 'n-preview', role: 'status', hidden: true });

    // Footer
    const error = h('p', { class: 'nc-error', id: 'n-error', role: 'alert', hidden: true });
    const productionNote = h('p', { class: 'nc-hint nc-warning', id: 'n-production-note', hidden: true });
    const create = h('button', { class: 'primary', id: 'n-create', value: 'create', type: 'submit' }, 'Start agent');

    const section = (number, title, ...children) => h('section', { class: 'nc-section', 'aria-label': title },
      h('h3', {}, h('span', { class: 'nc-number', 'aria-hidden': 'true' }, String(number)), title), ...children);
    const form = h('form', { id: 'new-form', method: 'dialog', class: 'nc-form', onsubmit: submit, onkeydown: (e) => { if (e.key === 'Enter' && (e.ctrlKey || e.metaKey)) { e.preventDefault(); form.requestSubmit(create); } } },
      h('header', { class: 'nc-head' }, h('strong', { id: 'nc-title' }, 'New card')),
      h('div', { class: 'nc-body' },
        section(1, 'What runs', modeGroup, flowPicker, kind),
        section(2, 'Input', agentRow, goalRow, demoNote, programFields, flowDesc, flowInputs, builtInFields, contextBox, advanced),
        section(3, 'Where it runs', whereNote, whereFields, placement),
        section(4, 'When',
          h('div', { class: 'nc-when' }, h('div', { class: 'nc-segments', role: 'radiogroup', 'aria-label': 'When to start' }, whenButtons), atWrap),
          h('div', { class: 'nc-when' }, h('label', { class: 'nc-field' }, 'Repeat', repeat), timeWrap, cronWrap),
          dayChips, overlapRow, preview)),
      h('footer', { class: 'nc-foot' }, error, productionNote,
        h('div', { class: 'nc-actions' },
          h('button', { class: 'secondary', value: 'cancel', formnovalidate: true, type: 'submit' }, 'Close'), create)));
    dialog.classList.add('nc-dialog');
    dialog.setAttribute('aria-labelledby', 'nc-title');
    dialog.replaceChildren(form);

    // ---- what runs
    function setMode(mode) {
      S.mode = mode;
      modeGroup.querySelectorAll('input').forEach((n) => { n.checked = n.value === mode; });
      if (mode !== 'flow') S.flowKey = null;
      else if (!S.flowKey) { const first = getFlows()[0]; S.flowKey = first ? `flow:${first.name}` : null; S.steps = {}; }
      flowInputs.dataset.flow = '';
      refresh();
      clearError();
    }
    function chooseFlow(key) {
      if (S.flowKey !== key) S.steps = {};
      S.flowKey = key;
      refresh();
      clearError();
    }
    function flowCard(key, title, description, facts) {
      return h('label', { class: 'nc-flow-card', 'data-flow': key },
        h('input', { type: 'radio', name: 'n-flow', value: key, checked: S.flowKey === key, onchange: () => chooseFlow(key) }),
        h('span', { class: 'nc-flow-title' }, title),
        description ? h('span', { class: 'nc-flow-desc' }, description) : null,
        facts ? h('span', { class: 'nc-flow-facts' }, facts) : null);
    }
    function renderFlowCards() {
      const flows = getFlows(), q = S.flowQuery.trim().toLowerCase();
      flowSearch.hidden = flows.length < FLOW_SEARCH_FROM;
      const hit = (name, description) => !q || name.toLowerCase().includes(q) || (description || '').toLowerCase().includes(q);
      const cards = flows.filter((f) => hit(f.name, f.description)).map((f) => flowCard(`flow:${f.name}`, f.name, f.description, flowFacts(f)));
      const builtIns = BUILT_IN.filter((b) => hit(b.title, b.description)).map((b) => flowCard(b.key, b.title, b.description, 'Built-in'));
      flowCards.replaceChildren(...cards, ...builtIns);
      if (!flows.length) flowCards.prepend(h('p', { class: 'nc-hint nc-empty' }, 'No flows are active yet. Create and activate one under Flows.'));
      else if (!cards.length && !builtIns.length) flowCards.append(h('p', { class: 'nc-hint nc-empty' }, 'No flow matches that search.'));
    }

    // ---- input
    function renderFlowInputs() {
      const flow = flowOf();
      flowDesc.hidden = !flow; flowInputs.hidden = !flow;
      if (!flow) { flowInputs.dataset.flow = ''; return; }
      flowDesc.textContent = Object.entries(flow.steps).map(([id, s]) => `${id} (${s.kind})`).join(' → ');
      if (flowInputs.dataset.flow === flow.name) return;
      flowInputs.dataset.flow = flow.name;
      const fields = flowFields(flow.input);
      flowInputs.replaceChildren(...fields.map(({ key, value, type, long }) => h('label', { class: 'nc-field' }, key,
        type === 'boolean' ? h('select', { 'data-key': key, value: String(value) }, h('option', { value: 'true', selected: value === true }, 'true'), h('option', { value: 'false', selected: value === false }, 'false'))
          : long ? h('textarea', { 'data-key': key, rows: 3, value: value ?? '' })
            : h('input', { 'data-key': key, value: value ?? '', type: type === 'number' ? 'number' : 'text' }))));
      if (!fields.length) flowInputs.append(h('label', { class: 'nc-field' }, 'Input', h('textarea', { id: 'n-flow-text', rows: 3, placeholder: 'Optional' }), h('small', {}, 'The flow’s steps can read this as {{ input.text }}.')));
    }

    const removeButton = (label, onclick) => h('button', { type: 'button', class: 'nc-remove', 'aria-label': label, title: label, onclick }, '×');
    function renderContext(focusLast) {
      fileList.hidden = !S.files.length;
      fileList.replaceChildren(...S.files.map((file) => h('li', { 'data-state': file.state },
        h('span', { class: 'nc-file-name' }, file.name),
        h('small', {}, file.state === 'uploading' ? 'Uploading…' : file.state === 'failed' ? file.error : formatBytes(file.size)),
        removeButton(`Remove ${file.name}`, () => removeFile(file)))));
      repoList.replaceChildren(...S.repositories.map((repo, index) => {
        const provider = h('span', { class: 'nc-provider' }, repoProvider(repo.url));
        return h('div', { class: 'nc-source', 'data-source': 'repository' }, provider,
          h('input', { class: 'nc-grow', 'data-field': 'url', value: repo.url, placeholder: 'https://github.com/team/project.git', 'aria-label': 'Repository URL', spellcheck: 'false',
            oninput: (e) => { repo.url = e.target.value; provider.textContent = repoProvider(repo.url); clearError(); } }),
          h('input', { class: 'nc-ref', 'data-field': 'ref', value: repo.ref, placeholder: 'Default branch', 'aria-label': 'Branch, tag or commit', spellcheck: 'false', oninput: (e) => { repo.ref = e.target.value; } }),
          removeButton('Remove repository', () => { S.repositories.splice(index, 1); renderContext(); }));
      }));
      confluenceList.replaceChildren(...S.confluence.map((page, index) => h('div', { class: 'nc-source', 'data-source': 'confluence' },
        h('span', { class: 'nc-provider' }, 'Confluence'),
        h('input', { class: 'nc-grow', 'data-field': 'url', value: page.url, placeholder: 'Link to the root page of the tree', 'aria-label': 'Confluence page link', spellcheck: 'false',
          oninput: (e) => { page.url = e.target.value; clearError(); } }),
        h('label', { class: 'nc-check' }, h('input', { type: 'checkbox', 'data-field': 'descendants', checked: page.include_descendants, onchange: (e) => { page.include_descendants = e.target.checked; } }), 'Child pages'),
        removeButton('Remove Confluence page', () => { S.confluence.splice(index, 1); renderContext(); }))));
      const remote = S.repositories.length || S.confluence.length;
      const hint = contextBox.querySelector('#n-context-hint');
      const size = S.files.reduce((sum, f) => sum + (f.size || 0), 0);
      const tooBig = size > MAX_ATTACHED_BYTES;
      hint.hidden = !remote && !tooBig;
      hint.classList.toggle('nc-warning', tooBig);
      hint.textContent = tooBig ? `The attached files are ${formatBytes(size)}; a run can carry at most 64 MB.`
        : `A worker fetches these with its own git and Confluence credentials. Everything attached must fit in 64 MB${S.mode === 'flow' ? '; every step gets a copy' : ''}.`;
      if (focusLast) (repoList.lastElementChild || confluenceList.lastElementChild)?.querySelector('[data-field="url"]')?.focus();
    }
    async function addFiles(list) {
      for (const source of [...list]) {
        const file = { name: source.name, size: source.size, state: 'uploading', id: null };
        S.files.push(file);
        renderContext(); refresh();
        const owner = S;
        try {
          const response = await fetch(`/api/uploads?name=${encodeURIComponent(source.name)}`, { method: 'POST', body: source, headers: { 'Content-Type': 'application/octet-stream' } });
          const data = await response.json().catch(() => ({}));
          if (!response.ok) throw new Error(data.error || response.statusText);
          file.id = data.id; file.state = 'ready';
        } catch (e) { file.state = 'failed'; file.error = e.message; }
        if (owner === S) { renderContext(); refresh(); }
      }
    }
    function removeFile(file) {
      S.files = S.files.filter((f) => f !== file);
      if (file.id) api('DELETE', `/api/uploads/${file.id}`).catch(() => {}); // unused uploads also expire on their own
      renderContext(); refresh();
    }

    // ---- where
    function cardBlock() {
      if (S.mode === 'flow') return getSystem() === 'trigger' ? 'Flows run on the local development worker. Choose the Trigger.dev · local tab, Hatchet or Prefect.' : null;
      if (!['hatchet', 'prefect'].includes(getSystem())) return 'Trigger.dev runs cards on the coordinator, not on fleet workers. Use Hatchet or Prefect to choose workers.';
      if (S.mode === 'agent' && (workspace.value.trim() || resume.value.trim())) return 'An existing workspace or resumed session stays on the coordinator. Clear them under Advanced to choose workers.';
      return null;
    }
    function chipButton(label, pressed, onclick, attrs = {}) {
      return h('button', { type: 'button', class: 'nc-chip', 'aria-pressed': String(pressed), onclick, ...attrs }, label);
    }
    function renderWhere() {
      const workers = getWorkers(), block = placeable() ? cardBlock() : null, pinned = !!S.workerId;
      const pools = [...new Set(workers.flatMap(poolsOf))].sort();
      S.pools = S.pools.filter((p) => pools.includes(p) || !workers.length);
      poolChips.replaceChildren(
        chipButton('All pools', !S.pools.length, () => { S.pools = []; renderWhere(); scheduleCheck(); }, { 'data-pool': '', disabled: pinned }),
        ...pools.map((pool) => chipButton(`${pool} · ${workers.filter((w) => poolsOf(w).includes(pool)).length}`, S.pools.includes(pool), () => {
          S.pools = S.pools.includes(pool) ? S.pools.filter((p) => p !== pool) : [...S.pools, pool];
          renderWhere(); scheduleCheck();
        }, { 'data-pool': pool, disabled: pinned })));
      const known = [...new Set([...tagCatalog, ...workers.flatMap((w) => w.tags || []), ...S.tags])].sort();
      tagChips.replaceChildren(...known.map((tag) => chipButton(tag, S.tags.includes(tag), () => {
        S.tags = S.tags.includes(tag) ? S.tags.filter((t) => t !== tag) : [...S.tags, tag];
        renderWhere(); scheduleCheck();
      }, { 'data-tag': tag })));
      tagOptions.replaceChildren(...known.filter((t) => !S.tags.includes(t)).map((t) => h('option', { value: t })));
      fit.textContent = fitText(workers, selection(S));
      fit.classList.toggle('nc-warning', !matchingWorkers(workers, selection(S)).length);
      specific.replaceChildren(h('option', { value: '' }, 'Any matching worker'),
        ...workers.map((w) => h('option', { value: w.id }, `${w.id}${w.online ? '' : ' (offline)'}`)));
      specific.value = S.workerId;
      const flow = flowOf();
      const stepKey = flow ? flow.name + JSON.stringify(workers) : '';
      if (stepWorkers.dataset.key !== stepKey) {
        stepWorkers.dataset.key = stepKey;
        stepWorkers.replaceChildren(...(flow ? [h('span', { class: 'nc-label' }, 'Choose workers for individual steps'),
          ...Object.keys(flow.steps).map((sid) => workerPicker(sid, S.steps[sid], (value) => {
            if (Object.keys(value).length) S.steps[sid] = value; else delete S.steps[sid];
            scheduleCheck();
          })),
          h('small', {}, flow.environment ? 'Environment context is prepared in setup_environment. File source paths refer to that worker PC.' : 'Steps receive files from their dependencies, including across worker PCs.')] : []));
      }
      whereFields.hidden = !placeable();
      whereFields.querySelectorAll('input, select, button').forEach((n) => { n.disabled = !!block || (pinned && n.dataset.pool !== undefined); });
      whereNote.hidden = !block && placeable();
      whereNote.textContent = !placeable() ? 'This built-in demo runs where its engine runs it.' : block || '';
      whereMore.hidden = !workers.length;
    }
    function addTag() {
      const tags = tagInput.value.split(',').map((t) => t.trim()).filter(Boolean);
      if (!tags.length) { tagInput.value = ''; return; }
      S.tags = [...new Set([...S.tags, ...tags])];
      tagInput.value = '';
      renderWhere(); scheduleCheck();
    }

    // Ask the flow host whether a registered worker can run what is about to start: ready, wait
    // (offline, busy or unchecked: it starts by itself later) or blocked (start is refused).
    function launchExecution() { return { default: selection(S), steps: S.steps }; }
    function cardSelection() {
      if (cardBlock()) return null;
      const wanted = selection(S);
      return Object.keys(wanted).length ? wanted : { tags: [] };
    }
    function launchRequest() {
      const system = getSystem();
      if (S.mode === 'flow') return flowOf() && system !== 'trigger' ? { flow: flowOf().name, execution: launchExecution(), system } : null;
      const worker = cardSelection();
      if (!worker) return null;
      if (S.mode === 'program') return command.value.trim() ? { card: { agent: 'program', worker, command: command.value.trim() }, system } : null;
      return { card: { agent: agentSelect.value, worker }, system };
    }
    function scheduleCheck() { clearTimeout(launch.timer); launch.timer = setTimeout(checkLaunch, 250); }
    async function checkLaunch() {
      const body = launchRequest(), key = body ? JSON.stringify(body) : null;
      if (key === launch.key) return;
      launch.key = key;
      const serial = ++launch.serial;
      if (!body) { Object.assign(launch, { result: null, blocked: false }); renderLaunch(); updateCreate(); return; }
      // Keep showing the previous answer for the same card or flow while the new one is on its way.
      const previous = launch.result?.body, same = previous && (body.card ? !!previous.card : previous.flow === body.flow);
      launch.result = same ? { ...launch.result, pending: true } : { body, pending: true };
      renderLaunch();
      let result;
      try { result = { body, data: await api('POST', '/api/launch/validate', body) }; } catch (e) { result = { body, error: e.message }; }
      if (serial !== launch.serial) return;
      Object.assign(launch, { result, blocked: result.data?.status === 'blocked' });
      renderLaunch(); updateCreate();
      if (result.data?.status === 'blocked') launch.timer = setTimeout(() => { if (dialog.open) { launch.key = null; checkLaunch(); } }, 5000);
    }
    function renderLaunch() {
      const r = launch.result, data = r?.data, wrap = placement;
      wrap.hidden = !r;
      if (!r) { wrap.replaceChildren(); return; }
      const parts = [];
      if (r.pending) parts.push(h('p', { class: 'status', role: 'status' }, 'Checking the fleet…'));
      if (r.error) parts.push(h('p', { class: 'status error' }, `Couldn't check the fleet: ${r.error}. The board checks again when you start.`));
      if (data?.card) parts.push(placementView(data.card, { title: 'Where this card can run' }));
      else if (data && data.fleet === false) parts.push(placementView(data));
      else if (data) parts.push(h('strong', {}, 'Where each step can run'),
        ...Object.entries(data.steps || {}).map(([sid, decision]) => h('section', { 'data-step': sid }, h('h5', {}, sid), placementView(decision))));
      if (data?.status === 'blocked') parts.push(h('p', { class: 'nc-hint nc-warning' }, r.body.card
        ? 'Launch is blocked until a registered worker can run this card. Change the selection, or fix the worker in the fleet tab.'
        : 'Launch is blocked until every step can run on a registered worker. The flow stays saved: change the workers here, or fix the flow in Flows or the worker in the fleet tab.'));
      const headline = data?.status === 'ready' ? 'Ready to start' : data?.status === 'wait' ? 'Waits for a worker' : data?.status === 'blocked' ? 'Cannot start'
        : r.error ? 'Couldn’t check the fleet' : 'Checking the fleet…';
      const details = wrap.querySelector('details');
      const open = data?.status === 'blocked' || !!r.error || (details ? details.open : false);
      wrap.replaceChildren(h('details', { class: `nc-more nc-check-${data?.status || 'pending'}`, open }, h('summary', {}, headline), h('div', { class: 'nc-stack' }, parts)));
    }

    // ---- when
    function ensureLater() {
      if (S.when === 'later' && !S.at) { S.at = localInputValue(defaultLater()); at.value = S.at; }
      syncTimeFromAt();
    }
    function syncTimeFromAt() {
      if (S.when === 'later' && S.at) { S.time = S.at.slice(11, 16); time.value = S.time; const date = new Date(S.at); if (!Number.isNaN(date.getTime())) S.days = [date.getDay()]; }
    }
    function ensureTime() {
      if (!S.time) { const now = new Date(); S.time = `${pad(now.getHours())}:${pad(now.getMinutes())}`; time.value = S.time; }
    }
    const scheduleTiming = () => {
      const cronText = cronFor(S);
      return {
        ...(S.when === 'later' && S.at ? { at: new Date(S.at).toISOString() } : {}),
        ...(cronText ? { cron: cronText } : {}),
        timezone: Intl.DateTimeFormat().resolvedOptions().timeZone,
        utcOffset: -new Date().getTimezoneOffset(),
        summary: repeatSummary(S),
      };
    };
    function renderWhen() {
      whenButtons.forEach((n) => { n.querySelector('input').checked = n.querySelector('input').value === S.when; });
      atWrap.hidden = S.when !== 'later';
      repeat.value = S.repeat;
      timeWrap.hidden = !['daily', 'weekdays', 'weekly'].includes(S.repeat);
      cronWrap.hidden = S.repeat !== 'custom';
      dayChips.hidden = S.repeat !== 'weekly';
      overlapRow.hidden = S.repeat === 'none';
      overlap.value = S.overlap;
      dayChips.replaceChildren(...DAYS.map(([day, name]) => chipButton(name, S.days.includes(day), () => {
        S.days = S.days.includes(day) ? S.days.filter((d) => d !== day) : [...S.days, day];
        if (!S.days.length) S.days = [day];
        renderWhen();
      }, { 'data-day': day })));
      updateCreate();
      schedulePreview();
    }
    function schedulePreview() {
      clearTimeout(previewTimer);
      const scheduled = S.when === 'later' || S.repeat !== 'none';
      if (!scheduled) { S.preview = S.previewError = null; preview.hidden = true; previewSerial++; updateCreate(); return; }
      previewTimer = setTimeout(async () => {
        const serial = ++previewSerial, timing = scheduleTiming();
        if (S.repeat === 'custom' && !S.cron.trim() && !timing.at) { preview.hidden = true; return; }
        try {
          const data = await api('POST', '/api/schedules/preview', timing);
          if (serial !== previewSerial) return;
          S.preview = data.next; S.previewError = null;
          const zone = timing.timezone ? ` (${timing.timezone})` : '';
          preview.textContent = `${S.when === 'now' ? 'Then at' : data.next.length > 1 ? 'Runs at' : 'Runs'} ${data.next.map(readableTime).join(', ')}${data.next.length > 1 ? '…' : ''}${zone}`;
          preview.classList.remove('nc-warning');
        } catch (e) {
          if (serial !== previewSerial) return;
          S.preview = null; S.previewError = e.message;
          preview.textContent = e.message;
          preview.classList.add('nc-warning');
        }
        preview.hidden = false;
        updateCreate();
      }, 200);
    }

    // ---- everything else
    function clearError() { error.hidden = true; error.textContent = ''; }
    function showError(message, field) {
      error.textContent = message; error.hidden = false;
      field?.focus?.();
    }
    function updateCreate() {
      const system = getSystem();
      const uploading = S.files.some((f) => f.state === 'uploading');
      const needsLocal = system === 'trigger' && !builtIn();
      create.disabled = needsLocal || launch.blocked || uploading || !!S.previewError;
      create.title = launch.blocked ? 'Launch is blocked: no registered worker can run it (see Where it runs)' : uploading ? 'Waiting for the uploads to finish' : '';
      create.textContent = startLabel({ mode: S.mode, when: S.when, repeat: S.repeat, session: !builtIn() });
      productionNote.hidden = !needsLocal;
      productionNote.textContent = S.mode === 'flow' ? 'Flows run on the local development worker. Choose the Trigger.dev · local tab, Hatchet or Prefect.'
        : 'Agents and programs need a local worker. Choose the Trigger.dev · local tab, Hatchet or Prefect.';
    }
    function refresh() {
      const flow = flowOf(), built = builtIn(), agent = S.mode === 'agent';
      kind.value = S.mode === 'program' ? 'program' : agent ? 'agent-session' : (S.flowKey || '');
      flowPicker.hidden = S.mode !== 'flow';
      if (S.mode === 'flow') renderFlowCards();
      agentRow.hidden = !agent;
      goalRow.hidden = !agent && !built;
      goalLabel.textContent = agent ? 'Prompt' : built?.label || 'Goal';
      goal.placeholder = agent ? 'What should the agent do?' : '';
      demoNote.hidden = !agent || agentSelect.value !== 'demo';
      programFields.hidden = S.mode !== 'program';
      builtInFields.hidden = S.flowKey !== 'steerable-agent';
      contextBox.hidden = !!built || (S.mode === 'flow' && !flow);
      advanced.hidden = !agent;
      renderFlowInputs();
      renderContext();
      renderWhere();
      updateCreate();
      scheduleCheck();
    }

    function requestBody() {
      const context = {
        ...(S.files.length ? { files: S.files.filter((f) => f.state === 'ready').map(({ id, name }) => ({ id, name })) } : {}),
        ...(S.repositories.length ? { repositories: S.repositories.map(({ url, ref }) => ({ url: url.trim(), ...(ref.trim() ? { ref: ref.trim() } : {}) })) } : {}),
        ...(S.confluence.length ? { confluence: S.confluence.map(({ url, include_descendants }) => ({ url: url.trim(), include_descendants })) } : {}),
      };
      const withContext = (text) => ({ ...context, ...(text ? { text } : {}) });
      const hasContext = (ctx) => (Object.keys(ctx).length ? { context: ctx } : {});
      const built = builtIn();
      if (built) return { kind: built.key, goal: goal.value, ...(built.key === 'steerable-agent' ? { turns: Number(turns.value), stepSeconds: Number(stepSeconds.value) } : {}) };
      if (S.mode === 'flow') {
        const flow = flowOf();
        const input = Object.fromEntries([...flowInputs.querySelectorAll('[data-key]')].map((n) => [n.dataset.key,
          n.tagName === 'SELECT' ? n.value === 'true' : n.type === 'number' ? Number(n.value) : n.value]));
        const text = flowInputs.querySelector('#n-flow-text')?.value.trim();
        return { kind: 'flow', flow: flow.name, execution: launchExecution(), input, ...hasContext(withContext(text)) };
      }
      const worker = cardSelection();
      if (S.mode === 'program') return { kind: 'program', command: command.value.trim(), ...(worker ? { worker } : {}), ...hasContext(withContext(programInput.value.trim())) };
      return { kind: 'agent-session', goal: goal.value, agent: agentSelect.value, workspace: workspace.value.trim(), resumeSessionId: resume.value.trim(),
        simulateFailures: Number(failures.value), ...(worker ? { worker } : {}), ...hasContext(context) };
    }
    function problem() {
      if (S.mode === 'agent' && !goal.value.trim()) return ['Write a prompt for the agent.', goal];
      if (S.mode === 'program' && !command.value.trim()) return ['Write the command to run.', command];
      if (S.mode === 'flow' && !S.flowKey) return ['Choose a flow to start.', flowCards.querySelector('input')];
      if (builtIn() && !goal.value.trim()) return [`Write the ${builtIn().label.toLowerCase()}.`, goal];
      if (S.files.some((f) => f.state === 'failed')) return ['A file did not upload. Remove it or attach it again.', null];
      if (S.files.reduce((sum, f) => sum + (f.size || 0), 0) > MAX_ATTACHED_BYTES) return ['The attached files exceed the 64 MB a run can carry.', null];
      const repo = S.repositories.findIndex((r) => !isGitUrl(r.url));
      if (repo >= 0) return ['Enter a git URL: https://, ssh:// or git@host:path.', repoList.children[repo]?.querySelector('[data-field="url"]')];
      const page = S.confluence.findIndex((p) => !isConfluenceUrl(p.url));
      if (page >= 0) return ['Paste the link of a Confluence page. It contains /spaces/<space>/pages/<id> or pageId=<id>.', confluenceList.children[page]?.querySelector('[data-field="url"]')];
      if (S.when === 'later' && (!S.at || new Date(S.at) <= new Date())) return ['Choose a start time in the future.', at];
      if (S.repeat === 'custom' && !S.cron.trim()) return ['Enter a cron expression.', cron];
      return null;
    }
    async function submit(e) {
      if (e.submitter?.value !== 'create') return;
      e.preventDefault();
      clearError();
      const wrong = problem();
      if (wrong) { showError(...wrong); return; }
      if (create.disabled) { updateCreate(); return; }
      const scheduled = S.when === 'later' || S.repeat !== 'none';
      const body = { ...requestBody(), ...(scheduled ? { schedule: { ...scheduleTiming(), ...(S.repeat !== 'none' ? { overlap: S.overlap } : {}), ...(S.when === 'now' ? { runNow: true } : {}) } } : {}) };
      const system = getSystem(), board = getBoard(), opensCard = S.mode !== 'flow' || !builtIn();
      create.disabled = true;
      try {
        const result = await api('POST', `/api/boards/${system}/${encodeURIComponent(board)}/cards`, body);
        reset();
        await onClose();
        if (result.schedule) onScheduled(result.schedule);
        if (result.id) onCreated(result.id, opensCard, system, board);
      } catch (err) { showError(err.message); }
      finally { updateCreate(); }
    }

    function reset() {
      S = fresh();
      launch.key = null;
      Object.assign(launch, { result: null, blocked: false });
      goal.value = ''; command.value = ''; programInput.value = ''; workspace.value = ''; resume.value = ''; failures.value = '0'; turns.value = '8'; stepSeconds.value = '5';
      cron.value = ''; at.value = ''; time.value = ''; repeat.value = 'none'; tagInput.value = ''; flowSearch.value = '';
      preview.hidden = true; advanced.open = false; whereMore.open = false;
      flowInputs.dataset.flow = ''; stepWorkers.dataset.key = '';
      modeGroup.querySelectorAll('input').forEach((n) => { n.checked = n.value === 'agent'; });
      clearError(); renderLaunch(); renderWhen(); refresh();
    }

    /** `kind` opens the form on one flow: flow:<name> (Run… in the flow builder). */
    function open(kindToOpen) {
      launch.key = null; // the fleet may have changed since the dialog was last open
      if (kindToOpen) { S.mode = 'flow'; S.flowKey = kindToOpen; modeGroup.querySelectorAll('input').forEach((n) => { n.checked = n.value === 'flow'; }); flowInputs.dataset.flow = ''; }
      refresh(); renderWhen();
      Promise.all([loadFleet(), api('GET', '/api/fleet/tags').then((d) => { tagCatalog = d.tags || []; }).catch(() => {})]).then(refresh);
      dialog.showModal();
      deps.onOpen?.();
      if (S.mode === 'agent') goal.focus();
    }

    refresh(); renderWhen();
    return { open, refresh, reset, selected: () => kind.value };
  }

  /** A schedule in the board's Scheduled column. */
  function scheduleNode(item, { run, toggle, remove }) {
    const paused = !item.enabled, repeating = !!item.cron;
    const button = (label, onclick, cls = 'secondary') => h('button', { type: 'button', class: `${cls} nc-mini`, onclick }, label);
    return h('div', { class: `card nc-schedule${paused ? ' nc-paused' : ''}`, 'data-schedule': item.id },
      h('div', { class: 'title' }, item.title),
      h('div', { class: 'meta' },
        h('span', { class: `chip ${paused ? '' : 'scheduled'}` }, paused ? 'paused' : repeating ? 'repeats' : 'once'),
        item.summary ? h('span', {}, item.summary) : null,
        repeating && item.overlap === 'start' ? h('span', { title: 'Starts even while the previous run is active' }, 'runs may overlap') : null,
        !paused && item.next_at ? h('span', {}, h('time', { datetime: item.next_at }, `next ${readableTime(item.next_at)}`)) : null),
      item.last_error ? h('div', { class: 'last nc-schedule-error' }, `Couldn’t start: ${item.last_error}`) : null,
      item.last_skip ? h('div', { class: 'last' }, `Skipped: ${item.last_skip}`) : null,
      h('div', { class: 'nc-schedule-actions' },
        button('Run now', () => run(item)),
        repeating || paused ? button(paused ? 'Resume' : 'Pause', () => toggle(item, paused)) : null,
        button('Delete', () => remove(item))));
  }

  return { mount, scheduleNode, selection, matchingWorkers, fitText, cronFor, repeatSummary, startLabel, repoProvider, isGitUrl, isConfluenceUrl,
    formatBytes, localInputValue, defaultLater, flowFields, flowFacts };
});
