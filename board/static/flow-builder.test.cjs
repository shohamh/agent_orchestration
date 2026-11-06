const { test } = require('node:test');
const assert = require('node:assert/strict');
const fb = require('./flow-builder.js');

// The multi-agent-design example: a brief, two agents in parallel, then a join.
const design = () => ({
  name: 'multi-agent-design', description: 'Design and review', input: { feature: 'status page' },
  environment: { agent: 'codex', files: [] },
  steps: {
    brief: { kind: 'program', run: "python -c \"print('brief')\"" },
    architecture: { kind: 'agent', needs: ['brief'], agent: 'codex', review: false, prompt: 'Design {{ input.feature }}\n{{ steps.brief.output.stdout }}' },
    risk_review: { kind: 'agent', needs: ['brief'], agent: 'opencode2', prompt: 'Review {{input.feature}} {{steps.brief.output}}', setup: { keep: true } },
    implementation_plan: { kind: 'agent', needs: ['architecture', 'risk_review'], agent: 'codex', worker: { pool: 'gpu', tags: ['x'] },
      prompt: 'A: {{ steps.architecture.output }}\nR: {{ steps.risk_review.output.summary }}\nNot a ref: steps.architecture.output, {{ steps.architecture_old.output }}' },
  },
});

test('renaming a step updates needs and template references in one edit, and keeps order and other fields', () => {
  const before = design();
  const result = fb.renameStep(before, 'architecture', 'design');
  assert.equal(result.ok, true);
  const doc = result.doc;
  assert.deepEqual(Object.keys(doc.steps), ['brief', 'design', 'risk_review', 'implementation_plan']);
  assert.deepEqual(doc.steps.implementation_plan.needs, ['design', 'risk_review']);
  assert.equal(doc.steps.implementation_plan.prompt,
    'A: {{ steps.design.output }}\nR: {{ steps.risk_review.output.summary }}\nNot a ref: steps.architecture.output, {{ steps.architecture_old.output }}');
  assert.deepEqual(doc.steps.implementation_plan.worker, { pool: 'gpu', tags: ['x'] });
  assert.deepEqual(doc.steps.risk_review.setup, { keep: true });
  assert.deepEqual(doc.environment, before.environment);
  assert.equal(before.steps.architecture.kind, 'agent', 'the original document is not mutated');

  const renamed = fb.renameStep(design(), 'brief', 'intro').doc;
  assert.equal(renamed.steps.risk_review.prompt, 'Review {{input.feature}} {{steps.intro.output}}', 'spacing inside braces does not matter');
  assert.match(renamed.steps.architecture.prompt, /\{\{ steps\.intro\.output\.stdout \}\}/);
});

test('a rename that collides or is not a valid ID is rejected without changes', () => {
  const doc = design();
  for (const to of ['brief', 'Bad-Name', '9steps', '', 'a'.repeat(49)]) {
    const result = fb.renameStep(doc, 'architecture', to);
    assert.equal(result.ok, false, to);
    assert.equal(result.doc, doc);
    assert.ok(result.error);
  }
  assert.equal(fb.renameStep(doc, 'brief', 'setup_environment').ok, false, 'reserved when an environment is configured');
  assert.equal(fb.renameStep(doc, 'missing', 'x').ok, false);
  assert.equal(fb.renameStep(doc, 'brief', 'brief').unchanged, true);
});

test('renaming an input updates {{ input.x }} references and keeps input order', () => {
  const doc = { ...design(), input: { feature: 'x', other: 1 } };
  const result = fb.renameInput(doc, 'feature', 'topic');
  assert.deepEqual(Object.keys(result.doc.input), ['topic', 'other']);
  assert.match(result.doc.steps.architecture.prompt, /\{\{ input\.topic \}\}/);
  assert.match(result.doc.steps.risk_review.prompt, /\{\{input\.topic\}\}/);
  assert.equal(fb.renameInput(doc, 'feature', 'other').ok, false);
});

test('the list of flows filters by name, state, steps and agents', () => {
  const entry = { name: 'nightly', state: 'inactive', document: { description: 'Audit', steps: { scan: { kind: 'agent', agent: 'codex' } } } };
  assert.equal(fb.flowMatches(entry, ''), true);
  assert.equal(fb.flowMatches(entry, 'night'), true);
  assert.equal(fb.flowMatches(entry, 'state:inactive'), true);
  assert.equal(fb.flowMatches(entry, 'agent:codex step:scan'), true);
  assert.equal(fb.flowMatches(entry, '-agent:codex'), false);
  assert.equal(fb.flowMatches(entry, 'agent:claude'), false);
});

test('duplicating a step copies its settings and what it waits for; nothing waits for the copy', () => {
  const doc = { name: 'f', steps: { a: { kind: 'program', run: 'x', needs: [] }, b: { kind: 'agent', agent: 'codex', prompt: 'p', needs: ['a'] }, c: { kind: 'program', run: 'y', needs: ['b'] } } };
  const first = fb.duplicateStep(doc, 'b');
  assert.equal(first.ok, true);
  assert.equal(first.id, 'b_copy');
  assert.deepEqual(first.doc.steps.b_copy, doc.steps.b);
  assert.deepEqual(first.doc.steps.c.needs, ['b'], 'dependents keep the original');
  assert.equal(doc.steps.b_copy, undefined, 'the document it was given is untouched');
  assert.equal(fb.duplicateStep(first.doc, 'b').id, 'b_copy_1', 'a second copy gets its own ID');
  assert.equal(fb.duplicateStep(doc, 'nope').ok, false);
});

test('dependencies: add, duplicate, remove, and cycle prevention', () => {
  let doc = design();
  assert.equal(fb.addDependency(doc, 'architecture', 'architecture').ok, false, 'self-dependency');
  const cycle = fb.addDependency(doc, 'implementation_plan', 'brief');
  assert.equal(cycle.ok, false);
  assert.match(cycle.error, /cycle/);
  assert.equal(fb.addDependency(doc, 'risk_review', 'architecture').ok, true, 'a sibling dependency is fine');
  assert.equal(fb.addDependency(doc, 'brief', 'architecture').unchanged, true);
  doc = fb.addDependency(doc, 'risk_review', 'architecture').doc;
  assert.equal(fb.addDependency(doc, 'architecture', 'risk_review').ok, false, 'now risk_review -> architecture would be a 2-cycle');
  assert.deepEqual(doc.steps.architecture.needs, ['brief', 'risk_review']);
  doc = fb.removeDependency(doc, 'brief', 'architecture').doc;
  doc = fb.removeDependency(doc, 'risk_review', 'architecture').doc;
  assert.deepEqual(doc.steps.architecture.needs, [], 'needs stays a list, as the flow host returns it');
  const options = fb.dependencyOptions(design(), 'brief');
  assert.deepEqual(options.map((o) => [o.id, o.disabled]), [['architecture', true], ['risk_review', true], ['implementation_plan', true]]);
  assert.deepEqual(fb.dependencyOptions(design(), 'architecture').map((o) => [o.id, o.disabled]), [['risk_review', false], ['implementation_plan', true]]);
});

test('adding and removing steps', () => {
  const added = fb.addStep(design(), { kind: 'agent', needs: ['brief', 'nope'] });
  assert.equal(added.id, 'agent_1');
  assert.deepEqual(added.doc.steps.agent_1.needs, ['brief']);
  assert.equal(added.doc.steps.agent_1.agent, 'demo');
  assert.equal(fb.addStep(added.doc, { kind: 'agent' }).id, 'agent_2');
  assert.equal(fb.addStep(design(), { id: 'brief' }).ok, false);
  const removed = fb.removeStep(design(), 'architecture');
  assert.deepEqual(removed.unlinked, ['implementation_plan']);
  assert.deepEqual(removed.doc.steps.implementation_plan.needs, ['risk_review']);
  assert.deepEqual(removed.dangling, ['implementation_plan'], 'prompts are never silently changed; validation points at them');
  assert.match(removed.doc.steps.implementation_plan.prompt, /steps\.architecture\.output/);
  assert.deepEqual(fb.referencesTo(design(), 'brief'), ['architecture', 'risk_review']);
});

test('auto-layout is layered by dependency depth and deterministic', () => {
  const a = fb.autoLayout(design()), b = fb.autoLayout(design());
  assert.deepEqual(a, b);
  const p = a.positions;
  assert.ok(p.architecture.y > p.brief.y);
  assert.equal(p.architecture.y, p.risk_review.y, 'parallel branches share a row');
  assert.ok(p.implementation_plan.y > p.architecture.y, 'the join sits below its branches');
  assert.ok(Math.abs(p.architecture.x - p.risk_review.x) >= fb.NODE_W, 'siblings do not overlap');
  const mid = (p.architecture.x + p.risk_review.x) / 2;
  assert.ok(Math.abs(p.implementation_plan.x - mid) < 1, 'the join is centred under its prerequisites');
  assert.ok(Math.abs(p.brief.x - mid) < 1);
  // Reordering keys of an unrelated object does not matter; a cycle from YAML still lays out.
  const cyclic = { steps: { a: { kind: 'program', needs: ['b'] }, b: { kind: 'program', needs: ['a'] }, c: { kind: 'program', needs: ['missing'] } } };
  const c = fb.autoLayout(cyclic);
  assert.deepEqual(Object.keys(c.positions), ['a', 'b', 'c']);
  assert.deepEqual(fb.autoLayout(cyclic), c);
  // Saved positions win; stale ones are ignored and missing ones are auto-placed.
  const merged = fb.layout(design(), { brief: { x: 500, y: 7 }, gone: { x: 1, y: 1 }, architecture: { x: 'bad' } });
  assert.deepEqual(merged.positions.brief, { x: 500, y: 7 });
  assert.deepEqual(merged.positions.architecture, p.architecture);
  assert.equal('gone' in merged.positions, false);
});

test('reference candidates are inputs plus direct prerequisites\' outputs only', () => {
  const doc = design();
  const texts = (id) => fb.referenceCandidates(doc, id).map((c) => c.text);
  assert.deepEqual(texts('architecture'), ['{{ input.feature }}', '{{ steps.brief.output }}', '{{ steps.brief.output.stdout }}']);
  assert.deepEqual(texts('implementation_plan'), ['{{ input.feature }}', '{{ steps.architecture.output }}', '{{ steps.risk_review.output }}', '{{ steps.risk_review.output.summary }}']);
  assert.deepEqual(texts('brief'), ['{{ input.feature }}'], 'no transitive or unrelated steps');
  assert.equal(texts('implementation_plan').some((t) => t.includes('brief')), false);
  assert.deepEqual(fb.insertText('ab', 1, 1, 'X'), { value: 'aXb', caret: 2 });
  assert.deepEqual(fb.insertText('abc', 1, 2, 'X'), { value: 'aXc', caret: 2 });
  assert.deepEqual(fb.insertText('ab', undefined, undefined, 'X'), { value: 'abX', caret: 3 });
});

test('compareDocuments summarises a conflict', () => {
  const mine = fb.addStep(design(), { id: 'extra' }).doc;
  const theirs = fb.removeStep(design(), 'risk_review').doc;
  theirs.description = 'changed';
  const diff = fb.compareDocuments(mine, theirs);
  assert.deepEqual(diff.onlyMine, ['risk_review', 'extra']);
  assert.deepEqual(diff.onlyTheirs, []);
  assert.deepEqual(diff.changed, ['implementation_plan']);
  assert.deepEqual(diff.flow, ['description']);
});

test('starter templates produce valid-looking documents', () => {
  for (const [key, template] of Object.entries(fb.TEMPLATES)) {
    const doc = template.make('x-flow');
    assert.equal(doc.name, 'x-flow', key);
    for (const [id, step] of Object.entries(doc.steps)) {
      assert.equal(fb.checkStepId({}, id), null);
      for (const need of step.needs || []) assert.ok(doc.steps[need], `${key}: ${id} needs ${need}`);
    }
  }
  const parallel = fb.layers(fb.TEMPLATES.parallel.make('p'));
  assert.deepEqual(parallel.rows, [['brief'], ['design', 'review'], ['plan']]);
});

test('placement words: every launch decision and execution state is a sentence, never a bare code', () => {
  assert.equal(fb.placementHeadline({ status: 'ready', candidates: ['pc-a', 'pc-b'] }), 'Can start now on pc-a, pc-b.');
  assert.match(fb.placementHeadline({ status: 'wait', waiting_reason: 'high_ram' }), /^Waits for a worker: RAM on the matching workers is at or above their limit\./);
  assert.equal(fb.placementHeadline({ status: 'blocked', message: 'No registered worker can run this step: x' }), 'No registered worker can run this step: x');
  assert.match(fb.placementHeadline({ status: 'ready', fleet: false, steps: {} }), /coordinator/);
  const record = (state, extra = {}) => fb.executionText({ state, waiting_reason: 'offline', ...extra });
  assert.equal(record('waiting'), 'Waiting for a worker: a matching worker is offline');
  assert.equal(record('waiting', { waiting_reason: 'intervention_needed', intervention_needed: true, message: 'pc-a lacks gpu' }), 'Intervention needed: pc-a lacks gpu');
  assert.equal(record('queued', { waiting_reason: 'awaiting_authorization' }), 'Queued on a worker: the worker admitted it and waits for the coordinator to confirm the start');
  assert.equal(record('queued', { waiting_reason: null }), 'Queued: choosing a worker');
  assert.equal(record('running', { waiting_reason: null, worker: { id: 'pc-a', hostname: 'build-01' } }), 'Running on pc-a (build-01)');
  assert.equal(record('cancelled'), 'Cancelled while waiting: a matching worker is offline');
  assert.equal(record('cancelled', { waiting_reason: null }), 'Cancelled');
  assert.equal(record('timed_out', { timeout_minutes: 60 }), 'Timed out after 60 minutes while waiting: a matching worker is offline');
  assert.equal(record('interrupted'), 'Interrupted by the engine while waiting: a matching worker is offline');
  for (const code of ['offline', 'credentials_unavailable', 'full_slots', 'high_cpu', 'high_ram', 'missing_metrics', 'cooldown', 'software_unknown', 'software_stale',
    'software_missing', 'awaiting_authorization', 'coordinator_unreachable']) assert.doesNotMatch(fb.waitingWords(code), /_/, code);
  assert.equal(fb.waitingWords('some_new_code'), 'some new code');
});
