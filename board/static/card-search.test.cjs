const { test } = require('node:test');
const assert = require('node:assert/strict');
const search = require('./card-search.js');

const card = (over = {}) => ({ id: 'run-1', system: 'hatchet', title: 'Compare vector databases', task: 'research-report',
  column: 'done', status: 'COMPLETED', agent: 'claude', last: 'wrote the summary',
  input: { goal: 'pick a store', options: { depth: 3 }, sources: ['arxiv', 'github'] },
  output: { summary: 'Recommend pgvector', score: 0.91 },
  workers: [{ step: 'fetch', id: 'gpu-02', hostname: 'gpu-02.lan', pool: 'gpu', tags: ['cuda', 'linux'] }], ...over });
const ids = (cards, query) => search.filter(cards, query).map((c) => c.id);

test('plain words must all appear somewhere on the card, ignoring case', () => {
  const cards = [card(), card({ id: 'run-2', title: 'Audit sbom tooling', output: null, workers: [] })];
  assert.deepEqual(ids(cards, 'VECTOR'), ['run-1']);
  assert.deepEqual(ids(cards, 'pgvector cuda'), ['run-1'], 'output and worker tags are searched');
  assert.deepEqual(ids(cards, 'arxiv'), ['run-1', 'run-2'], 'input values are searched');
  assert.deepEqual(ids(cards, 'vector sbom'), [], 'every word must match');
  assert.deepEqual(ids(cards, '   '), ['run-1', 'run-2'], 'an empty query keeps every card');
});

test('fields narrow a word to part of the card, including keys inside input and output', () => {
  const cards = [card(), card({ id: 'run-2', title: 'GPU notes', workers: [{ id: 'pc-a', pool: 'default', tags: [] }] })];
  assert.deepEqual(ids(cards, 'worker:gpu-02'), ['run-1']);
  assert.deepEqual(ids(cards, 'title:gpu'), ['run-2']);
  assert.deepEqual(ids(cards, 'pool:gpu'), ['run-1']);
  assert.deepEqual(ids(cards, 'goal:store'), ['run-1', 'run-2'], 'a bare key finds input.goal');
  assert.deepEqual(ids(cards, 'depth:3'), ['run-1', 'run-2'], 'nested keys match by their last part');
  assert.deepEqual(ids(cards, 'output.summary:pgvector'), ['run-1', 'run-2']);
  assert.deepEqual(ids(cards, 'input:github'), ['run-1', 'run-2'], 'input: searches all of the input');
  assert.deepEqual(ids(cards, 'output:arxiv'), [], 'output: does not look at the input');
});

test('status words cover the engine status, the column and its everyday names', () => {
  const cards = [card(), card({ id: 'f', column: 'stopped', status: 'CRASHED' }), card({ id: 'w', column: 'waiting', status: 'RUNNING', session_state: 'awaiting_review' })];
  assert.deepEqual(ids(cards, 'status:failed'), ['f']);
  assert.deepEqual(ids(cards, 'status:completed'), ['run-1']);
  assert.deepEqual(ids(cards, 'status:"awaiting review"'), ['w']);
});

test('phrases, negation and presence checks', () => {
  const cards = [card(), card({ id: 'run-2', title: 'vector of databases', workers: [] })];
  assert.deepEqual(ids(cards, '"vector databases"'), ['run-1']);
  assert.deepEqual(ids(cards, '-cuda'), ['run-2']);
  assert.deepEqual(ids(cards, '-worker:gpu'), ['run-2']);
  assert.deepEqual(ids(cards, 'worker:'), ['run-1']);
  assert.deepEqual(ids(cards, 'title:"vector of'), ['run-2'], 'an unclosed phrase runs to the end');
  assert.deepEqual(ids(cards, '-'), ['run-1', 'run-2'], 'a lone dash is ignored');
});

test('a changed card is indexed again; an unchanged one is not', () => {
  const a = card();
  assert.equal(search.index(a), search.index(a));
  const b = { ...a, title: 'Something else' };
  assert.deepEqual(ids([b], 'databases'), [], 'the new object is indexed from its own fields');
  assert.deepEqual(ids([b], 'title:something'), ['run-1']);
});

test('typing further into a query narrows the previous hits; deleting or negating more does not', () => {
  assert.equal(search.narrows('worker:pc', 'worker:pc-1'), true);
  assert.equal(search.narrows('gpu', 'gpu report'), true, 'another word');
  assert.equal(search.narrows('gpu', 'gpu -failed'), true, 'another exclusion');
  assert.equal(search.narrows('gpu report', 'gpu'), false);
  assert.equal(search.narrows('worker:pc-1', 'worker:pc'), false);
  assert.equal(search.narrows('-fail', '-failed'), false, 'a longer exclusion leaves more cards in');
  assert.equal(search.narrows('pc', 'worker:pc'), false, 'a different field');
  assert.equal(search.narrows('', 'gpu'), false, 'nothing to narrow from');
});

test('10,000 cards filter well within a frame', () => {
  const cards = Array.from({ length: 10000 }, (_, i) => card({ id: `c${i}`, title: `Task ${i} about topic ${i % 97}`,
    workers: [{ id: `pc-${i % 13}`, pool: i % 2 ? 'gpu' : 'default', tags: ['linux'] }] }));
  search.filter(cards, 'x'); // build the indexes once, as the first keystroke does
  const keystrokes = ['t', 'to', 'topic', 'topic 4', 'w', 'worker:', 'worker:pc', 'worker:pc-1', 'worker:pc-1 -pool:gpu', 'goal:store depth:3 cuda'];
  const started = performance.now();
  for (const query of keystrokes) search.filter(cards, query);
  const each = (performance.now() - started) / keystrokes.length;
  assert.ok(each < 16, `one query over 10,000 cards took ${each.toFixed(1)} ms`);
});
