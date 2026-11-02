const { test } = require('node:test');
const assert = require('node:assert/strict');
let chat = {};
try { chat = require('./chat.js'); } catch (error) { if (error.code !== 'MODULE_NOT_FOUND') throw error; }

test('streamed messages coalesce by message_id without merging different turns', () => {
  assert.equal(typeof chat.transcript, 'function', 'chat transcript reducer is available');
  const result = chat.transcript([
    { type: 'agent', message_id: 'a', text: 'Hello ' },
    { type: 'tool', id: 't', name: 'Read', status: 'running' },
    { type: 'agent', message_id: 'a', text: 'world' },
    { type: 'tool', id: 't', output: 'contents', status: 'ok' },
    { type: 'agent', message_id: 'b', text: 'Next turn' },
  ]);
  assert.equal(result.length, 3);
  assert.equal(result[0].text, 'Hello world');
  assert.deepEqual(result[1], { type: 'tool', id: 't', name: 'Read', output: 'contents', status: 'ok' });
  assert.equal(result[2].text, 'Next turn');
});

test('resolved and cancelled requests cannot keep interactive controls', () => {
  const result = chat.transcript([
    { type: 'approval', id: 'p', options: [{ id: 'once', label: 'Allow', kind: 'allow_once' }] },
    { type: 'question', id: 'q', schema: { type: 'object', properties: {} } },
    { type: 'approval_resolved', id: 'p', decision: 'cancelled' },
    { type: 'answer', id: 'q', cancelled: true },
  ]);
  assert.equal(result[0].resolved.decision, 'cancelled');
  assert.equal(result[1].resolved.cancelled, true);
});

test('JSON schema option values preserve boolean and number types', () => {
  assert.deepEqual(chat.choices({ oneOf: [{ const: false, title: 'No' }, { const: true, title: 'Yes' }] }), [
    { value: false, label: 'No', description: '' }, { value: true, label: 'Yes', description: '' },
  ]);
  assert.deepEqual(chat.choices({ enum: [2, 4] }).map(x => x.value), [2, 4]);
});

test('form answers preserve scalar and array types and omit optional empty fields', () => {
  const schema = { required: ['count', 'enabled', 'tags'], properties: {
    count: { type: 'integer', minimum: 1 }, enabled: { type: 'boolean' },
    tags: { type: 'array', items: { enum: [1, 2, 3] } }, note: { type: 'string' },
  } };
  assert.deepEqual(chat.answers(schema, { count: '2', enabled: false, tags: [1, 3], note: '' }), { count: 2, enabled: false, tags: [1, 3] });
  assert.throws(() => chat.answers(schema, { count: '1.5', enabled: false, tags: [] }), /whole number/);
  assert.throws(() => chat.answers(schema, { count: '0', enabled: false, tags: [] }), /at least 1/);
});

test('structured freeform answers validate JSON and required selections', () => {
  const schema = { required: ['config', 'selection'], properties: { config: { type: 'object' }, selection: { enum: [false, true] } } };
  assert.deepEqual(chat.answers(schema, { config: '{"retries":2}', selection: false }), { config: { retries: 2 }, selection: false });
  assert.throws(() => chat.answers(schema, { config: '{}', selection: '' }), /required/);
});

test('working and review states expose explicit actions without auto completion', () => {
  assert.deepEqual(chat.stateView({ session_state: 'working' }).actions, ['interrupt']);
  assert.deepEqual(chat.stateView({ session_state: 'awaiting_review' }).actions, ['continue', 'done']);
  assert.equal(chat.stateView({ session_state: 'working' }).queued, true);
  assert.equal(chat.stateView({ session_state: 'working', column: 'stopped', error: 'Worker exited' }).terminal, true);
  assert.equal(chat.stateView({ session_state: 'done', column: 'done' }).terminal, true);
});
