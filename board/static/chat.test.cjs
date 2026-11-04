const { test } = require('node:test');
const assert = require('node:assert/strict');
let chat = {};
try { chat = require('./chat.js'); } catch (error) { if (error.code !== 'MODULE_NOT_FOUND') throw error; }

test('engine times show in the viewer\'s timezone, whatever offset and precision they arrive with', () => {
  assert.equal(typeof chat.localTime, 'function', 'local time formatter is available');
  const zone = { locale: 'en-GB', timeZone: 'Asia/Jerusalem' };
  // Hatchet and Prefect send microseconds and a UTC offset; Trigger.dev sends milliseconds and Z.
  assert.equal(chat.localTime('2026-11-03T19:07:52.728953+00:00', zone), '3 Nov 2026, 21:07:52 GMT+2');
  assert.equal(chat.localTime('2026-11-03T19:07:52.728Z', zone), '3 Nov 2026, 21:07:52 GMT+2');
  assert.equal(chat.localTime('2026-11-03T19:07:52', zone), '3 Nov 2026, 21:07:52 GMT+2', 'no offset means UTC');
  assert.equal(chat.localTime('2026-07-01T09:00:00+00:00', zone), '1 Jul 2026, 12:00:00 GMT+3');
  assert.equal(chat.localTime('not a time', zone), 'not a time');
  assert.equal(chat.localTime(null, zone), '');
});

test('slash suggestions filter agent commands only at the start of a prompt', () => {
  const commands = [{ name: 'test', description: 'Run tests' }, { name: 'plan', description: 'Plan work' }];
  assert.deepEqual(chat.commandMatches(commands, '/TE').map(c => c.name), ['test']);
  assert.equal(chat.commandMatches(commands, '/').length, 2);
  assert.equal(chat.commandMatches(commands, 'look /te').length, 0);
  assert.equal(chat.commandMatches(commands, '/test argument').length, 0);
});

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

// Minimal element factory standing in for the page's el(): records tag, attributes and children.
const fakeH = (tag, attrs = {}, ...children) => ({ tag, attrs, children: children.filter(c => c != null) });
const textOf = node => typeof node === 'string' ? node : (node.children || []).map(textOf).join('');
const find = (node, predicate, found = []) => {
  if (node && typeof node === 'object') { if (predicate(node)) found.push(node); (node.children || []).forEach(c => find(c, predicate, found)); }
  return found;
};

test('markdown renders formatting without ever producing raw HTML', () => {
  const nodes = chat.markdown('2 + 2 = **4**\n\nUse `ls` and see [docs](https://example.com) <img src=x onerror=alert(1)>\n- one\n- two\n\n```json\n{"a":1}\n```', fakeH);
  const tree = { children: nodes };
  assert.equal(find(tree, n => n.tag === 'strong')[0].children[0], '4');
  assert.equal(find(tree, n => n.tag === 'code')[0].children[0], 'ls');
  assert.equal(find(tree, n => n.tag === 'a')[0].attrs.href, 'https://example.com');
  assert.equal(find(tree, n => n.tag === 'li').length, 2);
  assert.ok(find(tree, n => n.attrs?.class === 'json-view').length, 'json fence is highlighted');
  // The HTML stays literal text inside a text node; no element is created from it.
  assert.ok(textOf(tree).includes('<img src=x onerror=alert(1)>'));
  assert.equal(find(tree, n => n.tag === 'img').length, 0);
  assert.equal(chat.markdown('[x](javascript:alert(1))', fakeH).flatMap(p => find(p, n => n.tag === 'a')).length, 0);
});

test('json is pretty printed with token classes', () => {
  const view = chat.jsonView({ path: 'C:\cart.py', n: 2, ok: true, none: null }, fakeH);
  const classes = view.children.filter(c => typeof c === 'object').map(c => c.attrs.class);
  for (const kind of ['json-key', 'json-string', 'json-number', 'json-boolean', 'json-null']) assert.ok(classes.includes(kind), kind);
  assert.ok(textOf(view).includes('\n  "path"'), 'indented');
});

test('edits render as a line diff', () => {
  const view = chat.diffView('def total(p):\n    return sum(p[:-1])', 'def total(p):\n    return sum(p)', fakeH);
  assert.deepEqual(view.children.map(r => r.attrs.class), ['diff-ctx', 'diff-del', 'diff-add']);
});

test('tool rows preview their main parameter', () => {
  const windowsPath = String.raw`C:\Users\me\AppData\Local\Temp\demo\cart.py`;
  assert.equal(chat.toolPreview({ name: 'read', summary: 'read', detail: JSON.stringify({ filePath: windowsPath }) }), 'cart.py');
  assert.equal(chat.toolPreview({ name: 'execute', detail: { command: 'python -m unittest -v' } }), 'python -m unittest -v');
  assert.equal(chat.toolPreview({ name: 'edit', summary: windowsPath }), 'cart.py');
  assert.equal(chat.toolPreview({ name: 'think' }), '');
});
