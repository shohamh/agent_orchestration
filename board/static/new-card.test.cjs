/* The New card form's pure helpers. node --test board/static/new-card.test.cjs */
const { test } = require('node:test');
const assert = require('node:assert/strict');
const NewCard = require('./new-card.js');

const WORKERS = [
  { id: 'a', pools: ['build'], tags: ['gpu'], online: true },
  { id: 'b', pools: ['build', 'test'], tags: [], online: false },
  { id: 'c', pools: ['test'], tags: ['gpu', 'linux'], online: true },
];

test('no pools means all pools; several pools mean any of them; tags are all required', () => {
  const ids = (wanted) => NewCard.matchingWorkers(WORKERS, wanted).map((w) => w.id);
  assert.deepEqual(NewCard.selection({}), {});
  assert.deepEqual(ids(NewCard.selection({})), ['a', 'b', 'c']);
  assert.deepEqual(ids(NewCard.selection({ pools: ['test'] })), ['b', 'c']);
  assert.deepEqual(ids(NewCard.selection({ pools: ['build', 'test'] })), ['a', 'b', 'c']);
  assert.deepEqual(ids(NewCard.selection({ pools: ['build', 'test'], tags: ['gpu'] })), ['a', 'c']);
  assert.deepEqual(ids(NewCard.selection({ tags: ['gpu', 'linux'] })), ['c']);
  assert.deepEqual(NewCard.selection({ pools: ['build'], workerId: 'c', tags: ['gpu'] }), { id: 'c', tags: ['gpu'] }, 'one worker replaces the pools');
});

test('the worker count reads as a sentence', () => {
  assert.equal(NewCard.fitText(WORKERS, {}), '3 of 3 workers match, 2 online.');
  assert.equal(NewCard.fitText(WORKERS, { tags: ['nope'] }), 'None of the 3 registered workers match.');
  assert.equal(NewCard.fitText([], {}), 'No workers are registered yet. Add one in the Fleet tab.');
  assert.equal(NewCard.fitText(WORKERS.slice(0, 1), {}), '1 of 1 worker matches, 1 online.');
});

test('repeat choices become cron', () => {
  const cron = (repeat, extra = {}) => NewCard.cronFor({ repeat, time: '09:30', ...extra });
  assert.equal(cron('none'), null);
  assert.equal(cron('hourly'), '30 * * * *');
  assert.equal(cron('daily'), '30 9 * * *');
  assert.equal(cron('weekdays'), '30 9 * * 1-5');
  assert.equal(cron('weekly', { days: [5, 1] }), '30 9 * * 1,5');
  assert.equal(cron('weekly', { days: [] }), '30 9 * * 1');
  assert.equal(cron('custom', { cron: ' */5 * * * * ' }), '*/5 * * * *');
});

test('repeat choices read as text', () => {
  const summary = (repeat, extra = {}) => NewCard.repeatSummary({ repeat, time: '09:05', ...extra });
  assert.equal(summary('none'), '');
  assert.equal(summary('hourly'), 'Every hour at :05');
  assert.equal(summary('daily'), 'Every day at 09:05');
  assert.equal(summary('weekdays'), 'Every weekday at 09:05');
  assert.equal(summary('weekly', { days: [0, 1] }), 'Every Mon, Sun at 09:05');
});

test('the button says what will happen', () => {
  const label = (mode, when, repeat) => NewCard.startLabel({ mode, when, repeat });
  assert.equal(label('agent', 'now', 'none'), 'Start agent');
  assert.equal(label('program', 'now', 'none'), 'Run program');
  assert.equal(label('flow', 'now', 'none'), 'Start flow');
  assert.equal(label('flow', 'now', 'daily'), 'Start and repeat');
  assert.equal(label('agent', 'later', 'none'), 'Schedule');
  assert.equal(label('program', 'later', 'daily'), 'Schedule');
});

test('sources are recognised before they are sent', () => {
  assert.equal(NewCard.repoProvider('https://github.com/a/b.git'), 'GitHub');
  assert.equal(NewCard.repoProvider('git@gitlab.example.com:a/b.git'), 'GitLab');
  assert.equal(NewCard.repoProvider('https://git.example.com/a/b'), 'Git');
  for (const url of ['https://github.com/a/b', 'ssh://git@host/a.git', 'git@host:a/b.git']) assert.ok(NewCard.isGitUrl(url), url);
  for (const url of ['', 'github.com/a/b', 'ext::sh -c id', '/etc/passwd', 'file:///etc']) assert.ok(!NewCard.isGitUrl(url), url);
  assert.ok(NewCard.isConfluenceUrl('https://acme.atlassian.net/wiki/spaces/ENG/pages/123/Title'));
  assert.ok(NewCard.isConfluenceUrl('https://wiki.example.com/pages/viewpage.action?pageId=123'));
  assert.ok(!NewCard.isConfluenceUrl('https://acme.atlassian.net/wiki/home'));
});

test('flow inputs: the first text is the paragraph, others stay small', () => {
  const fields = NewCard.flowFields({ topic: 'edge', sources: 'a,b', limit: 3, deep: true, notes: 'x'.repeat(90) });
  assert.deepEqual(fields.map((f) => [f.key, f.type, f.long]), [
    ['topic', 'text', true], ['sources', 'text', false], ['limit', 'number', false], ['deep', 'boolean', false], ['notes', 'text', true]]);
  assert.deepEqual(NewCard.flowFields({}), []);
  assert.equal(NewCard.flowFacts({ steps: { a: { kind: 'agent' }, b: { kind: 'program' } } }), '2 steps · 1 agent');
  assert.equal(NewCard.flowFacts({ steps: { a: { kind: 'program' } } }), '1 step');
});

test('a default later time is a round hour in the future', () => {
  const now = new Date(2026, 10, 6, 14, 50);
  const later = NewCard.defaultLater(now);
  assert.equal(NewCard.localInputValue(later), '2026-11-06T16:00');
  assert.ok(later > now);
  assert.equal(NewCard.formatBytes(2048), '2 KB');
  assert.equal(NewCard.formatBytes(5 * 1024 * 1024), '5.0 MB');
});
