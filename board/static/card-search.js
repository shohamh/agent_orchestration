/* Live card filtering for the board: an index per card, built once per card version, and a small
 * query language.
 *
 *   gpu report          every word must appear somewhere (title, status, agent, worker, input, output…)
 *   "rust async"        a phrase
 *   worker:gpu-02       a field: title task status agent flow id last error worker pool tag step
 *   goal:vector         a key inside the run's input or output (input.goal), or input:/output: for all of it
 *   -status:failed      leave matching cards out
 *   pool:               the field is present
 *
 * Matching is case-insensitive substring search, so a query costs one scan of each card's index.
 */
(function (root) {
  'use strict';
  const MAX_VALUE = 2000;   // characters kept from one input/output value
  const MAX_ENTRIES = 400;  // input/output entries kept per card
  // Column names are engine-neutral; let the words people use for them match too.
  const COLUMN_WORDS = { done: 'done completed', stopped: 'stopped failed cancelled', queued: 'queued', scheduled: 'scheduled',
    running: 'running', waiting: 'waiting' };
  const cache = new WeakMap(); // card object -> index; a changed card arrives as a new object

  function flatten(value, key, entries) {
    if (entries.length >= MAX_ENTRIES || value == null) return;
    if (Array.isArray(value)) { for (const item of value) flatten(item, key, entries); return; }
    if (typeof value === 'object') { for (const [k, v] of Object.entries(value)) flatten(v, `${key}.${k.toLowerCase()}`, entries); return; }
    entries.push([key, String(value).slice(0, MAX_VALUE).toLowerCase()]);
  }

  function index(card) {
    let found = cache.get(card);
    if (found) return found;
    const entries = [];
    const add = (key, value) => { if (value != null && value !== '') entries.push([key, String(value).toLowerCase()]); };
    add('title', card.title); add('task', card.task); add('agent', card.agent); add('flow', card.flow); add('id', card.id);
    add('status', card.status); add('status', COLUMN_WORDS[card.column] || card.column); add('status', card.session_state?.replaceAll('_', ' '));
    add('last', card.last); add('error', card.error); add('system', card.system);
    for (const w of card.workers || []) {
      add('worker', w.id); add('worker', w.hostname); add('pool', w.pool); add('step', w.step);
      for (const tag of w.tags || []) add('tag', tag);
    }
    flatten(card.input, 'input', entries);
    flatten(card.output, 'output', entries);
    // Parallel arrays, so matching a field walks them without allocating.
    const values = entries.map((e) => e[1]);
    found = { keys: Int32Array.from(entries, (e) => keyId(e[0])), values, text: values.join('\n') };
    cache.set(card, found);
    return found;
  }

  // Words, "phrases", key:value and key:"a phrase", each optionally negated with a leading -.
  function parse(query) {
    const terms = [];
    const pattern = /(-?)(?:([\w.-]+):)?(?:"([^"]*)"?|(\S*))/g;
    for (const m of (query || '').toLowerCase().matchAll(pattern)) {
      if (!m[0]) { pattern.lastIndex++; continue; }
      const field = m[2] || null, value = m[3] ?? m[4] ?? '';
      if (!field && !value) continue;
      terms.push({ negate: m[1] === '-' && (!!field || !!value), field, value });
    }
    return terms;
  }

  // A field name matches an input/output key by its last parts (goal -> input.goal) or as a prefix (input -> input.*).
  function keyMatches(key, field) {
    return key === field || key.endsWith(`.${field}`) || key.startsWith(`${field}.`) || key.includes(`.${field}.`);
  }

  // Keys are numbered, and which keys a field covers is decided once per field and key, so
  // matching a field is an array lookup per entry: cards mostly share their keys.
  const keyNames = [], keyIds = new Map();
  const keyMemo = new Map(); // field -> [covers key 0, key 1, …] (undefined: not decided yet)
  function keyId(key) {
    let id = keyIds.get(key);
    if (id === undefined) { keyIds.set(key, id = keyNames.length); keyNames.push(key); }
    return id;
  }

  function termMatches(card, term) {
    const found = index(card);
    if (!term.field) return found.text.includes(term.value);
    const { keys, values } = found, memo = term.memo;
    for (let i = 0; i < keys.length; i++) {
      let hit = memo[keys[i]];
      if (hit === undefined) hit = memo[keys[i]] = keyMatches(keyNames[keys[i]], term.field);
      if (hit && values[i].includes(term.value)) return true;
    }
    return false;
  }

  function matcher(query) {
    const terms = parse(query);
    if (!terms.length) return null;
    for (const term of terms) {
      if (term.field && !keyMemo.has(term.field)) keyMemo.set(term.field, []);
      term.memo = keyMemo.get(term.field);
    }
    return (card) => terms.every((term) => termMatches(card, term) !== term.negate);
  }

  // Can every card matching `query` be found among those matching `previous`? True while typing
  // further into a query (worker:pc -> worker:pc-1) or adding words, so the board can filter the
  // previous hits instead of every card.
  function narrows(previous, query) {
    const before = parse(previous), after = parse(query);
    if (!before.length || after.length < before.length) return false;
    return before.every((old, i) => {
      const now = after[i];
      return now.field === old.field && now.negate === old.negate && (old.negate ? now.value === old.value : now.value.includes(old.value));
    });
  }

  function filter(cards, query) {
    const test = matcher(query);
    return test ? cards.filter(test) : cards;
  }

  const api = { parse, index, matcher, filter, narrows };
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  else root.CardSearch = api;
})(typeof window !== 'undefined' ? window : globalThis);
