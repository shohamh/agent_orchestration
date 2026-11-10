/* The one flow graph viewer. The flow editor and a run's drawer both show flows through it, so a
 * flow looks and behaves the same in both: layout, steps, dependencies, pan, zoom, fit, hover.
 *
 * It knows nothing about flow documents or engines. A context gives it what to draw (render) and
 * says what things do (options.on); handlers it leaves out are abilities that context doesn't have.
 * A graph with no `move` handler has steps that can't be moved, and with no `connect` handler has
 * no connection handles: that is all the difference between viewing a run and editing a flow.
 *
 *   const graph = FlowGraph.mount({ label, help, viewTools, autoFit, on: { select, menu, card, move, connect, … } });
 *   host.append(graph.element);
 *   graph.render({ nodes: [{ id, label, sub, code, kind, state, selected, problem, ariaLabel }],
 *                  edges: [{ from, to, kind, selected, ariaLabel }], positions: { id: { x, y } } });
 *
 * kind: "agent" | "program" picks a step's icon; state (a board column) colours it by how a run went.
 * Text from flows only ever goes into text nodes and attributes.
 */
(function (root) {
  'use strict';
  const NODE_W = 184, NODE_H = 56, GAP_X = 36, GAP_Y = 64, PAD = 24;
  const SVG_NS = 'http://www.w3.org/2000/svg';
  const ICONS = {
    agent: 'M26 21v-3M20 21h12a2 2 0 0 1 2 2v8a2 2 0 0 1-2 2H20a2 2 0 0 1-2-2v-8a2 2 0 0 1 2-2ZM22.500 26v1.500M29.500 26v1.500',
    program: 'm19 22 5 5.500-5 5.500M26 33h7',
    task: 'M19 20h14v16H19zM22 25h8M22 30h8',
    flow: 'M20.500 18.500h4a1 1 0 0 1 1 1v2a1 1 0 0 1-1 1h-4a1 1 0 0 1-1-1v-2a1 1 0 0 1 1-1ZM28.500 27.500h4a1 1 0 0 1 1 1v2a1 1 0 0 1-1 1h-4a1 1 0 0 1-1-1v-2a1 1 0 0 1 1-1ZM22.500 22.500V26a3.500 3.500 0 0 0 3.500 3.500h1.500',
  };
  // What a step or a card is, drawn the same everywhere. ICON_BOX frames one of them on its own.
  const ICON_BOX = '18 17 16 16';
  const TOOL_ICONS = { fit: 'M2 6V2h4M10 2h4v4M14 10v4h-4M6 14H2v-4' };
  const short = (text, max) => { const t = String(text ?? ''); return t.length > max ? `${t.slice(0, max - 1)}…` : t; };

  // ------------------------------------------------------------------ Layout (no DOM)
  // Rows by dependency depth (a cycle from hand-written YAML can't hang it).
  function layers(ids, predecessors) {
    const depth = {}, visiting = new Set();
    const rowOf = (id) => {
      if (id in depth) return depth[id];
      if (visiting.has(id)) return 0;
      visiting.add(id);
      const d = predecessors[id].length ? Math.max(...predecessors[id].map(rowOf)) + 1 : 0;
      visiting.delete(id);
      return (depth[id] = d);
    };
    ids.forEach(rowOf);
    const rows = [];
    for (const id of ids) (rows[depth[id]] ??= []).push(id);
    return { row: depth, rows: rows.filter(Boolean), preds: predecessors };
  }
  function bounds(positions) {
    const all = Object.values(positions);
    if (!all.length) return { minX: 0, minY: 0, width: 0, height: 0 };
    const minX = Math.min(...all.map((p) => p.x)), minY = Math.min(...all.map((p) => p.y));
    const maxX = Math.max(...all.map((p) => p.x)) + NODE_W, maxY = Math.max(...all.map((p) => p.y)) + NODE_H;
    return { minX, minY, width: maxX - minX, height: maxY - minY };
  }
  // Deterministic and top to bottom: each step wants to sit under the average of its prerequisites;
  // a row is pushed apart just enough not to overlap, keeping it centred on where it wanted to be.
  function autoLayout(ids, predecessors) {
    const { row, rows, preds } = layers(ids, predecessors);
    const step = NODE_W + GAP_X, x = {};
    rows.forEach((list) => {
      const order = [...list];
      const want = Object.fromEntries(order.map((id, i) => {
        const above = preds[id].filter((p) => row[p] < row[id] && p in x);
        return [id, above.length ? above.reduce((sum, p) => sum + x[p], 0) / above.length : i * step];
      }));
      order.sort((a, b) => want[a] - want[b]);
      order.forEach((id, i) => { x[id] = i === 0 ? want[id] : Math.max(want[id], x[order[i - 1]] + step); });
      const drift = order.reduce((sum, id) => sum + x[id] - want[id], 0) / order.length;
      order.forEach((id) => { x[id] -= drift; });
    });
    const xs = Object.values(x), minX = xs.length ? Math.min(...xs) : 0;
    const positions = {};
    for (const id of ids) positions[id] = { x: Math.round(x[id] - minX + PAD), y: PAD + row[id] * (NODE_H + GAP_Y) };
    return { positions, ...bounds(positions) };
  }

  // ------------------------------------------------------------------ The viewer
  function mount(options = {}) {
    const doc = root.document, on = options.on || {};
    const s = (tag, attrs = {}, ...children) => {
      const node = doc.createElementNS(SVG_NS, tag);
      for (const [k, v] of Object.entries(attrs)) if (v != null) node.setAttribute(k, v);
      for (const child of children) if (child != null) node.append(child);
      return node;
    };
    const h = (tag, attrs = {}, ...children) => {
      const node = doc.createElement(tag);
      for (const [k, v] of Object.entries(attrs)) {
        if (k.startsWith('on')) node.addEventListener(k.slice(2), v); else if (v != null) node.setAttribute(k, v);
      }
      for (const child of children) if (child != null) node.append(child);
      return node;
    };
    const svgEl = s('svg', { class: 'fg-svg', tabindex: '0', role: 'group', 'aria-label': options.label || 'Flow graph' });
    const emptyEl = h('p', { class: 'fg-empty fb-empty' });
    emptyEl.hidden = true;
    const zoomLabel = h('span', { class: 'fg-zoom', 'aria-live': 'polite' }, '100%');
    const tool = (text, click, attrs = {}) => h('button', { type: 'button', class: 'secondary', onclick: click, ...attrs }, text);
    const tools = h('div', { class: 'fg-tools fg-tools-view', role: 'toolbar', 'aria-label': 'View tools' },
      options.help ? h('button', { type: 'button', class: 'fg-help', 'aria-label': 'How the graph works', title: options.help }, '?') : null,
      tool('−', () => zoomBy(1 / 1.25), { 'aria-label': 'Zoom out', title: 'Zoom out' }), zoomLabel,
      tool('+', () => zoomBy(1.25), { 'aria-label': 'Zoom in', title: 'Zoom in' }),
      h('button', { type: 'button', class: 'secondary fg-icon', title: 'Fit the whole flow on screen', onclick: () => fit() }, h('span', { class: 'fg-sr' }, 'Fit'), glyph(TOOL_ICONS.fit)),
      ...(options.viewTools || []));
    const canvas = h('div', { class: 'fg-canvas' }, svgEl, emptyEl, tools);

    let model = null, drawn = null, vp = { x: 0, y: 0, k: 1 }, gesture = null, touched = false;
    const pointers = new Map();
    let touchLongPress = null;
    const TOUCH_MENU_DELAY = 500, TOUCH_MOVE_THRESHOLD = 5;


    function glyph(d) {
      return s('svg', { width: 15, height: 15, viewBox: '0 0 16 16', fill: 'none', stroke: 'currentColor', 'stroke-width': 1.4, 'stroke-linecap': 'round', 'stroke-linejoin': 'round', 'aria-hidden': 'true' }, s('path', { d }));
    }

    // ---- drawing
    function render(next) {
      model = next;
      if (next.empty || !next.nodes?.length) {
        drawn = null;
        svgEl.replaceChildren();
        emptyEl.hidden = !next.empty; emptyEl.textContent = next.empty || '';
        return;
      }
      emptyEl.hidden = true;
      // Live updates usually change nothing: keep the same elements so hover and focus aren't disturbed.
      const signature = `${vp.k}|${JSON.stringify(next)}`;
      if (signature === drawn && svgEl.firstChild) return;
      drawn = signature;
      const { nodes, positions: pos } = next, k = vp.k;
      const active = doc.activeElement;
      const focusKey = active?.dataset?.step ? `step:${active.dataset.step}` : active?.dataset?.edge ? `edge:${active.dataset.from}>${active.dataset.to}` : null;
      const edges = (next.edges || []).filter((e) => pos[e.from] && pos[e.to] && e.from !== e.to);
      // Each step's edge ends spread across its width, ordered by the x of the other end, so
      // edges into a step with several prerequisites don't all meet in a point.
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
        const a = pos[e.from], b = pos[e.to], kind = e.kind || 'depends';
        const x1 = a.x + outX.get(e), y1 = a.y + NODE_H, x2 = b.x + inX.get(e), y2 = b.y;
        const bend = Math.max(30, Math.abs(y2 - y1) / 2);
        // A dependency that points upwards (a cycle from YAML, or a manual layout) goes round the side.
        const side = Math.max(a.x, b.x) + NODE_W + 28;
        const d = y2 >= y1 + 12 ? `M${x1},${y1} C${x1},${y1 + bend} ${x2},${y2 - bend} ${x2},${y2 - 3}`
          : `M${x1},${y1} C${x1},${y1 + 50} ${side},${y1 + 50} ${side},${(y1 + y2) / 2} S${x2},${y2 - 50} ${x2},${y2 - 3}`;
        return s('g', { class: `fg-edge ${kind}${e.selected ? ' selected' : ''}`, 'data-edge': '1', 'data-from': e.from, 'data-to': e.to, tabindex: on.edgeKey ? '-1' : null, role: on.edgeKey ? 'button' : null,
          'aria-pressed': on.edgeKey ? String(!!e.selected) : null, 'aria-label': e.ariaLabel },
        s('path', { class: 'hit', d, 'stroke-width': Math.max(16, 24 / k) }), s('path', { class: 'line', d, 'marker-end': `url(#fg-arrow-${uid}-${kind})` }));
      });
      const hit = Math.max(23, 23 / k); // at least a 44px touch target at any zoom
      const nodeEls = nodes.map((n) => {
        const p0 = pos[n.id], kind = n.kind || 'task';
        return s('g', { class: `fg-node ${kind}${n.selected ? ' selected' : ''}${n.problem ? ' has-error' : ''}`, 'data-step': n.id, 'data-state': n.state || null, transform: `translate(${p0.x},${p0.y})`,
          tabindex: '0', role: 'button', 'aria-pressed': String(!!n.selected), 'aria-label': n.ariaLabel },
        s('rect', { class: 'box', width: NODE_W, height: NODE_H, rx: 10 }),
        s('rect', { class: 'tile', x: 10, y: 12, width: 32, height: 32, rx: 8 }),
        s('path', { class: 'tile-icon', d: ICONS[kind] || ICONS.task }),
        s('text', { class: 'label', x: 52, y: 25 }, short(n.label, 17)),
        s('text', { class: `sub${n.code ? ' code' : ''}`, x: 52, y: 42 }, short(n.sub, n.code ? 18 : 22)),
        n.problem ? s('g', { class: 'badge', 'aria-hidden': 'true' }, s('circle', { cx: NODE_W - 14, cy: 14, r: 9 }), s('text', { x: NODE_W - 14, y: 18, 'text-anchor': 'middle' }, '!')) : null,
        on.connect ? s('circle', { class: 'in', cx: NODE_W / 2, cy: 0, r: 4 }) : null,
        on.connect ? s('g', { class: 'handle', 'data-handle': n.id, 'aria-hidden': 'true' }, s('circle', { class: 'hit', cx: NODE_W / 2, cy: NODE_H, r: hit }), s('circle', { class: 'dot', cx: NODE_W / 2, cy: NODE_H, r: 7 })) : null);
      });
      const markers = ['depends', 'calls', 'then'].map((kind) => s('marker', { id: `fg-arrow-${uid}-${kind}`, viewBox: '0 0 10 10', refX: 8, refY: 5, markerWidth: 7, markerHeight: 7, orient: 'auto-start-reverse' },
        s('path', { d: 'M0,0 L10,5 L0,10 z', class: `fg-arrowhead ${kind}` })));
      svgEl.replaceChildren(s('defs', {}, ...markers), s('g', { class: 'fg-viewport' }, ...edgeEls, ...nodeEls, s('path', { class: 'fg-preview', d: '' })));
      if (next.ariaLabel) svgEl.setAttribute('aria-label', next.ariaLabel);
      applyViewport();
      if (focusKey) focusItem(focusKey);
    }
    const uid = (mount.count = (mount.count || 0) + 1); // two graphs on a page must not share marker ids
    const redraw = () => { if (model) { drawn = null; render(model); } };

    // ---- viewport
    function applyViewport() {
      svgEl.querySelector('.fg-viewport')?.setAttribute('transform', `translate(${vp.x},${vp.y}) scale(${vp.k})`);
      zoomLabel.textContent = `${Math.round(vp.k * 100)}%`;
      canvas.style.backgroundSize = `${20 * vp.k}px ${20 * vp.k}px`;
      canvas.style.backgroundPosition = `${vp.x}px ${vp.y}px`;
    }
    function zoomAt(k, cx, cy) {
      const next = Math.min(2.5, Math.max(0.25, k));
      vp.x = cx - ((cx - vp.x) / vp.k) * next; vp.y = cy - ((cy - vp.y) / vp.k) * next; vp.k = next;
    }
    function zoomBy(f) { touched = true; const r = svgEl.getBoundingClientRect(); zoomAt(vp.k * f, r.width / 2, r.height / 2); redraw(); }
    // The whole flow on screen; false while the graph has no size yet.
    function fit() {
      const rect = svgEl.getBoundingClientRect();
      if (!rect.width || !rect.height) return false;
      touched = false;
      const b = bounds(model?.positions || {});
      if (!b.width) { vp = { x: 0, y: 0, k: 1 }; redraw(); return true; }
      // Clear of the floating tools: the view tools at the bottom, and any a context adds at the top.
      const top = options.insetTop ?? 16, bottom = 52, room = Math.max(1, rect.height - top - bottom);
      const k = Math.min(1, Math.max(0.25, Math.min((rect.width - 32) / b.width, room / b.height)));
      vp = { k, x: (rect.width - b.width * k) / 2 - b.minX * k, y: top + Math.max(0, (room - b.height * k) / 2) - b.minY * k };
      redraw();
      return true;
    }
    // Pan just enough to show a step whole. The zoom stays unless the step can't fit at it.
    function reveal(id) {
      const at = model?.positions?.[id], rect = svgEl.getBoundingClientRect();
      if (!at || !rect.width || !rect.height) return;
      const margin = 16, fits = Math.min((rect.width - 2 * margin) / NODE_W, (rect.height - 2 * margin) / NODE_H);
      if (vp.k > fits) zoomAt(fits, rect.width / 2, rect.height / 2);
      const w = NODE_W * vp.k, ht = NODE_H * vp.k, left = at.x * vp.k + vp.x, top = at.y * vp.k + vp.y;
      const shift = (start, size, room) => (size + 2 * margin > room ? (room - size) / 2 - start
        : start < margin ? margin - start : start + size > room - margin ? room - margin - (start + size) : 0);
      vp.x += shift(left, w, rect.width); vp.y += shift(top, ht, rect.height);
      applyViewport();
    }
    // The middle of what's on screen, in graph coordinates; null while the graph has no size.
    function center() {
      const rect = svgEl.getBoundingClientRect();
      return rect.width ? { x: (rect.width / 2 - vp.x) / vp.k, y: (rect.height / 2 - vp.y) / vp.k } : null;
    }
    function focusItem(key) {
      const at = key.indexOf(':'), type = key.slice(0, at), rest = key.slice(at + 1);
      let node = null;
      if (type === 'step') node = svgEl.querySelector(`[data-step="${CSS.escape(rest)}"]`);
      else if (type === 'edge') { const [from, to] = rest.split('>'); node = svgEl.querySelector(`[data-edge][data-from="${CSS.escape(from)}"][data-to="${CSS.escape(to)}"]`); }
      node?.focus({ preventScroll: true });
    }

    // ---- what is under the pointer
    const local = (e) => { const r = svgEl.getBoundingClientRect(); return { x: e.clientX - r.left, y: e.clientY - r.top }; };
    const toGraph = (pt) => ({ x: (pt.x - vp.x) / vp.k, y: (pt.y - vp.y) / vp.k });
    function targetOf(node) {
      const step = node.closest?.('[data-step]'), edge = node.closest?.('[data-edge]');
      return step ? { type: 'node', id: step.dataset.step } : edge ? { type: 'edge', from: edge.dataset.from, to: edge.dataset.to } : { type: 'canvas' };
    }
    const dropTarget = (e) => doc.elementFromPoint(e.clientX, e.clientY)?.closest?.('[data-step]')?.dataset.step;
    function clearTouchLongPress(press = touchLongPress) {
      if (!press) return;
      clearTimeout(press.timer);
      root.removeEventListener('keydown', press.key, true);
      if (touchLongPress === press) touchLongPress = null;
    }
    function showTouchMenu(press) {
      if (touchLongPress !== press || press.opened) return;
      clearTimeout(press.timer);
      press.opened = true;
      gesture = null;
      clearPreview();
      const menuEvent = { target: press.target, clientX: press.clientX, clientY: press.clientY };
      on.menu(menuEvent, targetOf(press.target), toGraph(press.point));
    }

    function clearPreview() {
      svgEl.querySelector('.fg-preview')?.setAttribute('d', '');
      svgEl.classList.remove('connecting');
      svgEl.querySelectorAll('.fg-node.drop, .fg-node.no-drop').forEach((n) => n.classList.remove('drop', 'no-drop'));
    }

    // ---- gestures: pan and pinch everywhere; move and connect where the context allows; taps select.
    svgEl.addEventListener('pointerdown', (e) => {
      if ((e.pointerType === 'mouse' && e.button !== 0) || !model) return;
      try { svgEl.setPointerCapture(e.pointerId); } catch {}
      pointers.set(e.pointerId, local(e));
      if (pointers.size === 2) {
        clearTouchLongPress();
        const [a, b] = [...pointers.values()];
        gesture = { mode: 'pinch', dist: Math.hypot(a.x - b.x, a.y - b.y) || 1, mid: { x: (a.x + b.x) / 2, y: (a.y + b.y) / 2 }, vp: { ...vp } };
        clearPreview(); return;
      }
      if (pointers.size > 2) return;
      const handle = e.target.closest?.('[data-handle]'), target = targetOf(e.target), start = local(e);
      if (handle) {
        e.preventDefault();
        gesture = { mode: 'connect', from: handle.dataset.handle, start, moved: false };
        svgEl.classList.add('connecting');
        for (const node of svgEl.querySelectorAll('[data-step]')) node.classList.toggle('no-drop', !!on.canConnect && !on.canConnect(handle.dataset.handle, node.dataset.step));
      } else if (target.type === 'node' && on.move) gesture = { mode: 'move', id: target.id, start, origin: { ...model.positions[target.id] }, moved: false };
      else if (target.type === 'edge' && on.edgeKey) gesture = { mode: 'edge', target, start, moved: false };
      else {
        // Anything else pans; a tap without movement selects what was under it.
        gesture = { mode: 'pan', start, vp: { ...vp }, target, moved: false };
        if (target.type === 'canvas') { e.preventDefault(); svgEl.focus({ preventScroll: true }); }
      }
      if (e.pointerType === 'touch' && pointers.size === 1 && on.menu && root.Overlays) {
        const press = { pointerId: e.pointerId, target: e.target, point: start, clientX: e.clientX, clientY: e.clientY,
          opened: false, timer: null, key: null };
        touchLongPress = press;
        press.key = (event) => {
          if (event.key !== 'Escape' || touchLongPress !== press) return;
          clearTouchLongPress(press);
          gesture = null; pointers.clear(); clearPreview();
        };
        root.addEventListener('keydown', press.key, true);
        press.timer = setTimeout(() => {
          if (touchLongPress === press && pointers.has(press.pointerId)) showTouchMenu(press);
        }, TOUCH_MENU_DELAY);
      }
    });
    svgEl.addEventListener('pointermove', (e) => {
      if (!pointers.has(e.pointerId)) return;
      const pt = local(e);
      pointers.set(e.pointerId, pt);
      if (touchLongPress?.pointerId === e.pointerId && !touchLongPress.opened
        && Math.hypot(e.clientX - touchLongPress.clientX, e.clientY - touchLongPress.clientY) >= TOUCH_MOVE_THRESHOLD) {
        clearTouchLongPress();
      }
      if (!gesture) return;
      if (gesture.mode === 'pinch') {
        if (pointers.size < 2) return;
        touched = true;
        const [a, b] = [...pointers.values()], mid = { x: (a.x + b.x) / 2, y: (a.y + b.y) / 2 };
        vp = { ...gesture.vp, x: gesture.vp.x + mid.x - gesture.mid.x, y: gesture.vp.y + mid.y - gesture.mid.y };
        zoomAt(gesture.vp.k * (Math.hypot(a.x - b.x, a.y - b.y) / gesture.dist), mid.x, mid.y);
        applyViewport(); return;
      }
      const dx = pt.x - gesture.start.x, dy = pt.y - gesture.start.y;
      if (!gesture.moved && Math.hypot(dx, dy) < 5) return;
      gesture.moved = true;
      if (gesture.mode === 'pan') { touched = true; vp = { ...gesture.vp, x: gesture.vp.x + dx, y: gesture.vp.y + dy }; applyViewport(); }
      else if (gesture.mode === 'move') {
        // Steps line up on a 10px grid; Alt places them freely.
        const snap = (v) => (e.altKey ? Math.round(v) : Math.round(v / 10) * 10);
        on.move(gesture.id, { x: snap(gesture.origin.x + dx / vp.k), y: snap(gesture.origin.y + dy / vp.k) });
      } else if (gesture.mode === 'connect') {
        const from = model.positions[gesture.from], end = toGraph(pt);
        const x1 = from.x + NODE_W / 2, y1 = from.y + NODE_H;
        svgEl.querySelector('.fg-preview')?.setAttribute('d', `M${x1},${y1} C${x1},${y1 + 40} ${end.x},${end.y - 40} ${end.x},${end.y}`);
        const over = dropTarget(e);
        svgEl.querySelectorAll('.fg-node.drop').forEach((n) => n.classList.remove('drop'));
        if (over && over !== gesture.from) { const node = svgEl.querySelector(`[data-step="${CSS.escape(over)}"]`); if (!node?.classList.contains('no-drop')) node?.classList.add('drop'); }
      }
    });
    function endGesture(e) {
      if (!pointers.has(e.pointerId)) return;
      pointers.delete(e.pointerId);
      if (touchLongPress?.pointerId === e.pointerId) clearTouchLongPress();
      const g = gesture;
      if (!g) return;
      if (g.mode === 'pinch') { if (!pointers.size) { gesture = null; redraw(); } return; }
      gesture = null;
      if (e.type === 'pointercancel') { clearPreview(); return; }
      if (g.mode === 'connect') {
        clearPreview();
        const to = g.moved && dropTarget(e);
        if (to && to !== g.from) on.connect(g.from, to);
        else if (g.moved && to === g.from) on.connectSelf?.(g.from);
        else if (g.moved && !to) on.connectEmpty?.(e, g.from, toGraph(local(e)));
        else if (!g.moved) on.select?.({ type: 'node', id: g.from });
      } else if (g.mode === 'move') { if (g.moved) on.moveEnd?.(g.id); else on.select?.({ type: 'node', id: g.id }); }
      else if (g.mode === 'edge') { if (!g.moved) on.select?.(g.target); }
      else if (g.mode === 'pan' && !g.moved) on.select?.(g.target);
    }
    svgEl.addEventListener('pointerup', endGesture);
    svgEl.addEventListener('pointercancel', endGesture);
    // Assistive technology and scripts activate a step with a click that has no pointer behind it.
    svgEl.addEventListener('click', (e) => { if (e.detail) return; const target = targetOf(e.target); if (target.type === 'node') on.select?.(target); });
    svgEl.addEventListener('wheel', (e) => {
      if (!model?.nodes?.length) return;
      e.preventDefault();
      touched = true;
      if (e.ctrlKey || e.metaKey) { const pt = local(e); zoomAt(vp.k * Math.exp(-e.deltaY / 300), pt.x, pt.y); redraw(); }
      // Shift+scroll goes sideways. Browsers differ on whether they already moved the delta to x.
      else if (e.shiftKey) { vp.x -= e.deltaX || e.deltaY; applyViewport(); }
      else { vp.x -= e.deltaX; vp.y -= e.deltaY; applyViewport(); }
    }, { passive: false });
    svgEl.addEventListener('keydown', (e) => {
      const step = e.target.closest?.('[data-step]'), edge = e.target.closest?.('[data-edge]');
      if (step) { on.nodeKey?.(e, step.dataset.step); return; }
      if (edge) { on.edgeKey?.(e, edge.dataset.from, edge.dataset.to); return; }
      if (e.target !== svgEl) return;
      const by = 40, moves = { ArrowLeft: [by, 0], ArrowRight: [-by, 0], ArrowUp: [0, by], ArrowDown: [0, -by] };
      if (moves[e.key]) { e.preventDefault(); touched = true; vp.x += moves[e.key][0]; vp.y += moves[e.key][1]; applyViewport(); }
      else if (e.key === '+' || e.key === '=') { e.preventDefault(); zoomBy(1.25); }
      else if (e.key === '-') { e.preventDefault(); zoomBy(1 / 1.25); }
      else on.canvasKey?.(e);
    });
    // ---- right click and double click: the context decides what's on offer
    svgEl.addEventListener('contextmenu', (e) => {
      if (!model || !on.menu || !root.Overlays) return;
      if (e.pointerType === 'touch' || touchLongPress) {
        e.preventDefault();
        if (touchLongPress && !touchLongPress.opened) showTouchMenu(touchLongPress);
        return;
      }
      e.preventDefault();
      on.menu(e, targetOf(e.target), toGraph(local(e)));
    });
    svgEl.addEventListener('dblclick', (e) => { const target = targetOf(e.target); if (model) on.double?.(e, target, toGraph(local(e))); });
    // ---- hover: light up what a step connects to, and show its details in a card
    const unlight = () => { svgEl.classList.remove('tracing'); svgEl.querySelectorAll('.lit').forEach((n) => n.classList.remove('lit')); };
    svgEl.addEventListener('pointerover', (e) => {
      const node = e.target.closest?.('[data-step]'), edge = e.target.closest?.('[data-edge]');
      const ids = node ? [node.dataset.step] : edge ? [edge.dataset.from, edge.dataset.to] : null;
      unlight();
      if (!ids) return;
      svgEl.classList.add('tracing');
      const light = (id) => svgEl.querySelector(`[data-step="${CSS.escape(id)}"]`)?.classList.add('lit');
      ids.forEach(light);
      if (edge) edge.classList.add('lit');
      else for (const line of svgEl.querySelectorAll('[data-edge]')) if (line.dataset.from === ids[0] || line.dataset.to === ids[0]) {
        line.classList.add('lit');
        light(line.dataset.from === ids[0] ? line.dataset.to : line.dataset.from);
      }
    });
    svgEl.addEventListener('pointerleave', unlight);
    root.Overlays?.hoverCard(svgEl, '[data-step], [data-edge]', (node) => (gesture || svgEl.classList.contains('connecting') ? null : on.card?.(targetOf(node)) ?? null));
    // A graph that opens at a size it doesn't know yet, or changes size, refits until the user takes over.
    if (options.autoFit && root.ResizeObserver) new root.ResizeObserver(() => { if (!touched && model?.nodes?.length) fit(); }).observe(canvas);

    return { element: canvas, svg: svgEl, render, redraw, fit, reveal, center, focusItem, zoomBy,
      focus: () => svgEl.focus(), moved: () => touched };
  }

  const api = { NODE_W, NODE_H, GAP_X, GAP_Y, PAD, ICONS, ICON_BOX, layers, bounds, autoLayout, mount, short };
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  else root.FlowGraph = api;
})(typeof window !== 'undefined' ? window : globalThis);
