/* Things that float above the page: right-click menus, tooltips and hover cards. No dependencies.
 *
 *   Overlays.menu(event, items)      a context menu; items are {label, onSelect, shortcut, danger, disabled} or null for a line
 *   Overlays.hoverCard(root, selector, build)   after a pause over a match, shows build(match), a node or null
 *   title="…"                        any element's title shows as a quick styled tooltip, on hover and on keyboard focus
 *   .fg-help, .fb-info, .fl-info      help icons: a click (or Enter) also keeps their text open until you click away
 *
 * A modal dialog makes everything outside it inert, so each overlay is placed inside the dialog
 * its anchor belongs to. Touch has no hover, and a long press stays with the browser and the board.
 */
(function (root) {
  'use strict';
  const doc = root.document;
  const MARGIN = 8;
  const hostOf = (node) => node?.closest?.('dialog[open]') || doc.body;

  // Keeps a box on screen: at the wanted spot, or flipped or nudged back inside the window.
  function place(box, x, y, { avoid = null } = {}) {
    const { width, height } = box.getBoundingClientRect();
    let left = x, top = y;
    if (avoid) {
      // Beside the anchor, on whichever side has room; above or below when neither has.
      const right = avoid.right + MARGIN, leftSide = avoid.left - width - MARGIN;
      if (right + width <= root.innerWidth - MARGIN) left = right;
      else if (leftSide >= MARGIN) left = leftSide;
      else { left = avoid.left; top = avoid.bottom + MARGIN + height > root.innerHeight ? avoid.top - height - MARGIN : avoid.bottom + MARGIN; }
      if (left !== avoid.left) top = avoid.top;
    }
    box.style.left = `${Math.max(MARGIN, Math.min(left, root.innerWidth - width - MARGIN))}px`;
    box.style.top = `${Math.max(MARGIN, Math.min(top, root.innerHeight - height - MARGIN))}px`;
  }

  // ------------------------------------------------------------------ Context menu
  let closeMenu = null;
  function menu(event, items) {
    closeMenu?.();
    hideTip(); hideCard();
    const anchor = event.target || null;
    const returnFocus = doc.activeElement;
    const list = doc.createElement('div');
    list.className = 'ctx-menu';
    list.setAttribute('role', 'menu');
    const buttons = [];
    for (const item of items) {
      if (!item) { const line = doc.createElement('div'); line.className = 'ctx-line'; line.setAttribute('role', 'separator'); list.append(line); continue; }
      const button = doc.createElement('button');
      button.type = 'button';
      button.className = `ctx-item${item.danger ? ' is-danger' : ''}`;
      button.setAttribute('role', 'menuitem');
      button.tabIndex = -1;
      button.disabled = !!item.disabled;
      const label = doc.createElement('span');
      label.textContent = item.label;
      button.append(label);
      if (item.shortcut) { const key = doc.createElement('kbd'); key.textContent = item.shortcut; button.append(key); }
      if (item.title) button.title = item.title;
      button.addEventListener('click', () => { close(); item.onSelect?.(); });
      button.addEventListener('pointermove', () => { if (!button.disabled) button.focus({ preventScroll: true }); });
      list.append(button);
      if (!button.disabled) buttons.push(button);
    }
    hostOf(anchor).append(list);
    // The keyboard's menu key fires with no pointer position: open at the focused element.
    let { clientX: x, clientY: y } = event;
    if (!x && !y && anchor?.getBoundingClientRect) { const r = anchor.getBoundingClientRect(); x = r.left + Math.min(24, r.width / 2); y = r.top + Math.min(24, r.height / 2); }
    place(list, x, y);
    const left = parseFloat(list.style.left), top = parseFloat(list.style.top);
    if (left < x && left + list.offsetWidth >= root.innerWidth - MARGIN) list.style.left = `${Math.max(MARGIN, x - list.offsetWidth)}px`;
    if (top < y && top + list.offsetHeight >= root.innerHeight - MARGIN) list.style.top = `${Math.max(MARGIN, y - list.offsetHeight)}px`;

    const key = (e) => {
      const at = buttons.indexOf(doc.activeElement);
      const go = (index) => { e.preventDefault(); buttons[(index + buttons.length) % buttons.length]?.focus(); };
      if (e.key === 'Escape' || e.key === 'Tab') { e.preventDefault(); e.stopPropagation(); close(true); }
      else if (e.key === 'ArrowDown') go(at + 1);
      else if (e.key === 'ArrowUp') go(at < 0 ? buttons.length - 1 : at - 1);
      else if (e.key === 'Home') go(0);
      else if (e.key === 'End') go(buttons.length - 1);
      else if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); e.stopPropagation(); doc.activeElement?.click(); }
      else return;
      e.stopPropagation();
    };
    const away = (e) => { if (!list.contains(e.target)) close(); };
    const gone = () => close();
    function close(refocus = false) {
      if (closeMenu !== close) return;
      closeMenu = null;
      list.remove();
      root.removeEventListener('keydown', key, true);
      root.removeEventListener('pointerdown', away, true);
      root.removeEventListener('wheel', gone, true);
      root.removeEventListener('resize', gone);
      root.removeEventListener('blur', gone);
      if (refocus && returnFocus?.isConnected) returnFocus.focus({ preventScroll: true });
    }
    closeMenu = close;
    root.addEventListener('keydown', key, true);
    root.addEventListener('pointerdown', away, true);
    root.addEventListener('wheel', gone, true);
    root.addEventListener('resize', gone);
    root.addEventListener('blur', gone);
    (buttons[0] || list).focus?.({ preventScroll: true });
  }

  // ------------------------------------------------------------------ Tooltips
  const tip = doc.createElement('div');
  tip.className = 'tip';
  tip.setAttribute('role', 'tooltip');
  let tipTimer = null, tipFor = null;
  function showTip(node) {
    const text = node.dataset.tip;
    if (!text || !node.isConnected || closeMenu || node === pinFor) return;
    tip.textContent = text;
    hostOf(node).append(tip);
    const r = node.getBoundingClientRect(), { width, height } = tip.getBoundingClientRect();
    let y = r.bottom + 6;
    if (y + height > root.innerHeight - MARGIN) y = r.top - height - 6;
    place(tip, r.left + r.width / 2 - width / 2, y);
    tip.classList.add('on');
  }
  function hideTip() { clearTimeout(tipTimer); tipFor = null; tip.classList.remove('on'); tip.remove(); }
  // Help that stays: for reading without holding the pointer still, and for selecting its text.
  const HELP = '.fg-help, .fb-info, .fl-info';
  const pin = doc.createElement('div');
  pin.className = 'tip pinned';
  pin.setAttribute('role', 'status');
  let pinFor = null;
  function hidePin() { pinFor = null; pin.classList.remove('on'); pin.remove(); }
  function togglePin(node) {
    if (pinFor === node) { hidePin(); return; }
    hideTip();
    const text = tipTarget(node)?.dataset.tip;
    if (!text) return;
    pinFor = node;
    pin.textContent = text;
    hostOf(node).append(pin);
    const r = node.getBoundingClientRect(), { width, height } = pin.getBoundingClientRect();
    place(pin, r.left + r.width / 2 - width / 2, r.top - height - 8 < MARGIN ? r.bottom + 8 : r.top - height - 8);
    pin.classList.add('on');
  }
  // Capture: help sits inside labels, and a click there must not move focus to the field.
  doc.addEventListener('click', (e) => {
    const node = e.target.closest?.(HELP);
    if (!node) return;
    e.preventDefault(); e.stopPropagation();
    togglePin(node);
  }, true);
  root.addEventListener('pointerdown', (e) => { if (pinFor && !pin.contains(e.target) && !pinFor.contains(e.target)) hidePin(); }, true);
  root.addEventListener('keydown', (e) => {
    if (e.key === 'Escape' && pinFor) { e.preventDefault(); e.stopPropagation(); const back = pinFor; hidePin(); back.focus?.({ preventScroll: true }); }
    else if ((e.key === 'Enter' || e.key === ' ') && e.target.matches?.(HELP)) { e.preventDefault(); e.stopPropagation(); togglePin(e.target); }
  }, true);
  // A title becomes data-tip the first time it is needed, so the browser's slow one never shows too.
  function tipTarget(node) {
    const found = node.closest?.('[title], [data-tip]');
    if (!found) return null;
    if (found.hasAttribute('title')) {
      found.dataset.tip = found.getAttribute('title');
      if (!found.hasAttribute('aria-label')) found.setAttribute('aria-description', found.dataset.tip);
      found.removeAttribute('title');
    }
    return found.dataset.tip ? found : null;
  }
  doc.addEventListener('pointerover', (e) => {
    if (e.pointerType !== 'mouse' || e.target.closest?.('.tip')) return;
    const node = tipTarget(e.target);
    if (node === tipFor) return;
    hideTip();
    if (!node) return;
    tipFor = node;
    tipTimer = setTimeout(() => showTip(node), 220);
  });
  doc.addEventListener('focusin', (e) => {
    if (!e.target.matches?.(':focus-visible')) return;
    const node = tipTarget(e.target);
    if (node !== e.target) return;
    hideTip();
    tipFor = node;
    tipTimer = setTimeout(() => showTip(node), 350);
  });
  doc.addEventListener('focusout', hideTip);
  root.addEventListener('pointerdown', hideTip, true);
  root.addEventListener('keydown', hideTip, true);
  root.addEventListener('wheel', hideTip, { capture: true, passive: true });

  // ------------------------------------------------------------------ Hover cards
  const card = doc.createElement('div');
  card.className = 'hover-card';
  card.setAttribute('role', 'tooltip');
  let cardTimer = null, cardFor = null;
  function hideCard() { clearTimeout(cardTimer); cardFor = null; card.classList.remove('on'); card.remove(); }
  function hoverCard(container, selector, build, { delay = 450 } = {}) {
    container.addEventListener('pointerover', (e) => {
      if (e.pointerType !== 'mouse' || e.buttons) return;
      const node = e.target.closest?.(selector);
      if (node === cardFor) return;
      hideCard();
      if (!node) return;
      cardFor = node;
      cardTimer = setTimeout(() => {
        const content = node.isConnected && !closeMenu ? build(node) : null;
        if (!content) return;
        hideTip();
        card.replaceChildren(content);
        hostOf(node).append(card);
        place(card, 0, 0, { avoid: node.getBoundingClientRect() });
        card.classList.add('on');
      }, delay);
    });
    container.addEventListener('pointerout', (e) => {
      if (cardFor && !cardFor.contains(e.relatedTarget)) hideCard();
    });
  }
  root.addEventListener('pointerdown', hideCard, true);
  root.addEventListener('wheel', hideCard, { capture: true, passive: true });
  root.addEventListener('keydown', hideCard, true);

  root.Overlays = { menu, hoverCard, hideCard };
})(window);
