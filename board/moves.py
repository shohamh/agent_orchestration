"""Drag and drop on the board: which column moves make sense for a card, and what they do.

A move is (card, target column, neighbours in the target column after the drop). The board shows
only the moves returned by `moves_for(card)`, and the server checks them again before acting.

| From              | To       | Action      | What each engine does                                         |
|-------------------|----------|-------------|---------------------------------------------------------------|
| queued            | queued   | reorder     | Prefect: new start time (exact). Hatchet: re-create the        |
|                   |          |             | not-started run at priority 1-3 (coarse). Trigger.dev: cancel  |
|                   |          |             | and re-run with a priority offset (only runs not started yet)  |
| scheduled         | scheduled| reorder     | Prefect only: new start time                                   |
| scheduled         | queued   | start_now   | Prefect: start time = now. Trigger.dev: reschedule to now      |
| queued, scheduled | stopped  | cancel      | cancel the run                                                 |
| running, waiting  | stopped  | cancel      | cancel the run (hard interrupt)                                |
| running           | done     | stop        | steerable agent: ask it to finish gracefully                   |
| waiting           | done     | mark_done   | coding-agent session waiting for you: Mark done                |
| waiting           | running  | continue    | coding-agent session waiting for you: Continue                 |
| any started run   | queued   | restart     | a new run with the same input, placed at the top (high         |
|                   |          |             | priority) or bottom (low priority) of Queued; a running run    |
|                   |          |             | is cancelled first                                             |

Ordering inside Queued / Scheduled is what the engine will run first, top to bottom.
"""

from typing import Any

from cards import Card

# Session states in which a coding-agent session is waiting for the human (see agentdriver).
REVIEW = {"awaiting_review", "awaiting_input", "stuck"}

REORDER = {
    "prefect": ("native", None),
    "hatchet": ("coarse", "Hatchet has three priority levels and can't change a queued run's priority, so a moved "
                           "card is re-created as a new run at the nearest level; order within a level is by age."),
    "trigger": ("recreate", "Trigger.dev sets priority only when a run starts, so a moved card that hasn't started is "
                            "cancelled and run again with a new priority. Runs that already started can only be "
                            "restarted at the top or bottom of Queued."),
}
REORDER["trigger-dev"] = REORDER["trigger"]


def moves_for(card: Card) -> dict[str, str]:
    """Target column -> action for this card (its own column means reordering within it)."""
    moves: dict[str, str] = {}
    column, system = card.column, card.system
    if column == "queued":
        moves.update(queued="reorder", stopped="cancel")
    elif column == "scheduled":
        if system == "prefect":
            moves["scheduled"] = "reorder"
        if system in ("prefect", "trigger", "trigger-dev"):
            moves["queued"] = "start_now"
        moves["stopped"] = "cancel"
    elif column in ("running", "waiting"):
        moves.update(stopped="cancel", queued="restart")
        if column == "running" and card.task == "steerable-agent":
            moves["done"] = "stop"
        if card.task == "agent-session" and card.session_state in REVIEW:
            moves.update(done="mark_done", running="continue")
    elif column in ("done", "stopped"):
        moves["queued"] = "restart"
    return moves


def annotate(cards: list[Card], *, sort: bool = True) -> list[Card]:
    """Fill each card's moves and reorder mode; optionally return cards in display order."""
    for card in cards:
        card.moves = moves_for(card)
        card.reorder, card.reorder_note = REORDER.get(card.system, (None, None))
    return sorted(cards, key=_position) if sort else cards


def _position(card: Card) -> tuple:
    if card.column in ("queued", "scheduled"):
        return (0, card.order if card.order is not None else 0.0, card.created_at or "")
    if card.column in ("running", "waiting"):
        return (1, card.started_at or card.created_at or "", "")
    return (2, _desc(card.finished_at or card.created_at or ""), "")  # newest finished on top


def _desc(text: str) -> str:
    return "".join(chr(0x10FFFF - ord(c)) for c in text)


def placement(target: list[Card], moving_id: str, above: str | None, below: str | None) -> tuple[Card | None, Card | None]:
    """The cards just above and below the drop point, ignoring the moving card itself."""
    by_id = {c.id: c for c in target if c.id != moving_id}
    return by_id.get(above) if above else None, by_id.get(below) if below else None


def validate(card: Card, to: str, above: Card | None, below: Card | None) -> str:
    """The action for this drop, or ValueError explaining why it isn't allowed."""
    action = card_moves(card).get(to)
    if action is None:
        raise ValueError(f"A {card.column} card can't move to {to} on this backend")
    if action == "restart" and above is not None and below is not None:
        raise ValueError("A run that already started can only be restarted at the top or bottom of Queued")
    return action


def card_moves(card: Card) -> dict[str, Any]:
    return card.moves if card.moves else moves_for(card)
