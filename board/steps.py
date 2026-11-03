"""Shared transcript projection for agent steps on the three native backends."""
from transcript import Session, parse_evt_lines


async def node_session(adapter, ref):
    if hasattr(adapter, 'node_events'):
        dropped, events = await adapter.node_events(ref)
    else:
        dropped, events = 0, parse_evt_lines(await adapter.node_lines(ref))
    return Session(events, dropped)
