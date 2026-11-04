import unittest

import moves
from cards import Card


def card(column, system="hatchet", task="research-flow", session_state=None, **kw):
    return Card(id=kw.pop("id", f"{column}-1"), system=system, board="b", title="t", task=task, column=column,
                status="X", session_state=session_state, **kw)


class MovesTests(unittest.TestCase):
    def test_queued_can_reorder_or_cancel_only(self):
        self.assertEqual(moves.moves_for(card("queued")), {"queued": "reorder", "stopped": "cancel"})

    def test_scheduled_reorders_only_on_prefect(self):
        self.assertEqual(moves.moves_for(card("scheduled", "prefect")),
                         {"scheduled": "reorder", "queued": "start_now", "stopped": "cancel"})
        self.assertEqual(moves.moves_for(card("scheduled", "trigger")), {"queued": "start_now", "stopped": "cancel"})
        self.assertEqual(moves.moves_for(card("scheduled", "hatchet")), {"stopped": "cancel"})

    def test_running_and_waiting_actions_depend_on_the_task(self):
        self.assertEqual(moves.moves_for(card("running", task="steerable-agent"))["done"], "stop")
        self.assertNotIn("done", moves.moves_for(card("running")))
        session = moves.moves_for(card("waiting", task="agent-session", session_state="awaiting_review"))
        self.assertEqual((session["done"], session["running"], session["queued"]), ("mark_done", "continue", "restart"))
        self.assertNotIn("done", moves.moves_for(card("waiting", task="agent-session", session_state="approval")))

    def test_finished_runs_can_only_be_restarted(self):
        for column in ("done", "stopped"):
            self.assertEqual(moves.moves_for(card(column)), {"queued": "restart"})

    def test_restart_only_at_the_ends_of_queued(self):
        started = card("done")
        a, b = card("queued", id="a"), card("queued", id="b")
        self.assertEqual(moves.validate(started, "queued", None, a), "restart")
        self.assertEqual(moves.validate(started, "queued", b, None), "restart")
        with self.assertRaisesRegex(ValueError, "top or bottom"):
            moves.validate(started, "queued", a, b)

    def test_disallowed_target_is_refused(self):
        with self.assertRaisesRegex(ValueError, "can't move to done"):
            moves.validate(card("queued"), "done", None, None)

    def test_annotate_sorts_queue_by_engine_order_and_newest_finished_first(self):
        q1, q2 = card("queued", id="q1", order=5.0), card("queued", id="q2", order=1.0)
        d1, d2 = card("done", id="d1", finished_at="2026-01-01"), card("done", id="d2", finished_at="2026-02-01")
        ordered = [c.id for c in moves.annotate([d1, q1, d2, q2])]
        self.assertEqual(ordered, ["q2", "q1", "d2", "d1"])
        self.assertEqual(q1.reorder, "coarse")


if __name__ == "__main__":
    unittest.main()
