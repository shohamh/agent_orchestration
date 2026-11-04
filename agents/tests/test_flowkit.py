import asyncio
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from flowkit.definition import FLOWS_DIR, FlowError, load, parse, parse_text
from flowkit.executor import run_step, step_context
from flowkit.template import command_argv, render


def flow(**steps):
    return {"name": "t-flow", "input": {"topic": "x"}, "steps": steps}


class DefinitionTests(unittest.TestCase):
    def test_orders_steps_by_dependency(self):
        f = parse(flow(c={"kind": "program", "run": "c", "needs": ["a", "b"]},
                       b={"kind": "program", "run": "b", "needs": ["a"]}, a={"kind": "program", "run": "a"}))
        self.assertEqual(list(f.steps), ["a", "b", "c"])
        self.assertEqual(f.children("a"), ["b", "c"])

    def test_reports_every_problem(self):
        with self.assertRaises(FlowError) as raised:
            parse({"name": "Bad Name", "steps": {"a": {"kind": "agent", "needs": ["zzz"]},
                                                  "b": {"kind": "program", "run": "x", "colour": 1}}})
        message = str(raised.exception)
        for expected in ("name must be", "needs a prompt", "unknown step 'zzz'", "unknown keys"):
            self.assertIn(expected, message)

    def test_rejects_cycles_and_reserved_names(self):
        with self.assertRaisesRegex(FlowError, "cycle"):
            parse(flow(a={"kind": "program", "run": "a", "needs": ["b"]}, b={"kind": "program", "run": "b", "needs": ["a"]}))
        with self.assertRaisesRegex(FlowError, "reserved"):
            parse({"name": "agent-session", "steps": {"a": {"kind": "program", "run": "a"}}})

    def test_invalid_yaml_is_a_flow_error(self):
        with self.assertRaisesRegex(FlowError, "Not valid YAML"):
            parse_text("name: [unclosed")

    def test_example_flows_are_valid(self):
        for path in FLOWS_DIR.glob("*.yaml"):
            load(path)


class TemplateTests(unittest.TestCase):
    def test_substitutes_input_and_step_outputs(self):
        ctx = {"input": {"topic": "caching"}, "steps": {"a": {"output": {"n": [1, 2]}}}}
        self.assertEqual(render("{{ input.topic }}: {{steps.a.output.n}} in {flow_dir}", ctx, "/f"), "caching: [1, 2] in /f")

    def test_unknown_reference_is_an_error(self):
        with self.assertRaisesRegex(KeyError, "not available"):
            render("{{ steps.missing.output }}", {"input": {}, "steps": {}})

    @unittest.skipUnless(os.name == "nt", "cmd.exe rules")
    def test_windows_commands_split_like_the_c_runtime_and_keep_values_whole(self):
        ctx = {"input": {"v": 'a "b"\nc\\'}}
        argv = command_argv('python -c "print(1)" x"y z" \\\\"q\\" ""{{ input.v }}', ctx)
        self.assertEqual(argv, [sys.executable, "-c", "print(1)", "xy z", '\\q" "' + 'a "b"\nc\\'], "as CommandLineToArgvW splits it")
        self.assertEqual(command_argv('python s.py "{{ input.v }} tail"', ctx)[2], 'a "b"\nc\\ tail')
        for needs_cmd in ("python a.py {{ input.v }} | more", "python a.py %TEMP% {{ input.v }}", "echo {{ input.v }}", "python a.py > out {{ input.v }}"):
            self.assertIsNone(command_argv(needs_cmd, ctx), needs_cmd)

    @unittest.skipUnless(os.name == "nt", "cmd.exe rules")
    def test_windows_shell_values_are_quoted_and_unsafe_ones_refused(self):
        self.assertEqual(render("x {{ input.v }} y", {"input": {"v": "a&b|c\\"}}, shell=True), 'x "a&b|c\\\\" y')
        self.assertEqual(render('x "pre {{ input.v }}" y', {"input": {"v": "a&b"}}, shell=True), 'x "pre a&b" y')
        for unsafe in ('say "hi"', "100%", "two\nlines"):
            with self.assertRaisesRegex(ValueError, "FLOW_CONTEXT"):
                render("x {{ input.v }}", {"input": {"v": unsafe}}, shell=True)


class ExecutorTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        os.environ["FLOW_WORKSPACES"] = self.tmp.name

    async def asyncTearDown(self):
        self.tmp.cleanup()

    async def run_flow(self, f, params=None):
        outputs, events = {}, {}

        async def never():
            await asyncio.Event().wait()

        for sid, step in f.steps.items():
            events[sid] = []

            async def sink(event, sid=sid):
                events[sid].append(event)

            outputs[sid] = await run_step(f, sid, step_context(f, step, params, outputs), "run-1", never, sink)
        return outputs, events

    async def test_research_flow_runs_end_to_end(self):
        outputs, events = await self.run_flow(load(FLOWS_DIR / "research-flow.yaml"), {"topic": "edge caching"})
        self.assertEqual(len(outputs["fetch_sources"]["docs"]), 3)
        self.assertEqual(outputs["extract_entities"]["entities"], ["Acme Corp", "Globex", "Initech"])
        self.assertEqual(outputs["assess_risk"]["level"], "high")
        self.assertIn("Simulated agent", outputs["write_report"])
        logs = [e["text"] for e in events["fetch_sources"] if e["type"] == "log"]
        self.assertIn("fetched 3 documents", logs)
        self.assertEqual(events["summarize"][-1]["state"], "done")

    async def test_failing_program_raises_with_its_output(self):
        f = parse(flow(a={"kind": "program", "run": 'python -c "print(\'boom\'); raise SystemExit(3)"'}))
        with self.assertRaisesRegex(RuntimeError, "code 3: boom"):
            await self.run_flow(f)

    async def test_template_values_reach_a_program_exactly_and_never_run_as_commands(self):
        marker = Path(self.tmp.name) / "injected"
        nasty = f'line one\nline "two" & echo x > "{marker}" | more %PATH% !bang! ^caret tail\\'
        f = parse(flow(a={"kind": "program", "run": 'python -c "import json, sys; print(json.dumps(sys.argv[1:]))" {{ input.topic }}'}))
        outputs, _ = await self.run_flow(f, {"topic": nasty})
        self.assertEqual(outputs["a"]["stdout"], json.dumps([nasty]))
        self.assertFalse(marker.exists())

    async def test_shell_syntax_commands_quote_values_or_refuse_ones_the_shell_would_change(self):
        marker = Path(self.tmp.name) / "injected"
        f = parse(flow(a={"kind": "program", "run": 'python -c "import sys; print(sys.argv[1])" {{ input.topic }} | python -c "import sys; print(sys.stdin.read().strip())"'}))
        outputs, _ = await self.run_flow(f, {"topic": f"a&echo>{marker.name}"})
        self.assertEqual(outputs["a"]["stdout"], f"a&echo>{marker.name}")
        self.assertEqual(list(Path(self.tmp.name).rglob(marker.name)), [], "the value never ran as a command")
        if os.name == "nt":
            with self.assertRaisesRegex(ValueError, "FLOW_CONTEXT"):
                await self.run_flow(f, {"topic": "two\nlines"})

    async def test_agent_review_waits_for_mark_done(self):
        f = parse(flow(a={"kind": "agent", "agent": "demo", "prompt": "hi", "review": True}))
        inbox: asyncio.Queue = asyncio.Queue()
        seen = []

        async def sink(event):
            seen.append(event)
            if event.get("type") == "status" and event.get("state") == "awaiting_review":
                inbox.put_nowait({"type": "done"})

        # review=True uses the full demo (permission + question); answer both.
        async def answer(event):
            await sink(event)
            if event["type"] == "approval":
                inbox.put_nowait({"type": "approval", "id": event["id"], "optionId": "allow"})
            if event["type"] == "question":
                inbox.put_nowait({"type": "answer", "id": event["id"], "answers": {"style": "brief"}})

        out = await asyncio.wait_for(run_step(f, "a", step_context(f, f.steps["a"], {}, {}), "run-2", inbox.get, answer), 20)
        self.assertIn("Demo turn 1 finished", out)
        self.assertIn("awaiting_review", [e.get("state") for e in seen])


if __name__ == "__main__":
    unittest.main()
