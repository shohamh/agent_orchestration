import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from flowkit import document
from flowkit.definition import FLOWS_DIR, FlowError, parse, parse_text

RICH = """name: rich-flow
description: Résumé — 設計 review
input:
  topic: ünïcode ✓
  count: 3
  ratio: 0.5
  dry_run: false
  empty: ''
  nothing: null
worker:
  pool: gpu
environment:
  agent: codex
  worker:
    tags: [linux, big]
  repositories:
  - url: https://example.invalid/repo.git
    target: repo
    ref: main
  files:
  - source: notes.txt
    target: docs/notes.txt
steps:
  first:
    kind: program
    description: Prints the topic
    worker:
      id: pc-a
    retries: 2
    timeout_minutes: 5
    run: |
      python -c "print('{{ input.topic }}')"
      echo done
  second:
    kind: agent
    description: ''
    needs: [first]
    agent: opencode2
    review: true
    worker:
      pool: cpu
      tags: [x, y z]
    retries: 0
    timeout_minutes: 1440
    prompt: |
      Use {{ steps.first.output.stdout }} and {{ input.count }}.
        Indented line, tab-free.
      Last line without newline
  third:
    kind: agent
    needs: [first, second]
    prompt: '{{ steps.second.output }}'
"""


def errors_of(text, name=None):
    return document.parse(text, name)["errors"]


class RoundTripTests(unittest.TestCase):
    def assert_round_trip(self, text, name=None):
        first = document.parse(text, name)
        self.assertEqual(first["errors"], [], text)
        again = document.parse(document.serialize(first["document"]), name)
        self.assertEqual(again["document"], first["document"])
        self.assertTrue(again["lossless"])  # canonical output is a fixed point
        self.assertEqual(parse(again["document"]).to_json(), parse_text(text).to_json())
        return first

    def test_every_flow_file_round_trips(self):
        for path in sorted(FLOWS_DIR.glob("*.yaml")):
            with self.subTest(path.name):
                self.assert_round_trip(path.read_bytes().decode("utf-8"), path.stem)

    def test_canonical_flows_are_lossless_and_comments_are_not(self):
        for name in ("hello-flow", "multi-agent-design", "research-flow"):
            self.assertTrue(document.parse((FLOWS_DIR / f"{name}.yaml").read_text(encoding="utf-8"))["lossless"], name)
        self.assertFalse(document.parse((FLOWS_DIR / "ui-made-flow.yaml").read_text(encoding="utf-8"))["lossless"])
        self.assertFalse(document.parse("# comment\n" + document.serialize({"name": "a", "steps": {}}))["lossless"])

    def test_every_supported_field_survives(self):
        result = self.assert_round_trip(RICH)
        doc = result["document"]
        self.assertEqual(list(doc), ["name", "description", "input", "worker", "environment", "steps"])
        self.assertNotIn("setup_environment", doc["steps"])
        self.assertEqual(doc["steps"]["first"]["needs"], [])
        self.assertEqual(doc["steps"]["second"]["worker"], {"pool": "cpu", "tags": ["x", "y z"]})
        self.assertEqual(doc["input"]["nothing"], None)
        self.assertTrue(doc["steps"]["second"]["prompt"].endswith("without newline\n"))
        text = document.serialize(doc)
        self.assertIn("ünïcode ✓", text)
        self.assertIn("run: |", text)
        self.assertIn("needs: [first, second]", text)
        self.assertNotIn("setup_environment", text)
        self.assertTrue(result["lossless"], text)

    def test_generated_environment_step_is_never_written(self):
        flow = parse_text(RICH)
        self.assertIn("setup_environment", flow.steps)
        doc = document.parse(RICH)["document"]
        doc["steps"] = {"setup_environment": flow.steps["setup_environment"].to_json(), **doc["steps"]}
        self.assertNotIn("setup_environment", document.serialize(doc))

    def test_needs_given_as_text_becomes_a_list(self):
        result = document.parse("name: a-b\nsteps:\n  a: {kind: program, run: x}\n  b: {kind: program, run: y, needs: a}\n")
        self.assertEqual(result["document"]["steps"]["b"]["needs"], ["a"])
        self.assertFalse(result["lossless"])


class ValidationTests(unittest.TestCase):
    def test_invalid_yaml_has_no_document(self):
        result = document.parse("name: [unclosed")
        self.assertIsNone(result["document"])
        self.assertIn("Not valid YAML", result["errors"][0]["message"])

    def test_duplicate_keys_are_reported_with_their_location(self):
        errors = errors_of("name: a-b\nname: a-c\nsteps:\n  a: {kind: program, run: x, run: y}\n  a: {kind: program, run: z}\n")
        self.assertIn({"message": "duplicate key 'name' (line 2)", "step": None, "field": "name"}, errors)
        self.assertIn({"message": "steps.a: duplicate key 'run' (line 4)", "step": "a", "field": "run"}, errors)
        self.assertIn({"message": "step 'a' is defined more than once (line 5)", "step": "a", "field": None}, errors)

    def test_cycle_reports_each_member_only(self):
        errors = errors_of("name: a-b\nsteps:\n  a: {kind: program, run: x, needs: [b]}\n"
                           "  b: {kind: program, run: x, needs: [a]}\n  c: {kind: program, run: x, needs: [b]}\n")
        self.assertEqual([(e["step"], e["field"]) for e in errors], [("a", "needs"), ("b", "needs")])
        with self.assertRaisesRegex(FlowError, "steps form a cycle: a, b, c"):  # the text callers see is unchanged
            parse_text("name: a-b\nsteps:\n  a: {kind: program, run: x, needs: [b]}\n"
                       "  b: {kind: program, run: x, needs: [a]}\n  c: {kind: program, run: x, needs: [b]}\n")

    def test_unknown_and_self_dependencies(self):
        errors = errors_of("name: a-b\nsteps:\n  a: {kind: program, run: x, needs: [zzz]}\n  b: {kind: program, run: x, needs: [b]}\n")
        self.assertIn({"message": "step 'a': needs unknown step 'zzz'", "step": "a", "field": "needs"}, errors)
        self.assertIn({"message": "step 'b': cannot need itself", "step": "b", "field": "needs"}, errors)

    def test_template_references(self):
        errors = errors_of("""name: a-b
input: {topic: x}
steps:
  a: {kind: program, run: "echo {{ input.topic }} {{ input }} {{ input.missing }}"}
  b: {kind: program, run: x}
  c:
    kind: agent
    needs: [a]
    prompt: "{{ steps.a.output.x }} {{ steps.b.output }} {{ steps.gone.output }} {{ steps.a }} {{ secrets.k }}"
""")
        found = {(e["step"], e["field"], e["message"].split(": ", 1)[1]) for e in errors}
        self.assertEqual(found, {
            ("a", "run", "{{ input.missing }} is not a declared input"),
            ("c", "prompt", "{{ steps.b.output }}: 'b' is not in this step's needs"),
            ("c", "prompt", "{{ steps.gone.output }}: 'gone' is not a step"),
            ("c", "prompt", "{{ steps.a }} must be steps.<step>.output[.field]"),
            ("c", "prompt", "{{ secrets.k }} is unknown; use input.<name> or steps.<needed step>.output"),
        })

    def test_root_steps_may_use_the_environment_step(self):
        text = RICH.replace("python -c", "python {{ steps.setup_environment.output }} -c")
        self.assertEqual(errors_of(text), [])

    def test_definition_errors_are_located(self):
        errors = errors_of("name: Bad\nworker: {nope: 1}\nsteps:\n  a: {kind: agent, agent: gpt, prompt: ''}\n"
                           "  b: {kind: program, run: x, retries: 99, colour: red}\n", "bad")
        located = {(e["step"], e["field"]) for e in errors}
        for expected in ((None, "name"), (None, "worker"), ("a", "agent"), ("a", "prompt"), ("b", "retries"), ("b", "colour")):
            self.assertIn(expected, located)

    def test_environment_errors_point_at_environment(self):
        errors = errors_of("name: a-b\nenvironment: {agent: nobody}\nsteps:\n  a: {kind: program, run: x}\n")
        self.assertEqual({(e["step"], e["field"]) for e in errors}, {(None, "environment")})

    def test_unsupported_yaml_values_are_reported(self):
        result = document.parse("name: a-b\ninput: {day: 2026-01-01}\nsteps:\n  a: {kind: program, run: x}\n")
        self.assertEqual(result["document"]["input"]["day"], "2026-01-01")
        self.assertTrue(any(e["field"] == "input" and "not supported" in e["message"] for e in result["errors"]))

    def test_name_must_match_the_file(self):
        self.assertIn("saved as 'other'", errors_of((FLOWS_DIR / "hello-flow.yaml").read_text(encoding="utf-8"), "other")[0]["message"])

    def test_validate_and_serialize_documents(self):
        self.assertEqual(document.validate([]), [{"message": "A flow must be a mapping with name, input and steps", "step": None, "field": None}])
        doc = {"name": "a-b", "steps": {"a": {"run": "x", "kind": "program", "needs": []}}}
        self.assertEqual(document.validate(doc), [])
        self.assertEqual(document.serialize(doc), "name: a-b\nsteps:\n  a:\n    kind: program\n    run: x\n")


if __name__ == "__main__":
    unittest.main()
