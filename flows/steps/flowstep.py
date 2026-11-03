"""Helpers for program steps: read the step context, write the step output."""

import json
import os
import sys


def context() -> dict:
    """{"input": {...flow parameters}, "steps": {needed_step: {"output": ...}}}"""
    return json.loads(os.environ.get("FLOW_CONTEXT") or sys.stdin.read() or "{}")


def output(value) -> None:
    """The step's result, available to later steps as {{ steps.<id>.output }}."""
    with open(os.environ["FLOW_OUTPUT"], "w", encoding="utf-8") as f:
        json.dump(value, f, ensure_ascii=False)


def log(text: str) -> None:
    print(text, flush=True)
