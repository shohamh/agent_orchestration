"""A separate, deliberately broken project for every blank-workspace session."""

import tempfile
from pathlib import Path


def prepare_workspace(workspace: str | None) -> Path:
    if workspace:
        path = Path(workspace).expanduser().resolve()
        if not path.is_dir():
            raise ValueError(f"Workspace directory does not exist: {path}")
        return path
    path = Path(tempfile.mkdtemp(prefix="agent-board-demo-"))
    (path / "cart.py").write_text('def total(prices):\n    """Return the sum, including the last item."""\n    return sum(prices[:-1])\n', encoding="utf-8")
    (path / "test_cart.py").write_text(
        'import unittest\nfrom cart import total\n\nclass CartTests(unittest.TestCase):\n'
        '    def test_all_items(self):\n        self.assertEqual(total([2, 3, 5]), 10)\n'
        '    def test_empty(self):\n        self.assertEqual(total([]), 0)\n', encoding="utf-8")
    (path / "README.md").write_text("# Agent Board demo\n\nFix the failing cart total test. Run `python -m unittest -v`.\n", encoding="utf-8")
    return path
