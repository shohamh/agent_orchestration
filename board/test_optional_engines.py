"""The board works with any of the engines, and without their SDKs."""
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent))
import server


class OptionalEngines(unittest.TestCase):
    def load(self, **env):
        clean = {k: v for k, v in os.environ.items() if k not in
                 ("HATCHET_CLIENT_TOKEN", "TRIGGER_SECRET_KEY", "TRIGGER_DEV_SECRET_KEY", "PREFECT_API_URL", "BOARD_LOADTEST_CARDS")}
        with patch.dict(os.environ, {**clean, **env}, clear=True):
            return server.load_adapters()

    def test_no_engine_configured_is_a_working_board(self):
        self.assertEqual(self.load(), {})

    def test_a_missing_hatchet_sdk_skips_only_hatchet(self):
        # None in sys.modules makes the import raise ImportError, as in an image built without the SDK.
        with patch.dict(sys.modules, {"hatchet_sdk": None, "hatchet_adapter": None}):
            with self.assertLogs(level="ERROR") as logs:
                adapters = self.load(HATCHET_CLIENT_TOKEN="t", TRIGGER_SECRET_KEY="k")
        self.assertEqual(set(adapters), {"trigger"})
        self.assertIn("ENGINES=hatchet", logs.output[0])


if __name__ == "__main__":
    unittest.main()
