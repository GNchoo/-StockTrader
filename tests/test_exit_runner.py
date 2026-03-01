import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app.storage.db import DB
from app.scheduler.exit_runner import run_exit_cycle


class TestExitRunner(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / "exit_runner.db"
        self.db = DB(str(self.db_path))
        self.db.init()

    def tearDown(self) -> None:
        self.db.close()
        self.tmpdir.cleanup()

    def test_run_exit_cycle_returns_stage_counts(self):
        with patch("app.scheduler.exit_runner.sync_pending_entries", return_value=1), patch(
            "app.scheduler.exit_runner.trigger_opposite_signal_exit_orders", return_value=2
        ), patch("app.scheduler.exit_runner._build_broker") as build_mock, patch(
            "app.scheduler.exit_runner._collect_current_prices", return_value={"005930": 83500.0}
        ), patch("app.scheduler.exit_runner.trigger_trailing_stop_orders", return_value=3), patch(
            "app.scheduler.exit_runner.trigger_time_exit_orders", return_value=4
        ), patch("app.scheduler.exit_runner.sync_pending_exits", return_value=5):
            build_mock.return_value = object()
            out = run_exit_cycle(self.db)

        self.assertEqual(out["entry_sync"], 1)
        self.assertEqual(out["opposite_exit"], 2)
        self.assertEqual(out["trailing_exit"], 3)
        self.assertEqual(out["time_exit"], 4)
        self.assertEqual(out["exit_sync"], 5)


if __name__ == "__main__":
    unittest.main()
