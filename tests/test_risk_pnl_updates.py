import tempfile
import unittest
from pathlib import Path

from app.storage.db import DB


class TestRiskPnlUpdates(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db = DB(str(Path(self.tmpdir.name) / "pnl.db"))
        self.db.init()

    def tearDown(self) -> None:
        self.db.close()
        self.tmpdir.cleanup()

    def test_apply_realized_pnl_updates_daily_and_streak(self):
        d = "2026-03-02"
        self.db.apply_realized_pnl(d, -1000.0)
        rs = self.db.get_risk_state(d)
        self.assertIsNotNone(rs)
        self.assertEqual(float(rs["daily_realized_pnl"]), -1000.0)
        self.assertEqual(int(rs["consecutive_losses"]), 1)

        self.db.apply_realized_pnl(d, 500.0)
        rs = self.db.get_risk_state(d)
        self.assertEqual(float(rs["daily_realized_pnl"]), -500.0)
        self.assertEqual(int(rs["consecutive_losses"]), 0)


if __name__ == "__main__":
    unittest.main()
