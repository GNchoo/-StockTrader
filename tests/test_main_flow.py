import tempfile
import unittest
from pathlib import Path

from app.main import ingest_and_create_signal, execute_signal
from app.storage.db import DB


class TestMainFlow(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / "main_flow.db"
        self.db = DB(str(self.db_path))
        self.db.init()

    def tearDown(self) -> None:
        self.db.close()
        self.tmpdir.cleanup()

    def test_ingest_and_create_signal_success_then_duplicate(self) -> None:
        first = ingest_and_create_signal(self.db)
        self.assertIsNotNone(first)
        self.assertIn("signal_id", first)
        self.assertIn("ticker", first)

        second = ingest_and_create_signal(self.db)
        self.assertIsNone(second)

    def test_execute_signal_success_path(self) -> None:
        bundle = ingest_and_create_signal(self.db)
        self.assertIsNotNone(bundle)

        ok = execute_signal(self.db, bundle["signal_id"], bundle["ticker"], qty=1.0)
        self.assertTrue(ok)

        cur = self.db.conn.cursor()
        cur.execute("select status, exit_reason_code from positions order by position_id desc limit 1")
        pos = cur.fetchone()
        self.assertIsNotNone(pos)
        self.assertEqual(pos[0], "CLOSED")
        self.assertEqual(pos[1], "TIME_EXIT")

        cur.execute("select count(*) from orders")
        order_count = cur.fetchone()[0]
        self.assertEqual(order_count, 2)  # BUY + SELL

        cur.execute("select count(*) from position_events")
        ev_count = cur.fetchone()[0]
        self.assertGreaterEqual(ev_count, 2)  # ENTRY + FULL_EXIT (dup ignored)


if __name__ == "__main__":
    unittest.main()
