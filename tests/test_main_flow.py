import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from app.main import ingest_and_create_signal, execute_signal
from app.storage.db import DB
from app.risk.engine import kill_switch
from app.execution.broker_base import OrderResult


class TestMainFlow(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / "main_flow.db"
        self.db = DB(str(self.db_path))
        self.db.init()

    def tearDown(self) -> None:
        kill_switch.off()
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

    def test_execute_signal_blocked_by_risk_state(self) -> None:
        bundle = ingest_and_create_signal(self.db)
        self.assertIsNotNone(bundle)

        trade_date = datetime.now().date().isoformat()
        self.db.ensure_risk_state_today(trade_date)
        self.db.conn.execute("update risk_state set trading_enabled=0 where trade_date=?", (trade_date,))
        self.db.commit()

        ok = execute_signal(self.db, bundle["signal_id"], bundle["ticker"], qty=1.0)
        self.assertFalse(ok)

        cur = self.db.conn.cursor()
        cur.execute("select count(*) from orders")
        self.assertEqual(cur.fetchone()[0], 0)

    def test_execute_signal_blocked_by_kill_switch(self) -> None:
        bundle = ingest_and_create_signal(self.db)
        self.assertIsNotNone(bundle)
        kill_switch.on()

        ok = execute_signal(self.db, bundle["signal_id"], bundle["ticker"], qty=1.0)
        self.assertFalse(ok)

        cur = self.db.conn.cursor()
        cur.execute("select count(*) from orders")
        self.assertEqual(cur.fetchone()[0], 0)

    def test_execute_signal_order_not_filled(self) -> None:
        bundle = ingest_and_create_signal(self.db)
        self.assertIsNotNone(bundle)

        with patch("app.main.PaperBroker.send_order", return_value=OrderResult(status="REJECTED", filled_qty=0, avg_price=0, reason_code="SIM_REJECT")):
            ok = execute_signal(self.db, bundle["signal_id"], bundle["ticker"], qty=1.0)

        self.assertFalse(ok)
        cur = self.db.conn.cursor()
        cur.execute("select count(*) from orders")
        self.assertEqual(cur.fetchone()[0], 0)  # rolled back tx #2


if __name__ == "__main__":
    unittest.main()
