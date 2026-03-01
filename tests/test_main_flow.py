import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from app.main import ingest_and_create_signal, execute_signal, sync_pending_entries
from app.storage.db import DB, IllegalTransitionError
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

        status = execute_signal(self.db, bundle["signal_id"], bundle["ticker"], qty=1.0)
        self.assertEqual(status, "FILLED")

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

        status = execute_signal(self.db, bundle["signal_id"], bundle["ticker"], qty=1.0)
        self.assertEqual(status, "BLOCKED")

        cur = self.db.conn.cursor()
        cur.execute("select count(*) from orders")
        self.assertEqual(cur.fetchone()[0], 0)

    def test_execute_signal_blocked_by_kill_switch(self) -> None:
        bundle = ingest_and_create_signal(self.db)
        self.assertIsNotNone(bundle)
        kill_switch.on()

        status = execute_signal(self.db, bundle["signal_id"], bundle["ticker"], qty=1.0)
        self.assertEqual(status, "BLOCKED")

        cur = self.db.conn.cursor()
        cur.execute("select count(*) from orders")
        self.assertEqual(cur.fetchone()[0], 0)

    def test_execute_signal_order_not_filled(self) -> None:
        bundle = ingest_and_create_signal(self.db)
        self.assertIsNotNone(bundle)

        with patch("app.main.PaperBroker.send_order", return_value=OrderResult(status="REJECTED", filled_qty=0, avg_price=0, reason_code="SIM_REJECT")):
            status = execute_signal(self.db, bundle["signal_id"], bundle["ticker"], qty=1.0)

        self.assertEqual(status, "BLOCKED")
        cur = self.db.conn.cursor()
        cur.execute("select count(*) from orders")
        self.assertEqual(cur.fetchone()[0], 0)  # rolled back tx #2

    def test_execute_signal_order_sent_pending(self) -> None:
        bundle = ingest_and_create_signal(self.db)
        self.assertIsNotNone(bundle)

        with patch(
            "app.main.PaperBroker.send_order",
            return_value=OrderResult(
                status="SENT",
                filled_qty=0,
                avg_price=0,
                reason_code="ORDER_ACCEPTED:ABC",
                broker_order_id="ABC",
            ),
        ):
            status = execute_signal(self.db, bundle["signal_id"], bundle["ticker"], qty=1.0)

        self.assertEqual(status, "PENDING")
        cur = self.db.conn.cursor()
        cur.execute("select status from positions order by position_id desc limit 1")
        self.assertEqual(cur.fetchone()[0], "PENDING_ENTRY")
        cur.execute("select status, broker_order_id from orders order by id desc limit 1")
        row = cur.fetchone()
        self.assertEqual(row[0], "SENT")
        self.assertEqual(row[1], "ABC")

    def test_sync_pending_entries_fills_order(self) -> None:
        bundle = ingest_and_create_signal(self.db)
        self.assertIsNotNone(bundle)

        with patch(
            "app.main.PaperBroker.send_order",
            return_value=OrderResult(status="SENT", filled_qty=0, avg_price=0, broker_order_id="ABC"),
        ):
            status = execute_signal(self.db, bundle["signal_id"], bundle["ticker"], qty=1.0)
        self.assertEqual(status, "PENDING")

        with patch(
            "app.main.PaperBroker.inquire_order",
            return_value=OrderResult(status="FILLED", filled_qty=1, avg_price=83500.0, broker_order_id="ABC"),
        ):
            changed = sync_pending_entries(self.db)
        self.assertGreaterEqual(changed, 1)

        cur = self.db.conn.cursor()
        cur.execute("select status from positions order by position_id desc limit 1")
        self.assertEqual(cur.fetchone()[0], "OPEN")
        cur.execute("select status from orders where side='BUY' order by id desc limit 1")
        self.assertEqual(cur.fetchone()[0], "FILLED")

    def test_sync_pending_entries_rejected_cancels_position(self) -> None:
        bundle = ingest_and_create_signal(self.db)
        self.assertIsNotNone(bundle)

        with patch(
            "app.main.PaperBroker.send_order",
            return_value=OrderResult(status="SENT", filled_qty=0, avg_price=0, broker_order_id="ABC"),
        ):
            status = execute_signal(self.db, bundle["signal_id"], bundle["ticker"], qty=1.0)
        self.assertEqual(status, "PENDING")

        with patch(
            "app.main.PaperBroker.inquire_order",
            return_value=OrderResult(status="REJECTED", filled_qty=0, avg_price=0.0, reason_code="BROKER_REJECT", broker_order_id="ABC"),
        ):
            changed = sync_pending_entries(self.db)
        self.assertGreaterEqual(changed, 1)

        cur = self.db.conn.cursor()
        cur.execute("select status, exit_reason_code from positions order by position_id desc limit 1")
        row = cur.fetchone()
        self.assertEqual(row[0], "CANCELLED")
        self.assertEqual(row[1], "BROKER_REJECT")
        cur.execute("select status from orders where side='BUY' order by id desc limit 1")
        self.assertEqual(cur.fetchone()[0], "REJECTED")

    def test_order_terminal_transition_guard(self) -> None:
        bundle = ingest_and_create_signal(self.db)
        self.assertIsNotNone(bundle)

        with patch(
            "app.main.PaperBroker.send_order",
            return_value=OrderResult(status="SENT", filled_qty=0, avg_price=0, broker_order_id="ABC"),
        ):
            status = execute_signal(self.db, bundle["signal_id"], bundle["ticker"], qty=1.0)
        self.assertEqual(status, "PENDING")

        # 강제로 FILLED 처리 후 역전이 시도
        cur = self.db.conn.cursor()
        cur.execute("select id from orders where side='BUY' order by id desc limit 1")
        order_id = int(cur.fetchone()[0])
        self.db.update_order_filled(order_id=order_id, price=83500.0, broker_order_id="ABC")

        with self.assertRaises(IllegalTransitionError):
            self.db.update_order_status(order_id=order_id, status="SENT", broker_order_id="ABC")

    def test_sync_pending_entries_retries_stale_order(self) -> None:
        bundle = ingest_and_create_signal(self.db)
        self.assertIsNotNone(bundle)

        with patch(
            "app.main.PaperBroker.send_order",
            return_value=OrderResult(status="SENT", filled_qty=0, avg_price=0, broker_order_id="ABC"),
        ):
            status = execute_signal(self.db, bundle["signal_id"], bundle["ticker"], qty=1.0)
        self.assertEqual(status, "PENDING")

        # retry interval 경과 시뮬레이션
        self.db.conn.execute("update orders set sent_at = datetime('now','-120 seconds') where side='BUY'")
        self.db.conn.commit()

        with patch("app.main.PaperBroker.inquire_order", return_value=None), patch(
            "app.main.PaperBroker.send_order",
            return_value=OrderResult(status="SENT", filled_qty=0, avg_price=0, broker_order_id="DEF"),
        ):
            changed = sync_pending_entries(self.db)
        self.assertGreaterEqual(changed, 1)

        cur = self.db.conn.cursor()
        cur.execute("select status, attempt_no, broker_order_id from orders where side='BUY' order by id")
        rows = cur.fetchall()
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0][0], "EXPIRED")
        self.assertEqual(rows[1][0], "SENT")
        self.assertEqual(rows[1][1], 2)
        self.assertEqual(rows[1][2], "DEF")


if __name__ == "__main__":
    unittest.main()
