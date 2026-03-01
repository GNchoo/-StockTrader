import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from app.main import (
    ingest_and_create_signal,
    execute_signal,
    sync_pending_entries,
    sync_pending_exits,
    trigger_time_exit_orders,
    trigger_trailing_stop_orders,
    trigger_opposite_signal_exit_orders,
    _collect_current_prices,
)
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

    def test_sync_pending_entries_partial_fill_no_retry(self) -> None:
        bundle = ingest_and_create_signal(self.db)
        self.assertIsNotNone(bundle)

        with patch(
            "app.main.PaperBroker.send_order",
            return_value=OrderResult(status="SENT", filled_qty=0, avg_price=0, broker_order_id="ABC"),
        ):
            status = execute_signal(self.db, bundle["signal_id"], bundle["ticker"], qty=1.0)
        self.assertEqual(status, "PENDING")

        # retry interval 경과 + partial fill 상태
        self.db.conn.execute("update orders set sent_at = datetime('now','-120 seconds') where side='BUY'")
        self.db.conn.commit()

        with patch(
            "app.main.PaperBroker.inquire_order",
            return_value=OrderResult(status="PARTIAL_FILLED", filled_qty=0.4, avg_price=83400.0, broker_order_id="ABC"),
        ), patch("app.main.PaperBroker.send_order") as send_mock:
            changed = sync_pending_entries(self.db)
        self.assertEqual(send_mock.call_count, 0)
        self.assertGreaterEqual(changed, 0)

        cur = self.db.conn.cursor()
        cur.execute("select count(*) from orders where side='BUY'")
        self.assertEqual(cur.fetchone()[0], 1)
        cur.execute("select status, price, filled_qty from orders where side='BUY' order by id desc limit 1")
        row = cur.fetchone()
        self.assertEqual(row[0], "PARTIAL_FILLED")
        self.assertEqual(float(row[1]), 83400.0)
        self.assertAlmostEqual(float(row[2]), 0.4)

    def test_partial_fill_reaches_qty_opens_position(self) -> None:
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
            return_value=OrderResult(status="PARTIAL_FILLED", filled_qty=1.0, avg_price=83500.0, broker_order_id="ABC"),
        ):
            changed = sync_pending_entries(self.db)
        self.assertGreaterEqual(changed, 1)

        cur = self.db.conn.cursor()
        cur.execute("select status from positions order by position_id desc limit 1")
        self.assertEqual(cur.fetchone()[0], "OPEN")
        cur.execute("select status, filled_qty from orders where side='BUY' order by id desc limit 1")
        row = cur.fetchone()
        self.assertEqual(row[0], "FILLED")
        self.assertAlmostEqual(float(row[1]), 1.0)

    def test_retry_blocked_same_condition_reason(self) -> None:
        bundle = ingest_and_create_signal(self.db)
        self.assertIsNotNone(bundle)

        with patch(
            "app.main.PaperBroker.send_order",
            return_value=OrderResult(status="SENT", filled_qty=0, avg_price=0, broker_order_id="ABC"),
        ):
            status = execute_signal(self.db, bundle["signal_id"], bundle["ticker"], qty=1.0)
        self.assertEqual(status, "PENDING")

        # stale 만들기
        self.db.conn.execute("update orders set sent_at = datetime('now','-120 seconds') where side='BUY'")
        self.db.conn.commit()

        # 같은 거부 사유가 이미 있었다고 가정
        cur = self.db.conn.cursor()
        cur.execute("select position_id, id from orders where side='BUY' order by id desc limit 1")
        pos_id, order_id = cur.fetchone()
        self.db.insert_position_event(
            position_id=int(pos_id),
            event_type="BLOCK",
            action="BLOCKED",
            reason_code="BROKER_REJECT",
            detail_json='{"seed":true}',
            idempotency_key=f"seed-block:{pos_id}:{order_id}",
        )

        with patch("app.main.PaperBroker.inquire_order", return_value=None), patch(
            "app.main.PaperBroker.send_order",
            return_value=OrderResult(status="REJECTED", filled_qty=0, avg_price=0.0, reason_code="BROKER_REJECT", broker_order_id="DEF"),
        ):
            changed = sync_pending_entries(self.db)
        self.assertGreaterEqual(changed, 1)

        cur.execute("select status, exit_reason_code from positions where position_id=?", (pos_id,))
        row = cur.fetchone()
        self.assertEqual(row[0], "CANCELLED")
        self.assertEqual(row[1], "RETRY_BLOCKED_SAME_CONDITION")

    def test_sync_pending_exits_partial_then_full_close(self) -> None:
        # OPEN 포지션 + SELL 대기 주문 생성
        self.db.begin()
        pos_id = self.db.create_position("005930", 1, 1.0, autocommit=False)
        self.db.set_position_open(pos_id, avg_entry_price=83500.0, opened_value=83500.0, autocommit=False)
        sell_order_id = self.db.insert_order(
            position_id=pos_id,
            signal_id=1,
            ticker="005930",
            side="SELL",
            qty=1.0,
            order_type="MARKET",
            status="SENT",
            price=None,
            autocommit=False,
        )
        self.db.update_order_status(sell_order_id, "SENT", broker_order_id="S-1", autocommit=False)
        self.db.commit()

        # 1차 부분청산
        with patch(
            "app.main.PaperBroker.inquire_order",
            return_value=OrderResult(status="PARTIAL_FILLED", filled_qty=0.4, avg_price=83600.0, broker_order_id="S-1"),
        ):
            changed = sync_pending_exits(self.db)
        self.assertGreaterEqual(changed, 0)
        cur = self.db.conn.cursor()
        cur.execute("select status, exited_qty from positions where position_id=?", (pos_id,))
        row = cur.fetchone()
        self.assertEqual(row[0], "PARTIAL_EXIT")
        self.assertAlmostEqual(float(row[1]), 0.4)

        # 2차 전량청산
        with patch(
            "app.main.PaperBroker.inquire_order",
            return_value=OrderResult(status="FILLED", filled_qty=1.0, avg_price=83650.0, broker_order_id="S-1"),
        ):
            changed2 = sync_pending_exits(self.db)
        self.assertGreaterEqual(changed2, 1)
        cur.execute("select status, exited_qty from positions where position_id=?", (pos_id,))
        row2 = cur.fetchone()
        self.assertEqual(row2[0], "CLOSED")
        self.assertAlmostEqual(float(row2[1]), 1.0)

    def test_trigger_time_exit_orders_creates_sell(self) -> None:
        self.db.begin()
        pos_id = self.db.create_position("005930", 1, 1.0, autocommit=False)
        self.db.set_position_open(pos_id, avg_entry_price=83500.0, opened_value=83500.0, autocommit=False)
        # 오래된 포지션으로 만들어 트리거 대상화
        self.db.conn.execute("update positions set opened_at = datetime('now','-60 minutes') where position_id=?", (pos_id,))
        self.db.commit()

        with patch(
            "app.main.PaperBroker.send_order",
            return_value=OrderResult(status="SENT", filled_qty=0, avg_price=0.0, broker_order_id="SX-1"),
        ), patch(
            "app.main.PaperBroker.inquire_order",
            return_value=OrderResult(status="PARTIAL_FILLED", filled_qty=0.5, avg_price=83600.0, broker_order_id="SX-1"),
        ):
            created = trigger_time_exit_orders(self.db, max_hold_min=15)
        self.assertGreaterEqual(created, 1)

        cur = self.db.conn.cursor()
        cur.execute("select count(*) from orders where side='SELL'")
        self.assertEqual(cur.fetchone()[0], 1)
        cur.execute("select status from positions where position_id=?", (pos_id,))
        self.assertEqual(cur.fetchone()[0], "PARTIAL_EXIT")

    def test_trigger_trailing_stop_orders_creates_sell(self) -> None:
        self.db.begin()
        pos_id = self.db.create_position("005930", 1, 1.0, autocommit=False)
        self.db.set_position_open(pos_id, avg_entry_price=100.0, opened_value=100.0, autocommit=False)
        # 고점 형성
        self.db.update_position_high_watermark(pos_id, 110.0, autocommit=False)
        self.db.commit()

        with patch(
            "app.main.PaperBroker.send_order",
            return_value=OrderResult(status="SENT", filled_qty=0, avg_price=0.0, broker_order_id="TR-1"),
        ), patch(
            "app.main.PaperBroker.inquire_order",
            return_value=OrderResult(status="PARTIAL_FILLED", filled_qty=0.5, avg_price=106.0, broker_order_id="TR-1"),
        ):
            created = trigger_trailing_stop_orders(
                self.db,
                current_prices={"005930": 106.0},
                trailing_arm_pct=0.05,
                trailing_gap_pct=0.03,
            )
        self.assertGreaterEqual(created, 1)

        cur = self.db.conn.cursor()
        cur.execute("select count(*) from orders where side='SELL'")
        self.assertEqual(cur.fetchone()[0], 1)
        cur.execute("select status from positions where position_id=?", (pos_id,))
        self.assertEqual(cur.fetchone()[0], "PARTIAL_EXIT")

    def test_trigger_opposite_signal_exit_orders_creates_sell(self) -> None:
        self.db.begin()
        pos_id = self.db.create_position("005930", 1, 1.0, autocommit=False)
        self.db.set_position_open(pos_id, avg_entry_price=83500.0, opened_value=83500.0, autocommit=False)
        # 약화 신호 삽입 (score<70)
        self.db.insert_signal(
            {
                "news_id": 1,
                "event_ticker_id": 1,
                "ticker": "005930",
                "raw_score": 50,
                "total_score": 65,
                "components": "{}",
                "priced_in_flag": "LOW",
                "decision": "HOLD",
            },
            autocommit=False,
        )
        self.db.commit()

        with patch(
            "app.main.PaperBroker.send_order",
            return_value=OrderResult(status="SENT", filled_qty=0, avg_price=0.0, broker_order_id="OP-1"),
        ), patch(
            "app.main.PaperBroker.inquire_order",
            return_value=OrderResult(status="PARTIAL_FILLED", filled_qty=0.5, avg_price=83450.0, broker_order_id="OP-1"),
        ):
            created = trigger_opposite_signal_exit_orders(self.db, exit_score_threshold=70)
        self.assertGreaterEqual(created, 1)

        cur = self.db.conn.cursor()
        cur.execute("select count(*) from orders where side='SELL'")
        self.assertEqual(cur.fetchone()[0], 1)
        cur.execute("select status from positions where position_id=?", (pos_id,))
        self.assertEqual(cur.fetchone()[0], "PARTIAL_EXIT")

    def test_collect_current_prices_fallback_entry_price(self) -> None:
        self.db.begin()
        pos_id = self.db.create_position("005930", 1, 1.0, autocommit=False)
        self.db.set_position_open(pos_id, avg_entry_price=83500.0, opened_value=83500.0, autocommit=False)
        self.db.commit()

        class DummyBroker:
            def get_last_price(self, ticker: str):
                return None

        px = _collect_current_prices(self.db, DummyBroker())
        self.assertEqual(px.get("005930"), 83500.0)


if __name__ == "__main__":
    unittest.main()
