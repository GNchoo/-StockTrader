import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from app.execution.broker_base import OrderRequest
from app.execution.paper_broker import PaperBroker
from app.risk.engine import can_trade, kill_switch
from app.storage.db import DB
from tests.helpers import seed_signal


class TestRiskGate(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / "risk_test.db"
        self.db = DB(str(self.db_path))
        self.db.init()

    def tearDown(self) -> None:
        kill_switch.off()
        self.db.close()
        self.tmpdir.cleanup()

    def _seed_signal(self) -> int:
        _, _, signal_id = seed_signal(self.db, url="https://example.com/risk", raw_hash="risk-h1")
        return signal_id

    def test_risk_state_trading_disabled_blocks_flow(self) -> None:
        signal_id = self._seed_signal()
        trade_date = datetime.now().date().isoformat()

        self.db.begin()
        try:
            self.db.ensure_risk_state_today(trade_date)
            self.db.conn.execute(
                "update risk_state set trading_enabled=0 where trade_date=?",
                (trade_date,),
            )
            rs = self.db.get_risk_state(trade_date)
            self.assertIsNotNone(rs)
            self.assertEqual(int(rs["trading_enabled"]), 0)
            self.db.commit()
        except Exception:
            self.db.rollback()
            raise

        # Simulate execution gate in main flow
        rs = self.db.get_risk_state(trade_date)
        blocked_by_state = (not rs) or int(rs["trading_enabled"]) != 1
        self.assertTrue(blocked_by_state)

        # Ensure no position/order created when blocked
        cur = self.db.conn.cursor()
        cur.execute("select count(*) from positions where signal_id=?", (signal_id,))
        self.assertEqual(cur.fetchone()[0], 0)

    def test_kill_switch_blocks_trade(self) -> None:
        kill_switch.on()
        decision = can_trade(account_state={"trading_enabled": 1})
        self.assertFalse(decision.allowed)
        self.assertEqual(decision.reason_code, "KILL_SWITCH_ON")

    def test_enabled_risk_allows_paper_order(self) -> None:
        signal_id = self._seed_signal()
        trade_date = datetime.now().date().isoformat()
        self.db.ensure_risk_state_today(trade_date)
        rs = self.db.get_risk_state(trade_date)
        self.assertIsNotNone(rs)
        self.assertEqual(int(rs["trading_enabled"]), 1)

        decision = can_trade(account_state=rs)
        self.assertTrue(decision.allowed)

        broker = PaperBroker(base_latency_ms=1)
        result = broker.send_order(
            OrderRequest(
                signal_id=signal_id,
                ticker="005930",
                side="BUY",
                qty=1,
                expected_price=83500.0,
            )
        )
        self.assertEqual(result.status, "FILLED")
        self.assertEqual(result.avg_price, 83500.0)


if __name__ == "__main__":
    unittest.main()
