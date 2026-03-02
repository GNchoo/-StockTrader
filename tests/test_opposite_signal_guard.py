import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

from app.main import trigger_opposite_signal_exit_orders
from app.storage.db import DB
from tests.helpers import seed_signal


class TestOppositeSignalGuard(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / "opposite_guard.db"
        self.db = DB(str(self.db_path))
        self.db.init()

    def tearDown(self) -> None:
        self.db.close()
        self.tmpdir.cleanup()

    def test_skip_when_latest_signal_is_entry_signal(self) -> None:
        _, _, signal_id = seed_signal(self.db, url="https://example.com/oppo", raw_hash="oppo-h1")

        position_id = self.db.create_position("005930", signal_id, qty=1.0)
        self.db.set_position_open(position_id, avg_entry_price=83500.0, opened_value=83500.0)

        broker = Mock()
        broker.send_order.return_value = Mock(status="SENT", broker_order_id="X")

        created = trigger_opposite_signal_exit_orders(
            self.db,
            exit_score_threshold=85.0,  # latest score(80)면 원래는 청산 조건이 될 수 있음
            broker=broker,
        )

        self.assertEqual(created, 0)
        broker.send_order.assert_not_called()


if __name__ == "__main__":
    unittest.main()
