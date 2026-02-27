import json
import tempfile
import unittest
from pathlib import Path

from app.storage.db import DB
from tests.helpers import seed_signal


class TestLifecycle(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / "test.db"
        self.db = DB(str(self.db_path))
        self.db.init()

    def tearDown(self) -> None:
        self.db.close()
        self.tmpdir.cleanup()

    def _seed_signal(self) -> tuple[int, int, int]:
        return seed_signal(self.db, url="https://example.com/t1", raw_hash="h1")

    def test_position_lifecycle_pending_open_closed(self) -> None:
        _, _, signal_id = self._seed_signal()

        self.db.begin()
        try:
            position_id = self.db.create_position("005930", signal_id, qty=1.0, autocommit=False)
            self.db.set_position_open(position_id, avg_entry_price=83500.0, opened_value=83500.0, autocommit=False)
            self.db.set_position_closed(position_id, reason_code="TIME_EXIT", autocommit=False)
            self.db.commit()
        except Exception:
            self.db.rollback()
            raise

        cur = self.db.conn.cursor()
        cur.execute("select status, avg_entry_price, exit_reason_code from positions where position_id=?", (position_id,))
        row = cur.fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row[0], "CLOSED")
        self.assertEqual(float(row[1]), 83500.0)
        self.assertEqual(row[2], "TIME_EXIT")

    def test_idempotency_collision_returns_none(self) -> None:
        _, _, signal_id = self._seed_signal()
        position_id = self.db.create_position("005930", signal_id, qty=1.0)

        key = f"entry:{position_id}:1"
        first_id = self.db.insert_position_event(
            position_id=position_id,
            event_type="ENTRY",
            action="EXECUTED",
            reason_code="ENTRY_FILLED",
            detail_json=json.dumps({"ok": True}),
            idempotency_key=key,
        )
        second_id = self.db.insert_position_event(
            position_id=position_id,
            event_type="ENTRY",
            action="EXECUTED",
            reason_code="ENTRY_FILLED_DUP",
            detail_json=json.dumps({"dup": True}),
            idempotency_key=key,
        )

        self.assertIsNotNone(first_id)
        self.assertIsNone(second_id)


if __name__ == "__main__":
    unittest.main()
