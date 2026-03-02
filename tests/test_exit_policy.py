import unittest

from app.execution.exit_policy import should_exit_on_opposite_signal, should_exit_on_time


class TestExitPolicy(unittest.TestCase):
    def test_opposite_signal_guard_self_buy(self):
        self.assertFalse(
            should_exit_on_opposite_signal(
                latest_signal_id=10,
                entry_signal_id=10,
                decision="BUY",
                score=10,
                threshold=70,
            )
        )

    def test_opposite_signal_low_score(self):
        self.assertTrue(
            should_exit_on_opposite_signal(
                latest_signal_id=11,
                entry_signal_id=10,
                decision="BUY",
                score=60,
                threshold=70,
            )
        )

    def test_time_exit(self):
        self.assertTrue(should_exit_on_time(hold_minutes=16, max_hold_min=15))
        self.assertFalse(should_exit_on_time(hold_minutes=14.9, max_hold_min=15))


if __name__ == "__main__":
    unittest.main()
