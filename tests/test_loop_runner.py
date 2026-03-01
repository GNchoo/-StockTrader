import unittest
from unittest.mock import patch

from app.scheduler.loop_runner import run_exit_loop


class TestLoopRunner(unittest.TestCase):
    def test_run_exit_loop_single_tick_then_interrupt(self):
        calls = {"sleep": 0}

        def fake_sleep(_):
            calls["sleep"] += 1
            raise KeyboardInterrupt()

        with patch("app.scheduler.loop_runner.run_exit_cycle", return_value={"ok": 1}), patch(
            "app.scheduler.loop_runner.DB"
        ) as db_mock, patch("app.scheduler.loop_runner.time.sleep", side_effect=fake_sleep):
            with self.assertRaises(KeyboardInterrupt):
                run_exit_loop(interval_sec=1)

        self.assertEqual(calls["sleep"], 1)
        self.assertTrue(db_mock.called)


if __name__ == "__main__":
    unittest.main()
