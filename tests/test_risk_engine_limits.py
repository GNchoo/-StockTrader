import unittest
from datetime import datetime, timedelta

from app.risk.engine import RiskParams, can_trade


class TestRiskEngineLimits(unittest.TestCase):
    def setUp(self) -> None:
        self.params = RiskParams(
            max_loss_per_trade=100.0,
            daily_loss_limit=500.0,
            max_exposure_per_symbol=1000.0,
            max_concurrent_positions=2,
            loss_streak_cooldown=3,
            cooldown_minutes=60,
            assumed_stop_loss_pct=0.1,
        )

    def test_blocks_on_daily_loss_limit(self) -> None:
        decision = can_trade(
            account_state={"trading_enabled": 1, "daily_realized_pnl": -600.0},
            params=self.params,
        )
        self.assertFalse(decision.allowed)
        self.assertEqual(decision.reason_code, "RISK_DAILY_LIMIT")

    def test_blocks_on_max_positions(self) -> None:
        decision = can_trade(
            account_state={"trading_enabled": 1},
            current_open_positions=2,
            params=self.params,
        )
        self.assertFalse(decision.allowed)
        self.assertEqual(decision.reason_code, "RISK_MAX_POSITIONS")

    def test_blocks_on_symbol_exposure(self) -> None:
        decision = can_trade(
            account_state={"trading_enabled": 1},
            proposed_notional=400.0,
            current_symbol_exposure=700.0,
            params=self.params,
        )
        self.assertFalse(decision.allowed)
        self.assertEqual(decision.reason_code, "RISK_MAX_EXPOSURE")

    def test_blocks_on_max_loss_per_trade(self) -> None:
        params = RiskParams(
            max_loss_per_trade=100.0,
            daily_loss_limit=500.0,
            max_exposure_per_symbol=5000.0,
            max_concurrent_positions=2,
            loss_streak_cooldown=3,
            cooldown_minutes=60,
            assumed_stop_loss_pct=0.1,
        )
        decision = can_trade(
            account_state={"trading_enabled": 1},
            proposed_notional=1200.0,
            params=params,
        )
        self.assertFalse(decision.allowed)
        self.assertEqual(decision.reason_code, "RISK_MAX_LOSS_PER_TRADE")

    def test_blocks_on_loss_streak_cooldown(self) -> None:
        now = datetime(2026, 3, 2, 9, 0, 0)
        decision = can_trade(
            account_state={
                "trading_enabled": 1,
                "consecutive_losses": 3,
                "cooldown_until": (now + timedelta(minutes=30)).isoformat(sep=" "),
            },
            now=now,
            params=self.params,
        )
        self.assertFalse(decision.allowed)
        self.assertEqual(decision.reason_code, "RISK_COOLDOWN")


if __name__ == "__main__":
    unittest.main()
