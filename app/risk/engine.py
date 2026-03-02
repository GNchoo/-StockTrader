from dataclasses import dataclass
from datetime import datetime, timedelta

from app.config import settings


@dataclass
class RiskDecision:
    allowed: bool
    reason_code: str | None = None


@dataclass(frozen=True)
class RiskParams:
    max_loss_per_trade: float
    daily_loss_limit: float
    max_exposure_per_symbol: float
    max_concurrent_positions: int
    loss_streak_cooldown: int
    cooldown_minutes: int
    assumed_stop_loss_pct: float


class KillSwitch:
    def __init__(self) -> None:
        self.enabled = False

    def on(self) -> None:
        self.enabled = True

    def off(self) -> None:
        self.enabled = False


kill_switch = KillSwitch()


def _default_params() -> RiskParams:
    return RiskParams(
        max_loss_per_trade=max(0.0, float(settings.risk_max_loss_per_trade or 0.0)),
        daily_loss_limit=max(0.0, float(settings.risk_daily_loss_limit or 0.0)),
        max_exposure_per_symbol=max(0.0, float(settings.risk_max_exposure_per_symbol or 0.0)),
        max_concurrent_positions=max(1, int(settings.risk_max_concurrent_positions or 1)),
        loss_streak_cooldown=max(1, int(settings.risk_loss_streak_cooldown or 1)),
        cooldown_minutes=max(1, int(settings.risk_cooldown_minutes or 1)),
        assumed_stop_loss_pct=max(0.0001, float(settings.risk_assumed_stop_loss_pct or 0.015)),
    )


def _parse_ts(v: str | None) -> datetime | None:
    if not v:
        return None
    s = str(v).strip().replace("Z", "")
    try:
        return datetime.fromisoformat(s)
    except Exception:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            continue
    return None


def can_trade(
    account_state: dict | None = None,
    *,
    proposed_notional: float = 0.0,
    current_open_positions: int = 0,
    current_symbol_exposure: float = 0.0,
    now: datetime | None = None,
    params: RiskParams | None = None,
) -> RiskDecision:
    if kill_switch.enabled:
        return RiskDecision(False, "KILL_SWITCH_ON")

    p = params or _default_params()

    if account_state is not None:
        trading_enabled = int(account_state.get("trading_enabled", 1))
        if trading_enabled != 1:
            return RiskDecision(False, "RISK_DISABLED")

        if int(account_state.get("daily_loss_limit_hit", 0) or 0) == 1:
            return RiskDecision(False, "RISK_DAILY_LIMIT_HIT")

        daily_realized_pnl = float(account_state.get("daily_realized_pnl", 0.0) or 0.0)
        if p.daily_loss_limit > 0 and daily_realized_pnl <= -abs(p.daily_loss_limit):
            return RiskDecision(False, "RISK_DAILY_LIMIT")

        consecutive_losses = int(account_state.get("consecutive_losses", 0) or 0)
        if consecutive_losses >= p.loss_streak_cooldown:
            base_now = now or datetime.now()
            cooldown_until = _parse_ts(account_state.get("cooldown_until"))
            if cooldown_until is None:
                cooldown_until = base_now + timedelta(minutes=p.cooldown_minutes)
            if cooldown_until > base_now:
                return RiskDecision(False, "RISK_COOLDOWN")

    if current_open_positions >= p.max_concurrent_positions:
        return RiskDecision(False, "RISK_MAX_POSITIONS")

    projected_exposure = float(current_symbol_exposure or 0.0) + max(0.0, float(proposed_notional or 0.0))
    if p.max_exposure_per_symbol > 0 and projected_exposure > p.max_exposure_per_symbol:
        return RiskDecision(False, "RISK_MAX_EXPOSURE")

    estimated_loss = max(0.0, float(proposed_notional or 0.0)) * p.assumed_stop_loss_pct
    if p.max_loss_per_trade > 0 and estimated_loss > p.max_loss_per_trade:
        return RiskDecision(False, "RISK_MAX_LOSS_PER_TRADE")

    return RiskDecision(True)
