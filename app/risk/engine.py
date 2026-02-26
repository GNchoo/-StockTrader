from dataclasses import dataclass


@dataclass
class RiskDecision:
    allowed: bool
    reason_code: str | None = None


class KillSwitch:
    def __init__(self) -> None:
        self.enabled = False

    def on(self) -> None:
        self.enabled = True

    def off(self) -> None:
        self.enabled = False


kill_switch = KillSwitch()


def can_trade(account_state: dict | None = None) -> RiskDecision:
    if kill_switch.enabled:
        return RiskDecision(False, "KILL_SWITCH_ON")

    if account_state is not None:
        trading_enabled = int(account_state.get("trading_enabled", 1))
        if trading_enabled != 1:
            return RiskDecision(False, "RISK_DISABLED")

    return RiskDecision(True)
