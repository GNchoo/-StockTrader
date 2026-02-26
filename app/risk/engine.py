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
    # account_state reserved for v1.2.x expansion (daily loss, position caps, cooldown, etc.)
    _ = account_state
    if kill_switch.enabled:
        return RiskDecision(False, "KILL_SWITCH_ON")
    return RiskDecision(True)
