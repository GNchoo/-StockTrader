def should_exit_on_opposite_signal(
    *,
    latest_signal_id: int,
    entry_signal_id: int,
    decision: str,
    score: float,
    threshold: float,
) -> bool:
    d = (decision or "").upper()

    # 자기 자신의 매수 진입 신호는 즉시 청산 금지
    if latest_signal_id == entry_signal_id and d == "BUY":
        return False

    return d in {"IGNORE", "BLOCK"} or float(score) < float(threshold)


def should_exit_on_time(*, hold_minutes: float, max_hold_min: float) -> bool:
    return float(hold_minutes) >= float(max_hold_min)
