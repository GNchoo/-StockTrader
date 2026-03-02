from app.execution.runtime import build_broker, collect_current_prices

# backward-compatible test patch targets
_build_broker = build_broker
_collect_current_prices = collect_current_prices
from app.main import (
    sync_pending_entries,
    trigger_opposite_signal_exit_orders,
    trigger_trailing_stop_orders,
    trigger_time_exit_orders,
    sync_pending_exits,
)
from app.storage.db import DB


def run_exit_cycle(db: DB) -> dict[str, int]:
    """청산/동기화 사이클 1회 실행.

    반환값은 각 단계 처리 건수.
    """
    pol = db.get_exit_policy()
    broker = _build_broker()

    out = {
        "entry_sync": sync_pending_entries(db, broker=broker),
        "opposite_exit": trigger_opposite_signal_exit_orders(
            db,
            exit_score_threshold=float(pol.get("opposite_exit_score_threshold", 70.0)),
            broker=broker,
        ),
        "trailing_exit": 0,
        "time_exit": 0,
        "exit_sync": 0,
    }

    current_prices = _collect_current_prices(db, broker)
    out["trailing_exit"] = trigger_trailing_stop_orders(
        db,
        current_prices=current_prices,
        trailing_arm_pct=float(pol.get("trailing_arm_pct", 0.005)),
        trailing_gap_pct=float(pol.get("trailing_gap_pct", 0.003)),
        broker=broker,
    )
    out["time_exit"] = trigger_time_exit_orders(db, max_hold_min=int(pol.get("time_exit_min", 15)), broker=broker)
    out["exit_sync"] = sync_pending_exits(db, broker=broker)
    return out
