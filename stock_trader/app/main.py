import json
from datetime import datetime, timezone
from typing import TypedDict, Literal
from app.execution.sync_logic import sync_entry_order_once, sync_exit_order_once

from app.execution.broker_base import OrderRequest
from app.execution.paper_broker import PaperBroker  # backward-compatible test patch target
from app.execution.runtime import build_broker, resolve_expected_price, collect_current_prices
from app.risk.engine import can_trade
from app.storage.db import DB
from app.monitor.telegram_logger import log_and_notify
from app.config import settings
from app.common.timeutil import parse_utc_ts


class SignalBundle(TypedDict):
    signal_id: int
    ticker: str


ExecStatus = Literal["FILLED", "PENDING", "BLOCKED"]


def _build_broker():
    return build_broker()


def _resolve_expected_price(broker, ticker: str) -> float | None:
    return resolve_expected_price(broker, ticker)


def ingest_and_create_signal(db: DB) -> SignalBundle | None:
    from app.signal.ingest import ingest_and_create_signal as _ingest_impl
    return _ingest_impl(db, log_and_notify)


def _sync_entry_order_once(
    db: DB,
    broker,
    *,
    position_id: int,
    signal_id: int,
    order_id: int,
    ticker: str,
    qty: float,
    broker_order_id: str | None,
) -> ExecStatus:
    return sync_entry_order_once(
        db, broker, position_id=position_id, signal_id=signal_id,
        order_id=order_id, ticker=ticker, qty=qty, broker_order_id=broker_order_id,
        log_and_notify=log_and_notify
    )
                db.set_position_open(
                    position_id=position_id,
                    avg_entry_price=float(status.avg_price or 0.0),
                    opened_value=float(status.avg_price or 0.0) * qty,
                    autocommit=False,
                )
                db.insert_position_event(
                    position_id=position_id,
                    event_type="ENTRY",
                    action="EXECUTED",
                    reason_code="ENTRY_FILLED",
                    detail_json=json.dumps(
                        {
                            "signal_id": signal_id,
                            "order_id": order_id,
                            "filled_qty": filled_qty,
                            "avg_price": status.avg_price,
                        }
                    ),
                    idempotency_key=f"entry:{position_id}:{order_id}",
                    autocommit=False,
                )
                db.commit()
                log_and_notify(
                    f"ORDER_FILLED:{ticker}@{status.avg_price} "
                    f"(signal_id={signal_id}, position_id={position_id})"
                )
                return "FILLED"

            db.commit()
            return "PENDING"

        # SENT/NEW
        db.update_order_status(
            order_id=order_id,
            status=status.status,
            broker_order_id=broker_order_id,
            autocommit=False,
        )
        db.commit()
        return "PENDING"
    except Exception:
        db.rollback()
        raise


def _parse_sqlite_ts(ts: str | None) -> datetime | None:
    return parse_utc_ts(ts)


def sync_pending_entries(db: DB, limit: int = 100, broker=None) -> int:
    from app.execution.sync import sync_pending_entries_impl
    return sync_pending_entries_impl(
        db,
        limit=limit,
        broker=broker,
        _build_broker=_build_broker,
        _resolve_expected_price=_resolve_expected_price,
        _sync_entry_order_once=_sync_entry_order_once,
        log_and_notify=log_and_notify,
    )


def _sync_exit_order_once(
    db: DB,
    broker,
    *,
    position_id: int,
    signal_id: int,
    order_id: int,
    ticker: str,
    order_qty: float,
    broker_order_id: str | None,
) -> ExecStatus:
    return sync_exit_order_once(
        db, broker, position_id=position_id, signal_id=signal_id,
        order_id=order_id, ticker=ticker, order_qty=order_qty,
        broker_order_id=broker_order_id, log_and_notify=log_and_notify
    )


def sync_pending_exits(db: DB, limit: int = 100, broker=None) -> int:
    from app.execution.sync import sync_pending_exits_impl
    return sync_pending_exits_impl(
        db,
        limit=limit,
        broker=broker,
        _build_broker=_build_broker,
        _sync_exit_order_once=_sync_exit_order_once,
    )


def trigger_trailing_stop_orders(
    db: DB,
    current_prices: dict[str, float] | None = None,
    *,
    trailing_arm_pct: float = 0.005,
    trailing_gap_pct: float = 0.003,
    limit: int = 100,
    broker=None,
) -> int:
    from app.execution.triggers import trigger_trailing_stop_orders_impl
    return trigger_trailing_stop_orders_impl(
        db,
        current_prices=current_prices,
        trailing_arm_pct=trailing_arm_pct,
        trailing_gap_pct=trailing_gap_pct,
        limit=limit,
        broker=broker,
        _build_broker=_build_broker,
        _sync_exit_order_once=_sync_exit_order_once,
        log_and_notify=log_and_notify,
    )


def trigger_opposite_signal_exit_orders(
    db: DB,
    *,
    exit_score_threshold: float = 70.0,
    limit: int = 100,
    broker=None,
) -> int:
    from app.execution.triggers import trigger_opposite_signal_exit_orders_impl
    return trigger_opposite_signal_exit_orders_impl(
        db,
        exit_score_threshold=exit_score_threshold,
        limit=limit,
        broker=broker,
        _build_broker=_build_broker,
        _resolve_expected_price=_resolve_expected_price,
        _sync_exit_order_once=_sync_exit_order_once,
        log_and_notify=log_and_notify,
    )


def trigger_time_exit_orders(db: DB, max_hold_min: int = 15, limit: int = 100, broker=None) -> int:
    from app.execution.triggers import trigger_time_exit_orders_impl
    return trigger_time_exit_orders_impl(
        db,
        max_hold_min=max_hold_min,
        limit=limit,
        broker=broker,
        _build_broker=_build_broker,
        _resolve_expected_price=_resolve_expected_price,
        _parse_sqlite_ts=_parse_sqlite_ts,
        _sync_exit_order_once=_sync_exit_order_once,
        log_and_notify=log_and_notify,
    )


def execute_signal(
    db: DB,
    signal_id: int,
    ticker: str,
    qty: float = 1.0,
    demo_auto_close: bool | None = None,
) -> ExecStatus:
    from app.execution.entry import execute_signal_impl
    return execute_signal_impl(
        db,
        signal_id,
        ticker,
        qty=qty,
        demo_auto_close=demo_auto_close,
        _build_broker=_build_broker,
        _resolve_expected_price=_resolve_expected_price,
        _sync_entry_order_once=_sync_entry_order_once,
        log_and_notify=log_and_notify,
        settings=settings,
    )


def _collect_current_prices(db: DB, broker, limit: int = 100) -> dict[str, float]:
    return collect_current_prices(db, broker, limit=limit)


def run_happy_path_demo() -> None:
    # local import to avoid circular dependency (scheduler -> main)
    from app.scheduler.exit_runner import run_exit_cycle

    with DB("stock_trader.db") as db:
        db.init()
        run_exit_cycle(db)
        bundle = ingest_and_create_signal(db)
        if not bundle:
            return
        execute_signal(db, bundle["signal_id"], bundle["ticker"])


if __name__ == "__main__":
    run_happy_path_demo()
