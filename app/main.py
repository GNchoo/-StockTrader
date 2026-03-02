import json
from datetime import datetime, timezone
from typing import TypedDict, Literal

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
    if not broker_order_id:
        return "PENDING"

    status = broker.inquire_order(broker_order_id=broker_order_id, ticker=ticker, side="BUY")
    if status is None:
        return "PENDING"

    db.begin()
    try:
        if status.status == "FILLED":
            db.update_order_filled(
                order_id=order_id,
                price=float(status.avg_price or 0.0),
                filled_qty=float(status.filled_qty or qty),
                broker_order_id=broker_order_id,
                autocommit=False,
            )
            db.set_position_open(
                position_id=position_id,
                avg_entry_price=float(status.avg_price or 0.0),
                opened_value=float(status.avg_price or 0.0) * qty,
                autocommit=False,
            )
            entry_key = f"entry:{position_id}:{order_id}"
            db.insert_position_event(
                position_id=position_id,
                event_type="ENTRY",
                action="EXECUTED",
                reason_code="ENTRY_FILLED",
                detail_json=json.dumps(
                    {
                        "signal_id": signal_id,
                        "order_id": order_id,
                        "filled_qty": status.filled_qty,
                        "avg_price": status.avg_price,
                    }
                ),
                idempotency_key=entry_key,
                autocommit=False,
            )
            db.commit()
            log_and_notify(
                f"ORDER_FILLED:{ticker}@{status.avg_price} "
                f"(signal_id={signal_id}, position_id={position_id})"
            )
            return "FILLED"

        if status.status in {"REJECTED", "CANCELLED", "EXPIRED"}:
            db.update_order_status(
                order_id=order_id,
                status=status.status,
                broker_order_id=broker_order_id,
                autocommit=False,
            )
            db.set_position_cancelled(position_id=position_id, reason_code=status.reason_code or status.status, autocommit=False)
            db.insert_position_event(
                position_id=position_id,
                event_type="BLOCK",
                action="BLOCKED",
                reason_code=status.reason_code or status.status,
                detail_json=json.dumps({"signal_id": signal_id, "order_id": order_id}),
                idempotency_key=f"block:{position_id}:{order_id}",
                autocommit=False,
            )
            db.commit()
            log_and_notify(f"BLOCKED:{status.reason_code or status.status}")
            return "BLOCKED"

        # PARTIAL_FILLED는 평균가를 기록하고 유지
        if status.status == "PARTIAL_FILLED":
            filled_qty = float(status.filled_qty or 0.0)
            db.update_order_partial(
                order_id=order_id,
                price=float(status.avg_price or 0.0),
                filled_qty=filled_qty,
                broker_order_id=broker_order_id,
                autocommit=False,
            )
            db.insert_position_event(
                position_id=position_id,
                event_type="ADD",
                action="EXECUTED",
                reason_code="PARTIAL_FILLED",
                detail_json=json.dumps(
                    {
                        "signal_id": signal_id,
                        "order_id": order_id,
                        "filled_qty": status.filled_qty,
                        "avg_price": status.avg_price,
                    }
                ),
                idempotency_key=f"partial:{position_id}:{order_id}:{int(float(status.filled_qty or 0)*10000)}",
                autocommit=False,
            )

            # 누적 체결량이 주문수량에 도달하면 OPEN 전환
            if filled_qty >= float(qty) - 1e-9:
                db.update_order_filled(
                    order_id=order_id,
                    price=float(status.avg_price or 0.0),
                    filled_qty=filled_qty,
                    broker_order_id=broker_order_id,
                    autocommit=False,
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
    if not broker_order_id:
        return "PENDING"

    status = broker.inquire_order(broker_order_id=broker_order_id, ticker=ticker, side="SELL")
    if status is None:
        return "PENDING"

    db.begin()
    try:
        pos = db.conn.execute("select qty, exited_qty, avg_entry_price from positions where position_id=?", (position_id,)).fetchone()
        if not pos:
            db.rollback()
            return "BLOCKED"
        total_qty = float(pos[0] or 0.0)
        prev_exited = float(pos[1] or 0.0)
        avg_entry_price = float(pos[2] or 0.0)

        if status.status in {"PARTIAL_FILLED", "FILLED"}:
            filled_qty = float(status.filled_qty or 0.0)
            if status.status == "PARTIAL_FILLED":
                db.update_order_partial(
                    order_id=order_id,
                    price=float(status.avg_price or 0.0),
                    filled_qty=filled_qty,
                    broker_order_id=broker_order_id,
                    autocommit=False,
                )
            else:
                db.update_order_filled(
                    order_id=order_id,
                    price=float(status.avg_price or 0.0),
                    filled_qty=filled_qty or order_qty,
                    broker_order_id=broker_order_id,
                    autocommit=False,
                )

            cum_exit = prev_exited + min(filled_qty, order_qty)
            exit_px = float(status.avg_price or 0.0)
            pnl_delta = (exit_px - avg_entry_price) * min(filled_qty, order_qty)
            db.apply_realized_pnl(datetime.now().date().isoformat(), pnl_delta, autocommit=False)

            if cum_exit >= total_qty - 1e-9:
                db.set_position_closed(position_id=position_id, reason_code="FULL_EXIT_FILLED", exited_qty=total_qty, autocommit=False)
                db.insert_position_event(
                    position_id=position_id,
                    event_type="FULL_EXIT",
                    action="EXECUTED",
                    reason_code="FULL_EXIT_FILLED",
                    detail_json=json.dumps({"signal_id": signal_id, "order_id": order_id, "filled_qty": filled_qty, "avg_price": status.avg_price, "pnl_delta": pnl_delta}),
                    idempotency_key=f"exit-fill:{position_id}:{order_id}",
                    autocommit=False,
                )
                db.commit()
                return "FILLED"

            db.set_position_partial_exit(position_id=position_id, exited_qty=cum_exit, autocommit=False)
            db.insert_position_event(
                position_id=position_id,
                event_type="PARTIAL_EXIT",
                action="EXECUTED",
                reason_code="PARTIAL_EXIT_FILLED",
                detail_json=json.dumps({"signal_id": signal_id, "order_id": order_id, "filled_qty": filled_qty, "avg_price": status.avg_price, "pnl_delta": pnl_delta}),
                idempotency_key=f"partial-exit:{position_id}:{order_id}:{int(filled_qty*10000)}",
                autocommit=False,
            )
            db.commit()
            return "PENDING"

        if status.status in {"REJECTED", "CANCELLED", "EXPIRED"}:
            db.update_order_status(order_id=order_id, status=status.status, broker_order_id=broker_order_id, autocommit=False)
            db.insert_position_event(
                position_id=position_id,
                event_type="BLOCK",
                action="BLOCKED",
                reason_code=status.reason_code or status.status,
                detail_json=json.dumps({"signal_id": signal_id, "order_id": order_id}),
                idempotency_key=f"exit-block:{position_id}:{order_id}",
                autocommit=False,
            )
            db.commit()
            return "BLOCKED"

        db.update_order_status(order_id=order_id, status=status.status, broker_order_id=broker_order_id, autocommit=False)
        db.commit()
        return "PENDING"
    except Exception:
        db.rollback()
        raise


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
