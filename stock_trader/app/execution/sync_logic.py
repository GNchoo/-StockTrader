import json
from datetime import datetime, timezone
from app.storage.db import DB
from app.execution.broker_base import ExecStatus

def sync_entry_order_once(
    db: DB,
    broker,
    *,
    position_id: int,
    signal_id: int,
    order_id: int,
    ticker: str,
    qty: float,
    broker_order_id: str | None,
    log_and_notify,
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
            if filled_qty >= float(qty) - 1e-9:
                db.update_order_filled(order_id, float(status.avg_price or 0), filled_qty, broker_order_id, False)
                db.commit()
                return "FILLED"
            db.commit()
            return "PARTIAL_FILLED"

        db.update_order_status(order_id, status.status, broker_order_id, False)
        db.commit()
        return "PENDING"
    except Exception:
        db.rollback()
        raise

def sync_exit_order_once(
    db: DB,
    broker,
    *,
    position_id: int,
    signal_id: int,
    order_id: int,
    ticker: str,
    order_qty: float,
    broker_order_id: str | None,
    log_and_notify,
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
                db.update_order_partial(order_id, float(status.avg_price or 0.0), filled_qty, broker_order_id, False)
            else:
                db.update_order_filled(order_id, float(status.avg_price or 0.0), filled_qty or order_qty, broker_order_id, False)

            cum_exit = prev_exited + min(filled_qty, order_qty)
            exit_px = float(status.avg_price or 0.0)
            pnl_delta = (exit_px - avg_entry_price) * min(filled_qty, order_qty)
            db.apply_realized_pnl(datetime.now().date().isoformat(), pnl_delta, False)

            if cum_exit >= total_qty - 1e-9:
                db.set_position_closed(position_id, "FULL_EXIT_FILLED", total_qty, False)
                db.insert_position_event(position_id, "FULL_EXIT", "EXECUTED", "FULL_EXIT_FILLED", 
                                       json.dumps({"signal_id": signal_id, "order_id": order_id, "pnl": pnl_delta}),
                                       f"exit-fill:{position_id}:{order_id}", False)
                db.commit()
                return "FILLED"

            db.set_position_partial_exit(position_id, cum_exit, False)
            db.insert_position_event(position_id, "PARTIAL_EXIT", "EXECUTED", "PARTIAL_EXIT_FILLED",
                                   json.dumps({"signal_id": signal_id, "pnl": pnl_delta}),
                                   f"partial-exit:{position_id}:{order_id}:{int(filled_qty*10000)}", False)
            db.commit()
            return "PENDING"

        if status.status in {"REJECTED", "CANCELLED", "EXPIRED"}:
            db.update_order_status(order_id, status.status, broker_order_id, False)
            db.insert_position_event(position_id, "BLOCK", "BLOCKED", status.reason_code or status.status,
                                   json.dumps({"order_id": order_id}), f"exit-block:{position_id}:{order_id}", False)
            db.commit()
            return "BLOCKED"

        db.update_order_status(order_id, status.status, broker_order_id, False)
        db.commit()
        return "PENDING"
    except Exception:
        db.rollback()
        raise
