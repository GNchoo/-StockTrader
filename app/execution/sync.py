import json
from datetime import datetime, timezone
from typing import Callable

from app.execution.broker_base import OrderRequest
from app.storage.db import DB
from app.common.timeutil import parse_utc_ts


def sync_pending_entries_impl(
    db: DB,
    *,
    limit: int = 100,
    broker=None,
    _build_broker: Callable,
    _resolve_expected_price: Callable,
    _sync_entry_order_once: Callable,
    log_and_notify: Callable,
) -> int:
    broker = broker or _build_broker()
    rows = db.get_pending_entry_orders(limit=limit)
    retry_policy = db.get_retry_policy()
    max_attempts = int(retry_policy.get("max_attempts_per_signal", 2) or 2)
    min_retry_sec = int(retry_policy.get("min_retry_interval_sec", 30) or 30)

    changed = 0
    now = datetime.now(timezone.utc)

    for row in rows:
        position_id = int(row["position_id"])
        signal_id = int(row["signal_id"])
        order_id = int(row["order_id"])
        ticker = str(row["ticker"])
        qty = float(row["qty"])
        broker_order_id = row.get("broker_order_id")
        attempt_no = int(row.get("attempt_no") or 1)

        prev_order_status = row.get("status")
        prev_pos_status = row.get("position_status")

        rs = _sync_entry_order_once(
            db,
            broker,
            position_id=position_id,
            signal_id=signal_id,
            order_id=order_id,
            ticker=ticker,
            qty=qty,
            broker_order_id=broker_order_id,
        )

        if rs == "PENDING":
            current_status = db.get_order_status(order_id) or str(row.get("status") or "")
            if current_status == "PARTIAL_FILLED":
                continue

            sent_at = parse_utc_ts(row.get("sent_at"))
            age_sec = (now - sent_at).total_seconds() if sent_at else 10**9

            if age_sec >= min_retry_sec:
                if attempt_no >= max_attempts:
                    db.begin()
                    try:
                        db.update_order_status(order_id=order_id, status="EXPIRED", broker_order_id=broker_order_id, autocommit=False)
                        db.set_position_cancelled(position_id=position_id, reason_code="RETRY_EXHAUSTED", autocommit=False)
                        db.insert_position_event(
                            position_id=position_id,
                            event_type="BLOCK",
                            action="BLOCKED",
                            reason_code="RETRY_EXHAUSTED",
                            detail_json=json.dumps({"signal_id": signal_id, "order_id": order_id, "attempt_no": attempt_no}),
                            idempotency_key=f"block-retry:{position_id}:{order_id}",
                            autocommit=False,
                        )
                        db.commit()
                        log_and_notify(f"BLOCKED:RETRY_EXHAUSTED signal_id={signal_id} order_id={order_id}")
                        changed += 1
                    except Exception:
                        db.rollback()
                        raise
                else:
                    expected_price = _resolve_expected_price(broker, ticker)
                    if expected_price is None:
                        log_and_notify(
                            f"RETRY_SKIPPED:NO_PRICE ticker={ticker} signal_id={signal_id} order_id={order_id}"
                        )
                        continue

                    new_result = broker.send_order(
                        OrderRequest(
                            signal_id=signal_id,
                            ticker=ticker,
                            side="BUY",
                            qty=qty,
                            expected_price=expected_price,
                        )
                    )
                    db.begin()
                    try:
                        db.update_order_status(order_id=order_id, status="EXPIRED", broker_order_id=broker_order_id, autocommit=False)
                        new_order_id = db.insert_order(
                            position_id=position_id,
                            signal_id=signal_id,
                            ticker=ticker,
                            side="BUY",
                            qty=qty,
                            order_type="MARKET",
                            status="SENT",
                            price=None,
                            attempt_no=attempt_no + 1,
                            autocommit=False,
                        )

                        if new_result.status in {"SENT", "NEW", "PARTIAL_FILLED"}:
                            db.update_order_status(
                                order_id=new_order_id,
                                status=new_result.status,
                                broker_order_id=new_result.broker_order_id,
                                autocommit=False,
                            )
                            db.commit()
                            log_and_notify(
                                f"RETRY_SUBMITTED:{ticker} "
                                f"(signal_id={signal_id}, prev_order={order_id}, new_order={new_order_id}, attempt={attempt_no+1})"
                            )
                            changed += 1
                        elif new_result.status == "FILLED":
                            db.update_order_filled(
                                order_id=new_order_id,
                                price=new_result.avg_price,
                                broker_order_id=new_result.broker_order_id,
                                autocommit=False,
                            )
                            db.set_position_open(
                                position_id=position_id,
                                avg_entry_price=new_result.avg_price,
                                opened_value=new_result.avg_price * qty,
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
                                        "order_id": new_order_id,
                                        "filled_qty": new_result.filled_qty,
                                        "avg_price": new_result.avg_price,
                                    }
                                ),
                                idempotency_key=f"entry:{position_id}:{new_order_id}",
                                autocommit=False,
                            )
                            db.commit()
                            log_and_notify(
                                f"ORDER_FILLED:{ticker}@{new_result.avg_price} "
                                f"(signal_id={signal_id}, position_id={position_id}, order_id={new_order_id})"
                            )
                            changed += 1
                        else:
                            reason = new_result.reason_code or "ORDER_REJECTED"
                            prev_reason = db.get_latest_block_reason(position_id)
                            if prev_reason and prev_reason == reason:
                                reason = "RETRY_BLOCKED_SAME_CONDITION"

                            db.update_order_status(
                                order_id=new_order_id,
                                status=new_result.status,
                                broker_order_id=new_result.broker_order_id,
                                autocommit=False,
                            )
                            db.set_position_cancelled(position_id=position_id, reason_code=reason, autocommit=False)
                            db.insert_position_event(
                                position_id=position_id,
                                event_type="BLOCK",
                                action="BLOCKED",
                                reason_code=reason,
                                detail_json=json.dumps({"signal_id": signal_id, "order_id": new_order_id, "original_reason": new_result.reason_code}),
                                idempotency_key=f"block:{position_id}:{new_order_id}",
                                autocommit=False,
                            )
                            db.commit()
                            log_and_notify(f"BLOCKED:{reason}")
                            changed += 1
                    except Exception:
                        db.rollback()
                        raise

        if rs != "PENDING" or prev_order_status != "SENT" or prev_pos_status != "PENDING_ENTRY":
            changed += 1
    return changed


def sync_pending_exits_impl(
    db: DB,
    *,
    limit: int = 100,
    broker=None,
    _build_broker: Callable,
    _sync_exit_order_once: Callable,
) -> int:
    broker = broker or _build_broker()
    rows = db.get_pending_exit_orders(limit=limit)
    changed = 0
    for row in rows:
        rs = _sync_exit_order_once(
            db,
            broker,
            position_id=int(row["position_id"]),
            signal_id=int(row["signal_id"]),
            order_id=int(row["order_id"]),
            ticker=str(row["ticker"]),
            order_qty=float(row["qty"]),
            broker_order_id=row.get("broker_order_id"),
        )
        if rs != "PENDING":
            changed += 1
    return changed
