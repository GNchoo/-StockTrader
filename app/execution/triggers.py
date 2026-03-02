import json
from datetime import datetime, timezone
from typing import Callable

from app.execution.broker_base import OrderRequest
from app.execution.exit_policy import should_exit_on_opposite_signal, should_exit_on_time
from app.storage.db import DB


def trigger_trailing_stop_orders_impl(
    db: DB,
    current_prices: dict[str, float] | None = None,
    *,
    trailing_arm_pct: float = 0.005,
    trailing_gap_pct: float = 0.003,
    limit: int = 100,
    broker=None,
    _build_broker: Callable,
    _sync_exit_order_once: Callable,
    log_and_notify: Callable,
) -> int:
    if not current_prices:
        return 0

    broker = broker or _build_broker()
    created = 0

    for p in db.get_positions_for_exit_scan(limit=limit):
        if int(p.get("pending_sell_cnt") or 0) > 0:
            continue

        ticker = str(p["ticker"])
        cur_price = float(current_prices.get(ticker) or 0.0)
        if cur_price <= 0:
            continue

        position_id = int(p["position_id"])
        signal_id = int(p.get("signal_id") or 0)
        total_qty = float(p.get("qty") or 0.0)
        exited_qty = float(p.get("exited_qty") or 0.0)
        remain_qty = max(0.0, total_qty - exited_qty)
        if remain_qty <= 0:
            continue

        entry = float(p.get("avg_entry_price") or 0.0)
        if entry <= 0:
            continue

        db.update_position_high_watermark(position_id, cur_price)
        high = float(db.get_position_high_watermark(position_id) or cur_price)

        pnl_from_entry = (cur_price - entry) / max(entry, 1e-9)
        dd_from_high = (high - cur_price) / max(high, 1e-9)

        if pnl_from_entry < trailing_arm_pct:
            continue
        if dd_from_high < trailing_gap_pct:
            continue

        send = broker.send_order(
            OrderRequest(
                signal_id=signal_id,
                ticker=ticker,
                side="SELL",
                qty=remain_qty,
                expected_price=cur_price,
            )
        )

        db.begin()
        try:
            order_id = db.insert_order(
                position_id=position_id,
                signal_id=signal_id,
                ticker=ticker,
                side="SELL",
                qty=remain_qty,
                order_type="MARKET",
                status="SENT",
                price=None,
                autocommit=False,
            )

            if send.status in {"SENT", "NEW", "PARTIAL_FILLED"}:
                db.update_order_status(order_id=order_id, status=send.status, broker_order_id=send.broker_order_id, autocommit=False)
                db.commit()
                log_and_notify(
                    f"EXIT_ORDER_SENT:{ticker} (position_id={position_id}, order_id={order_id}, reason=TRAILING_STOP, dd={dd_from_high:.4f})"
                )
                _sync_exit_order_once(
                    db,
                    broker,
                    position_id=position_id,
                    signal_id=signal_id,
                    order_id=order_id,
                    ticker=ticker,
                    order_qty=remain_qty,
                    broker_order_id=send.broker_order_id,
                )
                created += 1
                continue

            if send.status == "FILLED":
                db.update_order_filled(
                    order_id=order_id,
                    price=float(send.avg_price or cur_price),
                    filled_qty=float(send.filled_qty or remain_qty),
                    broker_order_id=send.broker_order_id,
                    autocommit=False,
                )
                db.set_position_closed(position_id=position_id, reason_code="TRAILING_STOP", exited_qty=total_qty, autocommit=False)
                db.insert_position_event(
                    position_id=position_id,
                    event_type="FULL_EXIT",
                    action="EXECUTED",
                    reason_code="TRAILING_STOP",
                    detail_json=json.dumps({"signal_id": signal_id, "order_id": order_id, "filled_qty": send.filled_qty, "avg_price": send.avg_price}),
                    idempotency_key=f"trail-exit:{position_id}:{order_id}",
                    autocommit=False,
                )
                db.commit()
                created += 1
                continue

            db.update_order_status(order_id=order_id, status=send.status, broker_order_id=send.broker_order_id, autocommit=False)
            db.insert_position_event(
                position_id=position_id,
                event_type="BLOCK",
                action="BLOCKED",
                reason_code=send.reason_code or "EXIT_ORDER_REJECTED",
                detail_json=json.dumps({"signal_id": signal_id, "order_id": order_id}),
                idempotency_key=f"trail-block:{position_id}:{order_id}",
                autocommit=False,
            )
            db.commit()
            created += 1
        except Exception:
            db.rollback()
            raise

    return created


def trigger_opposite_signal_exit_orders_impl(
    db: DB,
    *,
    exit_score_threshold: float = 70.0,
    limit: int = 100,
    broker=None,
    _build_broker: Callable,
    _resolve_expected_price: Callable,
    _sync_exit_order_once: Callable,
    log_and_notify: Callable,
) -> int:
    broker = broker or _build_broker()
    created = 0

    for p in db.get_positions_for_exit_scan(limit=limit):
        if int(p.get("pending_sell_cnt") or 0) > 0:
            continue

        ticker = str(p["ticker"])
        sig = db.get_latest_signal_for_ticker(ticker)
        if not sig:
            continue

        decision = str(sig.get("decision") or "").upper()
        score = float(sig.get("total_score") or 0.0)

        position_id = int(p["position_id"])
        signal_id = int(p.get("signal_id") or 0)
        latest_signal_id = int(sig.get("id") or 0)
        should_exit = should_exit_on_opposite_signal(
            latest_signal_id=latest_signal_id,
            entry_signal_id=signal_id,
            decision=decision,
            score=score,
            threshold=exit_score_threshold,
        )
        if not should_exit:
            continue
        total_qty = float(p.get("qty") or 0.0)
        exited_qty = float(p.get("exited_qty") or 0.0)
        remain_qty = max(0.0, total_qty - exited_qty)
        if remain_qty <= 0:
            continue

        expected_price = float(p.get("avg_entry_price") or 0.0)
        if expected_price <= 0:
            expected_price = _resolve_expected_price(broker, ticker) or 0.0
        if expected_price <= 0:
            log_and_notify(f"EXIT_SKIPPED:NO_PRICE ticker={ticker} position_id={position_id} reason=OPPOSITE_SIGNAL")
            continue

        send = broker.send_order(
            OrderRequest(
                signal_id=signal_id,
                ticker=ticker,
                side="SELL",
                qty=remain_qty,
                expected_price=expected_price,
            )
        )

        db.begin()
        try:
            order_id = db.insert_order(
                position_id=position_id,
                signal_id=signal_id,
                ticker=ticker,
                side="SELL",
                qty=remain_qty,
                order_type="MARKET",
                status="SENT",
                price=None,
                autocommit=False,
            )

            if send.status in {"SENT", "NEW", "PARTIAL_FILLED"}:
                db.update_order_status(order_id=order_id, status=send.status, broker_order_id=send.broker_order_id, autocommit=False)
                db.commit()
                log_and_notify(
                    f"EXIT_ORDER_SENT:{ticker} (position_id={position_id}, order_id={order_id}, reason=OPPOSITE_SIGNAL, decision={decision}, score={score:.1f})"
                )
                _sync_exit_order_once(
                    db,
                    broker,
                    position_id=position_id,
                    signal_id=signal_id,
                    order_id=order_id,
                    ticker=ticker,
                    order_qty=remain_qty,
                    broker_order_id=send.broker_order_id,
                )
                created += 1
                continue

            if send.status == "FILLED":
                db.update_order_filled(
                    order_id=order_id,
                    price=float(send.avg_price or 0.0),
                    filled_qty=float(send.filled_qty or remain_qty),
                    broker_order_id=send.broker_order_id,
                    autocommit=False,
                )
                db.set_position_closed(position_id=position_id, reason_code="OPPOSITE_SIGNAL", exited_qty=total_qty, autocommit=False)
                db.insert_position_event(
                    position_id=position_id,
                    event_type="FULL_EXIT",
                    action="EXECUTED",
                    reason_code="OPPOSITE_SIGNAL",
                    detail_json=json.dumps({"signal_id": signal_id, "order_id": order_id, "decision": decision, "score": score}),
                    idempotency_key=f"oppo-exit:{position_id}:{order_id}",
                    autocommit=False,
                )
                db.commit()
                created += 1
                continue

            db.update_order_status(order_id=order_id, status=send.status, broker_order_id=send.broker_order_id, autocommit=False)
            db.insert_position_event(
                position_id=position_id,
                event_type="BLOCK",
                action="BLOCKED",
                reason_code=send.reason_code or "EXIT_ORDER_REJECTED",
                detail_json=json.dumps({"signal_id": signal_id, "order_id": order_id}),
                idempotency_key=f"oppo-block:{position_id}:{order_id}",
                autocommit=False,
            )
            db.commit()
            created += 1
        except Exception:
            db.rollback()
            raise

    return created


def trigger_time_exit_orders_impl(
    db: DB,
    max_hold_min: int = 15,
    limit: int = 100,
    broker=None,
    *,
    _build_broker: Callable,
    _resolve_expected_price: Callable,
    _parse_sqlite_ts: Callable,
    _sync_exit_order_once: Callable,
    log_and_notify: Callable,
) -> int:
    broker = broker or _build_broker()
    now = datetime.now(timezone.utc)
    created = 0

    for p in db.get_positions_for_exit_scan(limit=limit):
        if int(p.get("pending_sell_cnt") or 0) > 0:
            continue

        opened_at = _parse_sqlite_ts(p.get("opened_at"))
        if opened_at is None:
            continue
        hold_min = (now - opened_at).total_seconds() / 60.0
        if not should_exit_on_time(hold_minutes=hold_min, max_hold_min=max_hold_min):
            continue

        total_qty = float(p.get("qty") or 0.0)
        exited_qty = float(p.get("exited_qty") or 0.0)
        remain_qty = max(0.0, total_qty - exited_qty)
        if remain_qty <= 0:
            continue

        position_id = int(p["position_id"])
        signal_id = int(p.get("signal_id") or 0)
        ticker = str(p["ticker"])

        expected_price = _resolve_expected_price(broker, ticker)
        if expected_price is None:
            expected_price = float(p.get("avg_entry_price") or 0.0)
        if expected_price <= 0:
            log_and_notify(f"EXIT_SKIPPED:NO_PRICE ticker={ticker} position_id={position_id} reason=TIME_EXIT")
            continue

        send = broker.send_order(
            OrderRequest(
                signal_id=signal_id,
                ticker=ticker,
                side="SELL",
                qty=remain_qty,
                expected_price=expected_price,
            )
        )

        db.begin()
        try:
            order_id = db.insert_order(
                position_id=position_id,
                signal_id=signal_id,
                ticker=ticker,
                side="SELL",
                qty=remain_qty,
                order_type="MARKET",
                status="SENT",
                price=None,
                autocommit=False,
            )

            if send.status in {"SENT", "NEW", "PARTIAL_FILLED"}:
                db.update_order_status(order_id=order_id, status=send.status, broker_order_id=send.broker_order_id, autocommit=False)
                db.commit()
                log_and_notify(
                    f"EXIT_ORDER_SENT:{ticker} (position_id={position_id}, order_id={order_id}, reason=TIME_EXIT, hold_min={hold_min:.1f})"
                )
                _sync_exit_order_once(
                    db,
                    broker,
                    position_id=position_id,
                    signal_id=signal_id,
                    order_id=order_id,
                    ticker=ticker,
                    order_qty=remain_qty,
                    broker_order_id=send.broker_order_id,
                )
                created += 1
                continue

            if send.status == "FILLED":
                db.update_order_filled(
                    order_id=order_id,
                    price=float(send.avg_price or 0.0),
                    filled_qty=float(send.filled_qty or remain_qty),
                    broker_order_id=send.broker_order_id,
                    autocommit=False,
                )
                db.set_position_closed(
                    position_id=position_id,
                    reason_code="TIME_EXIT",
                    exited_qty=total_qty,
                    autocommit=False,
                )
                db.insert_position_event(
                    position_id=position_id,
                    event_type="FULL_EXIT",
                    action="EXECUTED",
                    reason_code="TIME_EXIT",
                    detail_json=json.dumps({"signal_id": signal_id, "order_id": order_id, "filled_qty": send.filled_qty, "avg_price": send.avg_price}),
                    idempotency_key=f"time-exit:{position_id}:{order_id}",
                    autocommit=False,
                )
                db.commit()
                log_and_notify(f"POSITION_CLOSED:{position_id} reason=TIME_EXIT")
                created += 1
                continue

            db.update_order_status(order_id=order_id, status=send.status, broker_order_id=send.broker_order_id, autocommit=False)
            db.insert_position_event(
                position_id=position_id,
                event_type="BLOCK",
                action="BLOCKED",
                reason_code=send.reason_code or "EXIT_ORDER_REJECTED",
                detail_json=json.dumps({"signal_id": signal_id, "order_id": order_id}),
                idempotency_key=f"exit-block:{position_id}:{order_id}",
                autocommit=False,
            )
            db.commit()
            created += 1
        except Exception:
            db.rollback()
            raise

    return created
