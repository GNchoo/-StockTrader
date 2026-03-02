import json
from datetime import datetime
from typing import Callable, Literal

from app.execution.broker_base import OrderRequest
from app.risk.engine import can_trade
from app.storage.db import DB

ExecStatus = Literal["FILLED", "PENDING", "BLOCKED"]


def execute_signal_impl(
    db: DB,
    signal_id: int,
    ticker: str,
    qty: float = 1.0,
    demo_auto_close: bool | None = None,
    *,
    _build_broker: Callable,
    _resolve_expected_price: Callable,
    _sync_entry_order_once: Callable,
    log_and_notify: Callable,
    settings,
) -> ExecStatus:
    """Tx #2 + Tx #3: risk gate, order/position lifecycle, and close simulation.

    Returns:
      - "FILLED": 진입 체결 및 (데모 모드) 청산까지 완료
      - "PENDING": 주문 접수만 완료(미체결)
      - "BLOCKED": 리스크/주문 거부로 실행 차단
    """
    trade_date = datetime.now().date().isoformat()

    db.begin()
    try:
        db.ensure_risk_state_today(trade_date, autocommit=False)
        rs = db.get_risk_state(trade_date)
        if not rs:
            db.rollback()
            log_and_notify("BLOCKED:RISK_STATE_MISSING")
            return "BLOCKED"

        broker = _build_broker()
        expected_price = _resolve_expected_price(broker, ticker)
        if expected_price is None:
            db.rollback()
            log_and_notify(f"BLOCKED:NO_PRICE ticker={ticker} signal_id={signal_id}")
            return "BLOCKED"

        risk = can_trade(
            account_state=rs,
            proposed_notional=qty * expected_price,
            current_open_positions=db.count_open_positions(),
            current_symbol_exposure=db.get_open_exposure_for_ticker(ticker),
        )
        if not risk.allowed:
            db.rollback()
            log_and_notify(f"BLOCKED:{risk.reason_code}")
            return "BLOCKED"

        position_id = db.create_position(ticker, signal_id, qty, autocommit=False)
        order_id = db.insert_order(
            position_id=position_id,
            signal_id=signal_id,
            ticker=ticker,
            side="BUY",
            qty=qty,
            order_type="MARKET",
            status="SENT",
            price=None,
            autocommit=False,
        )

        result = broker.send_order(
            OrderRequest(
                signal_id=signal_id,
                ticker=ticker,
                side="BUY",
                qty=qty,
                expected_price=expected_price,
            )
        )

        # 주문 접수(ACK)와 체결(FILL) 분리 처리
        if result.status in {"SENT", "NEW", "PARTIAL_FILLED"}:
            db.update_order_status(
                order_id=order_id,
                status=result.status,
                broker_order_id=result.broker_order_id,
                autocommit=False,
            )
            db.commit()
            log_and_notify(
                f"ORDER_SENT_PENDING:{ticker} "
                f"(signal_id={signal_id}, position_id={position_id}, order_id={order_id}, broker_order_id={result.broker_order_id or '-'})"
            )
            sync_result = _sync_entry_order_once(
                db,
                broker,
                position_id=position_id,
                signal_id=signal_id,
                order_id=order_id,
                ticker=ticker,
                qty=qty,
                broker_order_id=result.broker_order_id,
            )
            if sync_result == "FILLED":
                return "FILLED"
            return "PENDING"

        if result.status != "FILLED":
            db.insert_position_event(
                position_id=position_id,
                event_type="BLOCK",
                action="BLOCKED",
                reason_code=result.reason_code or "ORDER_NOT_FILLED",
                detail_json=json.dumps({"signal_id": signal_id, "order_id": order_id}),
                idempotency_key=f"block:{position_id}:{order_id}",
                autocommit=False,
            )
            db.rollback()
            log_and_notify(f"BLOCKED:{result.reason_code or 'ORDER_NOT_FILLED'}")
            return "BLOCKED"

        db.update_order_filled(
            order_id=order_id,
            price=result.avg_price,
            broker_order_id=result.broker_order_id,
            autocommit=False,
        )
        db.set_position_open(
            position_id=position_id,
            avg_entry_price=result.avg_price,
            opened_value=result.avg_price * qty,
            autocommit=False,
        )
        entry_key = f"entry:{position_id}:{order_id}"
        first_event_id = db.insert_position_event(
            position_id=position_id,
            event_type="ENTRY",
            action="EXECUTED",
            reason_code="ENTRY_FILLED",
            detail_json=json.dumps(
                {
                    "signal_id": signal_id,
                    "order_id": order_id,
                    "filled_qty": result.filled_qty,
                    "avg_price": result.avg_price,
                }
            ),
            idempotency_key=entry_key,
            autocommit=False,
        )
        db.commit()
        log_and_notify(
            f"ORDER_FILLED:{ticker}@{result.avg_price} "
            f"(signal_id={signal_id}, position_id={position_id}, entry_event_id={first_event_id})"
        )
    except Exception:
        db.rollback()
        raise

    auto_close = settings.enable_demo_auto_close if demo_auto_close is None else bool(demo_auto_close)
    if not auto_close:
        return "FILLED"

    # Tx #3 (optional): simple close simulation (OPEN -> CLOSED)
    db.begin()
    try:
        exit_order_id = db.insert_order(
            position_id=position_id,
            signal_id=signal_id,
            ticker=ticker,
            side="SELL",
            qty=qty,
            order_type="MARKET",
            status="SENT",
            price=None,
            autocommit=False,
        )
        exit_price = float(result.avg_price or 0.0)
        db.update_order_filled(order_id=exit_order_id, price=exit_price, autocommit=False)
        db.set_position_closed(position_id=position_id, reason_code="TIME_EXIT", autocommit=False)
        db.insert_position_event(
            position_id=position_id,
            event_type="FULL_EXIT",
            action="EXECUTED",
            reason_code="TIME_EXIT",
            detail_json=json.dumps(
                {
                    "signal_id": signal_id,
                    "exit_order_id": exit_order_id,
                    "exit_price": exit_price,
                }
            ),
            idempotency_key=f"exit:{position_id}:{exit_order_id}",
            autocommit=False,
        )
        db.commit()
        log_and_notify(f"POSITION_CLOSED:{position_id} reason=TIME_EXIT")
        return "FILLED"
    except Exception:
        db.rollback()
        raise
