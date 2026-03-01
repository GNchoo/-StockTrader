import json
from datetime import datetime
from typing import TypedDict, Literal

from app.ingestion.news_feed import sample_news, build_hash
from app.nlp.ticker_mapper import map_ticker, MappingResult
from app.signal.integrity import EventTicker, validate_signal_binding
from app.execution.paper_broker import PaperBroker
from app.execution.kis_broker import KISBroker
from app.execution.broker_base import OrderRequest
from app.risk.engine import can_trade
from app.storage.db import DB
from app.signal.scorer import ScoreInput, compute_scores
from app.monitor.telegram_logger import log_and_notify
from app.config import settings


class SignalBundle(TypedDict):
    signal_id: int
    ticker: str


ExecStatus = Literal["FILLED", "PENDING", "BLOCKED"]


def _build_broker():
    broker_name = (settings.broker or "paper").lower()
    if broker_name == "kis":
        return KISBroker()
    return PaperBroker()


def ingest_and_create_signal(db: DB) -> SignalBundle | None:
    """Tx #1: ingest + mapping + signal persistence.

    Returns:
      - SignalBundle on success
      - None when duplicate/skip case occurs
    """
    news = sample_news()
    raw_hash = build_hash(news)

    db.begin()
    try:
        news_id = db.insert_news_if_new(
            {
                "source": news.source,
                "tier": news.tier,
                "published_at": news.published_at.isoformat(),
                "title": news.title,
                "body": news.body,
                "url": news.url,
                "raw_hash": raw_hash,
            },
            autocommit=False,
        )

        if news_id is None:
            db.rollback()
            log_and_notify("DUP_NEWS_SKIPPED")
            return None

        mapping: MappingResult | None = map_ticker(news.title + " " + news.body)
        if not mapping:
            db.rollback()
            log_and_notify("NO_MAPPING")
            return None

        event_ticker_id = db.insert_event_ticker(
            news_id=news_id,
            ticker=mapping.ticker,
            company_name=mapping.company_name,
            confidence=mapping.confidence,
            method=mapping.method,
            autocommit=False,
        )

        row = db.get_event_ticker(event_ticker_id)
        if not row:
            db.rollback()
            log_and_notify("EVENT_TICKER_NOT_FOUND")
            return None

        event_ticker = EventTicker(
            id=int(row["id"]),
            news_id=int(row["news_id"]),
            map_confidence=float(row["map_confidence"]),
        )
        validate_signal_binding(input_news_id=news_id, event_ticker=event_ticker)

        components = {
            "impact": 75,
            "source_reliability": 70,
            "novelty": 90,
            "market_reaction": 50,
            "liquidity": 50,
            "risk_penalty": 10,
            "freshness_weight": 1.0,
        }
        weights = db.get_score_weights()
        raw_score, total_score = compute_scores(
            ScoreInput(
                impact=components["impact"],
                source_reliability=components["source_reliability"],
                novelty=components["novelty"],
                market_reaction=components["market_reaction"],
                liquidity=components["liquidity"],
                risk_penalty=components["risk_penalty"],
            ),
            weights=weights,
        )

        signal_id = db.insert_signal(
            {
                "news_id": news_id,
                "event_ticker_id": event_ticker_id,
                "ticker": mapping.ticker,
                "raw_score": raw_score,
                "total_score": total_score,
                "components": json.dumps(components, ensure_ascii=False),
                "priced_in_flag": "LOW",
                "decision": "BUY",
            },
            autocommit=False,
        )
        db.commit()
        return {"signal_id": signal_id, "ticker": mapping.ticker}
    except Exception:
        db.rollback()
        raise


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
            db.update_order_partial(
                order_id=order_id,
                price=float(status.avg_price or 0.0),
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
    if not ts:
        return None
    s = str(ts).strip()
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def sync_pending_entries(db: DB, limit: int = 100) -> int:
    """재시작/주기 동기화: PENDING_ENTRY 주문의 체결 상태를 동기화.

    retry_policy(max_attempts_per_signal/min_retry_interval_sec)를 적용해
    장시간 미체결 주문을 재시도 또는 종료한다.
    """
    broker = _build_broker()
    rows = db.get_pending_entry_orders(limit=limit)
    retry_policy = db.get_retry_policy()
    max_attempts = int(retry_policy.get("max_attempts_per_signal", 2) or 2)
    min_retry_sec = int(retry_policy.get("min_retry_interval_sec", 30) or 30)

    changed = 0
    now = datetime.now()

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
            # 부분체결 상태는 재주문하지 않고 체결 동기화만 유지
            current_status = db.get_order_status(order_id) or str(row.get("status") or "")
            if current_status == "PARTIAL_FILLED":
                continue

            sent_at = _parse_sqlite_ts(row.get("sent_at"))
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
                    # 기존 주문을 만료 처리하고 새 시도로 재주문
                    new_result = broker.send_order(
                        OrderRequest(
                            signal_id=signal_id,
                            ticker=ticker,
                            side="BUY",
                            qty=qty,
                            expected_price=83500.0,
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


def execute_signal(db: DB, signal_id: int, ticker: str, qty: float = 1.0) -> ExecStatus:
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

        risk = can_trade(account_state=rs)
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

        broker = _build_broker()
        result = broker.send_order(
            OrderRequest(
                signal_id=signal_id,
                ticker=ticker,
                side="BUY",
                qty=qty,
                expected_price=83500.0,
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

    # Tx #3: simple close simulation (OPEN -> CLOSED)
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
        db.update_order_filled(order_id=exit_order_id, price=83600.0, autocommit=False)
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
                    "exit_price": 83600.0,
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


def run_happy_path_demo() -> None:
    with DB("stock_trader.db") as db:
        db.init()
        sync_pending_entries(db)
        bundle = ingest_and_create_signal(db)
        if not bundle:
            return
        execute_signal(db, bundle["signal_id"], bundle["ticker"])


if __name__ == "__main__":
    run_happy_path_demo()
