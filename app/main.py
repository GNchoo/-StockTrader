import json
from datetime import datetime
from typing import TypedDict

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


def execute_signal(db: DB, signal_id: int, ticker: str, qty: float = 1.0) -> bool:
    """Tx #2 + Tx #3: risk gate, order/position lifecycle, and close simulation."""
    trade_date = datetime.now().date().isoformat()

    db.begin()
    try:
        db.ensure_risk_state_today(trade_date, autocommit=False)
        rs = db.get_risk_state(trade_date)
        if not rs:
            db.rollback()
            log_and_notify("BLOCKED:RISK_STATE_MISSING")
            return False

        risk = can_trade(account_state=rs)
        if not risk.allowed:
            db.rollback()
            log_and_notify(f"BLOCKED:{risk.reason_code}")
            return False

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
            return False

        db.update_order_filled(order_id=order_id, price=result.avg_price, autocommit=False)
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
        return True
    except Exception:
        db.rollback()
        raise


def run_happy_path_demo() -> None:
    with DB("stock_trader.db") as db:
        db.init()
        bundle = ingest_and_create_signal(db)
        if not bundle:
            return
        execute_signal(db, bundle["signal_id"], bundle["ticker"])


if __name__ == "__main__":
    run_happy_path_demo()
