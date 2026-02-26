import json
from datetime import datetime

from app.ingestion.news_feed import sample_news, build_hash
from app.nlp.ticker_mapper import map_ticker
from app.signal.integrity import EventTicker, validate_signal_binding
from app.execution.paper_broker import PaperBroker
from app.execution.broker_base import OrderRequest
from app.risk.engine import can_trade
from app.storage.db import DB
from app.signal.scorer import ScoreInput, compute_scores


def run_happy_path_demo() -> None:
    with DB("stock_trader.db") as db:
        db.init()

        # ---------- Tx #1: ingest + mapping + signal persistence ----------
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
                print("DUP_NEWS_SKIPPED")
                return

            mapping = map_ticker(news.title + " " + news.body)
            if not mapping:
                db.rollback()
                print("NO_MAPPING")
                return

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
                print("EVENT_TICKER_NOT_FOUND")
                return

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
            raw_score, total_score = compute_scores(
                ScoreInput(
                    impact=components["impact"],
                    source_reliability=components["source_reliability"],
                    novelty=components["novelty"],
                    market_reaction=components["market_reaction"],
                    liquidity=components["liquidity"],
                    risk_penalty=components["risk_penalty"],
                )
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
        except Exception:
            db.rollback()
            raise

        # ---------- Tx #2: execution + order/position lifecycle ----------
        trade_date = datetime.now().date().isoformat()
        db.begin()
        try:
            db.ensure_risk_state_today(trade_date)
            rs = db.get_risk_state(trade_date)
            if not rs or int(rs["trading_enabled"]) != 1:
                db.rollback()
                print("BLOCKED:RISK_DISABLED")
                return

            risk = can_trade(account_state=rs)
            if not risk.allowed:
                db.rollback()
                print(f"BLOCKED:{risk.reason_code}")
                return

            qty = 1.0
            position_id = db.create_position(mapping.ticker, signal_id, qty, autocommit=False)
            order_id = db.insert_order(
                position_id=position_id,
                signal_id=signal_id,
                ticker=mapping.ticker,
                side="BUY",
                qty=qty,
                order_type="MARKET",
                status="SENT",
                price=None,
                autocommit=False,
            )

            broker = PaperBroker()
            result = broker.send_order(
                OrderRequest(
                    signal_id=signal_id,
                    ticker=mapping.ticker,
                    side="BUY",
                    qty=qty,
                    expected_price=83500.0,
                )
            )

            if result.status == "FILLED":
                db.update_order_filled(order_id=order_id, price=result.avg_price, autocommit=False)
                db.set_position_open(
                    position_id=position_id,
                    avg_entry_price=result.avg_price,
                    opened_value=result.avg_price * qty,
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
                            "filled_qty": result.filled_qty,
                            "avg_price": result.avg_price,
                        }
                    ),
                    idempotency_key=f"entry:{position_id}:{order_id}",
                    autocommit=False,
                )
                db.commit()
                print(f"ORDER_FILLED:{mapping.ticker}@{result.avg_price} (signal_id={signal_id}, position_id={position_id})")
            else:
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
                print(f"BLOCKED:{result.reason_code or 'ORDER_NOT_FILLED'}")
        except Exception:
            db.rollback()
            raise


if __name__ == "__main__":
    run_happy_path_demo()
