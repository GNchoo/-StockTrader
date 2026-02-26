import json

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

            # P0 score placeholder (moved to scorer module)
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

        risk = can_trade(account_state={})
        if not risk.allowed:
            print(f"BLOCKED:{risk.reason_code}")
            return

        broker = PaperBroker()
        result = broker.send_order(
            OrderRequest(
                signal_id=signal_id,
                ticker=mapping.ticker,
                side="BUY",
                qty=1,
                expected_price=83500.0,
            )
        )
        print(f"ORDER_{result.status}:{mapping.ticker}@{result.avg_price} (signal_id={signal_id})")


if __name__ == "__main__":
    run_happy_path_demo()
