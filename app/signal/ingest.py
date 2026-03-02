import json
from typing import TypedDict

from app.config import settings
from app.ingestion.news_feed import NewsFetchError, build_hash, fetch_rss_news_items, sample_news
from app.nlp.ticker_mapper import MappingResult, map_ticker
from app.signal.decision import derive_signal_fields
from app.signal.integrity import EventTicker, validate_signal_binding
from app.signal.scorer import ScoreInput, compute_scores
from app.storage.db import DB


class SignalBundle(TypedDict):
    signal_id: int
    ticker: str


def _load_news_item(log_and_notify) -> object:
    mode = (settings.news_mode or "sample").lower()
    if mode == "rss":
        try:
            items = fetch_rss_news_items(settings.news_rss_url, limit=10)
            for n in items:
                if map_ticker((n.title or "") + " " + (n.body or "")):
                    return n
            return items[0]
        except NewsFetchError as e:
            log_and_notify(f"NEWS_FETCH_FALLBACK_SAMPLE:{e}")
            return sample_news()
    return sample_news()


def ingest_and_create_signal(db: DB, log_and_notify) -> SignalBundle | None:
    news = _load_news_item(log_and_notify)
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

        components, priced_in_flag, decision = derive_signal_fields(news)
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

        if total_score < 40:
            decision = "BLOCK"
        elif total_score < 55 and decision == "BUY":
            decision = "HOLD"

        signal_id = db.insert_signal(
            {
                "news_id": news_id,
                "event_ticker_id": event_ticker_id,
                "ticker": mapping.ticker,
                "raw_score": raw_score,
                "total_score": total_score,
                "components": json.dumps(components, ensure_ascii=False),
                "priced_in_flag": priced_in_flag,
                "decision": decision,
            },
            autocommit=False,
        )
        db.commit()
        if decision != "BUY":
            log_and_notify(f"SIGNAL_SKIPPED:{mapping.ticker} decision={decision} score={total_score:.1f}")
            return None
        return {"signal_id": signal_id, "ticker": mapping.ticker}
    except Exception:
        db.rollback()
        raise
