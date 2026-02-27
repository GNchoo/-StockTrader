import json
from app.storage.db import DB


def seed_signal(db: DB, *, url: str, raw_hash: str, title: str = "삼성전자 테스트") -> tuple[int, int, int]:
    news_id = db.insert_news_if_new(
        {
            "source": "test",
            "tier": 2,
            "published_at": "2026-01-01T00:00:00+00:00",
            "title": title,
            "body": "본문",
            "url": url,
            "raw_hash": raw_hash,
        }
    )
    if news_id is None:
        raise RuntimeError("seed_signal failed: duplicate news")

    event_ticker_id = db.insert_event_ticker(
        news_id=int(news_id),
        ticker="005930",
        company_name="삼성전자",
        confidence=0.98,
        method="alias_dict",
    )

    signal_id = db.insert_signal(
        {
            "news_id": int(news_id),
            "event_ticker_id": int(event_ticker_id),
            "ticker": "005930",
            "raw_score": 80,
            "total_score": 80,
            "components": json.dumps({"impact": 80}),
            "priced_in_flag": "LOW",
            "decision": "BUY",
        }
    )
    return int(news_id), int(event_ticker_id), int(signal_id)
