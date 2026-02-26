import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass
class NewsItem:
    source: str
    tier: int
    title: str
    body: str
    url: str
    published_at: datetime


# Placeholder for real connector

def build_hash(item: NewsItem) -> str:
    base = f"{item.source}|{item.url}|{item.title}".encode("utf-8")
    return hashlib.sha256(base).hexdigest()


def sample_news() -> NewsItem:
    return NewsItem(
        source="sample",
        tier=2,
        title="삼성전자, 신규 반도체 투자 발표",
        body="샘플 뉴스 본문",
        url="https://example.com/news/1",
        published_at=datetime.now(timezone.utc),
    )
