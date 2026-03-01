import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
import email.utils
import xml.etree.ElementTree as ET

import requests


@dataclass
class NewsItem:
    source: str
    tier: int
    title: str
    body: str
    url: str
    published_at: datetime


class NewsFetchError(RuntimeError):
    pass


def build_hash(item: NewsItem) -> str:
    base = f"{item.source}|{item.url}|{item.title}".encode("utf-8")
    return hashlib.sha256(base).hexdigest()


def _parse_pub_date(value: str | None) -> datetime:
    if not value:
        return datetime.now(timezone.utc)
    try:
        dt = email.utils.parsedate_to_datetime(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return datetime.now(timezone.utc)


def fetch_rss_news(rss_url: str, timeout: float = 5.0) -> NewsItem:
    try:
        r = requests.get(rss_url, timeout=timeout)
        r.raise_for_status()
    except Exception as e:
        raise NewsFetchError(f"rss fetch failed: {e}") from e

    try:
        root = ET.fromstring(r.text)
        item = root.find("./channel/item")
        if item is None:
            item = root.find(".//item")
        if item is None:
            raise NewsFetchError("rss has no item")

        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        desc = (item.findtext("description") or "").strip()
        pub = _parse_pub_date(item.findtext("pubDate"))

        if not title or not link:
            raise NewsFetchError("rss item missing title/link")

        return NewsItem(
            source="rss",
            tier=2,
            title=title,
            body=desc,
            url=link,
            published_at=pub,
        )
    except NewsFetchError:
        raise
    except Exception as e:
        raise NewsFetchError(f"rss parse failed: {e}") from e


def sample_news() -> NewsItem:
    return NewsItem(
        source="sample",
        tier=2,
        title="삼성전자, 신규 반도체 투자 발표",
        body="샘플 뉴스 본문",
        url="https://example.com/news/1",
        published_at=datetime.now(timezone.utc),
    )
