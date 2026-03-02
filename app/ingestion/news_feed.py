import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
import email.utils
import xml.etree.ElementTree as ET
from urllib.parse import urlparse

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


def _infer_tier(source: str, link: str) -> int:
    host = (urlparse(link).netloc or "").lower()
    src = (source or "").lower()
    text = f"{host} {src}"

    if any(k in text for k in ["finance.naver.com", "kind.krx.co.kr", "dart.fss.or.kr", "reuters", "bloomberg"]):
        return 1
    if any(k in text for k in ["mk.co.kr", "hankyung.com", "yna.co.kr", "newsis.com"]):
        return 2
    return 3


def fetch_rss_news_items(rss_url: str, limit: int = 10, timeout: float = 5.0) -> list[NewsItem]:
    try:
        r = requests.get(rss_url, timeout=timeout)
        r.raise_for_status()
    except Exception as e:
        raise NewsFetchError(f"rss fetch failed: {e}") from e

    try:
        root = ET.fromstring(r.text)
        items = root.findall("./channel/item")
        if not items:
            items = root.findall(".//item")
        if not items:
            raise NewsFetchError("rss has no item")

        out: list[NewsItem] = []
        for item in items[: max(1, int(limit))]:
            title = (item.findtext("title") or "").strip()
            link = (item.findtext("link") or "").strip()
            desc = (item.findtext("description") or "").strip()
            pub = _parse_pub_date(item.findtext("pubDate"))
            if not title or not link:
                continue
            out.append(
                NewsItem(
                    source="rss",
                    tier=_infer_tier("rss", link),
                    title=title,
                    body=desc,
                    url=link,
                    published_at=pub,
                )
            )

        if not out:
            raise NewsFetchError("rss item missing title/link")
        return out
    except NewsFetchError:
        raise
    except Exception as e:
        raise NewsFetchError(f"rss parse failed: {e}") from e


def fetch_rss_news(rss_url: str, timeout: float = 5.0) -> NewsItem:
    items = fetch_rss_news_items(rss_url, limit=1, timeout=timeout)
    return items[0]


def sample_news() -> NewsItem:
    return NewsItem(
        source="sample",
        tier=2,
        title="삼성전자, 신규 반도체 투자 발표",
        body="샘플 뉴스 본문",
        url="https://example.com/news/1",
        published_at=datetime.now(timezone.utc),
    )
