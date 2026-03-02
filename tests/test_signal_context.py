import unittest
from datetime import datetime, timezone

from app.ingestion.news_feed import NewsItem
from app.signal.decision import derive_signal_fields


class TestSignalContext(unittest.TestCase):
    def test_positive_word_with_negative_direction_is_not_buy(self):
        news = NewsItem(
            source="rss",
            tier=2,
            title="삼성전자 투자 감소 전망",
            body="투자 계획이 감소하며 실적 하락 우려",
            url="https://example.com/x",
            published_at=datetime.now(timezone.utc),
        )
        _, _, decision = derive_signal_fields(news)
        self.assertIn(decision, {"IGNORE", "BLOCK"})


if __name__ == "__main__":
    unittest.main()
