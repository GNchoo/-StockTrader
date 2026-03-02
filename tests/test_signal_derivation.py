import unittest
from datetime import datetime, timedelta, timezone

from app.ingestion.news_feed import NewsItem
from app.main import _derive_signal_fields


class TestSignalDerivation(unittest.TestCase):
    def test_positive_fresh_news_prefers_buy_and_low_priced_in(self) -> None:
        news = NewsItem(
            source="rss",
            tier=1,
            title="삼성전자 대규모 투자 확대 및 수주 증가",
            body="신규 공장 투자 발표로 실적 개선 기대",
            url="https://example.com/positive",
            published_at=datetime.now(timezone.utc) - timedelta(hours=1),
        )
        components, priced_in, decision = _derive_signal_fields(news)
        self.assertEqual(decision, "BUY")
        self.assertEqual(priced_in, "LOW")
        self.assertGreater(components["impact"], 50)

    def test_negative_news_blocks(self) -> None:
        news = NewsItem(
            source="rss",
            tier=2,
            title="대형 리콜 및 규제 조사로 생산 중단",
            body="실적 하락 우려와 소송 리스크 확대",
            url="https://example.com/negative",
            published_at=datetime.now(timezone.utc) - timedelta(hours=2),
        )
        _, _, decision = _derive_signal_fields(news)
        self.assertEqual(decision, "BLOCK")


if __name__ == "__main__":
    unittest.main()
