import unittest
from unittest.mock import patch

from app.ingestion.news_feed import fetch_rss_news, fetch_rss_news_items, NewsFetchError


class TestNewsFeed(unittest.TestCase):
    def test_fetch_rss_news_parses_first_item(self):
        xml = """
        <rss><channel>
          <item>
            <title>삼성전자 호재 뉴스</title>
            <link>https://example.com/a</link>
            <description>본문 요약</description>
            <pubDate>Mon, 02 Mar 2026 00:00:00 +0900</pubDate>
          </item>
        </channel></rss>
        """

        class R:
            text = xml
            def raise_for_status(self):
                return None

        with patch("app.ingestion.news_feed.requests.get", return_value=R()):
            item = fetch_rss_news("https://example.com/rss")

        self.assertEqual(item.source, "rss")
        self.assertEqual(item.title, "삼성전자 호재 뉴스")
        self.assertEqual(item.url, "https://example.com/a")

    def test_fetch_rss_news_items_multiple(self):
        xml = """
        <rss><channel>
          <item><title>A</title><link>https://example.com/a</link><description>a</description></item>
          <item><title>B</title><link>https://example.com/b</link><description>b</description></item>
        </channel></rss>
        """

        class R:
            text = xml
            def raise_for_status(self):
                return None

        with patch("app.ingestion.news_feed.requests.get", return_value=R()):
            items = fetch_rss_news_items("https://example.com/rss", limit=10)

        self.assertEqual(len(items), 2)
        self.assertEqual(items[0].title, "A")
        self.assertEqual(items[1].title, "B")

    def test_fetch_rss_news_raises_on_missing_item(self):
        class R:
            text = "<rss><channel></channel></rss>"
            def raise_for_status(self):
                return None

        with patch("app.ingestion.news_feed.requests.get", return_value=R()):
            with self.assertRaises(NewsFetchError):
                fetch_rss_news("https://example.com/rss")


if __name__ == "__main__":
    unittest.main()
