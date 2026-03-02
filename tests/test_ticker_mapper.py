import unittest

from app.nlp.ticker_mapper import map_ticker


class TestTickerMapper(unittest.TestCase):
    def test_maps_multiple_symbols(self):
        self.assertEqual(map_ticker("현대차 투자 확대").ticker, "005380")
        self.assertEqual(map_ticker("SK hynix capex").ticker, "000660")

    def test_ambiguous_returns_none(self):
        self.assertIsNone(map_ticker("삼성 관련 뉴스"))


if __name__ == "__main__":
    unittest.main()
