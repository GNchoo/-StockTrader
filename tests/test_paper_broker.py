import unittest

from app.execution.broker_base import OrderRequest
from app.execution.paper_broker import PaperBroker


class TestPaperBroker(unittest.TestCase):
    def test_inquire_order_returns_sent_result(self):
        broker = PaperBroker(base_latency_ms=0)
        res = broker.send_order(
            OrderRequest(signal_id=1, ticker="005930", side="BUY", qty=1, expected_price=83000.0)
        )
        self.assertIsNotNone(res.broker_order_id)

        q = broker.inquire_order(res.broker_order_id, ticker="005930", side="BUY")
        self.assertIsNotNone(q)
        self.assertEqual(q.status, "FILLED")
        self.assertEqual(q.avg_price, 83000.0)


if __name__ == "__main__":
    unittest.main()
