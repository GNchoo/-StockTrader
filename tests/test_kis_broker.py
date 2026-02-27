import unittest

from app.execution.kis_broker import KISBroker
from app.execution.broker_base import OrderRequest


class TestKISBroker(unittest.TestCase):
    def test_health_check_shape(self):
        b = KISBroker()
        out = b.health_check()
        self.assertIn(out.get("status"), {"OK", "WARN", "CRITICAL"})
        self.assertIn("checks", out)
        self.assertEqual(out["checks"].get("broker"), "kis")

    def test_send_order_maps_accept_response(self):
        b = KISBroker()

        # 네트워크 호출 대신 내부 order 함수만 모킹
        b._order_cash = lambda req: {"rt_cd": "0", "output": {"ODNO": "12345"}}  # type: ignore[attr-defined]
        req = OrderRequest(signal_id=1, ticker="005930", side="BUY", qty=1, expected_price=83500)
        out = b.send_order(req)
        self.assertEqual(out.status, "FILLED")
        self.assertEqual(out.filled_qty, 1)
        self.assertEqual(out.avg_price, 83500)
        self.assertTrue((out.reason_code or "").startswith("ORDER_ACCEPTED"))

    def test_send_order_maps_reject_response(self):
        b = KISBroker()
        b._order_cash = lambda req: {"rt_cd": "1", "msg1": "INVALID"}  # type: ignore[attr-defined]
        req = OrderRequest(signal_id=1, ticker="005930", side="BUY", qty=1)
        out = b.send_order(req)
        self.assertEqual(out.status, "REJECTED")
        self.assertEqual(out.filled_qty, 0)


if __name__ == "__main__":
    unittest.main()
