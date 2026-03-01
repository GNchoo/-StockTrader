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
        self.assertEqual(out.status, "SENT")
        self.assertEqual(out.filled_qty, 0)
        self.assertEqual(out.avg_price, 0.0)
        self.assertEqual(out.broker_order_id, "12345")
        self.assertTrue((out.reason_code or "").startswith("ORDER_ACCEPTED"))

    def test_send_order_maps_reject_response(self):
        b = KISBroker()
        b._order_cash = lambda req: {"rt_cd": "1", "msg1": "INVALID"}  # type: ignore[attr-defined]
        req = OrderRequest(signal_id=1, ticker="005930", side="BUY", qty=1)
        out = b.send_order(req)
        self.assertEqual(out.status, "REJECTED")
        self.assertEqual(out.filled_qty, 0)

    def test_inquire_order_filled_parse(self):
        b = KISBroker()
        b._auth_header = lambda: {"authorization": "Bearer X"}  # type: ignore[attr-defined]

        class R:
            ok = True
            text = "{}"

            def json(self):
                return {
                    "rt_cd": "0",
                    "output1": [
                        {"odno": "12345", "ord_qty": "10", "tot_ccld_qty": "10", "avg_pric": "83,500"}
                    ],
                }

        b.session.get = lambda *a, **k: R()  # type: ignore[method-assign]
        out = b.inquire_order("12345", "005930")
        self.assertIsNotNone(out)
        self.assertEqual(out.status, "FILLED")
        self.assertEqual(out.filled_qty, 10)
        self.assertEqual(out.avg_price, 83500)

    def test_inquire_order_partial_parse(self):
        b = KISBroker()
        b._auth_header = lambda: {"authorization": "Bearer X"}  # type: ignore[attr-defined]

        class R:
            ok = True
            text = "{}"

            def json(self):
                return {
                    "rt_cd": "0",
                    "output1": [
                        {"ODNO": "12345", "ORD_QTY": "10", "TOT_CCLD_QTY": "4", "TOT_CCLD_AMT": "334000"}
                    ],
                }

        b.session.get = lambda *a, **k: R()  # type: ignore[method-assign]
        out = b.inquire_order("12345", "005930")
        self.assertIsNotNone(out)
        self.assertEqual(out.status, "PARTIAL_FILLED")
        self.assertEqual(out.filled_qty, 4)
        self.assertEqual(out.avg_price, 83500)

    def test_inquire_order_rejected_rt_cd(self):
        b = KISBroker()
        b._auth_header = lambda: {"authorization": "Bearer X"}  # type: ignore[attr-defined]

        class R:
            ok = True
            text = "{}"

            def json(self):
                return {"rt_cd": "1", "msg1": "NO_DATA"}

        b.session.get = lambda *a, **k: R()  # type: ignore[method-assign]
        out = b.inquire_order("12345", "005930")
        self.assertIsNotNone(out)
        self.assertEqual(out.status, "REJECTED")
        self.assertEqual(out.reason_code, "NO_DATA")


if __name__ == "__main__":
    unittest.main()
