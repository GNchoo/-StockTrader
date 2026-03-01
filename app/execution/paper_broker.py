import random
import time
from .broker_base import BrokerBase, OrderRequest, OrderResult


class PaperBroker(BrokerBase):
    """P0 minimal: market order full-fill only."""

    def __init__(self, base_latency_ms: int = 100):
        self.base_latency_ms = base_latency_ms

    def send_order(self, req: OrderRequest) -> OrderResult:
        latency = self.base_latency_ms + random.randint(0, 80)
        time.sleep(latency / 1000)
        # P0: use caller-provided expected_price when available
        mock_price = req.expected_price if req.expected_price is not None else 100.0
        return OrderResult(
            status="FILLED",
            filled_qty=req.qty,
            avg_price=float(mock_price),
            broker_order_id=f"PAPER-{int(time.time()*1000)}",
        )

    def get_last_price(self, ticker: str) -> float | None:
        # deterministic pseudo quote for demo/testing loops
        base = 80000 + (sum(ord(c) for c in ticker) % 4000)
        return float(base)

    def health_check(self) -> dict:
        return {"status": "OK", "latency_ms": self.base_latency_ms, "checks": {"broker": "paper"}}
