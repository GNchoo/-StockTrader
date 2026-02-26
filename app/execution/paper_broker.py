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
        # TODO: replace with market data driven fill
        mock_price = 100.0
        return OrderResult(status="FILLED", filled_qty=req.qty, avg_price=mock_price)

    def health_check(self) -> dict:
        return {"status": "OK", "latency_ms": self.base_latency_ms, "checks": {"broker": "paper"}}
