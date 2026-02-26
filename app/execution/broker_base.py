from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class OrderRequest:
    signal_id: int
    ticker: str
    side: str
    qty: float
    order_type: str = "MARKET"
    expected_price: float | None = None


@dataclass
class OrderResult:
    status: str
    filled_qty: float
    avg_price: float
    reason_code: str | None = None


class BrokerBase(ABC):
    @abstractmethod
    def send_order(self, req: OrderRequest) -> OrderResult:
        raise NotImplementedError

    @abstractmethod
    def health_check(self) -> dict:
        raise NotImplementedError
