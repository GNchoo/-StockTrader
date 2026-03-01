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
    broker_order_id: str | None = None


class BrokerBase(ABC):
    @abstractmethod
    def send_order(self, req: OrderRequest) -> OrderResult:
        raise NotImplementedError

    def inquire_order(self, broker_order_id: str, ticker: str, side: str = "BUY") -> OrderResult | None:
        """주문 상태 조회 (미구현 브로커는 None 반환)."""
        return None

    def get_last_price(self, ticker: str) -> float | None:
        """현재가 조회 (미구현 브로커는 None 반환)."""
        return None

    @abstractmethod
    def health_check(self) -> dict:
        raise NotImplementedError
