import random
import time
from .broker_base import BrokerBase, OrderRequest, OrderResult


class PaperBroker(BrokerBase):
    """P0 minimal: market order full-fill only."""

    def __init__(self, base_latency_ms: int = 100):
        self.base_latency_ms = base_latency_ms
        self._orders: dict[str, OrderResult] = {}

    def send_order(self, req: OrderRequest) -> OrderResult:
        latency = self.base_latency_ms + random.randint(0, 80)
        time.sleep(latency / 1000)
        # P0: use caller-provided expected_price when available
        mock_price = req.expected_price if req.expected_price is not None else 100.0
        oid = f"PAPER-{int(time.time()*1000)}"
        result = OrderResult(
            status="FILLED",
            filled_qty=req.qty,
            avg_price=float(mock_price),
            broker_order_id=oid,
        )
        self._orders[oid] = result
        return result

    def inquire_order(self, broker_order_id: str, ticker: str, side: str = "BUY") -> OrderResult | None:
        return self._orders.get(broker_order_id)

    def get_last_price(self, ticker: str) -> float | None:
        # 한국 주식 시장의 실제 가격 범위를 반영한 의사 가격 생성
        # 주요 종목별 대표 가격 매핑
        ticker_price_map = {
            "005930": 75000.0,  # 삼성전자
            "000660": 120000.0, # SK하이닉스
            "035420": 40000.0,  # NAVER
            "035720": 45000.0,  # 카카오
            "005380": 200000.0, # 현대차
            "051910": 500000.0, # LG화학
            "006400": 30000.0,  # 삼성SDI
            "005490": 80000.0,  # POSCO홀딩스
            "012330": 150000.0, # 현대모비스
            "068270": 150000.0, # 셀트리온
            "105560": 80000.0,  # KB금융
            "055550": 60000.0,  # 신한지주
            "000270": 35000.0,  # 기아
            "032640": 20000.0,  # LG유플러스
            "034730": 40000.0,  # SK
            "028260": 50000.0,  # 삼성물산
            "017670": 70000.0,  # SK텔레콤
            "009540": 15000.0,  # 현대중공업지주
            "096770": 30000.0,  # SK이노베이션
            "010130": 30000.0,  # 고려아연
        }
        
        # 매핑된 종목이 있으면 해당 가격 사용
        if ticker in ticker_price_map:
            base_price = ticker_price_map[ticker]
        else:
            # 없는 종목은 ticker 해시 기반으로 합리적인 범위 내에서 가격 생성
            hash_val = sum(ord(c) for c in ticker)
            # 한국 주식의 일반적인 가격 범위: 5,000원 ~ 500,000원
            base_price = 5000 + (hash_val % 495000)
        
        # 약간의 변동성 추가 (±5%)
        volatility = random.uniform(0.95, 1.05)
        return float(base_price * volatility)

    def get_recent_closes(self, ticker: str, count: int = 30) -> list[float] | None:
        # 가상의 가격 배열 반환 (기술적 지표 테스트용)
        base = self.get_last_price(ticker) or 10000.0
        # 최근 상승 추세처럼 보이는 가상 데이터 생성
        prices = []
        for i in range(count):
            # 오름차순으로 가격이 약간씩 오르는 형태 + 랜덤 노이즈
            factor = 1.0 - ((count - i) * 0.005) + random.uniform(-0.02, 0.02)
            prices.append(base * factor)
        return prices

    def health_check(self) -> dict:
        return {"status": "OK", "latency_ms": self.base_latency_ms, "checks": {"broker": "paper"}}
