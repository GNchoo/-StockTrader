from .broker_base import BrokerBase, OrderRequest, OrderResult
from app.config import settings


class KISBroker(BrokerBase):
    """한국투자증권(KIS) 브로커 연동 스켈레톤.

    현재는 연결 정보 검증/헬스체크만 제공하고,
    실제 주문 API 연동은 다음 단계에서 구현합니다.
    """

    def __init__(self):
        mode = (settings.kis_mode or "paper").lower()
        if settings.kis_base_url:
            self.base_url = settings.kis_base_url
        else:
            self.base_url = (
                "https://openapivts.koreainvestment.com:29443"
                if mode == "paper"
                else "https://openapi.koreainvestment.com:9443"
            )
        self.mode = mode

    def send_order(self, req: OrderRequest) -> OrderResult:
        # TODO: KIS 주문 엔드포인트 연동 (토큰 발급/주문요청/체결조회)
        raise NotImplementedError("KISBroker.send_order is not implemented yet")

    def health_check(self) -> dict:
        has_keys = bool(settings.kis_app_key and settings.kis_app_secret)
        has_account = bool(settings.kis_account_no)

        if has_keys and has_account:
            status = "OK"
            reason = None
        elif has_keys and not has_account:
            status = "WARN"
            reason = "MISSING_ACCOUNT"
        else:
            status = "CRITICAL"
            reason = "MISSING_CREDENTIALS"

        return {
            "status": status,
            "reason_code": reason,
            "checks": {
                "broker": "kis",
                "mode": self.mode,
                "base_url": self.base_url,
                "has_app_key": bool(settings.kis_app_key),
                "has_app_secret": bool(settings.kis_app_secret),
                "has_account_no": has_account,
                "product_code": settings.kis_product_code,
            },
        }
