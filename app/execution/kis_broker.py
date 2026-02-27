import json
from dataclasses import dataclass
from typing import Any

import requests

from .broker_base import BrokerBase, OrderRequest, OrderResult
from app.config import settings


class KISBrokerError(RuntimeError):
    pass


@dataclass
class KISToken:
    access_token: str
    token_type: str = "Bearer"


class KISBroker(BrokerBase):
    """한국투자증권(KIS) 브로커.

    - OAuth 토큰 발급
    - 현금주문(매수/매도)
    - 기본 헬스체크

    주의: 체결동기화(정정/취소/체결조회)는 후속 단계에서 확장.
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
        self.session = requests.Session()
        self._token: KISToken | None = None

    def _split_account(self) -> tuple[str, str]:
        raw = (settings.kis_account_no or "").strip()
        if "-" in raw:
            cano, prod = raw.split("-", 1)
            return cano.strip(), (prod.strip() or settings.kis_product_code)
        return raw, settings.kis_product_code

    def _ensure_credentials(self) -> None:
        if not settings.kis_app_key or not settings.kis_app_secret:
            raise KISBrokerError("KIS credentials missing")
        cano, _ = self._split_account()
        if not cano:
            raise KISBrokerError("KIS account number missing")

    def _issue_token(self) -> KISToken:
        self._ensure_credentials()
        url = f"{self.base_url}/oauth2/tokenP"
        payload = {
            "grant_type": "client_credentials",
            "appkey": settings.kis_app_key,
            "appsecret": settings.kis_app_secret,
        }
        r = self.session.post(url, json=payload, timeout=8)
        if not r.ok:
            raise KISBrokerError(f"token issue failed: HTTP {r.status_code} {r.text[:200]}")
        data = r.json()
        token = data.get("access_token")
        if not token:
            raise KISBrokerError(f"token missing in response: {json.dumps(data, ensure_ascii=False)[:250]}")
        self._token = KISToken(access_token=token, token_type=data.get("token_type", "Bearer"))
        return self._token

    def _auth_header(self) -> dict[str, str]:
        tok = self._token or self._issue_token()
        return {"authorization": f"Bearer {tok.access_token}"}

    def _tr_id_order(self, side: str) -> str:
        s = (side or "").upper()
        if self.mode == "paper":
            return "VTTT0802U" if s == "BUY" else "VTTT0801U"
        return "TTTC0802U" if s == "BUY" else "TTTC0801U"

    def _order_cash(self, req: OrderRequest) -> dict[str, Any]:
        cano, prdt = self._split_account()
        tr_id = self._tr_id_order(req.side)
        url = f"{self.base_url}/uapi/domestic-stock/v1/trading/order-cash"
        qty = max(1, int(round(req.qty)))

        # 시장가(01) 고정. 지정가 확장은 후속 단계에서.
        body = {
            "CANO": cano,
            "ACNT_PRDT_CD": prdt,
            "PDNO": req.ticker,
            "ORD_DVSN": "01",
            "ORD_QTY": str(qty),
            "ORD_UNPR": "0",
        }
        headers = {
            **self._auth_header(),
            "appkey": settings.kis_app_key,
            "appsecret": settings.kis_app_secret,
            "tr_id": tr_id,
            "custtype": "P",
            "content-type": "application/json; charset=utf-8",
        }
        r = self.session.post(url, headers=headers, json=body, timeout=8)
        if not r.ok:
            raise KISBrokerError(f"order failed: HTTP {r.status_code} {r.text[:200]}")
        return r.json()

    def send_order(self, req: OrderRequest) -> OrderResult:
        """주문 전송.

        현재 단계에서는 주문 접수 성공(rt_cd==0) 시 FILLED로 간주해
        파이프라인을 유지합니다. 체결조회 연동은 후속 단계에서 보강.
        """
        data = self._order_cash(req)
        rt_cd = str(data.get("rt_cd", ""))
        if rt_cd != "0":
            msg = data.get("msg1") or data.get("msg_cd") or "KIS_ORDER_REJECTED"
            return OrderResult(status="REJECTED", filled_qty=0, avg_price=0.0, reason_code=str(msg))

        out = data.get("output", {}) if isinstance(data, dict) else {}
        ord_no = out.get("ODNO") or out.get("odno") or ""
        avg = float(req.expected_price or 0.0)
        return OrderResult(
            status="FILLED",
            filled_qty=max(1, int(round(req.qty))),
            avg_price=avg,
            reason_code=f"ORDER_ACCEPTED:{ord_no}" if ord_no else "ORDER_ACCEPTED",
        )

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
