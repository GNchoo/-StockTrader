import json
from dataclasses import dataclass
from typing import Any
from datetime import datetime

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
    @staticmethod
    def _to_float(v: Any) -> float:
        if v is None:
            return 0.0
        if isinstance(v, (int, float)):
            return float(v)
        s = str(v).strip().replace(",", "")
        if s == "":
            return 0.0
        try:
            return float(s)
        except Exception:
            return 0.0

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

        실거래에서 주문 접수(ACK)와 체결(FILL)을 분리한다.
        rt_cd==0은 주문 접수 성공으로만 처리하며 status='SENT'를 반환한다.
        """
        data = self._order_cash(req)
        rt_cd = str(data.get("rt_cd", ""))
        if rt_cd != "0":
            msg = data.get("msg1") or data.get("msg_cd") or "KIS_ORDER_REJECTED"
            return OrderResult(status="REJECTED", filled_qty=0, avg_price=0.0, reason_code=str(msg))

        out = data.get("output", {}) if isinstance(data, dict) else {}
        ord_no = out.get("ODNO") or out.get("odno") or ""
        return OrderResult(
            status="SENT",
            filled_qty=0,
            avg_price=0.0,
            reason_code=f"ORDER_ACCEPTED:{ord_no}" if ord_no else "ORDER_ACCEPTED",
            broker_order_id=str(ord_no) if ord_no else None,
        )

    def inquire_order(self, broker_order_id: str, ticker: str, side: str = "BUY") -> OrderResult | None:
        """주문 체결 상태 조회 (best-effort).

        참고: KIS 계좌조회 API 응답 포맷은 계좌/상품/모드에 따라 달라질 수 있어
        필드 파싱은 방어적으로 처리한다.
        """
        if not broker_order_id:
            return None

        cano, prdt = self._split_account()
        today = datetime.now().strftime("%Y%m%d")
        url = f"{self.base_url}/uapi/domestic-stock/v1/trading/inquire-daily-ccld"
        params = {
            "CANO": cano,
            "ACNT_PRDT_CD": prdt,
            "INQR_STRT_DT": today,
            "INQR_END_DT": today,
            "SLL_BUY_DVSN_CD": "00",
            "INQR_DVSN": "00",
            "PDNO": ticker,
            "CCLD_DVSN": "00",
            "ORD_GNO_BRNO": "",
            "ODNO": str(broker_order_id),
            "INQR_DVSN_3": "00",
            "INQR_DVSN_1": "",
        }
        headers = {
            **self._auth_header(),
            "appkey": settings.kis_app_key,
            "appsecret": settings.kis_app_secret,
            "tr_id": "VTTC8001R" if self.mode == "paper" else "TTTC8001R",
            "custtype": "P",
        }

        try:
            r = self.session.get(url, headers=headers, params=params, timeout=8)
            if not r.ok:
                return None
            data = r.json() if r.text else {}
        except Exception:
            return None

        if isinstance(data, dict):
            rt_cd = str(data.get("rt_cd", "0"))
            if rt_cd and rt_cd != "0":
                msg = data.get("msg1") or data.get("msg_cd") or "KIS_INQUIRE_FAILED"
                return OrderResult(status="REJECTED", filled_qty=0, avg_price=0.0, reason_code=str(msg), broker_order_id=str(broker_order_id))

        rows = []
        if isinstance(data, dict):
            rows = data.get("output1") or data.get("output") or []
        if not isinstance(rows, list):
            return None

        row = None
        for item in rows:
            if str(item.get("odno") or item.get("ODNO") or "") == str(broker_order_id):
                row = item
                break
        if not row:
            return None

        ord_status = str(row.get("ord_sts") or row.get("ORD_STS") or "").upper()
        if ord_status in {"CANCELLED", "REJECTED", "EXPIRED", "취소", "거부"}:
            return OrderResult(status="REJECTED", filled_qty=0, avg_price=0.0, reason_code=ord_status or "ORDER_REJECTED", broker_order_id=str(broker_order_id))

        ord_qty = self._to_float(row.get("ord_qty") or row.get("ORD_QTY"))
        ccld_qty = self._to_float(row.get("tot_ccld_qty") or row.get("TOT_CCLD_QTY") or row.get("ccld_qty") or row.get("CCLD_QTY"))
        avg_price = self._to_float(row.get("avg_prvs") or row.get("avg_pric") or row.get("AVG_PRIC") or row.get("tot_ccld_unpr") or row.get("TOT_CCLD_UNPR"))

        if avg_price <= 0 and ccld_qty > 0:
            total_amt = self._to_float(row.get("tot_ccld_amt") or row.get("TOT_CCLD_AMT"))
            if total_amt > 0:
                avg_price = total_amt / max(ccld_qty, 1)

        if ccld_qty <= 0:
            return OrderResult(status="SENT", filled_qty=0, avg_price=0.0, broker_order_id=str(broker_order_id))
        if ord_qty > 0 and ccld_qty < ord_qty:
            return OrderResult(status="PARTIAL_FILLED", filled_qty=ccld_qty, avg_price=avg_price, broker_order_id=str(broker_order_id))
        return OrderResult(status="FILLED", filled_qty=ccld_qty or ord_qty, avg_price=avg_price, broker_order_id=str(broker_order_id))

    def get_last_price(self, ticker: str) -> float | None:
        """현재가 조회 (국내주식 현재가 시세)."""
        url = f"{self.base_url}/uapi/domestic-stock/v1/quotations/inquire-price"
        headers = {
            **self._auth_header(),
            "appkey": settings.kis_app_key,
            "appsecret": settings.kis_app_secret,
            "tr_id": "FHKST01010100",
            "custtype": "P",
        }
        params = {"fid_cond_mrkt_div_code": "J", "fid_input_iscd": str(ticker)}
        try:
            r = self.session.get(url, headers=headers, params=params, timeout=5)
            if not r.ok:
                return None
            data = r.json() if r.text else {}
            out = data.get("output", {}) if isinstance(data, dict) else {}
            px = self._to_float(out.get("stck_prpr") or out.get("stck_clpr") or out.get("bstp_nmix_prpr"))
            return px if px > 0 else None
        except Exception:
            return None

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
