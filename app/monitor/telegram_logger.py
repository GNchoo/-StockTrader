import json
import logging
import os
import sys
from urllib import request, parse

from app.config import settings

logger = logging.getLogger("stock_trader")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def send_telegram(text: str) -> bool:
    if os.getenv("STOCK_TRADER_NOTIFY", "1") != "1":
        return False
    if "unittest" in sys.modules:
        return False
    if not settings.telegram_bot_token or not settings.telegram_chat_id:
        return False
    url = f"https://api.telegram.org/bot{settings.telegram_bot_token}/sendMessage"
    payload = parse.urlencode({"chat_id": settings.telegram_chat_id, "text": text}).encode("utf-8")
    req = request.Request(url, data=payload, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    try:
        with request.urlopen(req, timeout=10) as resp:
            body = resp.read().decode("utf-8")
            ok = json.loads(body).get("ok", False)
            return bool(ok)
    except Exception:
        return False


def log_and_notify(text: str) -> None:
    logger.info(text)
    send_telegram(text)
