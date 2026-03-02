from app.config import settings
from app.execution.kis_broker import KISBroker
from app.execution.paper_broker import PaperBroker
from app.storage.db import DB


def build_broker():
    broker_name = (settings.broker or "paper").lower()
    if broker_name == "kis":
        return KISBroker()
    return PaperBroker()


def resolve_expected_price(broker, ticker: str) -> float | None:
    px = broker.get_last_price(ticker)
    if px is None:
        return None
    px = float(px)
    if px <= 0:
        return None
    return px


def collect_current_prices(db: DB, broker, limit: int = 100) -> dict[str, float]:
    prices: dict[str, float] = {}
    for p in db.get_positions_for_exit_scan(limit=limit):
        ticker = str(p["ticker"])
        if ticker in prices:
            continue
        px = broker.get_last_price(ticker)
        if px and px > 0:
            prices[ticker] = float(px)
            continue
        fallback = float(p.get("avg_entry_price") or 0.0)
        if fallback > 0:
            prices[ticker] = fallback
    return prices
