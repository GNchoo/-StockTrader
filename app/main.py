from app.ingestion.news_feed import sample_news
from app.nlp.ticker_mapper import map_ticker
from app.signal.integrity import EventTicker, validate_signal_binding
from app.execution.paper_broker import PaperBroker
from app.execution.broker_base import OrderRequest
from app.risk.engine import can_trade


def run_happy_path_demo() -> None:
    news = sample_news()
    mapping = map_ticker(news.title)
    if not mapping:
        print("NO_MAPPING")
        return

    # Simulate DB ids from persisted rows
    event_ticker = EventTicker(id=1, news_id=1, map_confidence=mapping.confidence)
    validate_signal_binding(input_news_id=1, event_ticker=event_ticker)

    risk = can_trade()
    if not risk.allowed:
        print(f"BLOCKED:{risk.reason_code}")
        return

    broker = PaperBroker()
    result = broker.send_order(OrderRequest(signal_id=1, ticker=mapping.ticker, side="BUY", qty=1))
    print(f"ORDER_{result.status}:{mapping.ticker}@{result.avg_price}")


if __name__ == "__main__":
    run_happy_path_demo()
