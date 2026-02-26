from dataclasses import dataclass


@dataclass
class EventTicker:
    id: int
    news_id: int
    map_confidence: float


class IntegrityError(Exception):
    pass


def validate_signal_binding(input_news_id: int, event_ticker: EventTicker, min_conf: float = 0.92) -> None:
    if event_ticker.news_id != input_news_id:
        raise IntegrityError("NEWS_MISMATCH")
    if event_ticker.map_confidence < min_conf:
        raise IntegrityError("LOW_CONFIDENCE")
