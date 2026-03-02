from dataclasses import dataclass


@dataclass
class MappingResult:
    ticker: str
    company_name: str
    confidence: float
    method: str = "alias_dict"


# P0 baseline alias map (expand later)
ALIASES = {
    "삼성전자": ("005930", "삼성전자", 0.98),
    "Samsung Electronics": ("005930", "삼성전자", 0.92),
    "SK하이닉스": ("000660", "SK하이닉스", 0.98),
    "SK hynix": ("000660", "SK하이닉스", 0.92),
    "현대차": ("005380", "현대차", 0.98),
    "기아": ("000270", "기아", 0.98),
    "NAVER": ("035420", "NAVER", 0.97),
    "카카오": ("035720", "카카오", 0.97),
    "LG에너지솔루션": ("373220", "LG에너지솔루션", 0.97),
    "POSCO홀딩스": ("005490", "POSCO홀딩스", 0.97),
    "셀트리온": ("068270", "셀트리온", 0.97),
    "삼성바이오로직스": ("207940", "삼성바이오로직스", 0.97),
    "한화에어로스페이스": ("012450", "한화에어로스페이스", 0.96),
    "삼성": ("", "AMBIGUOUS", 0.20),
}


def map_ticker(text: str) -> MappingResult | None:
    # Longest-key-first to avoid short alias preemption (e.g., "삼성" before "삼성전자")
    for k in sorted(ALIASES.keys(), key=len, reverse=True):
        if k in text:
            ticker, name, conf = ALIASES[k]
            if ticker == "":
                return None
            return MappingResult(ticker=ticker, company_name=name, confidence=conf)
    return None
