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
    "SK하이닉스": ("000660", "SK하이닉스", 0.98),
    "삼성": ("", "AMBIGUOUS", 0.20),
}


def map_ticker(text: str) -> MappingResult | None:
    for k, v in ALIASES.items():
        if k in text:
            ticker, name, conf = v
            if ticker == "":
                return None
            return MappingResult(ticker=ticker, company_name=name, confidence=conf)
    return None
