from dataclasses import dataclass


@dataclass
class ScoreInput:
    impact: float
    source_reliability: float
    novelty: float
    market_reaction: float
    liquidity: float
    risk_penalty: float


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


DEFAULT_WEIGHTS = {
    "impact": 0.30,
    "source_reliability": 0.20,
    "novelty": 0.20,
    "market_reaction": 0.15,
    "liquidity": 0.15,
}


def compute_scores(inp: ScoreInput, weights: dict[str, float] | None = None) -> tuple[float, float]:
    w = {**DEFAULT_WEIGHTS, **(weights or {})}
    raw_score = (
        w["impact"] * inp.impact
        + w["source_reliability"] * inp.source_reliability
        + w["novelty"] * inp.novelty
        + w["market_reaction"] * inp.market_reaction
        + w["liquidity"] * inp.liquidity
        - inp.risk_penalty
    )
    total_score = clamp(raw_score, 0.0, 100.0)
    return raw_score, total_score
