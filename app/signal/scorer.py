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


def compute_scores(inp: ScoreInput) -> tuple[float, float]:
    raw_score = (
        0.30 * inp.impact
        + 0.20 * inp.source_reliability
        + 0.20 * inp.novelty
        + 0.15 * inp.market_reaction
        + 0.15 * inp.liquidity
        - inp.risk_penalty
    )
    total_score = clamp(raw_score, 0.0, 100.0)
    return raw_score, total_score
