from datetime import datetime, timezone


def bounded(v: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, float(v)))


def derive_signal_fields(news) -> tuple[dict[str, float], str, str]:
    text = f"{news.title} {news.body}".lower()

    positive_terms = ["투자", "수주", "실적", "호재", "상승", "증가", "확대", "승인"]
    negative_terms = ["적자", "하락", "감소", "리콜", "규제", "소송", "중단", "악재", "파업"]

    pos_hits = sum(1 for t in positive_terms if t in text)
    neg_hits = sum(1 for t in negative_terms if t in text)

    # 문맥 보정: 긍정 키워드 + 부정 방향어가 함께 있으면 부정 가중
    negative_context_patterns = [
        ("투자", "감소"),
        ("실적", "하락"),
        ("증가", "둔화"),
        ("확대", "중단"),
        ("승인", "취소"),
    ]
    for a, b in negative_context_patterns:
        if a in text and b in text:
            neg_hits += 1

    impact = bounded(45 + 12 * pos_hits - 10 * neg_hits)
    source_reliability = bounded(85 - (int(getattr(news, "tier", 2)) - 1) * 15)

    age_hours = max(0.0, (datetime.now(timezone.utc) - news.published_at.astimezone(timezone.utc)).total_seconds() / 3600.0)
    freshness = bounded(100 - age_hours * 4)
    novelty = bounded(35 + freshness * 0.65)

    market_reaction = bounded(50 + 10 * pos_hits - 12 * neg_hits)
    liquidity = 55.0

    risk_penalty = bounded(8 + 8 * neg_hits + max(0.0, age_hours - 12) * 0.5, 0.0, 60.0)

    components = {
        "impact": round(impact, 2),
        "source_reliability": round(source_reliability, 2),
        "novelty": round(novelty, 2),
        "market_reaction": round(market_reaction, 2),
        "liquidity": round(liquidity, 2),
        "risk_penalty": round(risk_penalty, 2),
        "freshness_weight": round(freshness / 100.0, 3),
        "positive_hits": float(pos_hits),
        "negative_hits": float(neg_hits),
    }

    if freshness >= 75:
        priced_in_flag = "LOW"
    elif freshness >= 40:
        priced_in_flag = "MEDIUM"
    else:
        priced_in_flag = "HIGH"

    if neg_hits >= 2:
        decision = "BLOCK"
    elif neg_hits > pos_hits:
        decision = "IGNORE"
    elif pos_hits == 0:
        decision = "HOLD"
    else:
        decision = "BUY"

    return components, priced_in_flag, decision
