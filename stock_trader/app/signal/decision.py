from datetime import datetime, timezone


def bounded(v: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, float(v)))


def derive_signal_fields(news, tech_score: float = 0.0, tech_rec: str = "NEUTRAL") -> tuple[dict[str, float], str, str]:
    """
    뉴스 데이터에서 신호 필드를 추출합니다.
    
    Args:
        news: 뉴스 객체 (title, body, published_at, tier, source 속성 필요)
        tech_score: 기술적 차트 기반 점수 (-100 ~ 100)
        tech_rec: 기술적 차트 추천 ("BUY", "SELL", "NEUTRAL")
        
    Returns:
        tuple: (components_dict, priced_in_flag, decision)
    """
    text = f"{news.title} {news.body or ''}".lower()
    
    # 한국어 뉴스 특성을 반영한 키워드 사전 확장
    positive_terms = {
        "투자": 2.0, "수주": 3.0, "실적": 2.5, "호재": 3.0, "상승": 2.0,
        "증가": 1.5, "확대": 2.0, "승인": 2.5, "개발": 1.5, "성장": 2.0,
        "혁신": 1.5, "수익": 2.0, "이익": 2.0, "흑자": 2.5, "회복": 1.5,
        "강세": 2.0, "매수": 1.5, "추천": 2.0, "목표가": 1.5, "상향": 2.0,
        "낙관": 1.5, "긍정": 1.5, "호조": 2.0, "선방": 1.5, "돌파": 1.5,
    }
    
    negative_terms = {
        "적자": 3.0, "하락": 2.0, "감소": 1.5, "리콜": 3.0, "규제": 2.0,
        "소송": 2.5, "중단": 2.5, "악재": 3.0, "파업": 2.5, "손실": 2.5,
        "부진": 2.0, "약세": 2.0, "악화": 2.5, "위기": 3.0, "경고": 2.0,
        "조정": 1.5, "하향": 2.0, "비관": 1.5, "부정": 1.5, "악영향": 2.5,
        "충격": 2.5, "불확실": 2.0, "리스크": 1.5, "우려": 1.5, "부담": 1.5,
    }
    
    # 가중치 기반 키워드 카운팅
    pos_score = sum(weight for term, weight in positive_terms.items() if term in text)
    neg_score = sum(weight for term, weight in negative_terms.items() if term in text)
    
    # 문맥 분석: 한국어 특성 반영
    context_patterns = {
        # 긍정 문맥이 부정어와 함께 있을 때
        "negative_context": [
            ("투자", ["감소", "중단", "축소"]),
            ("실적", ["하락", "부진", "악화"]),
            ("증가", ["둔화", "정체", "감소"]),
            ("확대", ["중단", "축소", "연기"]),
            ("승인", ["취소", "반려", "거부"]),
            ("개발", ["중단", "실패", "지연"]),
        ],
        # 부정 문맥이 긍정어와 함께 있을 때 (덜 부정적)
        "positive_context": [
            ("하락", ["일시적", "단기", "조정"]),
            ("감소", ["미미", "소폭", "일부"]),
            ("악재", ["완화", "해소", "극복"]),
            ("위기", ["극복", "탈출", "관리"]),
        ]
    }
    
    # 문맥 보정 적용
    context_neg_boost = 0.0  # 부정 문맥 강화 (neg_score에 가산)
    context_pos_reduction = 0.0  # 부정 문맥으로 인한 긍정 점수 차감
    context_neg_reduction = 0.0  # 긍정 문맥으로 인한 부정 점수 차감
    
    for positive_word, negative_words in context_patterns["negative_context"]:
        if positive_word in text:
            for negative_word in negative_words:
                if negative_word in text:
                    context_neg_boost += 1.5  # 부정적 문맥 → neg_score 증가
                    context_pos_reduction += 1.5  # 긍정 속 부정 → 긍정 점수 차감
                    break
    
    for negative_word, positive_words in context_patterns["positive_context"]:
        if negative_word in text:
            for positive_word in positive_words:
                if positive_word in text:
                    context_neg_reduction += 1.0  # 부정 완화
                    break
    
    # 최종 점수 계산 (문맥 보정 적용)
    final_pos_score = max(0, pos_score - context_pos_reduction)
    final_neg_score = max(0, neg_score + context_neg_boost - context_neg_reduction)
    context_adjustment = -context_neg_boost - context_pos_reduction + context_neg_reduction
    
    # Impact: 긍정/부정 점수 차이에 기반
    impact_base = 50.0
    impact = bounded(impact_base + 8 * final_pos_score - 7 * final_neg_score)
    
    # Source reliability: 출처 신뢰도 (티어 기반)
    tier = int(getattr(news, "tier", 2))
    source_map = {1: 90.0, 2: 75.0, 3: 60.0}
    source_reliability = bounded(source_map.get(tier, 70.0))
    
    # Freshness & Novelty: 시간에 따른 감쇠
    age_hours = max(0.0, (datetime.now(timezone.utc) - news.published_at.astimezone(timezone.utc)).total_seconds() / 3600.0)
    
    # 시간 감쇠 곡선: 지수 감쇠 적용
    freshness_decay = 0.85  # 시간당 15% 감쇠
    freshness = bounded(100 * (freshness_decay ** min(age_hours, 24)))
    
    # Novelty: 신선도와 긍정 점수에 기반
    novelty_base = 40.0
    novelty = bounded(novelty_base + freshness * 0.4 + final_pos_score * 3)
    
    # Market reaction: 시장 반응 예측
    market_reaction = bounded(55 + 6 * final_pos_score - 8 * final_neg_score)
    
    # Liquidity: 기본 유동성 (종목별 차이는 ticker_mapper에서 처리)
    liquidity = 60.0
    
    # Risk penalty: 위험 패널티
    risk_penalty = bounded(10 + 5 * final_neg_score + max(0.0, age_hours - 6) * 0.8, 0.0, 50.0)
    
    # 기술적 점수 결합 (기술적 점수가 양수면 매수 강도 강화, 음수면 억제)
    combined_pos_score = final_pos_score
    combined_neg_score = final_neg_score
    
    if tech_score > 0:
        combined_pos_score += (tech_score / 100.0) * 1.5 # 최대 1.5점 가산
    elif tech_score < 0:
        combined_neg_score += (abs(tech_score) / 100.0) * 2.0 # 차트가 나쁘면 더 엄격하게 (최대 2.0 차감 효과)
    
    components = {
        "impact": round(impact, 2),
        "source_reliability": round(source_reliability, 2),
        "novelty": round(novelty, 2),
        "market_reaction": round(market_reaction, 2),
        "liquidity": round(liquidity, 2),
        "risk_penalty": round(risk_penalty, 2),
        "freshness": round(freshness, 2),
        "positive_score": round(final_pos_score, 2),
        "negative_score": round(final_neg_score, 2),
        "tech_score": round(tech_score, 1),
        "context_adjustment": round(context_adjustment, 2),
        "age_hours": round(age_hours, 2),
    }
    
    # Priced-in flag: 시장 반영도
    if freshness >= 80:
        priced_in_flag = "LOW"
    elif freshness >= 50:
        priced_in_flag = "MEDIUM"
    else:
        priced_in_flag = "HIGH"
    
    # Decision: 의사결정 (종합 점수 기반)
    if combined_neg_score >= 4.0 or tech_rec == "SELL":
        decision = "BLOCK"
    elif combined_pos_score >= 3.0 and combined_neg_score <= 1.0:
        if tech_rec in ("BUY", "NEUTRAL"):  # 뉴스 강 + 차트 중립 이상이면 매수
            decision = "BUY"
        else:  # tech_rec == "SELL"은 위 BLOCK에서 처리됨
            decision = "HOLD"
    elif combined_pos_score >= 2.0 and combined_neg_score <= 1.5 and tech_rec == "BUY":
        decision = "BUY"  # 뉴스 보통이지만 차트가 강하면 매수
    elif combined_pos_score >= 1.5 and combined_pos_score > combined_neg_score:
        decision = "HOLD"  # 약한 매수 신호이거나 차트가 뒷받침되지 않음
    else:
        decision = "IGNORE"  # 부정이 강하거나 긍정이 너무 약함 (0 포함)
    
    return components, priced_in_flag, decision
