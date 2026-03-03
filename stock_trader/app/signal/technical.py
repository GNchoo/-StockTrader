"""
기술적 지표 모듈 — 주식 가격 데이터 기반 분석

CoinTrader의 strategy_engine.py에서 검증된 지표들을 주식 시장에 적용.
뉴스 기반 신호와 결합하여 더 정확한 매매 판단을 지원합니다.
"""

from typing import Optional


def sma(values: list[float], period: int) -> Optional[float]:
    """단순 이동평균 (Simple Moving Average)"""
    if len(values) < period:
        return None
    return sum(values[-period:]) / period


def ema(values: list[float], period: int) -> Optional[float]:
    """지수 이동평균 (Exponential Moving Average)"""
    if len(values) < period:
        return None
    k = 2 / (period + 1)
    result = sum(values[:period]) / period
    for v in values[period:]:
        result = v * k + result * (1 - k)
    return result


def rsi(closes: list[float], period: int = 14) -> Optional[float]:
    """Relative Strength Index (상대강도지수)"""
    if len(closes) < period + 1:
        return None
    gains = 0.0
    losses = 0.0
    for i in range(1, period + 1):
        diff = closes[-period - 1 + i] - closes[-period - 1 + i - 1]
        if diff > 0:
            gains += diff
        else:
            losses -= diff
    if losses == 0:
        return 100.0
    rs = (gains / period) / (losses / period)
    return 100 - (100 / (1 + rs))


def bollinger_position(closes: list[float], period: int = 20, std_mult: float = 2.0) -> Optional[float]:
    """볼린저 밴드 내 현재 위치 (0=하단, 0.5=중간, 1=상단)"""
    if len(closes) < period:
        return None
    window = closes[-period:]
    mean = sum(window) / period
    std = (sum((x - mean) ** 2 for x in window) / period) ** 0.5
    if std == 0:
        return 0.5
    upper = mean + std_mult * std
    lower = mean - std_mult * std
    band_width = upper - lower
    if band_width == 0:
        return 0.5
    return min(1.0, max(0.0, (closes[-1] - lower) / band_width))


def compute_technical_score(closes: list[float]) -> dict:
    """
    기술적 지표 종합 점수 산출
    
    Returns:
        {
            "score": -100 ~ +100 (음수=매도, 양수=매수),
            "rsi": RSI 값,
            "sma5_above_sma20": bool,
            "bb_position": 볼린저 밴드 내 위치,
            "recommendation": "BUY" | "SELL" | "NEUTRAL"
        }
    """
    if len(closes) < 25:
        return {
            "score": 0,
            "rsi": None,
            "sma5_above_sma20": None,
            "bb_position": None,
            "recommendation": "NEUTRAL",
        }
    
    score = 0.0
    
    # 1. RSI 분석 (0-100)
    rsi_val = rsi(closes, 14)
    if rsi_val is not None:
        if rsi_val < 30:
            score += 30  # 과매도 → 매수 기회
        elif rsi_val < 40:
            score += 15
        elif rsi_val > 70:
            score -= 30  # 과매수 → 매도 신호
        elif rsi_val > 60:
            score -= 15
    
    # 2. MA 교차 분석
    ma5 = sma(closes, 5)
    ma20 = sma(closes, 20)
    sma5_above = None
    if ma5 is not None and ma20 is not None:
        sma5_above = ma5 > ma20
        gap_pct = (ma5 - ma20) / ma20 * 100
        if sma5_above:
            score += min(20, gap_pct * 10)  # 골든크로스 → 매수
        else:
            score -= min(20, abs(gap_pct) * 10)  # 데드크로스 → 매도
    
    # 3. 볼린저 밴드 위치
    bb_pos = bollinger_position(closes, 20)
    if bb_pos is not None:
        if bb_pos < 0.1:
            score += 20  # 하단 이탈 → 반등 가능
        elif bb_pos < 0.3:
            score += 10
        elif bb_pos > 0.9:
            score -= 20  # 상단 이탈 → 과매수
        elif bb_pos > 0.7:
            score -= 10
    
    # 4. 모멘텀 (최근 5일 방향)
    if len(closes) >= 6:
        mom = (closes[-1] - closes[-6]) / closes[-6] * 100
        score += max(-15, min(15, mom * 5))
    
    # 범위 제한
    score = max(-100, min(100, score))
    
    # 추천
    if score >= 25:
        rec = "BUY"
    elif score <= -25:
        rec = "SELL"
    else:
        rec = "NEUTRAL"
    
    return {
        "score": round(score, 1),
        "rsi": round(rsi_val, 1) if rsi_val else None,
        "sma5_above_sma20": sma5_above,
        "bb_position": round(bb_pos, 3) if bb_pos else None,
        "recommendation": rec,
    }
