from dataclasses import dataclass


@dataclass
class MappingResult:
    ticker: str
    company_name: str
    confidence: float
    method: str = "alias_dict"


# 한국 주식 시장 주요 종목 매핑 (KOSPI/KOSDAQ 상위 100개 종목 기반)
ALIASES = {
    # 삼성그룹
    "삼성전자": ("005930", "삼성전자", 0.98),
    "Samsung Electronics": ("005930", "삼성전자", 0.95),
    "삼성SDI": ("006400", "삼성SDI", 0.97),
    "삼성SDS": ("018260", "삼성SDS", 0.97),
    "삼성물산": ("028260", "삼성물산", 0.97),
    "삼성생명": ("032830", "삼성생명", 0.97),
    "삼성카드": ("029780", "삼성카드", 0.97),
    "삼성증권": ("016360", "삼성증권", 0.97),
    "삼성엔지니어링": ("028050", "삼성엔지니어링", 0.97),
    
    # SK그룹
    "SK하이닉스": ("000660", "SK하이닉스", 0.98),
    "SK hynix": ("000660", "SK하이닉스", 0.95),
    "SK텔레콤": ("017670", "SK텔레콤", 0.97),
    "SK이노베이션": ("096770", "SK이노베이션", 0.97),
    "SK": ("034730", "SK", 0.96),
    "SKC": ("011790", "SKC", 0.96),
    "SK네트웍스": ("001740", "SK네트웍스", 0.96),
    
    # 현대자동차그룹
    "현대차": ("005380", "현대자동차", 0.98),
    "현대자동차": ("005380", "현대자동차", 0.98),
    "Hyundai Motor": ("005380", "현대자동차", 0.95),
    "기아": ("000270", "기아", 0.98),
    "Kia": ("000270", "기아", 0.95),
    "현대모비스": ("012330", "현대모비스", 0.97),
    "Hyundai Mobis": ("012330", "현대모비스", 0.95),
    "현대글로비스": ("086280", "현대글로비스", 0.97),
    
    # 인터넷/IT
    "NAVER": ("035420", "NAVER", 0.98),
    "카카오": ("035720", "카카오", 0.98),
    "Kakao": ("035720", "카카오", 0.95),
    "카카오뱅크": ("323410", "카카오뱅크", 0.97),
    "카카오페이": ("377300", "카카오페이", 0.97),
    "쿠팡": ("035720", "쿠팡", 0.90),  # 주의: 미국 상장
    
    # LG그룹
    "LG에너지솔루션": ("373220", "LG에너지솔루션", 0.98),
    "LG화학": ("051910", "LG화학", 0.98),
    "LG전자": ("066570", "LG전자", 0.98),
    "LG유플러스": ("032640", "LG유플러스", 0.97),
    "LG생활건강": ("051900", "LG생활건강", 0.97),
    "LG이노텍": ("011070", "LG이노텍", 0.97),
    
    # 포스코
    "POSCO홀딩스": ("005490", "POSCO홀딩스", 0.98),
    "POSCO": ("005490", "POSCO홀딩스", 0.97),
    "포스코인터내셔널": ("047050", "포스코인터내셔널", 0.97),
    "포스코스틸리온": ("058430", "포스코스틸리온", 0.96),
    
    # 바이오/제약
    "셀트리온": ("068270", "셀트리온", 0.98),
    "Celltrion": ("068270", "셀트리온", 0.95),
    "삼성바이오로직스": ("207940", "삼성바이오로직스", 0.98),
    "한미약품": ("128940", "한미약품", 0.97),
    "유한양행": ("000100", "유한양행", 0.97),
    "GC녹십자": ("006280", "GC녹십자", 0.97),
    
    # 금융
    "KB금융": ("105560", "KB금융지주", 0.98),
    "신한지주": ("055550", "신한지주", 0.98),
    "하나금융지주": ("086790", "하나금융지주", 0.98),
    "우리금융지주": ("316140", "우리금융지주", 0.97),
    "NH투자증권": ("005940", "NH투자증권", 0.97),
    "미래에셋증권": ("006800", "미래에셋증권", 0.97),
    
    # 증권
    "한국투자증권": ("005930", "한국투자증권", 0.90),  # 주의: 티커 확인 필요
    "키움증권": ("039490", "키움증권", 0.97),
    "대신증권": ("003540", "대신증권", 0.97),
    
    # 건설/조선
    "현대건설": ("000720", "현대건설", 0.97),
    "대우건설": ("047040", "대우건설", 0.97),
    "GS건설": ("006360", "GS건설", 0.97),
    "현대중공업": ("009540", "현대중공업지주", 0.97),
    "대한조선": ("042660", "대한조선해양", 0.97),
    
    # 유통/서비스
    "신세계": ("004170", "신세계", 0.97),
    "이마트": ("139480", "이마트", 0.97),
    "롯데쇼핑": ("023530", "롯데쇼핑", 0.97),
    "CJ제일제당": ("097950", "CJ제일제당", 0.97),
    
    # 에너지/화학
    "S-Oil": ("010950", "S-Oil", 0.97),
    "GS칼텍스": ("078930", "GS칼텍스", 0.97),
    "한화솔루션": ("009830", "한화솔루션", 0.97),
    "롯데케미칼": ("011170", "롯데케미칼", 0.97),
    
    # 철강/비철금속
    "고려아연": ("010130", "고려아연", 0.97),
    "포스코케미칼": ("003670", "포스코케미칼", 0.97),
    
    # 운송
    "대한항공": ("003490", "대한항공", 0.97),
    "아시아나항공": ("020560", "아시아나항공", 0.97),
    "한진칼": ("180640", "한진칼", 0.97),
    
    # 미디어/엔터테인먼트
    "CJ ENM": ("035760", "CJ ENM", 0.97),
    "SM엔터테인먼트": ("041510", "SM엔터테인먼트", 0.97),
    "YG엔터테인먼트": ("122870", "YG엔터테인먼트", 0.97),
    "JYP엔터테인먼트": ("035900", "JYP엔터테인먼트", 0.97),
    
    # 반도체/전자
    "DB하이텍": ("000990", "DB하이텍", 0.97),
    "한미반도체": ("042700", "한미반도체", 0.97),
    "아이에이": ("038880", "아이에이", 0.96),
    
    # 애매한 매핑 (주의 필요)
    "삼성": ("", "AMBIGUOUS", 0.20),
    "현대": ("", "AMBIGUOUS", 0.20),
    "LG": ("", "AMBIGUOUS", 0.20),
    "SK": ("034730", "SK", 0.70),  # SK주식회사로 매핑 (신뢰도 낮음)
}


def map_ticker(text: str) -> MappingResult | None:
    """
    텍스트에서 종목명을 찾아 티커로 매핑합니다.
    
    Args:
        text: 뉴스 제목이나 본문 텍스트
        
    Returns:
        MappingResult 또는 None (매핑 실패 시)
    """
    # 텍스트 정규화
    normalized_text = text.lower().replace(" ", "")
    lower_text = text.lower()
    
    # 1. 정확한 매칭 시도 (긴 문자열 우선)
    exact_matches = []
    for alias, (ticker, name, confidence) in ALIASES.items():
        if ticker == "":  # 애매한 매핑 스킵
            continue
        # 원문(공백 포함)과 정규화(공백 제거) 모두 시도
        alias_lower = alias.lower()
        alias_normalized = alias_lower.replace(" ", "")
        if alias_lower in lower_text or alias_normalized in normalized_text:
            exact_matches.append((len(alias), alias, ticker, name, confidence))
    
    if exact_matches:
        # 가장 긴 정확한 매칭 선택
        exact_matches.sort(reverse=True, key=lambda x: x[0])
        _, alias, ticker, name, confidence = exact_matches[0]
        
        # 애매한 매핑 차단: 매칭된 alias 자체가 짧은 애매한 키워드인 경우만 차단
        # (예: "SK"가 매칭되면 차단, "SK하이닉스"가 매칭되면 통과)
        ambiguous_short_forms = {"삼성", "현대", "LG", "SK"}
        is_ambiguous = alias in ambiguous_short_forms
        
        if not is_ambiguous:
            return MappingResult(ticker=ticker, company_name=name, confidence=confidence, method="exact_match")
    
    # 2. 부분 매칭 시도 (신뢰도 낮춤)
    for alias, (ticker, name, confidence) in ALIASES.items():
        if ticker == "":
            continue
        
        # 별도 처리: "삼성", "현대", "LG", "SK"는 부분 매칭에서 제외
        if alias in ["삼성", "현대", "LG", "SK"]:
            continue
            
        # 단어 경계를 고려한 부분 매칭
        words = text.split()
        for word in words:
            if alias in word and len(alias) >= 3:  # 최소 3글자 이상 매칭
                # 신뢰도 조정: 부분 매칭이므로 신뢰도 낮춤
                adjusted_confidence = confidence * 0.7
                return MappingResult(
                    ticker=ticker, 
                    company_name=name, 
                    confidence=adjusted_confidence,
                    method="partial_match"
                )
    
    # 3. 티커 직접 검색 (예: "005930" 형태)
    import re
    ticker_pattern = r'\b(\d{6})\b'
    ticker_matches = re.findall(ticker_pattern, text)
    
    for ticker_match in ticker_matches:
        # 알려진 티커인지 확인
        for alias, (known_ticker, name, confidence) in ALIASES.items():
            if known_ticker == ticker_match:
                return MappingResult(
                    ticker=known_ticker,
                    company_name=name,
                    confidence=confidence * 0.9,  # 티커 직접 매칭은 높은 신뢰도
                    method="ticker_direct"
                )
    
    return None
