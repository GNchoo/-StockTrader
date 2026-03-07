from datetime import datetime, timezone, timedelta


# 한국 표준시(KST) 오프셋
KST = timezone(timedelta(hours=9))


def parse_utc_ts(ts: str | None) -> datetime | None:
    if not ts:
        return None
    s = str(ts).strip()
    if not s:
        return None

    dt = None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        pass

    if dt is None:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
            try:
                dt = datetime.strptime(s, fmt)
                break
            except Exception:
                continue

    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def is_market_open(now: datetime | None = None) -> bool:
    """한국 주식시장(KOSPI/KOSDAQ) 정규장 시간인지 확인.

    정규장: 월~금 09:00~15:30 KST (공휴일 미반영 — 추후 확장)
    """
    kst_now = (now or datetime.now(KST)).astimezone(KST)
    # 토·일 제외
    if kst_now.weekday() >= 5:
        return False
    t = kst_now.time()
    from datetime import time as _time
    return _time(8, 59) <= t <= _time(15, 30)
