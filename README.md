# stock_trader (v1.2.3 scaffold)

초기 구현 스캐폴드입니다.

## 포함된 것
- v1.2.3 통합 DDL: `sql/schema_v1_2_3.sql`
- SQLite 기반 로컬 E2E 어댑터 (`app/storage/db.py`)
- 트랜잭션 분리 메인 플로우
  - Tx#1: 뉴스 수집/매핑/신호 저장
  - Tx#2: 리스크 게이트/주문/포지션 OPEN
  - Tx#3: 샘플 청산(OPEN -> CLOSED)
- 텔레그램 로그 알림 (`app/monitor/telegram_logger.py`)
- 테스트 10종 (lifecycle/risk/main flow)

## 실행
```bash
cd stock_trader
python3 -m app.main
```

예상 출력(예시):
```text
ORDER_FILLED:005930@83500.0 (signal_id=1, position_id=1, entry_event_id=1)
POSITION_CLOSED:1 reason=TIME_EXIT
```

중복 실행 시:
```text
DUP_NEWS_SKIPPED
```

## 테스트
```bash
cd stock_trader
python3 -m unittest discover -s tests -v
```

## 다음 작업(P1)
- PostgreSQL 실제 연동으로 전환
- scorer 가중치 `parameter_registry` 연동
- 상태 전이 가드 강화(IllegalTransitionError)
- repository 분리(`NewsRepo`, `OrderRepo`, `PositionRepo`)
