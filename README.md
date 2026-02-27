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
cp .env.example .env
python3 -m app.main
```

환경 변수는 `.env`(또는 로컬 전용 `.env.local`)에 설정하세요.
- Telegram: `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`
- KIS(한국투자증권): `KIS_APP_KEY`, `KIS_APP_SECRET`, `KIS_ACCOUNT_NO`, `KIS_PRODUCT_CODE`, `KIS_MODE`
- `.env`/`.env.local`은 커밋되지 않고, `.env.example`만 커밋됩니다.

## 브로커 연동 방향
- 현재 기본 실행은 `PaperBroker`(모의 브로커) 기반입니다.
- 실거래 브로커는 **한국투자증권(KIS)** 기준으로 진행합니다.
- `app/execution/kis_broker.py`에 KIS 연동 스켈레톤을 추가해 두었고,
  다음 단계에서 인증 토큰 발급/주문 API 연결을 붙일 예정입니다.

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
