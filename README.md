# stock_trader (v1.2.3 scaffold)

초기 구현 스캐폴드입니다.

## 포함된 것
- v1.2.3 통합 DDL: `sql/schema_v1_2_3.sql`
- P0 최소 해피패스 코드
  - 뉴스 샘플 수집
  - 티커 매핑 baseline
  - 신호 정합성 검증
  - Kill Switch 기본 게이트
  - Paper Broker (시장가 전량 체결)

## 실행
```bash
cd stock_trader
python3 -m app.main
```

예상 출력:
```text
ORDER_FILLED:005930@100.0
```

## 다음 작업
- 실제 DB 연동 (repo/storage)
- 실제 뉴스 소스 커넥터
- 상태머신/재시도/리스크 트랜잭션 구현
- 테스트 코드 추가
