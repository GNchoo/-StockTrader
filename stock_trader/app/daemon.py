import time
from datetime import datetime
import traceback

from app.storage.db import DB
from app.config import settings
from app.monitor.telegram_logger import log_and_notify
from app.signal.ingest import ingest_and_create_signal
from app.execution.runtime import build_broker
from app.execution.entry import execute_signal_impl
from app.execution.sync import sync_pending_entries_impl, sync_pending_exits_impl
from app.execution.triggers import (
    trigger_trailing_stop_orders_impl,
    trigger_opposite_signal_exit_orders_impl,
    trigger_time_exit_orders_impl,
)
from app.common.timeutil import parse_utc_ts
from app.main import (
    _build_broker,
    _resolve_expected_price,
    _sync_entry_order_once,
    _sync_exit_order_once,
)


def daemon_loop():
    broker = build_broker()
    log_and_notify(f"🚀 StockTrader Daemon Started [Mode: {settings.broker}]")

    db = DB("stock_trader.db")
    db.init()

    # 초당 주기 (API 제한 및 리소스 절약 위해 1분마다 뉴스 확인)
    POLL_INTERVAL = 60
    
    # 익절 주기 (TRAILING STOP)
    TRAIL_INTERVAL = 30
    
    last_news_poll = 0
    last_trail_poll = 0

    while True:
        try:
            now = time.time()
            
            # --- 1. 뉴스 갱신 및 신호 생성 ---
            if now - last_news_poll >= POLL_INTERVAL:
                last_news_poll = now
                
                bundle = ingest_and_create_signal(db, log_and_notify)
                if bundle:
                    signal_id = bundle["signal_id"]
                    ticker = bundle["ticker"]
                    log_and_notify(f"💡 New Signal Generated: {ticker} (ID: {signal_id})")
                    
                    # 매수 실행
                    status = execute_signal_impl(
                        db,
                        signal_id,
                        ticker,
                        qty=1.0, # 기본 매수 단위. engine.py가 target_position_value를 바탕으로 보정함
                        demo_auto_close=False,
                        _build_broker=_build_broker,
                        _resolve_expected_price=_resolve_expected_price,
                        _sync_entry_order_once=_sync_entry_order_once,
                        log_and_notify=log_and_notify,
                        settings=settings,
                    )
                    log_and_notify(f"🛒 Execute Signal Status: {status}")
            
            # --- 2. 체결 상태 동기화 ---
            sync_pending_entries_impl(
                db,
                limit=100,
                broker=broker,
                _build_broker=_build_broker,
                _resolve_expected_price=_resolve_expected_price,
                _sync_entry_order_once=_sync_entry_order_once,
                log_and_notify=log_and_notify,
            )
            
            sync_pending_exits_impl(
                db,
                limit=100,
                broker=broker,
                _build_broker=_build_broker,
                _sync_exit_order_once=_sync_exit_order_once,
            )

            # --- 3. 위험 관리 및 강제 청산 루프 ---
            if now - last_trail_poll >= TRAIL_INTERVAL:
                last_trail_poll = now
                
                # 트레일링 스탑
                trigger_trailing_stop_orders_impl(
                    db,
                    current_prices=None, # 매번 수집
                    trailing_arm_pct=0.01, # 1% 수익 발생 시 트레일링 활성화
                    trailing_gap_pct=0.005, # 최고점 대비 0.5% 하락 시 매도
                    limit=100,
                    broker=broker,
                    _build_broker=_build_broker,
                    _sync_exit_order_once=_sync_exit_order_once,
                    log_and_notify=log_and_notify,
                )
                
                # 보유 시간 제한 청산 (기본값 설정된 90분 등)
                # 한국 주식 시장은 정규장 시간이 있으므로 길게 가져갈 수도 있음.
                trigger_time_exit_orders_impl(
                    db,
                    max_hold_min=60 * 6, # 6시간 (1일 단타 기준)
                    limit=100,
                    broker=broker,
                    _build_broker=_build_broker,
                    _resolve_expected_price=_resolve_expected_price,
                    _parse_sqlite_ts=parse_utc_ts,
                    _sync_exit_order_once=_sync_exit_order_once,
                    log_and_notify=log_and_notify,
                )
                
            time.sleep(5)
            
        except KeyboardInterrupt:
            log_and_notify("🛑 Daemon gracefully stopping by KeyboardInterrupt.")
            break
        except Exception as e:
            err_msg = traceback.format_exc()
            log_and_notify(f"⚠️ Daemon Exception: {e}\n{err_msg[:500]}")
            time.sleep(15)

    db.close()

if __name__ == "__main__":
    daemon_loop()
