import time

from app.config import settings
from app.scheduler.exit_runner import run_exit_cycle
from app.storage.db import DB
from app.monitor.telegram_logger import log_and_notify


def run_exit_loop(db_path: str = "stock_trader.db", interval_sec: int | None = None) -> None:
    """Exit cycle을 주기적으로 실행하는 루프.

    Ctrl+C(KeyboardInterrupt)로 종료.
    """
    iv = int(interval_sec if interval_sec is not None else settings.exit_cycle_interval_sec)
    iv = max(1, iv)

    log_and_notify(f"EXIT_LOOP_STARTED interval_sec={iv}")
    while True:
        try:
            with DB(db_path) as db:
                db.init()
                out = run_exit_cycle(db)
            log_and_notify(f"EXIT_LOOP_TICK {out}")
        except Exception as e:
            log_and_notify(f"EXIT_LOOP_ERROR:{e}")
        time.sleep(iv)


if __name__ == "__main__":
    run_exit_loop()
