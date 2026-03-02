from dataclasses import dataclass
import os
from pathlib import Path


def _parse_env_file(env_path: Path) -> None:
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def _load_local_env() -> None:
    base = Path(__file__).resolve().parents[1]

    # python-dotenv 사용 가능하면 .env + .env.local 자동 로드
    try:
        from dotenv import load_dotenv
        load_dotenv(dotenv_path=base / ".env")
        load_dotenv(dotenv_path=base / ".env.local")
    except Exception:
        _parse_env_file(base / ".env")
        _parse_env_file(base / ".env.local")


_load_local_env()


@dataclass(frozen=True)
class Settings:
    database_url: str = os.getenv("DATABASE_URL", "postgresql://localhost:5432/stock_trader")
    min_map_confidence: float = float(os.getenv("MIN_MAP_CONFIDENCE", "0.92"))
    risk_penalty_cap: float = float(os.getenv("RISK_PENALTY_CAP", "30"))
    telegram_bot_token: str = os.getenv("TELEGRAM_BOT_TOKEN", os.getenv("TelegramBotToken", ""))
    telegram_chat_id: str = os.getenv("TELEGRAM_CHAT_ID", os.getenv("TelegramChatId", ""))

    # 브로커 선택: paper | kis
    broker: str = os.getenv("BROKER", "paper").lower()

    # KIS (한국투자증권) 연동 설정
    kis_app_key: str = os.getenv("KIS_APP_KEY", "")
    kis_app_secret: str = os.getenv("KIS_APP_SECRET", "")
    kis_account_no: str = os.getenv("KIS_ACCOUNT_NO", "")
    kis_product_code: str = os.getenv("KIS_PRODUCT_CODE", "01")
    kis_mode: str = os.getenv("KIS_MODE", "paper")
    kis_base_url: str = os.getenv("KIS_BASE_URL", "")

    # Demo behavior
    enable_demo_auto_close: bool = os.getenv("ENABLE_DEMO_AUTO_CLOSE", "0").strip().lower() in {"1", "true", "yes", "on"}

    # Scheduler
    exit_cycle_interval_sec: int = int(os.getenv("EXIT_CYCLE_INTERVAL_SEC", "60"))

    # Risk limits
    risk_max_loss_per_trade: float = float(os.getenv("RISK_MAX_LOSS_PER_TRADE", "30000"))
    risk_daily_loss_limit: float = float(os.getenv("RISK_DAILY_LOSS_LIMIT", "100000"))
    risk_max_exposure_per_symbol: float = float(os.getenv("RISK_MAX_EXPOSURE_PER_SYMBOL", "300000"))
    risk_max_concurrent_positions: int = int(os.getenv("RISK_MAX_CONCURRENT_POSITIONS", "3"))
    risk_loss_streak_cooldown: int = int(os.getenv("RISK_LOSS_STREAK_COOLDOWN", "3"))
    risk_cooldown_minutes: int = int(os.getenv("RISK_COOLDOWN_MINUTES", "60"))
    risk_assumed_stop_loss_pct: float = float(os.getenv("RISK_ASSUMED_STOP_LOSS_PCT", "0.015"))

    # News ingestion
    news_mode: str = os.getenv("NEWS_MODE", "sample").lower()  # sample | rss
    news_rss_url: str = os.getenv("NEWS_RSS_URL", "https://www.mk.co.kr/rss/30000001/")


settings = Settings()
