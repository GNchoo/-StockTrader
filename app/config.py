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


settings = Settings()
