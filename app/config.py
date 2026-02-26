from dataclasses import dataclass
import os
from pathlib import Path


def _load_local_env() -> None:
    env_path = Path(__file__).resolve().parents[1] / ".env.local"
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip())


_load_local_env()


@dataclass(frozen=True)
class Settings:
    database_url: str = os.getenv("DATABASE_URL", "postgresql://localhost:5432/stock_trader")
    min_map_confidence: float = float(os.getenv("MIN_MAP_CONFIDENCE", "0.92"))
    risk_penalty_cap: float = float(os.getenv("RISK_PENALTY_CAP", "30"))
    telegram_bot_token: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    telegram_chat_id: str = os.getenv("TELEGRAM_CHAT_ID", "")


settings = Settings()
