from dataclasses import dataclass
import os


@dataclass(frozen=True)
class Settings:
    database_url: str = os.getenv("DATABASE_URL", "postgresql://localhost:5432/stock_trader")
    min_map_confidence: float = float(os.getenv("MIN_MAP_CONFIDENCE", "0.92"))
    risk_penalty_cap: float = float(os.getenv("RISK_PENALTY_CAP", "30"))


settings = Settings()
