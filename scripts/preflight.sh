#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PYTHONPATH="$ROOT" python3 - <<'PY'
from app.config import settings
from app.execution.paper_broker import PaperBroker
from app.execution.kis_broker import KISBroker

broker_name = (settings.broker or "paper").lower()
broker = KISBroker() if broker_name == "kis" else PaperBroker()
health = broker.health_check()

print("[Preflight] broker:", broker_name)
print("[Preflight] health:", health)
print("[Preflight] min_map_confidence:", settings.min_map_confidence)
print("[Preflight] risk_penalty_cap:", settings.risk_penalty_cap)

status = str(health.get("status", "")).upper()
if status == "CRITICAL":
    raise SystemExit("Preflight FAILED: broker health is CRITICAL")

print("Preflight OK")
PY
