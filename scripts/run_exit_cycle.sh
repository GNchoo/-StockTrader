#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PYTHONPATH="$ROOT" python3 - <<'PY'
from app.storage.db import DB
from app.scheduler.exit_runner import run_exit_cycle

with DB("stock_trader.db") as db:
    db.init()
    out = run_exit_cycle(db)
print(out)
PY
