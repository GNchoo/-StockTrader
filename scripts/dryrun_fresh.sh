#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PYTHONPATH="$ROOT" python3 - <<'PY'
import tempfile
from pathlib import Path
from app.storage.db import DB
from app.main import ingest_and_create_signal, execute_signal

with tempfile.TemporaryDirectory() as td:
    db_path = Path(td) / "dryrun.db"
    with DB(str(db_path)) as db:
        db.init()
        bundle = ingest_and_create_signal(db)
        if not bundle:
            raise SystemExit("Dry-run failed: no signal bundle")
        ok = execute_signal(db, bundle["signal_id"], bundle["ticker"], qty=1.0)
        if not ok:
            raise SystemExit("Dry-run failed: execute_signal returned False")
        print(f"Dry-run OK: signal_id={bundle['signal_id']} ticker={bundle['ticker']}")
PY
