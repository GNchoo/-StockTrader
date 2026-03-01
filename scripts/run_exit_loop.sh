#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PYTHONPATH="$ROOT" python3 -m app.scheduler.loop_runner
