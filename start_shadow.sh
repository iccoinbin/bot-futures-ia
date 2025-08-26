#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

# ativa venv se existir
[ -f ".venv/bin/activate" ] && source .venv/bin/activate

TF="${1:-5m}"
shift || true
SYMS=("$@")
if [ ${#SYMS[@]} -eq 0 ]; then
  SYMS=(BTCUSDT ETHUSDT)
fi

mkdir -p logs pids
for S in "${SYMS[@]}"; do
  LOG="logs/shadow_${S}_${TF}.log"
  PIDF="pids/shadow_${S}_${TF}.pid"
  nohup python -m src.scripts.run_shadow_v33 --symbol "$S" --timeframe "$TF" > "$LOG" 2>&1 &
  PID=$!
  echo $PID > "$PIDF"
  echo "[shadow] $S $TF -> PID $PID | log: $LOG"
done

echo "Use: tail -f logs/shadow_<SYMBOL>_${TF}.log"
