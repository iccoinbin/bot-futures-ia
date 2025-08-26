#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

# ativa venv
[ -f ".venv/bin/activate" ] && source .venv/bin/activate

# exige chaves em ambiente (mais seguro que YAML)
: "${BINANCE_API_KEY:?Defina BINANCE_API_KEY no ambiente}"
: "${BINANCE_API_SECRET:?Defina BINANCE_API_SECRET no ambiente}"

DRY="${1:-true}"   # true|false
TF="${2:-5m}"
shift || true
shift || true
SYMS=("$@")
if [ ${#SYMS[@]} -eq 0 ]; then
  SYMS=(BTCUSDT ETHUSDT)
fi

mkdir -p logs pids
for S in "${SYMS[@]}"; do
  LOG="logs/exec_${S}_${TF}.log"
  PIDF="pids/exec_${S}_${TF}.pid"
  nohup python -m src.scripts.executor_testnet_v33 --symbol "$S" --timeframe "$TF" --dry-run "$DRY" > "$LOG" 2>&1 &
  PID=$!
  echo $PID > "$PIDF"
  echo "[exec] $S $TF dry-run=$DRY -> PID $PID | log: $LOG"
done

echo "Use: tail -f logs/exec_<SYMBOL>_${TF}.log"
