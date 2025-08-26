#!/usr/bin/env bash
set -euo pipefail

mkdir -p logs

MODE="${MODE:-shadow}"
SYMS="${SYMBOLS:-BTCUSDT}"
TF="${TIMEFRAME:-5m}"

echo ">> Iniciando modo: $MODE | symbols=${SYMS} | tf=${TF}"

pids=()

run_one() {
  local sym="$1"
  local logfile="logs/${MODE}_${sym}_${TF}.log"
  : > "$logfile"
  echo "[start] $MODE $sym tf=${TF}" | tee -a "$logfile"

  if [ "$MODE" = "executor" ]; then
    cmd=(python -u -m src.scripts.executor_testnet_v33 --symbol "$sym" --timeframe "$TF")
    if [ -n "${DRY_RUN:-}" ]; then cmd+=("--dry-run" "$DRY_RUN"); fi
  else
    cmd=(python -u -m src.scripts.run_shadow_v33 --symbol "$sym" --timeframe "$TF")
  fi

  stdbuf -oL -eL "${cmd[@]}" 2>&1 | tee -a "$logfile" &
  pids+=("$!")
}

IFS=',' read -ra ARR <<< "$SYMS"
for s in "${ARR[@]}"; do
  s="$(echo "$s" | xargs)"   # trim
  [ -n "$s" ] && run_one "$s"
done

wait -n || true
wait || true
