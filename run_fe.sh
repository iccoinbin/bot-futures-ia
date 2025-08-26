cd "$HOME/bot-futures-ia" || exit 1
. .venv/bin/activate || { echo "Venv não encontrada"; exit 1; }
[ -f .env ] && set -a && . .env && set +a
export PYTHONPATH="$PWD:${PYTHONPATH:-}"; mkdir -p logs/feature_engine; TS=$(date +'%Y%m%d_%H%M%S'); LOG="logs/feature_engine/fe_v1_${TS}.log"
for EP in src/scripts/feature_engine_v1.py src/feature_engine/run_feature_engine_v1.py src/feature_engine/main_v1.py src/feature_engine/run_v1.py scripts/feature_engine_v1.sh; do [ -f "$EP" ] && FOUND="$EP" && break; done
[ -z "${FOUND:-}" ] && echo "Script da Feature Engine v1 não encontrado" && exit 2
echo "[OK] Script: $FOUND"; echo "[LOG] $LOG"
if echo "$FOUND" | grep -q '\.sh$'; then bash "$FOUND" --symbols BTCUSDT,ETHUSDT --timeframes 1m,5m --lookback-minutes 180 --batch-size 5000 --commit-every 2000 --feature-set v1 --sink postgres --table features --ignore-missing-order-flow 2>&1 | tee "$LOG"; else python "$FOUND" --symbols BTCUSDT,ETHUSDT --timeframes 1m,5m --lookback-minutes 180 --batch-size 5000 --commit-every 2000 --feature-set v1 --sink postgres --table features --ignore-missing-order-flow 2>&1 | tee "$LOG"; fi
tail -n 30 "$LOG" || true
