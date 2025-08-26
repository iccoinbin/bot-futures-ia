#!/usr/bin/env bash
set -Eeuo pipefail
cd /home/iccoin_bin/bot-futures-ia

LOG_DIR="logs"
mkdir -p "$LOG_DIR"

# Garante arquivos de log
touch "$LOG_DIR/dbcheck.log" "$LOG_DIR/dbcheck.err"

while true; do
  # Espera o Postgres do compose estar pronto
  if ! docker compose -f docker-compose.datahub.yml exec -T pg pg_isready -U bot -d market >/dev/null 2>&1; then
    echo "$(date '+%F %T') [DBCHECK] Postgres não pronto, tentando novamente em 10s..." >> "$LOG_DIR/dbcheck.log"
    sleep 10
    continue
  fi

  # Consulta rápida de saúde (top 5 por contagem)
  RES="$(docker compose -f docker-compose.datahub.yml exec -T pg \
    psql -U bot -d market -At -c \
    "SELECT symbol||' '||interval||' '||COUNT(*) FROM md_candles GROUP BY 1,2 ORDER BY 3 DESC LIMIT 5;")"

  echo "$(date '+%F %T') [DBCHECK] $RES" >> "$LOG_DIR/dbcheck.log"
  sleep 60
done
