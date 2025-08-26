#!/usr/bin/env bash
set -euo pipefail
SERVICE="feature-engine-v1"
REPO="/home/iccoin_bin/bot-futures-ia"
OUT="/home/iccoin_bin/FEATURE_STATUS.txt"
ALR="/home/iccoin_bin/FEATURE_ALERTS.txt"
LOG="/var/log/bot-futures-ia/${SERVICE}.log"

MAX_AGE_MIN_DEFAULT=15
MAX_ERROR_LINES_DEFAULT=200
ERR_PATTERNS_DEFAULT="ERROR|EXC|Traceback|MISSING_TS"

DBH="localhost"; DBP="5432"; DBN="botfutures"; DBU="botfutures_user"
MAX_AGE_MIN="$MAX_AGE_MIN_DEFAULT"
MAX_ERROR_LINES="$MAX_ERROR_LINES_DEFAULT"
ERR_PATTERNS="$ERR_PATTERNS_DEFAULT"
if [[ -f "${REPO}/.env" ]]; then
  export PGPASSWORD="$(grep -E '^DB_PASSWORD=' "${REPO}/.env" | cut -d= -f2-)"
  DBH="$(grep -E '^DB_HOST=' "${REPO}/.env" | cut -d= -f2- || echo localhost)"
  DBP="$(grep -E '^DB_PORT=' "${REPO}/.env" | cut -d= -f2- || echo 5432)"
  DBN="$(grep -E '^DB_NAME=' "${REPO}/.env" | cut -d= -f2- || echo botfutures)"
  DBU="$(grep -E '^DB_USER=' "${REPO}/.env" | cut -d= -f2- || echo botfutures_user)"
  MAX_AGE_MIN="$(grep -E '^FEATURE_MAX_AGE_MIN=' "${REPO}/.env" | cut -d= -f2- || true)"; MAX_AGE_MIN="${MAX_AGE_MIN:-$MAX_AGE_MIN_DEFAULT}"
  MAX_ERROR_LINES="$(grep -E '^FEATURE_MAX_ERROR_LINES=' "${REPO}/.env" | cut -d= -f2- || true)"; MAX_ERROR_LINES="${MAX_ERROR_LINES:-$MAX_ERROR_LINES_DEFAULT}"
  ERR_PATTERNS="$(grep -E '^FEATURE_ERR_PATTERNS=' "${REPO}/.env" | cut -d= -f2- || true)"; ERR_PATTERNS="${ERR_PATTERNS:-$ERR_PATTERNS_DEFAULT}"
fi

TMP_DB="$(mktemp)"
PSQLRC=/dev/null psql -X -P pager=off -h "$DBH" -p "$DBP" -U "$DBU" -d "$DBN" -A -F $'\t' -t -c \
"WITH last AS (
  SELECT symbol,timeframe, MAX(ts) AS last_ts
  FROM public.features
  GROUP BY 1,2
)
SELECT symbol, timeframe, last_ts,
       EXTRACT(EPOCH FROM ((now() AT TIME ZONE 'UTC') - last_ts))::bigint AS age_secs
FROM last
ORDER BY symbol, timeframe;" > "$TMP_DB" 2>/dev/null || true

TAIL_OUT="$(mktemp)"
if [[ -f "$LOG" ]]; then
  tail -n "$MAX_ERROR_LINES" "$LOG" > "$TAIL_OUT" || true
else
  journalctl -u "${SERVICE}.service" -n "$MAX_ERROR_LINES" --no-pager > "$TAIL_OUT" || true
fi
ERR_COUNT="$(grep -E -c "$ERR_PATTERNS" "$TAIL_OUT" || true)"
TS_ERR_COUNT="$(grep -E -c "'ts'|MISSING_TS" "$TAIL_OUT" || true)"

NOW="$(date -Is)"
ALERTS=()
ENABLED="$(sudo -n systemctl is-enabled ${SERVICE} 2>/dev/null || true)"
ACTIVE="$(sudo -n systemctl is-active ${SERVICE} 2>/dev/null || true)"
if [[ "$ENABLED" != "enabled" ]]; then ALERTS+=("SERVIÇO: não habilitado ($ENABLED)"); fi
if [[ "$ACTIVE" != "active" ]]; then ALERTS+=("SERVIÇO: não ativo ($ACTIVE)"); fi

OVERS="$(mktemp)"
if [[ -s "$TMP_DB" ]]; then
  awk -v thr_min="$MAX_AGE_MIN" -F '\t' '
    { age_min=int($4/60); if(age_min>thr_min){ printf("%s\t%s\t%s\t%d\n",$1,$2,$3,age_min) } }
  ' "$TMP_DB" > "$OVERS"
  if [[ -s "$OVERS" ]]; then ALERTS+=("ATRASO: atraso > ${MAX_AGE_MIN} min detectado"); fi
else
  ALERTS+=("DB: não foi possível ler últimos timestamps da tabela features")
fi

if [[ "${ERR_COUNT:-0}" -gt 0 ]]; then ALERTS+=("LOG: ${ERR_COUNT} erros recentes"); fi
if [[ "${TS_ERR_COUNT:-0}" -gt 0 ]]; then ALERTS+=("LOG: ${TS_ERR_COUNT} falhas envolvendo coluna TS"); fi

{
  echo "==== FEATURE ENGINE STATUS ${NOW} ===="
  if [[ "${#ALERTS[@]}" -gt 0 ]]; then
    echo "ALERTAS:"; for a in "${ALERTS[@]}"; do echo " - ${a}"; done; echo
  else
    echo "ALERTAS: nenhum"; echo
  fi
  echo "-- Serviço --"
  echo "enabled: ${ENABLED}"
  echo "active : ${ACTIVE}"
  echo
  echo "-- Últimas 15 linhas de log --"
  tail -n 15 "$TAIL_OUT" 2>/dev/null || echo "sem log"
  echo
  echo "-- DB atraso por par/timeframe --"
  if [[ -s "$TMP_DB" ]]; then
    awk -F '\t' '{printf "%-10s %-7s %-25s %d\n",$1,$2,$3,int($4/60)}' "$TMP_DB"
  fi
  if [[ -s "$OVERS" ]]; then
    echo
    echo "-- ATRASOS detectados --"
    awk -F '\t' '{printf "%-10s %-7s %-25s %d\n",$1,$2,$3,$4}' "$OVERS"
  fi
} > "$OUT"

{
  echo "==== ALERTS ${NOW} ===="
  if [[ "${#ALERTS[@]}" -gt 0 ]]; then for a in "${ALERTS[@]}"; do echo "- ${a}"; done
  else echo "- Nenhum alerta"; fi
} > "$ALR"

rm -f "$TMP_DB" "$TAIL_OUT" "$OVERS"
