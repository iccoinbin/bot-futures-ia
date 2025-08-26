#!/usr/bin/env bash
set -e
cd "$(dirname "$0")/.." || exit 0

export PGPASSWORD="$(grep -E '^DB_PASSWORD=' .env | cut -d= -f2-)"
DBH="$(grep -E '^DB_HOST=' .env | cut -d= -f2-)"; DBH="${DBH:-localhost}"
DBP="$(grep -E '^DB_PORT=' .env | cut -d= -f2-)"; DBP="${DBP:-5432}"
DBN="$(grep -E '^DB_NAME=' .env | cut -d= -f2-)"; DBN="${DBN:-botfutures}"
DBU="$(grep -E '^DB_USER=' .env | cut -d= -f2-)"; DBU="${DBU:-botfutures_user}"

PSQLRC=/dev/null psql -X -P pager=off -h "$DBH" -p "$DBP" -U "$DBU" -d "$DBN" -c "
SELECT symbol, timeframe,
       COUNT(*) AS total_rows,
       MAX(ts)   AS last_feature
FROM public.features
GROUP BY symbol, timeframe
ORDER BY symbol, timeframe;"
