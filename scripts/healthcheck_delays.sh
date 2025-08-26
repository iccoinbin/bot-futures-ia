#!/usr/bin/env bash
# Compara last_ts de candles e features por símbolo/TF e mostra delays.
set -e
cd "$(dirname "$0")/.." || exit 0

export PGPASSWORD="$(grep -E '^DB_PASSWORD=' .env | cut -d= -f2-)"
DBH="$(grep -E '^DB_HOST=' .env | cut -d= -f2-)"; DBH="${DBH:-localhost}"
DBP="$(grep -E '^DB_PORT=' .env | cut -d= -f2-)"; DBP="${DBP:-5432}"
DBN="$(grep -E '^DB_NAME=' .env | cut -d= -f2-)"; DBN="${DBN:-botfutures}"
DBU="$(grep -E '^DB_USER=' .env | cut -d= -f2-)"; DBU="${DBU:-botfutures_user}"

# thresholds (minutos) por timeframe; ajuste se quiser
TH_1M=${TH_1M:-5}
TH_5M=${TH_5M:-15}
TH_15M=${TH_15M:-45}
TH_1H=${TH_1H:-180}

PSQLRC=/dev/null psql -X -P pager=off -h "$DBH" -p "$DBP" -U "$DBU" -d "$DBN" -c "
WITH c AS (
  SELECT symbol, timeframe, MAX(ts) AS last_candle FROM public.candles GROUP BY symbol,timeframe
),
f AS (
  SELECT symbol, timeframe, MAX(ts) AS last_feature FROM public.features GROUP BY symbol,timeframe
),
j AS (
  SELECT COALESCE(c.symbol,f.symbol) AS symbol,
         COALESCE(c.timeframe,f.timeframe) AS timeframe,
         c.last_candle, f.last_feature, NOW() AS now_ts
  FROM c FULL OUTER JOIN f
    ON c.symbol=f.symbol AND c.timeframe=f.timeframe
)
SELECT symbol, timeframe,
       last_candle, last_feature,
       (now_ts - last_candle)  AS delay_candles,
       (now_ts - last_feature) AS delay_features,
       CASE
         WHEN timeframe='1m'  AND (now_ts - last_feature) <= (TH_1M  || ' minutes')::interval THEN 'OK'
         WHEN timeframe='5m'  AND (now_ts - last_feature) <= (TH_5M  || ' minutes')::interval THEN 'OK'
         WHEN timeframe='15m' AND (now_ts - last_feature) <= (TH_15M || ' minutes')::interval THEN 'OK'
         WHEN timeframe='1h'  AND (now_ts - last_feature) <= (TH_1H  || ' minutes')::interval THEN 'OK'
         ELSE 'LAG'
       END AS status
FROM j
ORDER BY symbol, timeframe;"
