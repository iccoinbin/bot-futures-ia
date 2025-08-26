#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.." || exit 1
# Carregar .env para variáveis de símbolo/timeframe se existirem
if [[ -f .env ]]; then
  set -a; source .env; set +a
fi
source .venv/bin/activate
exec python src/jobs/feature_engine_v1.py
