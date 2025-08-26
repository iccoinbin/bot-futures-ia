#!/usr/bin/env bash
set -euo pipefail
cd "$(cd "$(dirname "$0")" && pwd)"

if [ -d pids ]; then
  for P in pids/shadow_*.pid; do
    [ -f "$P" ] || continue
    PID=$(cat "$P" || true)
    if [ -n "${PID:-}" ] && kill -0 "$PID" 2>/dev/null; then
      kill "$PID" || true
      echo "Stopped PID $PID ($P)"
    fi
    rm -f "$P"
  done
else
  echo "Nenhum PID de shadow encontrado (pasta pids/)."
fi
