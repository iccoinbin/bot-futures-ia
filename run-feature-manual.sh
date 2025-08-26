#!/bin/bash
set -euo pipefail
sudo -n systemctl --user stop features-engine.timer
source datahub/.venv/bin/activate
PYTHONPATH=$PWD/datahub/src python -m features.engine
sudo -n systemctl --user start features-engine.timer
