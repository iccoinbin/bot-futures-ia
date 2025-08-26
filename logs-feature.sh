#!/bin/bash
set -euo pipefail
journalctl --user -u features-engine.service -n 20 --no-pager --output cat -e
