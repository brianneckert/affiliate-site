#!/bin/zsh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
if [[ -n "${PYTHON_BIN:-}" ]]; then
  PYTHON="${PYTHON_BIN}"
elif [[ -x /opt/homebrew/bin/python3 ]]; then
  PYTHON=/opt/homebrew/bin/python3
else
  PYTHON="$(command -v python3)"
fi
BATCH_SIZE="${BATCH_SIZE:-8}"
RUN_DATE="${RUN_DATE:-$(date +%F)}"
EXTRA_ARGS="${EXTRA_ARGS:-}"

exec "$PYTHON" "$SCRIPT_DIR/run_continuous_batch.py" --date "$RUN_DATE" --batch-size "$BATCH_SIZE" "$@" $=EXTRA_ARGS
