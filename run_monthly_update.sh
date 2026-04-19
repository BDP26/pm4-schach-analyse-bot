#!/bin/bash
# Manual launcher for the Lichess monthly update pipeline.
# Run this from the project root directory.
#
# Usage:
#   bash run_monthly_update.sh             # process up to 3 new months
#   bash run_monthly_update.sh --dry-run   # list new months without downloading
#   bash run_monthly_update.sh --max-months 1  # process only the oldest new month
#
# To schedule monthly on the cluster, add to crontab (crontab -e):
#   0 3 2 * * cd /path/to/pm4-schach-analyse-bot && bash run_monthly_update.sh >> logs/update.log 2>&1

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

mkdir -p logs

TIMESTAMP=$(date +"%Y-%m-%d_%H-%M-%S")
LOG_FILE="logs/update_${TIMESTAMP}.log"

echo "Starting Lichess monthly update at $(date)" | tee "$LOG_FILE"
echo "Log file: $LOG_FILE"

python lichess_auto_update.py "$@" 2>&1 | tee -a "$LOG_FILE"

echo "Finished at $(date)" | tee -a "$LOG_FILE"
