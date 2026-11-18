#!/usr/bin/env bash
# Run the full pipeline at several data scales and build the bottleneck report.
#   scripts/run_scaling.sh [profile] [scale ...]      e.g.  scripts/run_scaling.sh docker 0.05 0.1 0.25 0.5 1.0
set -euo pipefail
cd "$(dirname "$0")/.."
export PYTHONPATH=src
PROFILE=${1:-}; shift || true
SCALES=${*:-"0.1 0.25 0.5 1.0"}
ARGS=()
[ -n "$PROFILE" ] && ARGS+=(--profile "$PROFILE")
for s in $SCALES; do
  echo "=== scale $s ==="
  python -m da.pipeline run "${ARGS[@]}" --sample "$s" --scale "$s" \
      --stages ingest,clean,dedup,entities,embed_input,embed,cluster,marts
done
python -m da.bench_report > /dev/null
echo "report: bench/report.md  bench/breakdown.png  bench/scaling.png"
