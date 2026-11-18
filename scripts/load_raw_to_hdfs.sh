#!/usr/bin/env bash
# Copy everything in ./data/raw (mounted at /raw in the namenode container) into hdfs:///da/raw
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose exec -T namenode bash -c 'hdfs dfsadmin -safemode wait >/dev/null; hdfs dfs -mkdir -p /da/raw && hdfs dfs -put -f /raw/* /da/raw/ && hdfs dfs -ls -h /da/raw'
