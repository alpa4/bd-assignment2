#!/usr/bin/env bash
# load_data_psql.sh — start PostgreSQL container and load data
set -e

CONTAINER="some-postgres"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "=== Starting PostgreSQL container ==="
docker start "$CONTAINER"

echo "=== Installing dependencies ==="
pip install psycopg2-binary pandas --quiet

echo "=== Running loader ==="
python "$SCRIPT_DIR/load_data_psql.py"
