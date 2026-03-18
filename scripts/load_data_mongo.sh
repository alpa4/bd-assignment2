#!/usr/bin/env bash
# load_data_mongo.sh — start MongoDB container and load data
set -e

CONTAINER="mongodb_server"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "=== Starting MongoDB container ==="
docker start "$CONTAINER"

echo "=== Installing dependencies ==="
pip install pymongo pandas --quiet

echo "=== Running loader ==="
python "$SCRIPT_DIR/load_data_mongo.py"
