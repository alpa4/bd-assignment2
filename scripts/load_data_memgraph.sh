#!/usr/bin/env bash
# load_data_memgraph.sh — start Memgraph containers and load data

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "=== Starting Memgraph containers ==="
docker start memgraph-mage
docker start memgraph-lab 2>/dev/null || echo "(memgraph-lab not found, skipping)"

echo "=== Waiting for Memgraph to be ready ==="
sleep 5

echo "=== Installing dependencies ==="
pip install neo4j pandas --quiet

echo "=== Running loader ==="
python "$SCRIPT_DIR/load_data_memgraph.py"
