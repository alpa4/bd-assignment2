#!/usr/bin/env bash
# Run q2_mongo.py
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
python "$SCRIPT_DIR/q2_mongo.py" "$@"
