#!/usr/bin/env bash
# Run benchmark.py
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
python "$SCRIPT_DIR/benchmark.py" "$@"
