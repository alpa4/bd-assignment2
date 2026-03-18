#!/usr/bin/env bash
# Run q3_psql_own.py
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
python "$SCRIPT_DIR/q3_psql_own.py" "$@"
