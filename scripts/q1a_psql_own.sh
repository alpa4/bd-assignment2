#!/usr/bin/env bash
# Run q1a_psql_own.py
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
python "$SCRIPT_DIR/q1a_psql_own.py" "$@"
