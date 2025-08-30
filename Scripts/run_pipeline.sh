#!/usr/bin/env bash
set -euo pipefail
export DB_DSN="postgres://energy:energy@localhost:5432/energy?sslmode=disable"
export DATABASE_URL="postgresql+psycopg2://energy:energy@localhost:5432/energy"

# Ingest last 6 hours every 30 minutes
LOOKBACK_HOURS=6 DB_DSN="$DB_DSN" /path/to/ingest_carbon_binary

# Transform to facts/dims
source /path/to/.venv/bin/activate
python /path/to/transform.py
