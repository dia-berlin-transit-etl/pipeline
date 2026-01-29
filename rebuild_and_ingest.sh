#!/usr/bin/env bash


# make it executable and run it
# chmod +x rebuild_and_ingest.sh
# docker compose up -d
# ./rebuild_and_ingest.sh

set -euo pipefail

# config (env overrides)
DB_HOST="${DB_HOST:-localhost}"
DB_PORT="${DB_PORT:-5434}"
DB_NAME="${DB_NAME:-db_berlin}"
DB_USER="${DB_USER:-dia_user}"
DB_PASSWORD="${DB_PASSWORD:-dia}"

SQL_REBUILD="${SQL_REBUILD:-admin/rebuild_dw.sql}"

echo "== Rebuild DW schema in DB '${DB_NAME}' on ${DB_HOST}:${DB_PORT} as '${DB_USER}' =="

# Run the SQL rebuild script against the Docker Postgres
PGPASSWORD="$DB_PASSWORD" psql \
  "host=${DB_HOST} port=${DB_PORT} dbname=${DB_NAME} user=${DB_USER}" \
  -v "dw_user=${DB_USER}" \
  -f "$SQL_REBUILD"

echo "== Running ingestion steps =="
python ingestion.py --step stations
python ingestion.py --step trains
python ingestion.py --step time
python ingestion.py --step planned --threshold 0.52
python ingestion.py --step changed --threshold 0.52

echo "== Done =="
