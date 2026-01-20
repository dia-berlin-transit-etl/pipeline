#!/usr/bin/env bash

# Make it executable:
# chmod +x rebuild_and_ingest.sh

set -euo pipefail

# -------- config (env overrides) --------
DB_NAME="${DB_NAME:-public_transport_db}"
: "${DB_USER:?DB_USER is required (e.g., export DB_USER=...)}"

# paths
SQL_REBUILD="${SQL_REBUILD:-admin/rebuild_dw.sql}"

echo "== Rebuild DW schema in DB '${DB_NAME}' for user '${DB_USER}' =="

# 1) Rebuild schema + extensions + grants (runs as postgres)
echo "-- Dropping/recreating schema + extensions + grants..."
sudo -u postgres psql -d "$DB_NAME" \
  -v "dw_user=${DB_USER}" \
  -f "$SQL_REBUILD"

echo "== Running ingestion steps =="
# 2) Run ingestion scripts as the current shell user (uses ~/.pgpass)
python ingestion.py --step stations
python ingestion.py --step trains
python ingestion.py --step time
python ingestion.py --step planned --threshold 0.52 # --snapshot 2509021400
python ingestion.py --step changed --threshold 0.52 # --snapshot 2509021400

echo "== Done =="
