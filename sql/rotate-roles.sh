#!/bin/sh
set -eu
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres \
  -v airflow_password="$AIRFLOW_DB_PASSWORD" \
  -v loader_password="$LOADER_PASSWORD" \
  -v extractor_password="$EXTRACTOR_PASSWORD" \
  -f /seed/rotate-roles.sql
