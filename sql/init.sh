#!/bin/sh
set -eu
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  -v extractor_password="$EXTRACTOR_PASSWORD" \
  -v loader_password="$LOADER_PASSWORD" \
  -v airflow_password="$AIRFLOW_DB_PASSWORD" \
  -f /seed/init.sql
