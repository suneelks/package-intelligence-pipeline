#!/bin/bash
# Runs once on first Postgres boot. Creates the Dagster system DB
# alongside the application DB (POSTGRES_DB is created by the image).
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE dagster_storage;
EOSQL
