#!/usr/bin/env bash
set -Eeuo pipefail

: "${POSTGRES_USER:?POSTGRES_USER must be set by the PostgreSQL image}"
: "${POSTGRES_DB:?POSTGRES_DB must be set by the PostgreSQL image}"
: "${AETHERFLOW_MIGRATION_PASSWORD:?Set AETHERFLOW_MIGRATION_PASSWORD in .env}"
: "${AETHERFLOW_RUNTIME_PASSWORD:?Set AETHERFLOW_RUNTIME_PASSWORD in .env}"

psql \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" \
  --set ON_ERROR_STOP=1 \
  --set database_name="$POSTGRES_DB" \
  --set migration_password="$AETHERFLOW_MIGRATION_PASSWORD" \
  --set runtime_password="$AETHERFLOW_RUNTIME_PASSWORD" <<'SQL'
REVOKE CREATE ON SCHEMA public FROM PUBLIC;

CREATE ROLE aetherflow_migrator
    LOGIN
    NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE NOREPLICATION NOINHERIT
    PASSWORD :'migration_password';

CREATE ROLE aetherflow_runtime
    LOGIN
    NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE NOREPLICATION NOINHERIT
    PASSWORD :'runtime_password';

GRANT CONNECT ON DATABASE :"database_name"
    TO aetherflow_migrator, aetherflow_runtime;

GRANT USAGE, CREATE ON SCHEMA public TO aetherflow_migrator;
GRANT USAGE ON SCHEMA public TO aetherflow_runtime;

ALTER DEFAULT PRIVILEGES FOR ROLE aetherflow_migrator IN SCHEMA public
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO aetherflow_runtime;

ALTER DEFAULT PRIVILEGES FOR ROLE aetherflow_migrator IN SCHEMA public
    GRANT USAGE, SELECT ON SEQUENCES TO aetherflow_runtime;
SQL

echo "Created separate aetherflow_migrator and least-privilege aetherflow_runtime roles."
