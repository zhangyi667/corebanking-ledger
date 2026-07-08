#!/bin/bash
# Creates the posting_manager + account_service databases with matching
# per-app credentials so each service's default JDBC URL Just Works.
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE USER posting WITH PASSWORD 'posting';
    CREATE DATABASE posting_manager OWNER posting;

    CREATE USER account WITH PASSWORD 'account';
    CREATE DATABASE account_service OWNER account;
EOSQL
