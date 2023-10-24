#!/bin/bash

set -eo pipefail

# NOTE(sileht): ensure we don't print any environment variables
# like PRODUCTION_DATABASE_BASE_URL that contains password
set +xv

if [ -z "$PRODUCTION_DATABASE_BASE_URL" ]; then
    echo "PRODUCTION_DATABASE_BASE_URL is not set"
    exit 1
fi

echo "* Creating the new database..."
psql "$PRODUCTION_DATABASE_BASE_URL/postgres" -c "DROP DATABASE IF EXISTS engine_ci_testing WITH (FORCE);"
psql "$PRODUCTION_DATABASE_BASE_URL/postgres" -c "CREATE DATABASE engine_ci_testing;"

echo "* Wait for the new database to be ready..."
while ! psql "$PRODUCTION_DATABASE_BASE_URL/engine_ci_testing" -c "select 1 as connected" ; do sleep 1 ; done

echo "* Production database size"
psql "$PRODUCTION_DATABASE_BASE_URL/postgres" -c "select pg_size_pretty(pg_database_size('engine_prod'));"

echo "* Production database number of rows"
psql "$PRODUCTION_DATABASE_BASE_URL/engine_prod" -c "SELECT schemaname,relname,n_live_tup FROM pg_stat_user_tables ORDER BY n_live_tup DESC;"

echo "Syncing databases..."
pg_dump --no-acl --no-owner --no-comments --format=c --encoding=UTF8 --extension=vector "$PRODUCTION_DATABASE_BASE_URL/engine_prod" \
 | pg_restore --no-owner --no-acl --no-comments --exit-on-error -d "$PRODUCTION_DATABASE_BASE_URL/engine_ci_testing"

echo "* CI Testing database size"
psql "$PRODUCTION_DATABASE_BASE_URL/postgres" -c "select pg_size_pretty(pg_database_size('engine_ci_testing'));"

echo "* CI Testing database number of rows"
psql "$PRODUCTION_DATABASE_BASE_URL/engine_ci_testing" -c "SELECT schemaname,relname,n_live_tup FROM pg_stat_user_tables ORDER BY n_live_tup DESC;"

