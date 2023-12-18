#!/usr/bin/env bash
# based on original ./anon.sh to let script log
set -e

export PGDATA=/var/lib/postgresql/data/
export PGDATABASE=postgres
export PGUSER=postgres

mkdir -p $PGDATA
chown postgres $PGDATA
gosu postgres initdb
gosu postgres pg_ctl start
gosu postgres psql -c "ALTER SYSTEM SET session_preload_libraries = 'anon';"
gosu postgres psql -c "SELECT pg_reload_conf();"

gosu postgres psql -v ON_ERROR_STOP=1

/usr/bin/pg_dump_anon
