#!/bin/bash

set -eo pipefail

PYTHON_EXTRACT_DOCKER_PORT="
import json, sys
data = json.load(sys.stdin)
if isinstance(data, list):
    publishers = data[0]['Publishers']
else:
    publishers = data['Publishers']

for pub in publishers:
    if pub['PublishedPort'] > 0:
        print(pub['PublishedPort'])
        sys.exit(0)

sys.exit(1)
"

if [ "$CI" == "true" ]; then
    REDIS_PORT=6363
    POSTGRES_PORT=5432
    GOOGLE_CLOUD_STORAGE_PORT=8000
else
    docker compose up -d --force-recreate --always-recreate-deps --remove-orphans

    cleanup () {
        ret="$?"
        set +exo pipefail

        docker compose down --remove-orphans --volumes
        [ "$ret" == "0" -a "$MERGIFYENGINE_RECORD" == "1" ] && git add zfixtures/

        exit "$ret"
    }
    trap cleanup EXIT

    POSTGRES_PORT=$(docker compose ps postgres --format=json | python3 -c "$PYTHON_EXTRACT_DOCKER_PORT")
    REDIS_PORT=$(docker compose ps redis --format=json | python3 -c "$PYTHON_EXTRACT_DOCKER_PORT")
    GOOGLE_CLOUD_STORAGE_PORT=$(docker compose ps google_cloud_storage --format=json | python3 -c "$PYTHON_EXTRACT_DOCKER_PORT")
fi


# nosemgrep: generic.secrets.security.detected-username-and-password-in-uri.detected-username-and-password-in-uri
export MERGIFYENGINE_DATABASE_URL=postgresql://postgres:password@localhost:${POSTGRES_PORT}/postgres
export MERGIFYENGINE_DEFAULT_REDIS_URL="redis://localhost:${REDIS_PORT}"
export MERGIFYENGINE_DATABASE_OAUTH_TOKEN_SECRET_CURRENT=$(pwgen -1 48)
export STORAGE_EMULATOR_HOST="http://localhost:${GOOGLE_CLOUD_STORAGE_PORT}"

set -x

while ! docker run --net host --rm redis redis-cli -h localhost -p "$REDIS_PORT" keys '*' ; do sleep 0.1 ; done
while ! docker run --net host --rm postgres psql "$MERGIFYENGINE_DATABASE_URL" -c "select 1 as connected" ; do sleep 0.1 ; done

cmd="$1"
shift
"$cmd" "$@"
