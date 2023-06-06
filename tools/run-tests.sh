#!/bin/bash

set -exo pipefail

if [ "$CI" == "true" ]; then
    REDIS_PORT=6363
    POSTGRES_PORT=5432
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

    POSTGRES_PORT=$(docker compose ps postgres --format=json | python3 -c "import json,sys; print(json.load(sys.stdin)[0]['Publishers'][0]['PublishedPort'])")
    REDIS_PORT=$(docker compose ps redis --format=json | python3 -c "import json,sys; print(json.load(sys.stdin)[0]['Publishers'][0]['PublishedPort'])")
fi


# nosemgrep: generic.secrets.security.detected-username-and-password-in-uri.detected-username-and-password-in-uri
export MERGIFYENGINE_DATABASE_URL=postgresql://postgres:password@localhost:${POSTGRES_PORT}/postgres
export MERGIFYENGINE_DEFAULT_REDIS_URL="redis://localhost:${REDIS_PORT}"
export MERGIFYENGINE_DATABASE_OAUTH_TOKEN_SECRET_CURRENT=$(pwgen -1 48)

while ! docker run -t --net host --rm redis redis-cli -h localhost -p "$REDIS_PORT" keys '*' ; do sleep 1 ; done
while ! docker run -t --net host --rm postgres psql "$MERGIFYENGINE_DATABASE_URL" -c "select 1 as connected" ; do sleep 1 ; done

cmd="$1"
shift
"$cmd" "$@"
