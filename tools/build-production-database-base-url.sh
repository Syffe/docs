#!/bin/bash

set -eo pipefail

# NOTE(sileht): don't use this script in pull_request_target GitHub Workflow as it could leak the password

function urlencode() {
   python3 -c "from urllib.parse import quote, sys; print(quote(sys.stdin.read().encode().strip()))"
}

PRODUCTION_DATABASE_PASSWORD="$(aws rds generate-db-auth-token --hostname $PRODUCTION_DATABASE_HOST --port $PRODUCTION_DATABASE_PORT --region us-east-1 --username $PRODUCTION_DATABASE_USERNAME | urlencode)"
echo "postgres://${PRODUCTION_DATABASE_USERNAME}:${PRODUCTION_DATABASE_PASSWORD}@${PRODUCTION_DATABASE_HOST}:${PRODUCTION_DATABASE_PORT}"
