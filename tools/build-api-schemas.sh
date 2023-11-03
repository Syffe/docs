#!/bin/bash

set -eo pipefail
set -x

mkdir -p api-schemas
mergify-devtools generate-openapi-spec --visibility public api-schemas/public.json
mergify-devtools generate-openapi-spec --visibility public_future api-schemas/public-future.json
mergify-devtools generate-openapi-spec --visibility internal api-schemas/internal.json

if [ "$CI" == "true" ]; then
    FORMAT=github-actions
else
    FORMAT="stylish"
fi

npx -y --package @stoplight/spectral-cli -c "spectral lint -F hint -f $FORMAT api-schemas/public.json"
npx -y --package @stoplight/spectral-cli -c "spectral lint -F hint -f $FORMAT api-schemas/public-future.json"
