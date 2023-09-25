#!/bin/bash

set -eo pipefail
set -x

mergify-devtools generate-openapi-spec --visibility public docs/openapi.json
mergify-devtools generate-openapi-spec --visibility public_future docs/openapi-future.json
mergify-devtools generate-openapi-spec --visibility internal docs/openapi-internal.json

(
    cd docs
    npm ci
    npm exec spectral lint -F hint openapi-future.json
)
