#!/bin/bash

set -eo pipefail

git ls-files docs/*.rst | while read file; do 
    if ! grep -q :description: $file; then 
        echo E: no description meta tag in $file;
        exit 1
    fi
done

set -x

rm -rf docs/build

mergify-devtools generate-openapi-spec --visibility public docs/build/api/openapi.json
mergify-devtools generate-openapi-spec --visibility public_future docs/build/api/openapi-future.json
mergify-devtools generate-openapi-spec --visibility internal docs/build/api/openapi-internal.json

(
    cd docs
    npm ci
    npm exec spectral lint -F hint build/api/openapi-future.json
    npm exec sass source/scss/main.scss build/_bootstrap/bootstrap.css
    cp -f node_modules/bootstrap/dist/js/bootstrap.min.js* build/_bootstrap/
)

sphinx-build -W -b spelling docs/source docs/build
sphinx-build -W -b dirhtml docs/source docs/build
