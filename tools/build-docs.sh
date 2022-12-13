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

mergify-openapi-spec docs/build/api/openapi.json

yarn --cwd docs
yarn --cwd docs spectral lint -F hint build/api/openapi.json
yarn --cwd docs sass source/scss/main.scss build/_bootstrap/bootstrap.css

cp -f docs/node_modules/bootstrap/dist/js/bootstrap.min.js* docs/build/_bootstrap/

sphinx-build -W -b spelling docs/source docs/build
sphinx-build -W -b dirhtml docs/source docs/build
