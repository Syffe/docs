#!/bin/bash

export MERGIFYENGINE_LOG_LEVEL=warning

set -ex

mergify-database-update --help
mergify-engine-worker --enabled-services ""
python3 -m mergify_engine.web.asgi
if [ -n "$IMPORT_CHECKS_TEST_DEV_DEPS" ]; then
    pytest --collect-only -q
fi
