#!/bin/bash

export MERGIFYENGINE_LOG_LEVEL=warning

set -ex

mergify-database-update --help
mergify-engine-worker --enabled-services ""
python3 -m mergify_engine.web.asgi
