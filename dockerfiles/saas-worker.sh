#!/bin/bash

DYNOTYPE=${DYNO%%.*}
DYNOTYPE=${DYNOTYPE//-/_} # replace - by _
DYNOTYPE=${DYNOTYPE^^} # uppercase

ENV_VAR_NAME="MERGIFYENGINE_${DYNOTYPE}_SERVICES"

SERVICES_ENABLED=${!ENV_VAR_NAME}

if [ -z "$SERVICES_ENABLED" ]; then
    echo "$ENV_VAR_NAME is empty"
    exit 1
fi

exec mergify-engine-worker --enabled-services "${SERVICES_ENABLED}"
