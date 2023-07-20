#!/bin/bash

set -e
set -o pipefail

echo "Starting Mergify Enterprise"
echo "MERGIFYENGINE_VERSION=$MERGIFYENGINE_VERSION"
echo "MERGIFYENGINE_SHA=$MERGIFYENGINE_SHA"

if [ "$MERGIFYENGINE_INTEGRATION_ID" ]; then
  cd /onpremise
  mergify-database-update
  exec honcho start
elif [ "$MERGIFYENGINE_INSTALLER" ]; then
  cd /installer
  python -m http.server ${PORT:=5000}
else
  echo "MERGIFYENGINE_INTEGRATION_ID or MERGIFYENGINE_INSTALLER must set"
fi
exit 1
