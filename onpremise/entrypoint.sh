#!/bin/bash


echo "Starting Mergify Enterprise"
echo "MERGIFYENGINE_VERSION=$MERGIFYENGINE_VERSION"
echo "MERGIFYENGINE_REVISION=$MERGIFYENGINE_REVISION"
echo "MERGIFYENGINE_SHA=$MERGIFYENGINE_SHA"

cd /app

get_command() {
    sed -n -e "s/^$1://p" Procfile
}

MODE="${1:-aio}"

if [ "$MERGIFYENGINE_INTEGRATION_ID" ]; then
  case "${MODE}" in
      # nosemgrep: bash.lang.correctness.unquoted-expansion.unquoted-command-substitution-in-command
      web|worker) exec $(get_command "$1");;
      aio) exec honcho start;;
      *) echo "usage: $0 (web|worker|aio)";;
  esac
elif [ "$MERGIFYENGINE_INSTALLER" ]; then
  exec honcho -f installer/Procfile start
else
  echo "MERGIFYENGINE_INTEGRATION_ID or MERGIFYENGINE_INSTALLER must set"
fi
exit 1
