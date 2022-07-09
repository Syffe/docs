#/bin/bash

# Fake prod-like environements
export DD_AGENT_MAJOR_VERSION=7
export DD_API_KEY=whatever
export DD_APM_ENABLED=1
export DD_DYNO_HOST=true
export DD_ENV=test
export DD_LOG_LEVEL=ERROR
export DD_PROCESS_AGENT=true
export DD_PROFILING_ENABLED=0
export DD_PROFILING_HEAP_ENABLED=1
export DD_PYTHON_VERSION=3
export DEBUG=True
# nosemgrep: generic.secrets.security.detected-heroku-api-key.detected-heroku-api-key
export HEROKU_APP_ID=006dd18f-f30f-4df5-a517-14255b75115c
export HEROKU_APP_NAME=mergify-engine
export HEROKU_RELEASE_CREATED_AT='2022-06-08T09:20:00Z'
export HEROKU_RELEASE_VERSION=v636
export HEROKU_SLUG_COMMIT=
export HEROKU_SLUG_DESCRIPTION='Deployed worker-dedicated (0297bc232de5)'
export MERGIFYENGINE_DEFAULT_REDIS_URL='rediss://:pass@ec2-3-226-11-67.compute-1.amazonaws.com:26099'
export MERGIFYENGINE_EVENTLOGS_URL='rediss://:pass@ec2-3-210-75-103.compute-1.amazonaws.com:12379?db=1'
export MERGIFYENGINE_SAAS_MODE=True

export DYNO=web.1
export DYNOTYPE=web

export DD_CONF_DIR=/tmp/dd-prerun-check-config
export APP_DATADOG_CONF_DIR=/tmp/dd-prerun-check-copy-config
export DATADOG_CONF="${DD_CONF_DIR}/datadog.conf"

mkdir -p $DD_CONF_DIR/conf.d/redisdb.d
mkdir -p $APP_DATADOG_CONF_DIR

touch "$DATADOG_CONF"
cp -a conf.d/redisdb.yaml $DD_CONF_DIR/conf.d/redisdb.d/conf.yaml

bash -x ./prerun.sh
