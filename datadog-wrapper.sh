#!/bin/bash

# NOTE(sileht): Heroku doesn't log stderr of containers, so use only stdout
exec 2>&1



url_decode() {
    python3 -c "import urllib.parse as p, sys;print(p.unquote(sys.stdin.read()))"
}

startup_message() {
    echo "Starting Mergify SaaS"
    echo "MERGIFYENGINE_VERSION=$MERGIFYENGINE_VERSION"
    echo "MERGIFYENGINE_SHA=$MERGIFYENGINE_SHA"
    echo "DYNO=$DYNO"
    echo "DYNOTYPE=$DYNOTYPE"
    echo "DYNOHOST=$DYNOHOST"
    echo "DD_TAGS=$DD_TAGS"
    echo "DD_HOSTNAME=$DD_HOSTNAME"
}

export DYNOHOST="$(hostname)"
export DYNOTYPE=${DYNO%%.*}

if [ -z "$DD_API_KEY" ]; then
    startup_message
    echo '$DD_API_KEY missing, skipping datadog-agent setup...'
    exec "$@"

elif [ -z "$DYNO" ]; then
    startup_message
    echo '$DYNO missing, skipping datadog-agent setup...'
    exec "$@"

elif [ "$DYNOTYPE" == "run" ] || [ "$DYNOTYPE" == "scheduler" ] || [ "$DYNOTYPE" == "release" ]; then
    startup_message
    echo 'short lived DYNOTYPE: $DYNOHOST, skipping datadog-agent setup...'
    exec "$@"
fi

export DD_CONF_DIR="/etc/datadog-agent"

### Configure all datadog.yaml settings via env
export DD_TAGS="dyno:$DYNO dynotype:$DYNOTYPE appname:$HEROKU_APP_NAME"
# We want always to have the Dyno ID as a host alias to improve correlation
export DD_HOST_ALIASES="$DYNOHOST"
# Set the hostname to dyno name and ensure rfc1123 compliance.
HAN="$(echo "$HEROKU_APP_NAME" | sed -e 's/[^a-zA-Z0-9-]/-/g' -e 's/^-//g')"
D="$(echo "$DYNO" | sed -e 's/[^a-zA-Z0-9.-]/-/g' -e 's/^-//g')"
export DD_HOSTNAME="$HAN.$D"
export DD_PROCESS_CONFIG_ENABLED=true
export DD_LOGS_ENABLED=true
export DD_RUNTIME_METRICS_ENABLED=true
export DD_DBM_PROPAGATION_MODE=full
export DD_LOGS_CONFIG_FRAME_SIZE=30000

# Copy the empty config file
cp -f "${DD_CONF_DIR}/datadog.yaml.example" "$DD_CONF_DIR/datadog.yaml"


MERGIFY_CONF_FILE="$DD_CONF_DIR/conf.d/mergify.d/conf.yaml"
if [ -n "$DD_MERGIFY_API_TOKEN" ]; then
    echo "Installing datadog-mergify integration"
    sed -i "s/<YOUR DD_MERGIFY_API_TOKEN>/${DD_MERGIFY_API_TOKEN}/" "$MERGIFY_CONF_FILE"
else
    rm -f  "$MERGIFY_CONF_FILE"
fi

POSTGRES_REGEX='^postgres://([^:]+):([^@]+)@([^:]+):([^/]+)/(.*)$'
POSTGRES_CONF_FILE="$DD_CONF_DIR/conf.d/postgres.d/conf.yaml"

if [ -n "$MERGIFYENGINE_DATABASE_URL" ]; then
    if [[ $MERGIFYENGINE_DATABASE_URL =~ $POSTGRES_REGEX ]]; then
        DB_USERNAME="datadog"
        DB_PASSWORD=$(echo "${DATADOG_POSTGRES_PASSWORD}" | url_decode)
        sed -i "s/<YOUR HOSTNAME>/${BASH_REMATCH[3]}/" "$POSTGRES_CONF_FILE"
        sed -i "s/<YOUR USERNAME>/${DB_USERNAME}/" "$POSTGRES_CONF_FILE"
        sed -i "s/<YOUR PASSWORD>/${DB_PASSWORD}/" "$POSTGRES_CONF_FILE"
        sed -i "s/<YOUR PORT>/${BASH_REMATCH[4]}/" "$POSTGRES_CONF_FILE"
        sed -i "s/<YOUR DBNAME>/${BASH_REMATCH[5]}/" "$POSTGRES_CONF_FILE"
    fi
    LOG_EMBEDDER_MONITORED_ACCOUNTS="$(python3 -c "import sys; print('\'' + '\',\''.join(sys.argv[1].split(',')) + '\'')" $MERGIFYENGINE_LOG_EMBEDDER_ENABLED_ORGS)"
    sed -i "s/<LOG_EMBEDDER_MONITORED_ACCOUNTS>/${LOG_EMBEDDER_MONITORED_ACCOUNTS}/" "$POSTGRES_CONF_FILE"

fi

REDIS_REGEX='^redis(s?)://([^:]*):([^@]+)@([^:]+):([^/?]+)'
REDIS_FILE="$DD_CONF_DIR/conf.d/redisdb.d/conf.yaml"

if [ -n "$MERGIFYENGINE_EVENTLOGS_URL" ]; then
    if [[ $MERGIFYENGINE_EVENTLOGS_URL =~ $REDIS_REGEX ]]; then
        [ "${BASH_REMATCH[1]}" ] && REDIS_SSL="true" || REDIS_SSL="false"
        sed -i "s/<EVENTLOGS SSL>/$REDIS_SSL/" "$REDIS_FILE"
        sed -i "s/<EVENTLOGS HOST>/${BASH_REMATCH[4]}/" "$REDIS_FILE"
        sed -i "s/<EVENTLOGS PASSWORD>/${BASH_REMATCH[3]}/" "$REDIS_FILE"
        sed -i "s/<EVENTLOGS PORT>/${BASH_REMATCH[5]}/" "$REDIS_FILE"
    fi
fi

if [ -n "$MERGIFYENGINE_DEFAULT_REDIS_URL" ]; then
    if [[ $MERGIFYENGINE_DEFAULT_REDIS_URL =~ $REDIS_REGEX ]]; then
        [ "${BASH_REMATCH[1]}" ] && REDIS_SSL="true" || REDIS_SSL="false"
        sed -i "s/<DEFAULT REDIS SSL>/$REDIS_SSL/" "$REDIS_FILE"
        sed -i "s/<DEFAULT REDIS HOST>/${BASH_REMATCH[4]}/" "$REDIS_FILE"
        sed -i "s/<DEFAULT REDIS PASSWORD>/${BASH_REMATCH[3]}/" "$REDIS_FILE"
        sed -i "s/<DEFAULT REDIS PORT>/${BASH_REMATCH[5]}/" "$REDIS_FILE"
    fi
fi

export DD_DOGSTATSD_DISABLE=0
export DD_TRACE_ENABLED=1

# FIXME(sileht): This is not enabled by default as it's recommended to use the
# underlying driver integration, but since they don't support psycopg3 yet, we
# must enable it.
# https://ddtrace.readthedocs.io/en/stable/integrations.html#sqlalchemy
# When psycopg3 is supported -> MRGFY-2024
export DD_TRACE_SQLALCHEMY_ENABLED=1

unset DD_CONF_DIR
echo 'Datadog-agent startup.'
datadog-agent run &
/opt/datadog-agent/embedded/bin/trace-agent --config=/etc/datadog-agent/datadog.yaml &
/opt/datadog-agent/embedded/bin/process-agent --config=/etc/datadog-agent/datadog.yaml &

startup_message
exec /app/.venv/bin/ddtrace-run "$@"
