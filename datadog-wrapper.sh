#!/bin/bash

# NOTE(sileht): Heroku doesn't log stderr of containers, so use only stdout
exec 2>&1

startup_message() {
    echo "Starting Mergify SaaS"
    echo "MERGIFYENGINE_VERSION=$MERGIFYENGINE_VERSION"
    echo "MERGIFYENGINE_REVISION=$MERGIFYENGINE_REVISION"
    echo "MERGIFYENGINE_SHA=$MERGIFYENGINE_SHA"
}

export DYNOHOST="$(hostname)"
export DYNOTYPE=${DYNO%%.*}

if [ -z "$DD_API_KEY" ]; then
    startup_message
    echo '$DD_API_KEY missing, skipping datadog-agent setup...'
    export DD_DOGSTATSD_DISABLE=1
    export DD_TRACE_ENABLED=0
    exec "$@"

elif [ -z "$DYNO" ]; then
    startup_message
    echo '$DYNO missing, skipping datadog-agent setup...'
    export DD_DOGSTATSD_DISABLE=1
    export DD_TRACE_ENABLED=0
    exec "$@"

elif [ -z "$DYNO" -o "$DYNOHOST" == "run" ]; then
    startup_message
    echo '$DYNOHOST == run , skipping datadog-agent setup...'
    export DD_DOGSTATSD_DISABLE=1
    export DD_TRACE_ENABLED=0
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
export DD_LOGS_CONFIG_FRAME_SIZE=30000

# Copy the empty config file
cp -f "${DD_CONF_DIR}/datadog.yaml.example" "$DD_CONF_DIR/datadog.yaml"

case $DYNOTYPE in
    web)
        export DD_EXTRA_TAGS="service:engine-web"

        cat > "$DD_CONF_DIR/conf.d/process.d/conf.yaml" <<EOF
init_config:

instances:
  - name: gunicorn-worker
    search_string: ['^gunicorn: worker']
    exact_match: false
  - name: gunicorn-master
    search_string: ['^gunicorn: master']
    exact_match: false
EOF

        mkdir -p "$DD_CONF_DIR/conf.d/engine-web.d"
        cat > "$DD_CONF_DIR/conf.d/engine-web.d/conf.yaml" <<EOF
init_config:

instances:

logs:
  - type: udp
    port: 10518
    source: python
    service: engine-web
    sourcecategory: sourcecode
EOF
        ;;

    worker-*)
        export DD_EXTRA_TAGS="service:engine-worker"

        cat > "$DD_CONF_DIR/conf.d/process.d/conf.yaml" <<EOF
init_config:

instances:
  - name: mergify-engine-worker
    search_string: ['bin/mergify-engine-worker']
    exact_match: false
EOF

        mkdir -p "$DD_CONF_DIR/conf.d/engine-worker.d"
        cat > "$DD_CONF_DIR/conf.d/engine-worker.d/conf.yaml" <<EOF
init_config:

instances:

logs:
  - type: udp
    port: 10518
    source: python
    service: engine-worker
    sourcecategory: sourcecode
EOF
        ;;

esac

cat > "$DD_CONF_DIR/conf.d/redisdb.d/conf.yaml" <<EOF
init_config:

instances:
  - host: <CACHE HOST>
    port: <CACHE PORT>
    password: <CACHE PASSWORD>
    db: <CACHE DB>
    ssl: <CACHE SSL>
    ssl_cert_reqs: 0
    slowlog-max-len: 0
    tags:
      - role:cache

  - host: <STREAM HOST>
    port: <STREAM PORT>
    password: <STREAM PASSWORD>
    db: <STREAM DB>
    ssl: <STREAM SSL>
    ssl_cert_reqs: 0
    slowlog-max-len: 0
    keys:
      - streams
      - attempts
    tags:
      - role:streams
EOF


REDIS_REGEX='^redis(s?)://([^:]*):([^@]+)@([^:]+):([^/?]+)\?db=([^&]*)'

if [ -n "$MERGIFYENGINE_STORAGE_URL" ]; then
    if [[ $MERGIFYENGINE_STORAGE_URL =~ $REDIS_REGEX ]]; then
        [ "${BASH_REMATCH[1]}" ] && REDIS_SSL="true" || REDIS_SSL="false"
        sed -i "s/<CACHE SSL>/$REDIS_SSL/" "$DD_CONF_DIR/conf.d/redisdb.d/conf.yaml"
        sed -i "s/<CACHE HOST>/${BASH_REMATCH[4]}/" "$DD_CONF_DIR/conf.d/redisdb.d/conf.yaml"
        sed -i "s/<CACHE PASSWORD>/${BASH_REMATCH[3]}/" "$DD_CONF_DIR/conf.d/redisdb.d/conf.yaml"
        sed -i "s/<CACHE PORT>/${BASH_REMATCH[5]}/" "$DD_CONF_DIR/conf.d/redisdb.d/conf.yaml"
        sed -i "s/<CACHE DB>/${BASH_REMATCH[6]}/" "$DD_CONF_DIR/conf.d/redisdb.d/conf.yaml"
    fi
fi

if [ -n "$MERGIFYENGINE_STREAM_URL" ]; then
    if [[ $MERGIFYENGINE_STREAM_URL =~ $REDIS_REGEX ]]; then
        [ "${BASH_REMATCH[1]}" ] && REDIS_SSL="true" || REDIS_SSL="false"
        sed -i "s/<STREAM SSL>/$REDIS_SSL/" "$DD_CONF_DIR/conf.d/redisdb.d/conf.yaml"
        sed -i "s/<STREAM HOST>/${BASH_REMATCH[4]}/" "$DD_CONF_DIR/conf.d/redisdb.d/conf.yaml"
        sed -i "s/<STREAM PASSWORD>/${BASH_REMATCH[3]}/" "$DD_CONF_DIR/conf.d/redisdb.d/conf.yaml"
        sed -i "s/<STREAM PORT>/${BASH_REMATCH[5]}/" "$DD_CONF_DIR/conf.d/redisdb.d/conf.yaml"
        sed -i "s/<STREAM DB>/${BASH_REMATCH[6]}/" "$DD_CONF_DIR/conf.d/redisdb.d/conf.yaml"
    fi
fi


unset DD_CONF_DIR
echo 'Datadog-agent startup.'
datadog-agent run &
/opt/datadog-agent/embedded/bin/trace-agent --config=/etc/datadog-agent/datadog.yaml &
/opt/datadog-agent/embedded/bin/process-agent --config=/etc/datadog-agent/datadog.yaml &

startup_message
exec /venv/bin/ddtrace-run "$@"
