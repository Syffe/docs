ARG ENGINE_PATH=mergify-engine
ARG PYTHON_VERSION
# Used to rebuild everything without cache everyday
ARG BUILD_DATE

### BASE ###
FROM python:${PYTHON_VERSION}-slim as python-base
ARG BUILD_DATE
RUN test -n "$BUILD_DATE"
RUN useradd -m mergify
ENV DEBIAN_FRONTEND=noninteractive
RUN printf 'APT::Get::Install-Recommends "false";\nAPT::Get::Install-Suggests "false";\n' > /etc/apt/apt.conf.d/99local
RUN apt update -y && apt upgrade -y && apt install -y git && apt autoremove --purge -y

### BUILDER JS ###
FROM node:16-buster-slim as js-builder
ARG BUILD_DATE
RUN test -n "$BUILD_DATE"
ARG ENGINE_PATH
# Real install that can't be cached
ADD ${ENGINE_PATH}/installer /installer
WORKDIR /installer
RUN npm ci
RUN npm run build
RUN rm -rf node_modules

### BUILDER PYTHON ###
FROM python-base as python-builder
ARG ENGINE_PATH
ARG ENGINE_SIGNAL_PATH

# Required because hiredis is lagging a lot are providing prebuild wheel, last
# version if for py39
RUN apt install -y gcc

RUN python3 -m venv /venv
ENV VIRTUAL_ENV=/venv
ENV PATH="/venv/bin:${PATH}"

# First cache build requirements
RUN python3 -m pip install wheel
ADD ${ENGINE_PATH}/requirements.txt /
# nosemgrep: generic.ci.security.use-frozen-lockfile.use-frozen-lockfile-pip
RUN python3 -m pip install --no-cache-dir -r /requirements.txt

ADD ${ENGINE_PATH} /app

# nosemgrep: generic.ci.security.use-frozen-lockfile.use-frozen-lockfile-pip
RUN python3 -m pip install --no-cache-dir -c /requirements.txt -e /app

### BASE RUNNER ###
FROM python-base as runner-base
ARG PYTHON_VERSION
ARG DD_AGENT_VERSION=7.34.0-1
ARG MERGIFYENGINE_SHA
LABEL python.version="$PYTHON_VERSION"
LABEL mergify-engine.sha="$MERGIFYENGINE_SHA"
LABEL datadog-agent.version="$DD_AGENT_VERSION"
ENV MERGIFYENGINE_SHA=$MERGIFYENGINE_SHA
RUN test -n "$PYTHON_VERSION"
RUN test -n "$MERGIFYENGINE_SHA"

# Add Datadog repository, signing keys and packages
RUN apt update -y \
 && apt install -y gnupg apt-transport-https gpg-agent curl ca-certificates
ENV DATADOG_APT_KEYRING="/usr/share/keyrings/datadog-archive-keyring.gpg"
ENV DATADOG_APT_KEYS_URL="https://keys.datadoghq.com"
RUN sh -c "echo 'deb [signed-by=${DATADOG_APT_KEYRING}] https://apt.datadoghq.com/ stable 7' > /etc/apt/sources.list.d/datadog.list"
RUN touch ${DATADOG_APT_KEYRING}
RUN curl -o /tmp/DATADOG_APT_KEY_CURRENT.public "${DATADOG_APT_KEYS_URL}/DATADOG_APT_KEY_CURRENT.public" && \
    gpg --ignore-time-conflict --no-default-keyring --keyring ${DATADOG_APT_KEYRING} --import /tmp/DATADOG_APT_KEY_CURRENT.public
RUN curl -o /tmp/DATADOG_APT_KEY_F14F620E.public "${DATADOG_APT_KEYS_URL}/DATADOG_APT_KEY_F14F620E.public" && \
    gpg --ignore-time-conflict --no-default-keyring --keyring ${DATADOG_APT_KEYRING} --import /tmp/DATADOG_APT_KEY_F14F620E.public
RUN curl -o /tmp/DATADOG_APT_KEY_382E94DE.public "${DATADOG_APT_KEYS_URL}/DATADOG_APT_KEY_382E94DE.public" && \
    gpg --ignore-time-conflict --no-default-keyring --keyring ${DATADOG_APT_KEYRING} --import /tmp/DATADOG_APT_KEY_382E94DE.public
RUN apt-get update && apt-get -y --force-yes install --reinstall datadog-agent=1:${DD_AGENT_VERSION}

RUN apt purge -y libcurl4 curl openssh-client gnupg apt-transport-https gpg-agent curl ca-certificates libldap-common openssl patch
RUN apt autoremove --purge -y && apt clean -y && rm -rf /var/lib/apt/lists/*
RUN apt purge -y --allow-remove-essential libsepol1 apt libudev1 gpgv login
COPY --from=python-builder /app /app
COPY --from=python-builder /venv /venv
ADD datadog-wrapper.sh /
WORKDIR /app
ENV VIRTUAL_ENV=/venv
ENV PYTHONUNBUFFERED=1
ENV PATH="/venv/bin:${PATH}"
USER mergify

# ALL LAYER ABOVE MUST BE THE SAME FOR ALL VERSIONS, BUILD ARGS MUST BE THE SAME FOR ALL VERSIONS
### We don't put MERGIFYENGINE_VERSION inside runner-base, to ensure runner-base is the same layer between onpremise and saas
FROM runner-base as runner-tagged
ARG MERGIFYENGINE_VERSION
LABEL mergify-engine.version="$MERGIFYENGINE_VERSION"
ENV MERGIFYENGINE_VERSION=$MERGIFYENGINE_VERSION
RUN test -n "$MERGIFYENGINE_VERSION"

### WEB ###
FROM runner-tagged as saas-web
ENV PORT=8002
EXPOSE $PORT
USER mergify
CMD ["/datadog-wrapper.sh", "gunicorn", "--worker-class=uvicorn.workers.UvicornH11Worker", "--statsd-host=localhost:8125", "--log-level=warning", "mergify_engine.web.asgi"]

### WORKER-SHARED ###
FROM runner-tagged as saas-worker-shared
USER mergify
CMD ["/datadog-wrapper.sh", "mergify-engine-worker", "--enabled-services=shared-stream"]

### WORKER-DEDICATED ###
FROM runner-tagged as saas-worker-dedicated
CMD ["/datadog-wrapper.sh", "mergify-engine-worker", "--enabled-services=dedicated-stream,stream-monitoring,delayed-refresh"]

### ON PREMISE ###
FROM runner-tagged as onpremise
USER root
COPY --from=js-builder /installer/build /app/installer/build
ADD onpremise/Procfile /app/
ADD onpremise/entrypoint.sh /
ENV DD_DOGSTATSD_DISABLE=1
ENV DD_TRACE_ENABLED=0
USER mergify
ENTRYPOINT ["/entrypoint.sh"]
