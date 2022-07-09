#/bin/bash

# NOTE(sileht): build date is always the same to leverage/test the layer caching mechanism
# if devs want to rebuild they just need to use --no-cache
exec docker buildx build \
    --platform linux/amd64 \
    --build-arg PYTHON_VERSION="$(cut -d- -f2 runtime.txt)" \
    --build-arg MERGIFYENGINE_SHA="$(git log -1 --format='%H')" \
    --build-arg MERGIFYENGINE_VERSION=dev \
    --build-arg BUILD_DATE="never" \
    "$@"
