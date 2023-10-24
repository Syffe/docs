#!/bin/bash

rootdir=$(cd "$(dirname $0)"; pwd)/..
cd $rootdir

TO_BUILD="${@:-saas-web saas-worker onpremise}"

# NOTE: build date is always the same to leverage/test the layer caching mechanism
# if devs want to rebuild they just need to use --no-cache
for img in ${TO_BUILD[@]}; do
    # NOTE(sileht): build date is always the same to leverage/test the layer caching mechanism
    # if devs want to rebuild they just need to use --no-cache
    if [ "$img" == "onpremise" ]; then
      cat \
        dockerfiles/Dockerfile.onpremise.header \
        dockerfiles/Dockerfile.common \
        dockerfiles/Dockerfile.onpremise.footer \
        > Dockerfile
    else
      cat \
        dockerfiles/Dockerfile.common \
        dockerfiles/Dockerfile.saas \
        > Dockerfile
    fi
    docker buildx build \
        --progress=plain \
        --platform linux/amd64 \
        --build-arg PYTHON_VERSION="$(cut -d- -f2 runtime.txt)" \
        --build-arg MERGIFYENGINE_SHA="$(git log -1 --format='%H')" \
        --build-arg MERGIFYENGINE_VERSION=dev \
        --build-arg MERGIFYUI_VERSION=latest \
        --build-arg MERGIFYINSTALLER_VERSION=latest \
        --build-arg BUILD_DATE="never" \
	    --target "$img" \
	    --tag engine-"$img" \
	    .
done
