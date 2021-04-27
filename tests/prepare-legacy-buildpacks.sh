#/bin/bash

script_dir=$(readlink -f $(dirname $0))

SHIM_VERSION="v0.3"
SHIM_TARBALL="cnb-shim-${SHIM_VERSION}.tgz"
if [ ! -f "$SHIM_TARBALL" ]; then
    wget https://github.com/heroku/cnb-shim/releases/download/${SHIM_VERSION}/${SHIM_TARBALL}
fi

declare -A BUILDPACKS
BUILDPACKS[git-submodule]="https://github.com/Mergifyio/heroku-buildpack-git-submodule"
BUILDPACKS[subdir]="https://github.com/Mergifyio/heroku-buildpack-subdir"

rm -rf buildpacks
for name in "${!BUILDPACKS[@]}"; do
    mkdir -p buildpacks/${name}
    tar -xzf ${SHIM_TARBALL} -C buildpacks/${name}
    git clone --depth 1 ${BUILDPACKS[$name]} buildpacks/${name}/target
    chmod +x buildpacks/${name}/target/bin/*
    sed -e "s/##NAME##/${name}/g" ${script_dir}/buildpack.toml.tmpl > buildpacks/${name}/buildpack.toml
done
