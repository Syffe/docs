#!/bin/bash

set -eo pipefail

if [ -z $(which brew) ]; then
    exit 0
fi

PACKAGES="git pwgen pkg-config overmind"

for package in $PACKAGES; do
    if [ -z "$(which $package)" ]; then
        brew install $package
    fi
done
