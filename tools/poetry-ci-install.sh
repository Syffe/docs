#!/bin/bash

setuptools_version=$(grep -A1 'name = "setuptools"' poetry.lock | tail -n 1 | cut -d \" -f2)

if [ "$CI" ]; then
    poetry run pip install --no-cache-dir "setuptools==${setuptools_version}"
    poetry install --sync --no-cache
else
    poetry run pip install "setuptools==${setuptools_version}"
    poetry install --sync
fi
