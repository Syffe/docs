#!/bin/bash

setuptools_version=$(grep -A1 'name = "setuptools"' poetry.lock | tail -n 1 | cut -d \" -f2)

poetry run pip install "setuptools==${setuptools_version}"
poetry install --sync
