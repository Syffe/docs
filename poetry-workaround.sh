#!/bin/bash

setuptools_version=$(sed -n -e '/name = "setuptools"/{n;s/.* = "\(.*\)"/\1/p}' poetry.lock)
exec pip install "setuptools==${setuptools_version}"
