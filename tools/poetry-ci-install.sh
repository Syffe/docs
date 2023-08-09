#!/bin/bash

if [ "$CI" ]; then
    poetry install --sync --no-cache
else
    poetry install --sync
fi
