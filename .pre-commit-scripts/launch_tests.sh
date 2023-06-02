#!/usr/bin/env bash

git log --name-only --oneline main.. | tail -n +2 | grep '^\(mergify_engine\|zfixtures\)/'
if [[ "$?" == "0" ]]; then
	poetry run poe test-parallel -vv;
fi
