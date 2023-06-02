#!/usr/bin/env bash

git log --name-only --oneline main.. | tail -n +2 | grep '^docs/'
if [[ "$?" == "0" ]]; then
	poetry run poe docs;
fi
