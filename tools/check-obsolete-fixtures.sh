#!/bin/bash

TESTS="$(pytest --collect-only | sed -n -e '/\(TestCaseFunction\|Function\|Coroutine\)/s/.* \([^ >]*\)>.*/\1/gp')"
ret=0
for f in $(find zfixtures -name config.json -o -name 'git-*.json' -o -name 'http.yaml' | xargs dirname | uniq); do
    f=$(basename $f)
    used=$(echo -e "$TESTS" | grep "^$f\$")
    if [ ! "$used" ]; then
        if [ "$ret" -eq 0 ] ; then
            echo "- This directory are unused by tests:"
        fi
        echo "zfixtures/cassettes/*/$f unused"
        ret=1
    fi
done
exit "$ret"
