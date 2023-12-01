#!/bin/bash

count_requests() {
    python3 -c "import msgpack,sys; print(len([i for i in msgpack.unpack(sys.stdin.buffer)['interactions'] if 'https://api.github.com' in i['request']['uri']]));"
}

backlook=${1:=1}
git_ref="HEAD~${backlook}"

files=$(git diff --name-only $git_ref..HEAD 'zfixtures/*/http.msgpack')

[ -z "$files" ] && echo "No fixtures changed" && exit 0

for file in $files; do
    test=${file#zfixtures/cassettes/}
    test=${test%/http.msgpack}
    test=${test//\//::}
    before=$(git cat-file blob $git_ref:$file | count_requests)
    after=$(cat $file | count_requests)
    echo "$test: $before -> $after ($(($after-$before))) requests to github"
done
