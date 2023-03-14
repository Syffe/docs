#!/bin/bash

count_requests() {
    yq '.interactions[].request.uri | select(. | contains("https://api.github.com"))' | wc -l
}

backlook=${1:=1}
git_ref="HEAD~${backlook}"

files=$(git diff --name-only $git_ref..HEAD 'zfixtures/*/http.yaml')

[ -z "$files" ] && echo "No fixtures changed" && exit 0

for file in $files; do
    test=${file#zfixtures/cassettes/}
    test=${test%/http.yaml}
    test=${test//\//::}
    before=$(git cat-file blob $git_ref:$file | count_requests)
    after=$(cat $file | count_requests)
    echo "$test: $before -> $after ($(($after-$before))) requests to github"
done
