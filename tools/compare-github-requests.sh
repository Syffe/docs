#!/bin/bash

count_requests() {
    yq '.interactions[].request.uri | select(. | startswith("https://api.github.com"))' | wc -l
}

git_ref=${1:-HEAD^}

files=$(git diff --name-only "$git_ref" 'zfixtures/*/http.yaml')

[ -z "$files" ] && echo "No fixtures changed" && exit 0

for file in $files; do
    test=${file#zfixtures/cassette/}
    test=${test%/http.yaml}
    before=$(git cat-file blob $git_ref:$file | count_requests)
    after=$(cat $file | count_requests)
    echo "$test: $before -> $after ($(($after-$before))) requests to github"
done
