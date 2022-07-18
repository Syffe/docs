#!/bin/bash

set -e

curl -o dd_packages.gz "https://s3.amazonaws.com/apt.datadoghq.com/dists/stable/7/binary-amd64/Packages.gz"
gzip -d dd_packages.gz
cat dd_packages | grep -A 1 'Package: datadog-agent$' | tail -n 1 | awk '{print $2}'
rm dd_packages
