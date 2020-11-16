#!/bin/bash
set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
poetry export -f requirements.txt -o requirements.txt
docker build --rm -t $1 -f ${DIR}/Dockerfile .
