#!/bin/bash
#set -eux # for debug only
set -eu

SMROOT=$(cd "$(dirname "$0")" && cd .. && pwd)

TESTS=tests

if [ $# -ge 1 ] && [ -n "$1" ]; then
    echo "Only testing $1"
    TESTS=$*
fi

(
    cd "$SMROOT"
    PYTHONPATH="$SMROOT/tests/mocks:$SMROOT/drivers/" \
        coverage run --branch \
            --source="$SMROOT/drivers,$SMROOT/tests" \
            -m unittest discover -s "$TESTS" -p "*.py" -v

    echo "Test coverage"
    if ! coverage report -m --fail-under=100 --include="$SMROOT/tests/*"
    then
        echo "Test code not fully covered"
        exit 1
    fi

    echo "Code coverage"
    OMITS="$SMROOT/tests/*,$SMROOT/.env/*,$SMROOT/tests/mocks/*"
    for format in html report; do
        coverage $format --include="$SMROOT/*" --omit="$OMITS"
    done

    coverage xml --include="$SMROOT/*"
)
