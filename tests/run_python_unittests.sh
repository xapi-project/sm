#!/bin/bash
#set -eux # for debug only
set -eu

SMROOT=$(cd $(dirname $0) && cd .. && pwd)

TESTS=tests
FILES="*.py"

if [ $# -ge 1 ] && [ -n "$1" ]; then
    echo "Only testing $1"
    FILES=$1
fi

if  [ ! -v RPM_BUILD_ROOT ]; then
    echo "Activating virtual env"

    ENVDIR="$SMROOT/.env"

    if [ ! -d $ENVDIR ]; then
        $(dirname $0)/setup_env_for_python_unittests.sh
    fi

    set +u
    . "$ENVDIR/bin/activate"
    set -u
fi

(
    cd "$SMROOT"
    PYTHONPATH="$SMROOT/tests/mocks:$SMROOT/drivers/" \
        coverage run --branch \
            --source="$SMROOT/drivers,$SMROOT/tests" \
            -m unittest discover -s "$TESTS" -p "$FILES" -v

    # Handle coverage errors explicitly
    set +e

    echo "Test coverage"
    coverage report -m --fail-under=100 --include=$SMROOT/tests/*
    if [ $? -gt 0 -a $# -eq 0 ]
    then
        echo "Test code not fully covered"
        exit 1
    fi

    set -e

    echo "Code coverage"
    OMITS="$SMROOT/tests/*,$SMROOT/.env/*,$SMROOT/tests/mocks/*"
    for format in html report; do
        coverage $format --include="$SMROOT/*" --omit=$OMITS
    done

    coverage xml --include="$SMROOT/*"
)
