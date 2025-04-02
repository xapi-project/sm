#!/usr/bin/bash
#set -eux # for debug only
set -eu

SMROOT=$(cd "$(dirname "$0")" && cd .. && pwd)

TESTS=tests

COVERAGE=$(command -v "coverage3" || :)

FILES="*.py"

CHECK_TEST_COVERAGE=1

if [ $# -ge 1 ] && [ -n "$1" ]; then
    echo "Only testing $1"
    CHECK_TEST_COVERAGE=0
    FILES=$1
fi

(
    cd "$SMROOT"
    PYTHONPATH="$SMROOT/mocks:$SMROOT/drivers:$SMROOT/libs:$SMROOT/misc/fairlock" \
        $COVERAGE run --branch \
            --source="$SMROOT/drivers,$SMROOT/libs/sm/core,$SMROOT/tests,$SMROOT/misc/fairlock" \
            -m unittest discover -f -s "$TESTS" -p "$FILES" -v

    echo "Test coverage"
    if ! $COVERAGE report -m --fail-under=100 --include="$SMROOT/tests/*"
    then
        echo "Test code not fully covered"
        if [ "$CHECK_TEST_COVERAGE" == 1 ]; then
            exit 1
        fi
    fi

    echo "Code coverage"
    OMITS="$SMROOT/tests/*,$SMROOT/.env/*,$SMROOT/tests/mocks/*"
    for format in html report; do
        $COVERAGE $format --include="$SMROOT/*" --omit="$OMITS"
    done

    $COVERAGE xml --include="$SMROOT/*"
)
