#!/bin/bash
#set -eux # for debug only
set -eu

SMROOT=$(cd $(dirname $0) && cd .. && pwd)

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
            $(which nosetests) \
            -c .noserc \
            --with-xunit \
            --xunit-file=nosetests.xml \
            tests

    # Handle coverage errors explicitly
    set +e

    echo "Test coverage"
    coverage report -m --fail-under=100 --include=$SMROOT/tests/*

    if [ $? -ne 0 ]
    then
        echo "Test code not fully covered"
        exit 1
    fi

    set -e

    echo "Code coverage"
    OMITS="$SMROOT/tests/*,$SMROOT/.env/*,$SMROOT/tests/mocks/*"
    for format in xml html report; do
        coverage $format --include="$SMROOT/*" --omit=$OMITS
    done
)
