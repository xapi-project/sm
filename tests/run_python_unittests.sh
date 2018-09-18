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
    PYTHONPATH="$SMROOT/tests/mocks:$SMROOT/drivers/:$SMROOT/scripts/" \
        coverage run --branch \
            --source="$SMROOT/drivers,$SMROOT/tests,$SMROOT/scripts/" \
            $(which nosetests) \
            -c .noserc \
            --with-xunit \
            --xunit-file=nosetests.xml \
            tests

    for format in xml html report; do
        coverage $format --include="$SMROOT/*" --omit="$SMROOT/.env/*,$SMROOT/tests/mocks/*"
    done
)
