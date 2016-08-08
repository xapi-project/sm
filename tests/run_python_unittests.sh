#!/bin/bash
#set -eux # for debug only
set -eu

SMROOT=$(cd $(dirname $0) && cd .. && pwd)
ENVDIR="$SMROOT/.env"

if [ -z "${CHROOT-default}" ]; then
    if [ ! -d $ENVDIR ]; then
        $(dirname $0)/setup_env_for_python_unittests.sh
    fi

    set +u
    . "$ENVDIR/bin/activate"
    set -u
fi

(
    cd "$SMROOT"
    PYTHONPATH="$SMROOT/drivers/" \
        coverage run $(which nosetests) \
            --with-xunit \
            --xunit-file=nosetests.xml \
            tests
    coverage xml --include "$SMROOT/drivers/*"
)
