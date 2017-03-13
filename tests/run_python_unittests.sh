#!/bin/bash
#set -eux # for debug only
set -eu

SMROOT=$(cd $(dirname $0) && cd .. && pwd)


(
    cd "$SMROOT"
    PYTHONPATH="$SMROOT/tests/mocks:$SMROOT/drivers/" \
        coverage run --branch $(which nosetests) \
            --with-xunit \
            --xunit-file=nosetests.xml \
            tests
    coverage xml --include "$SMROOT/drivers/*"
    coverage report --include="$SMROOT/drivers/*"
)
