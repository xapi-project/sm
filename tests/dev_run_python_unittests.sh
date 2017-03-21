#!/bin/bash
#set -eux # for debug only
set -eu

SCRIPTDIR=$(dirname $0)
SMROOT=$(cd $SCRIPTDIR && cd .. && pwd)
ENVDIR="$SMROOT/.env"

if [ ! -d $ENVDIR ]; then
    $(dirname $0)/setup_env_for_python_unittests.sh
fi

set +u
. "$ENVDIR/bin/activate"
set -u

$SCRIPTDIR/run_python_unittests.sh
