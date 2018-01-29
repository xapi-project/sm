#!/bin/bash
#set -eux # for debug only
set -eu

SMROOT=$(cd $(dirname $0) && cd .. && pwd)
ENVDIR="$SMROOT/.env"

virtualenv "$ENVDIR"

set +u
. "$ENVDIR/bin/activate"
set -u

pip install six
pip install packaging
pip install appdirs
pip install --upgrade setuptools
pip install -r $SMROOT/dev_requirements.txt


