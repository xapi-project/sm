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
pip install nose
pip install coverage
pip install mock==1.0.1
pip install bitarray
