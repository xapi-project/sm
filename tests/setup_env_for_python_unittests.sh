#!/bin/bash
set -eux

SMROOT=$(cd $(dirname $0) && cd .. && pwd)
ENVDIR="$SMROOT/.env"

if [ "${USE_PYTHON24:-yes}" == "yes" ]; then
    virtualenv-2.4 --no-site-packages "$ENVDIR"
else
    virtualenv "$ENVDIR"
fi

set +u
. "$ENVDIR/bin/activate"
set -u

if [ "${USE_PYTHON24:-yes}" == "yes" ]; then
    pip install nose==1.2.1
else
    pip install nose
fi

pip install xenapi
pip install mock

TEMPDIR=$(mktemp -d)
# build xslib.py
# I need -fPIC otherwise I get "relocation R_X86_64_32 against" type errors
PYTHONLIBS=$(dirname $(find /usr/include/ -maxdepth 2 -path \*/python\*/Python.h -type f | head -1))
make -C "$SMROOT/snapwatchd" DESTDIR=$TEMPDIR CFLAGS="-O2 -I${PYTHONLIBS}/ -I/usr/include -shared -fPIC"
rm -rf "$TEMPDIR"
