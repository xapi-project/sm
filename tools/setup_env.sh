#!/bin/bash
set -eux

SMROOT=$(cd $(dirname $0) && cd .. && pwd)
ENVDIR="$SMROOT/.env"

virtualenv-2.4 --no-site-packages "$ENVDIR"

set +u
. "$ENVDIR/bin/activate"
set -u

#pip install pep8==1.2
#pep8 --ignore=W601,E501,E401,W603,E711 drivers/ISCSISR.py

# 1.3 is not working
pip install nose==1.2.1
pip install xenapi
pip install mock

# build xslib.py
# I need -fPIC otherwise I get "relocation R_X86_64_32 against" type errors
make -C "$SMROOT/snapwatchd" CFLAGS="-O2 -I/usr/include/python2.4/ -I/usr/include -shared -fPIC"
