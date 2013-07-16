Storage Manager for XenServer
=============================

This repository contains the code which forms the Stroage Management layer for
XenSever. It consists of a series of "plug-ins" to xapi (the Xen management
layer) which are written primarily in python.

## Run Python unit tests

### Install prerequisites

On an Ubuntu 12.04 64 bit system, run:

    sudo bash tools/install_prerequisites.sh

### Setup Virtual Environment

This script will create a new python virtualenv, install the dependencies to
it, compile the required python files for `snapwatchd`:

    tools/setup_env.sh

### Run the Tests

This script activates the virtual environment, and runs the tests with 
`nosetests`. It also tweaks `PYTHONPATH`, so python finds `xslib.py`

    tools/run_tests.sh
