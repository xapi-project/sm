#!/bin/bash
set -eux

# Get apt-add-repository
apt-get -qy install python-software-properties

# Add deadsnakes for ancient python packages
apt-add-repository ppa:fkrull/deadsnakes -y

apt-get update

# Install python 2.4
apt-get -qy install python2.4-dev python-distribute-deadsnakes

# Get virtualenv
easy_install-2.4 virtualenv==1.7.2

# Install other dependences
apt-get -qy install swig libxen-dev make
