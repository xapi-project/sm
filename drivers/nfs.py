#!/usr/bin/python
#
# Copyright (C) Citrix Systems Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published
# by the Free Software Foundation; version 2.1 only.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
#
# nfs.py: NFS related utility functions

import util
import errno
import os
import xml.dom.minidom
import time

# The algorithm for tcp and udp (at least in the linux kernel) for
# NFS timeout on softmounts is as follows:
#
# UDP:
# As long as the request wasn't started more than timeo * (2 ^ retrans)
# in the past, keep doubling the timeout.
#
# TCP:
# As long as the request wasn't started more than timeo * (1 + retrans)
# in the past, keep increaing the timeout by timeo.
#
# The time when the retrans may retry has been made will be:
# For udp: timeo * (2 ^ retrans * 2 - 1)
# For tcp: timeo * n! where n is the smallest n for which n! > 1 + retrans
#
# thus for retrans=1, timeo can be the same for both tcp and udp,
# because the first doubling (timeo*2) is the same as the first increment
# (timeo+timeo).

RPCINFO_BIN = "/usr/sbin/rpcinfo"
SHOWMOUNT_BIN = "/usr/sbin/showmount"

DEFAULT_NFSVERSION = '3'

NFS_VERSION = [
    'nfsversion', 'for type=nfs, NFS protocol version - 3, 4']

NFS_SERVICE_WAIT = 30
NFS_SERVICE_RETRY = 6

class NfsException(Exception):

    def __init__(self, errstr):
        self.errstr = errstr


def check_server_tcp(server, nfsversion=DEFAULT_NFSVERSION):
    """Make sure that NFS over TCP/IP V3 is supported on the server.

    Returns True if everything is OK
    False otherwise.
    """
    try:
        sv = get_supported_nfs_versions(server)
        return (True if nfsversion in sv else False)
    except util.CommandException, inst:
        raise NfsException("rpcinfo failed or timed out: return code %d" %
                           inst.code)

def check_server_service(server):
    """Ensure NFS service is up and available on the remote server.

    Returns False if fails to detect service after 
    NFS_SERVICE_RETRY * NFS_SERVICE_WAIT
    """

    retries = 0
    errlist = [errno.EPERM, errno.EPIPE, errno.EIO]

    while True:
        try:
            services = util.pread([RPCINFO_BIN, "-s", "%s" % server])
            services = services.split("\n")
            for i in range(len(services)):
                if services[i].find("nfs") > 0:
                    return True
        except util.CommandException, inst:
            if not int(inst.code) in errlist:
                raise

        util.SMlog("NFS service not ready on server %s" % server)
        retries += 1
        if retries >= NFS_SERVICE_RETRY:
            break

        time.sleep(NFS_SERVICE_WAIT)

    return False


def validate_nfsversion(nfsversion):
    """Check the validity of 'nfsversion'.

    Raise an exception for any invalid version.
    """
    if not nfsversion:
        nfsversion = DEFAULT_NFSVERSION
    else:
        if nfsversion not in ['3', '4']:
            raise NfsException("Invalid nfsversion.")
    return nfsversion


def soft_mount(mountpoint, remoteserver, remotepath, transport, useroptions='',
               timeout=None, nfsversion=DEFAULT_NFSVERSION, retrans=None):
    """Mount the remote NFS export at 'mountpoint'.

    The 'timeout' param here is in seconds
    """
    try:
        if not util.ioretry(lambda: util.isdir(mountpoint)):
            util.ioretry(lambda: util.makedirs(mountpoint))
    except util.CommandException, inst:
        raise NfsException("Failed to make directory: code is %d" %
                           inst.code)


    # Wait for NFS service to be available
    try: 
        if not check_server_service(remoteserver):
            raise util.CommandException(code=errno.EOPNOTSUPP,
                    reason="No NFS service on host")
    except util.CommandException, inst: 
        raise NfsException("Failed to detect NFS service on server %s" 
                           % remoteserver)

    mountcommand = 'mount.nfs'
    if nfsversion == '4':
        mountcommand = 'mount.nfs4'

    options = "soft,proto=%s,vers=%s" % (
        transport,
        nfsversion)
    options += ',acdirmin=0,acdirmax=0'

    if timeout != None:
        options += ",timeo=%s" % (timeout * 10)
    if retrans != None:
        options += ",retrans=%s" % retrans
    if useroptions != '':
        options += ",%s" % useroptions

    try:
        util.ioretry(lambda:
                     util.pread([mountcommand, "%s:%s"
                                 % (remoteserver, remotepath),
                                 mountpoint, "-o", options]),
                     errlist=[errno.EPIPE, errno.EIO],
                     maxretry=2, nofail=True)
    except util.CommandException, inst:
        raise NfsException("mount failed with return code %d" % inst.code)


def unmount(mountpoint, rmmountpoint):
    """Unmount the mounted mountpoint"""
    try:
        util.pread(["umount", mountpoint])
    except util.CommandException, inst:
        raise NfsException("umount failed with return code %d" % inst.code)

    if rmmountpoint:
        try:
            os.rmdir(mountpoint)
        except OSError, inst:
            raise NfsException("rmdir failed with error '%s'" % inst.strerror)


def scan_exports(target):
    """Scan target and return an XML DOM with target, path and accesslist."""
    util.SMlog("scanning")
    cmd = [SHOWMOUNT_BIN, "--no-headers", "-e", target]
    dom = xml.dom.minidom.Document()
    element = dom.createElement("nfs-exports")
    dom.appendChild(element)
    for val in util.pread2(cmd).split('\n'):
        if not len(val):
            continue
        entry = dom.createElement('Export')
        element.appendChild(entry)

        subentry = dom.createElement("Target")
        entry.appendChild(subentry)
        textnode = dom.createTextNode(target)
        subentry.appendChild(textnode)

        (path, access) = val.split()
        subentry = dom.createElement("Path")
        entry.appendChild(subentry)
        textnode = dom.createTextNode(path)
        subentry.appendChild(textnode)

        subentry = dom.createElement("Accesslist")
        entry.appendChild(subentry)
        textnode = dom.createTextNode(access)
        subentry.appendChild(textnode)

    return dom


def scan_srlist(path, dconf):
    """Scan and report SR, UUID."""
    dom = xml.dom.minidom.Document()
    element = dom.createElement("SRlist")
    dom.appendChild(element)
    for val in filter(util.match_uuid, util.ioretry(
            lambda: util.listdir(path))):
        fullpath = os.path.join(path, val)
        if not util.ioretry(lambda: util.isdir(fullpath)):
            continue

        entry = dom.createElement('SR')
        element.appendChild(entry)

        subentry = dom.createElement("UUID")
        entry.appendChild(subentry)
        textnode = dom.createTextNode(val)
        subentry.appendChild(textnode)

    from NFSSR import PROBEVERSION
    if dconf.has_key(PROBEVERSION):
        util.SMlog("Add supported nfs versions to sr-probe")
        supported_versions = get_supported_nfs_versions(dconf.get('server'))
        supp_ver = dom.createElement("SupportedVersions")
        element.appendChild(supp_ver)

        for ver in supported_versions:
            version = dom.createElement('Version')
            supp_ver.appendChild(version)
            textnode = dom.createTextNode(ver)
            version.appendChild(textnode)

    return dom.toprettyxml()


def get_supported_nfs_versions(server):
    """Return list of supported nfs versions."""
    valid_versions = set(['3', '4'])
    cv = set()
    try:
        ns = util.pread2([RPCINFO_BIN, "-p", "%s" % server])
        ns = ns.split("\n")
        for i in range(len(ns)):
            if ns[i].find("nfs") > 0:
                cvi = ns[i].split()[1]
                cv.add(cvi)
        return list(cv & valid_versions)
    except:
        util.SMlog("Unable to obtain list of valid nfs versions")

def get_nfs_timeout(other_config):
    nfs_timeout = 10

    if other_config.has_key('nfs-timeout'):
        val = int(other_config['nfs-timeout'])
        if val < 1:
            util.SMlog("Invalid nfs-timeout value: %d" % val)
        else:
            nfs_timeout = val

    return nfs_timeout

def get_nfs_retrans(other_config):
    nfs_retrans = 12

    if other_config.has_key('nfs-retrans'):
        val = int(other_config['nfs-retrans']) 
        if val < 0:
            util.SMlog("Invalid nfs-retrans value: %d" % val)
        else:
            nfs_retrans = val

    return nfs_retrans
