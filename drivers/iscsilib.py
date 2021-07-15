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

INITIATORNAME_FILE = '/etc/iscsi/initiatorname.iscsi'

import util
import os
import scsiutil
import time
import xs_errors
import socket
import re
import shutil
import xs_errors
import lock
import glob
import tempfile
from cleanup import LOCK_TYPE_RUNNING
from ConfigParser import RawConfigParser
import StringIO

# The 3.x kernel brings with it some iSCSI path changes in sysfs
_KERNEL_VERSION = os.uname()[2]
if _KERNEL_VERSION.startswith('2.6'):
    _GENERIC_SESSION_PATH = ('/sys/class/iscsi_host/host%s/device/session*/' +
            'iscsi_session*/')
    _GENERIC_CONNECTION_PATH = ('/sys/class/iscsi_host/host%s/device/' +
            'session*/connection*/iscsi_connection*/')
else:
    _GENERIC_SESSION_PATH = ('/sys/class/iscsi_host/host%s/device/session*/' +
            'iscsi_session/session*/')
    _GENERIC_CONNECTION_PATH = ('/sys/class/iscsi_host/host%s/device/' +
            'session*/connection*/iscsi_connection/connection*/')

_REPLACEMENT_TMO_MPATH = 15
_REPLACEMENT_TMO_DEFAULT = 144
_REPLACEMENT_TMO_STANDARD = 120

_ISCSI_DB_PATH = '/var/lib/iscsi'


def doexec_locked(cmd):
    '''Executes via util.doexec the command specified whilst holding lock'''
    _lock = None
    if os.path.basename(cmd[0]) == 'iscsiadm':
        _lock = lock.Lock(LOCK_TYPE_RUNNING, 'iscsiadm')
        _lock.acquire()
    #util.SMlog("%s" % (cmd))
    (rc, stdout, stderr) = util.doexec(cmd)
    if _lock is not None and _lock.held():
        _lock.release()
    return (rc, stdout, stderr)


def noexn_on_failure(cmd):
    '''Executes via util.doexec the command specified as best effort.'''
    (rc, stdout, stderr) = doexec_locked(cmd)
    return (stdout, stderr)


def exn_on_failure(cmd, message):
    '''Executes via util.doexec the command specified. If the return code is 
    non-zero, raises an ISCSIError with the given message'''
    (rc, stdout, stderr) = doexec_locked(cmd)
    if rc == 0:
        return (stdout, stderr)
    else:
        msg = 'rc: %d, stdout: %s, stderr: %s' % (rc, stdout, stderr)
        raise xs_errors.XenError('SMGeneral', opterr=msg)


def parse_node_output(text, targetIQN):
    """helper function - parses the output of iscsiadm for discovery and
    get_node_records"""

    def dotrans(x):
        (rec, iqn) = x.split()
        (portal, tpgt) = rec.split(',')
        return (portal, tpgt, iqn)
    unfiltered_map = map(dotrans, (filter(lambda x: match_targetIQN(targetIQN, x),
                                       text.split('\n'))))
    # We need to filter duplicates orignating from doing the discovery using
    # multiple interfaces
    filtered_map = []
    for input_value in unfiltered_map:
        if input_value not in filtered_map:
            filtered_map.append(input_value)
    return filtered_map


def parse_IP_port(portal):
    """Extract IP address and port number from portal information.

    Input: String encoding the IP address and port of form:
        - x.x.x.x:p                 (IPv4)
    or
        - [xxxx:xxxx:...:xxxx]:p    (IPv6)

    Return tuple of IP and port (without square brackets in case of IPv6):
    """
    (ipaddr, port) = portal.split(',')[0].rsplit(':', 1)
    if ipaddr[0] == '[':
        # This is IPv6, strip off [ ] surround
        ipaddr = ipaddr[1:-1]
    return (ipaddr, port)


def save_rootdisk_nodes(tmpdirname):
    root_iqns = get_rootdisk_IQNs()
    if root_iqns:
        srcdirs = map(lambda iqn: os.path.join(_ISCSI_DB_PATH, 'nodes', iqn),
                      root_iqns)
        util.doexec(['/bin/cp', '-a'] + srcdirs + [tmpdirname])


def restore_rootdisk_nodes(tmpdirname):
    root_iqns = get_rootdisk_IQNs()
    if root_iqns:
        srcdirs = map(lambda iqn: os.path.join(tmpdirname, iqn), root_iqns)
        util.doexec(['/bin/cp', '-a'] + srcdirs +
                    [os.path.join(_ISCSI_DB_PATH, 'nodes')])


def discovery(target, port, chapuser, chappass, targetIQN="any",
              interfaceArray=["default"]):
    """Run iscsiadm in discovery mode to obtain a list of the 
    TargetIQNs available on the specified target and port. Returns
    a list of triples - the portal (ip:port), the tpgt (target portal
    group tag) and the target name"""

    # Save configuration of root LUN nodes and restore after discovery
    # otherwise when we do a discovery on the same filer as is hosting
    # our root disk we'll reset the config of the root LUNs

    # FIXME: Replace this with TemporaryDirectory when moving to Python3
    tmpdirname = tempfile.mkdtemp()
    try:
        save_rootdisk_nodes(tmpdirname)

        if ':' in target:
            targetstring = "[%s]:%s" % (target, str(port))
        else:
            targetstring = "%s:%s" % (target, str(port))
        cmd_base = ["-t", "st", "-p", targetstring]
        for interface in interfaceArray:
            cmd_base.append("-I")
            cmd_base.append(interface)
        cmd_disc = ["iscsiadm", "-m", "discovery"] + cmd_base
        cmd_discdb = ["iscsiadm", "-m", "discoverydb"] + cmd_base
        auth_args = ["-n", "discovery.sendtargets.auth.authmethod", "-v", "CHAP",
                      "-n", "discovery.sendtargets.auth.username", "-v", chapuser,
                      "-n", "discovery.sendtargets.auth.password", "-v", chappass]
        fail_msg = "Discovery failed. Check target settings and " \
                   "username/password (if applicable)"
        try:
            if chapuser != "" and chappass != "":
                exn_on_failure(cmd_discdb + ["-o", "new"], fail_msg)
                exn_on_failure(cmd_discdb + ["-o", "update"] + auth_args, fail_msg)
                cmd = cmd_discdb + ["--discover"]
            else:
                cmd = cmd_disc
            (stdout, stderr) = exn_on_failure(cmd, fail_msg)
        except:
            raise xs_errors.XenError('ISCSILogin')
        finally:
            restore_rootdisk_nodes(tmpdirname)
    finally:
        shutil.rmtree(tmpdirname)

    return parse_node_output(stdout, targetIQN)


def get_node_records(targetIQN="any"):
    """Return the node records that the iscsi daemon already knows about"""
    cmd = ["iscsiadm", "-m", "node"]
    failuremessage = "Failed to obtain node records from iscsi daemon"
    (stdout, stderr) = exn_on_failure(cmd, failuremessage)
    return parse_node_output(stdout, targetIQN)


def set_chap_settings (portal, targetIQN, username, password, username_in, password_in):
    """Sets the username and password on the session identified by the 
    portal/targetIQN combination"""
    failuremessage = "Failed to set CHAP settings"
    cmd = ["iscsiadm", "-m", "node", "-p", portal, "-T", targetIQN, "--op",
           "update", "-n", "node.session.auth.authmethod", "-v", "CHAP"]
    (stdout, stderr) = exn_on_failure(cmd, failuremessage)

    cmd = ["iscsiadm", "-m", "node", "-p", portal, "-T", targetIQN, "--op",
           "update", "-n", "node.session.auth.username", "-v",
           username]
    (stdout, stderr) = exn_on_failure(cmd, failuremessage)

    cmd = ["iscsiadm", "-m", "node", "-p", portal, "-T", targetIQN, "--op",
           "update", "-n", "node.session.auth.password", "-v",
           password]
    (stdout, stderr) = exn_on_failure(cmd, failuremessage)

    if (username_in != ""):
        cmd = ["iscsiadm", "-m", "node", "-p", portal, "-T", targetIQN, "--op",
               "update", "-n", "node.session.auth.username_in", "-v",
               username_in]
        (stdout, stderr) = exn_on_failure(cmd, failuremessage)

        cmd = ["iscsiadm", "-m", "node", "-p", portal, "-T", targetIQN, "--op",
               "update", "-n", "node.session.auth.password_in", "-v",
               password_in]
        (stdout, stderr) = exn_on_failure(cmd, failuremessage)


def remove_chap_settings(portal, targetIQN):
    cmd = ["iscsiadm", "-m", "node", "-p", portal, "-T", targetIQN, "--op",
           "update", "-n", "node.session.auth.authmethod", "-v", "None"]
    (stdout, stderr) = noexn_on_failure(cmd)

    cmd = ["iscsiadm", "-m", "node", "-p", portal, "-T", targetIQN, "--op",
           "update", "-n", "node.session.auth.username", "-v", ""]
    (stdout, stderr) = noexn_on_failure(cmd)

    cmd = ["iscsiadm", "-m", "node", "-p", portal, "-T", targetIQN, "--op",
           "update", "-n", "node.session.auth.password", "-v", ""]
    (stdout, stderr) = noexn_on_failure(cmd)

    cmd = ["iscsiadm", "-m", "node", "-p", portal, "-T", targetIQN, "--op",
           "update", "-n", "node.session.auth.username_in", "-v", ""]
    (stdout, stderr) = noexn_on_failure(cmd)

    cmd = ["iscsiadm", "-m", "node", "-p", portal, "-T", targetIQN, "--op",
           "update", "-n", "node.session.auth.password_in", "-v", ""]
    (stdout, stderr) = noexn_on_failure(cmd)


def get_node_config (portal, targetIQN):
    ''' Using iscsadm to get the current configuration of a iscsi node.
    The output is parsed in ini format, and returned as a dictionary.'''
    failuremessage = "Failed to get node configurations"
    cmd = ["iscsiadm", "-m", "node", "-p", portal, "-T", targetIQN, "-S"]
    (stdout, stderr) = exn_on_failure(cmd, failuremessage)
    ini_sec = "root"
    str_fp = StringIO.StringIO("[%s]\n%s" % (ini_sec, stdout))
    parser = RawConfigParser()
    parser.readfp(str_fp)
    str_fp.close()
    return dict(parser.items(ini_sec))


def set_replacement_tmo (portal, targetIQN, mpath):
    key = "node.session.timeo.replacement_timeout"
    try:
        current_tmo = int((get_node_config(portal, targetIQN))[key])
    except:
        # Assume a standard TMO setting if get_node_config fails
        current_tmo = _REPLACEMENT_TMO_STANDARD
    # deliberately leave the "-p portal" arguments out, so that all the portals
    # always share the same config (esp. in corner case when switching from
    # mpath -> non-mpath, where we are only going to operate on one path). The
    # parameter could be useful if we want further flexibility in the future.
    cmd = ["iscsiadm", "-m", "node", "-T", targetIQN,  # "-p", portal,
           "--op", "update", "-n", key, "-v"]
    fail_msg = "Failed to set replacement timeout"
    if mpath:
        # Only switch if the current config is a well-known non-mpath setting
        if current_tmo in [_REPLACEMENT_TMO_DEFAULT, _REPLACEMENT_TMO_STANDARD]:
            cmd.append(str(_REPLACEMENT_TMO_MPATH))
            (stdout, stderr) = exn_on_failure(cmd, fail_msg)
        else:
            # the current_tmo is a customized value, no change
            util.SMlog("Keep the current replacement_timout value: %d." % current_tmo)
    else:
        # Only switch if the current config is a well-known mpath setting
        if current_tmo in [_REPLACEMENT_TMO_MPATH, _REPLACEMENT_TMO_STANDARD]:
            cmd.append(str(_REPLACEMENT_TMO_DEFAULT))
            (stdout, stderr) = exn_on_failure(cmd, fail_msg)
        else:
            # the current_tmo is a customized value, no change
            util.SMlog("Keep the current replacement_timout value: %d." % current_tmo)


def get_current_initiator_name():
    """Looks in the config file to see if we've already got a initiator name, 
    returning it if so, or else returning None"""
    if os.path.exists(INITIATORNAME_FILE):
        try:
            f = open(INITIATORNAME_FILE, 'r')
            for line in f.readlines():
                if line.strip().startswith("#"):
                    continue
                if "InitiatorName" in line:
                    IQN = line.split("=")[1]
                    currentIQN = IQN.strip()
                    f.close()
                    return currentIQN
            f.close()
        except IOError as e:
            return None
    return None


def get_system_alias():
    return socket.gethostname()


def set_current_initiator_name(localIQN):
    """Sets the initiator name in the config file. Raises an xs_error on error"""
    try:
        alias = get_system_alias()
        # MD3000i alias bug workaround
        if len(alias) > 30:
            alias = alias[0:30]
        f = open(INITIATORNAME_FILE, 'w')
        f.write('InitiatorName=%s\n' % localIQN)
        f.write('InitiatorAlias=%s\n' % alias)
        f.close()
    except IOError as e:
        raise xs_errors.XenError('ISCSIInitiator', \
                   opterr='Could not set initator name')


def login(portal, target, username, password, username_in="", password_in="",
          multipath=False):
    if username != "" and password != "":
        set_chap_settings(portal, target, username, password, username_in, password_in)
    else:
        remove_chap_settings(portal, target)

    set_replacement_tmo(portal, target, multipath)
    cmd = ["iscsiadm", "-m", "node", "-p", portal, "-T", target, "-l"]
    failuremessage = "Failed to login to target."
    try:
        (stdout, stderr) = exn_on_failure(cmd, failuremessage)
    except:
        raise xs_errors.XenError('ISCSILogin')


def logout(portal, target, all=False):
    if all:
        cmd = ["iscsiadm", "-m", "node", "-T", target, "-u"]
    else:
        cmd = ["iscsiadm", "-m", "node", "-p", portal, "-T", target, "-u"]
    failuremessage = "Failed to log out of target"
    try:
        (stdout, stderr) = exn_on_failure(cmd, failuremessage)
    except:
        raise xs_errors.XenError('ISCSILogout')


def delete(target):
    cmd = ["iscsiadm", "-m", "node", "-o", "delete", "-T", target]
    failuremessage = "Failed to delete target"
    try:
        (stdout, stderr) = exn_on_failure(cmd, failuremessage)
    except:
        raise xs_errors.XenError('ISCSIDelete')


def get_luns(targetIQN, portal):
    refresh_luns(targetIQN, portal)
    luns = []
    path = os.path.join("/dev/iscsi", targetIQN, portal)
    try:
        for file in util.listdir(path):
            if file.find("LUN") == 0 and file.find("_") == -1:
                lun = file.replace("LUN", "")
                luns.append(lun)
        return luns
    except util.CommandException as inst:
        raise xs_errors.XenError('ISCSIDevice', opterr='Failed to find any LUNs')


def is_iscsi_daemon_running():
    cmd = ["/sbin/pidof", "-s", "/sbin/iscsid"]
    (rc, stdout, stderr) = util.doexec(cmd)
    return (rc == 0)


def stop_daemon():
    if is_iscsi_daemon_running():
        cmd = ["service", "iscsid", "stop"]
        failuremessage = "Failed to stop iscsi daemon"
        exn_on_failure(cmd, failuremessage)


def restart_daemon():
    stop_daemon()
    if os.path.exists(os.path.join(_ISCSI_DB_PATH, 'nodes')):
        try:
            shutil.rmtree(os.path.join(_ISCSI_DB_PATH, 'nodes'))
        except:
            pass
        try:
            shutil.rmtree(os.path.join(_ISCSI_DB_PATH, 'send_targets'))
        except:
            pass
    cmd = ["service", "iscsid", "start"]
    failuremessage = "Failed to start iscsi daemon"
    exn_on_failure(cmd, failuremessage)


def wait_for_devs(targetIQN, portal):
    path = os.path.join("/dev/iscsi", targetIQN, portal)
    for i in range(0, 15):
        if os.path.exists(path):
            return True
        time.sleep(1)
    return False


def refresh_luns(targetIQN, portal):
    wait_for_devs(targetIQN, portal)
    try:
        path = os.path.join("/dev/iscsi", targetIQN, portal)
        id = scsiutil.getSessionID(path)
        f = open('/sys/class/scsi_host/host%s/scan' % id, 'w')
        f.write('- - -\n')
        f.close()
        time.sleep(2)  # FIXME
    except:
        pass


def get_IQN_paths():
    """Return the list of iSCSI session directories"""
    return glob.glob(_GENERIC_SESSION_PATH % '*')


def get_targetIQN(iscsi_host):
    """Get target IQN from sysfs for given iSCSI host number"""
    iqn_file = os.path.join(_GENERIC_SESSION_PATH % iscsi_host, 'targetname')
    targetIQN = util.get_single_entry(glob.glob(iqn_file)[0])
    return targetIQN


def get_targetIP_and_port(iscsi_host):
    """Get target IP address and port for given iSCSI host number"""
    connection_dir = _GENERIC_CONNECTION_PATH % iscsi_host
    ip = util.get_single_entry(glob.glob(os.path.join(
            connection_dir, 'persistent_address'))[0])
    port = util.get_single_entry(glob.glob(os.path.join(
            connection_dir, 'persistent_port'))[0])
    return (ip, port)


def get_path(targetIQN, portal, lun):
    """Gets the path of a specified LUN - this should be e.g. '1' or '5'"""
    path = os.path.join("/dev/iscsi", targetIQN, portal)
    return os.path.join(path, "LUN" + lun)


def get_path_safe(targetIQN, portal, lun):
    """Gets the path of a specified LUN, and ensures that it exists.
    Raises an exception if it hasn't appeared after the timeout"""
    path = get_path(targetIQN, portal, lun)
    for i in range(0, 15):
        if os.path.exists(path):
            return path
        time.sleep(1)
    raise xs_errors.XenError('ISCSIDevice', \
                       opterr='LUN failed to appear at path %s' % path)


def match_target(tgt, s):
    regex = re.compile(tgt)
    return regex.search(s, 0)


def match_targetIQN(tgtIQN, s):
    if not len(s):
        return False
    if tgtIQN == "any":
        return True
    # Extract IQN from iscsiadm -m session
    # Ex: tcp: [17] 10.220.98.9:3260,1 iqn.2009-01.xenrt.test:iscsi4181a93e
    siqn = s.split(",")[1].split()[1]
    return (siqn == tgtIQN)


def match_session(s):
    regex = re.compile("^tcp:")
    return regex.search(s, 0)


def _checkTGT(tgtIQN, tgt=''):
    if not is_iscsi_daemon_running():
        return False
    failuremessage = "Failure occured querying iscsi daemon"
    cmd = ["iscsiadm", "-m", "session"]
    try:
        (stdout, stderr) = exn_on_failure(cmd, failuremessage)
    # Recent versions of iscsiadm return error it this list is empty.
    # Quick and dirty handling
    except Exception as e:
        util.SMlog("%s failed with %s" % (cmd, e.args))
        stdout = ""
    for line in stdout.split('\n'):
        if match_targetIQN(tgtIQN, line) and match_session(line):
            if len(tgt):
                if match_target(tgt, line):
                    return True
            else:
                return True
    return False


def get_rootdisk_IQNs():
    """Return the list of IQNs for targets required by root filesystem"""
    if not os.path.isdir('/sys/firmware/ibft/'):
        return []
    dirs = filter(lambda x: x.startswith('target'), os.listdir('/sys/firmware/ibft/'))
    return map(lambda d: open('/sys/firmware/ibft/%s/target-name' % d).read().strip(), dirs)


def _checkAnyTGT():
    if not is_iscsi_daemon_running():
        return False
    rootIQNs = get_rootdisk_IQNs()
    failuremessage = "Failure occured querying iscsi daemon"
    cmd = ["iscsiadm", "-m", "session"]
    try:
        (stdout, stderr) = exn_on_failure(cmd, failuremessage)
    # Recent versions of iscsiadm return error it this list is empty.
    # Quick and dirty handling
    except Exception as e:
        util.SMlog("%s failed with %s" % (cmd, e.args))
        stdout = ""
    for e in filter(match_session, stdout.split('\n')):
        iqn = e.split()[-1]
        if not iqn in rootIQNs:
            return True
    return False


def ensure_daemon_running_ok(localiqn):
    """Check that the daemon is running and the initiator name is correct"""
    if not is_iscsi_daemon_running():
        set_current_initiator_name(localiqn)
        restart_daemon()
    else:
        currentiqn = get_current_initiator_name()
        if currentiqn != localiqn:
            if _checkAnyTGT():
                raise xs_errors.XenError('ISCSIInitiator', \
                          opterr='Daemon already running with ' \
                          + 'target(s) attached using ' \
                          + 'different IQN')
            set_current_initiator_name(localiqn)
            restart_daemon()


def get_iscsi_interfaces():
    result = []
    try:
        # Get all configured iscsiadm interfaces
        cmd = ["iscsiadm", "-m", "iface"]
        (stdout, stderr) = exn_on_failure(cmd,
                            "Failure occured querying iscsi daemon")
        # Get the interface (first column) from a line such as default
        # tcp,<empty>,<empty>,<empty>,<empty>
        for line in stdout.split("\n"):
            line_element = line.split(" ")
            interface_name = line_element[0]
            # ignore interfaces which aren't marked as starting with
            # c_.
            if len(line_element) == 2 and interface_name[:2] == "c_":
                result.append(interface_name)
    except:
        # Ignore exception from exn on failure, still return the default
        # interface
        pass
    # In case there are no configured interfaces, still add the default
    # interface
    if len(result) == 0:
        result.append("default")
    return result
