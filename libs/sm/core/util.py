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
# Miscellaneous utility functions
#

import os
import re
import sys
import subprocess
import shutil
import tempfile
import signal
import time
import datetime
import errno
import socket
import xml.dom.minidom
from sm.core import scsiutil
import stat
from sm.core import xs_errors
from sm.core import f_exceptions
import XenAPI # pylint: disable=import-error
import xmlrpc.client
import base64
import syslog
import resource
import traceback
import glob
import copy
import tempfile

from functools import reduce

NO_LOGGING_STAMPFILE = '/etc/xensource/no_sm_log'

IORETRY_MAX = 20  # retries
IORETRY_PERIOD = 1.0  # seconds

LOGGING = not (os.path.exists(NO_LOGGING_STAMPFILE))
_SM_SYSLOG_FACILITY = syslog.LOG_LOCAL2
LOG_EMERG = syslog.LOG_EMERG
LOG_ALERT = syslog.LOG_ALERT
LOG_CRIT = syslog.LOG_CRIT
LOG_ERR = syslog.LOG_ERR
LOG_WARNING = syslog.LOG_WARNING
LOG_NOTICE = syslog.LOG_NOTICE
LOG_INFO = syslog.LOG_INFO
LOG_DEBUG = syslog.LOG_DEBUG

ISCSI_REFDIR = '/run/sr-ref'

CMD_DD = "/bin/dd"
CMD_KICKPIPE = '/usr/libexec/sm/kickpipe'

FIST_PAUSE_PERIOD = 30  # seconds


class SMException(Exception):
    """Base class for all SM exceptions for easier catching & wrapping in
    XenError"""


class CommandException(SMException):
    def error_message(self, code):
        if code > 0:
            return os.strerror(code)
        elif code < 0:
            return "Signalled %s" % (abs(code))
        return "Success"

    def __init__(self, code, cmd="", reason='exec failed'):
        self.code = code
        self.cmd = cmd
        self.reason = reason
        Exception.__init__(self, self.error_message(code))


class SRBusyException(SMException):
    """The SR could not be locked"""
    pass


def logException(tag):
    info = sys.exc_info()
    if info[0] == SystemExit:
        # this should not be happening when catching "Exception", but it is
        sys.exit(0)
    tb = reduce(lambda a, b: "%s%s" % (a, b), traceback.format_tb(info[2]))
    str = "***** %s: EXCEPTION %s, %s\n%s" % (tag, info[0], info[1], tb)
    SMlog(str)


def roundup(divisor, value):
    """Retruns the rounded up value so it is divisible by divisor."""

    if value == 0:
        value = 1
    if value % divisor != 0:
        return ((int(value) // divisor) + 1) * divisor
    return value


def to_plain_string(obj):
    if obj is None:
        return None
    if type(obj) == str:
        return obj
    return str(obj)


def shellquote(arg):
    return '"%s"' % arg.replace('"', '\\"')


def make_WWN(name):
    hex_prefix = name.find("0x")
    if (hex_prefix >= 0):
        name = name[name.find("0x") + 2:len(name)]
    # inject dashes for each nibble
    if (len(name) == 16):  # sanity check
        name = name[0:2] + "-" + name[2:4] + "-" + name[4:6] + "-" + \
               name[6:8] + "-" + name[8:10] + "-" + name[10:12] + "-" + \
               name[12:14] + "-" + name[14:16]
    return name


def _logToSyslog(ident, facility, priority, message):
    syslog.openlog(ident, 0, facility)
    syslog.syslog(priority, "[%d] %s" % (os.getpid(), message))
    syslog.closelog()


def SMlog(message, ident="SM", priority=LOG_INFO):
    if LOGGING:
        for message_line in str(message).split('\n'):
            _logToSyslog(ident, _SM_SYSLOG_FACILITY, priority, message_line)


def _getDateString():
    d = datetime.datetime.now()
    t = d.timetuple()
    return "%s-%s-%s:%s:%s:%s" % \
          (t[0], t[1], t[2], t[3], t[4], t[5])


def doexec(args, inputtext=None, new_env=None, text=True):
    """Execute a subprocess, then return its return code, stdout and stderr"""
    env = None
    if new_env:
        env = dict(os.environ)
        env.update(new_env)
    proc = subprocess.Popen(args, stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            close_fds=True, env=env,
                            universal_newlines=text)

    if not text and inputtext is not None:
        inputtext = inputtext.encode()

    (stdout, stderr) = proc.communicate(inputtext)

    rc = proc.returncode
    return rc, stdout, stderr


def is_string(value):
    return isinstance(value, str)


# These are partially tested functions that replicate the behaviour of
# the original pread,pread2 and pread3 functions. Potentially these can
# replace the original ones at some later date.
#
# cmdlist is a list of either single strings or pairs of strings. For
# each pair, the first component is passed to exec while the second is
# written to the logs.
def pread(cmdlist, close_stdin=False, scramble=None, expect_rc=0,
          quiet=False, new_env=None, text=True):
    cmdlist_for_exec = []
    cmdlist_for_log = []
    for item in cmdlist:
        if is_string(item):
            cmdlist_for_exec.append(item)
            if scramble:
                if item.find(scramble) != -1:
                    cmdlist_for_log.append("<filtered out>")
                else:
                    cmdlist_for_log.append(item)
            else:
                cmdlist_for_log.append(item)
        else:
            cmdlist_for_exec.append(item[0])
            cmdlist_for_log.append(item[1])

    if not quiet:
        SMlog(cmdlist_for_log)
    (rc, stdout, stderr) = doexec(cmdlist_for_exec, new_env=new_env, text=text)
    if rc != expect_rc:
        SMlog("FAILED in util.pread: (rc %d) stdout: '%s', stderr: '%s'" % \
                (rc, stdout, stderr))
        if quiet:
            SMlog("Command was: %s" % cmdlist_for_log)
        if '' == stderr:
            stderr = stdout
        raise CommandException(rc, str(cmdlist), stderr.strip())
    if not quiet:
        SMlog("  pread SUCCESS")
    return stdout


# POSIX guaranteed atomic within the same file system.
# Supply directory to ensure tempfile is created
# in the same directory.
def atomicFileWrite(targetFile, directory, text):

    file = None
    try:
        # Create file only current pid can write/read to
        # our responsibility to clean it up.
        _, tempPath = tempfile.mkstemp(dir=directory)
        file = open(tempPath, 'w')
        file.write(text)

        # Ensure flushed to disk.
        file.flush()
        os.fsync(file.fileno())
        file.close()

        os.rename(tempPath, targetFile)
    except OSError:
        SMlog("FAILED to atomic write to %s" % (targetFile))

    finally:
        if (file is not None) and (not file.closed):
            file.close()

        if os.path.isfile(tempPath):
            os.remove(tempPath)


#Read STDOUT from cmdlist and discard STDERR output
def pread2(cmdlist, quiet=False, text=True):
    return pread(cmdlist, quiet=quiet, text=text)


#Read STDOUT from cmdlist, feeding 'text' to STDIN
def pread3(cmdlist, text):
    SMlog(cmdlist)
    (rc, stdout, stderr) = doexec(cmdlist, text)
    if rc:
        SMlog("FAILED in util.pread3: (errno %d) stdout: '%s', stderr: '%s'" % \
                (rc, stdout, stderr))
        if '' == stderr:
            stderr = stdout
        raise CommandException(rc, str(cmdlist), stderr.strip())
    SMlog("  pread3 SUCCESS")
    return stdout


def listdir(path, quiet=False):
    cmd = ["ls", path, "-1", "--color=never"]
    try:
        text = pread2(cmd, quiet=quiet)[:-1]
        if len(text) == 0:
            return []
        return text.split('\n')
    except CommandException as inst:
        if inst.code == errno.ENOENT:
            raise CommandException(errno.EIO, inst.cmd, inst.reason)
        else:
            raise CommandException(inst.code, inst.cmd, inst.reason)


def gen_uuid():
    cmd = ["uuidgen", "-r"]
    return pread(cmd)[:-1]


def match_uuid(s):
    regex = re.compile("^[0-9a-f]{8}-(([0-9a-f]{4})-){3}[0-9a-f]{12}")
    return regex.search(s, 0)


def findall_uuid(s):
    regex = re.compile("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
    return regex.findall(s, 0)


def exactmatch_uuid(s):
    regex = re.compile("^[0-9a-f]{8}-(([0-9a-f]{4})-){3}[0-9a-f]{12}$")
    return regex.search(s, 0)


def start_log_entry(srpath, path, args):
    logstring = str(datetime.datetime.now())
    logstring += " log: "
    logstring += srpath
    logstring += " " + path
    for element in args:
        logstring += " " + element
    try:
        file = open(srpath + "/filelog.txt", "a")
        file.write(logstring)
        file.write("\n")
        file.close()
    except:
        pass

        # failed to write log ...

def end_log_entry(srpath, path, args):
    # for teminating, use "error" or "done"
    logstring = str(datetime.datetime.now())
    logstring += " end: "
    logstring += srpath
    logstring += " " + path
    for element in args:
        logstring += " " + element
    try:
        file = open(srpath + "/filelog.txt", "a")
        file.write(logstring)
        file.write("\n")
        file.close()
    except:
        pass

        # failed to write log ...
        # for now print
        # print "%s" % logstring

def ioretry(f, errlist=[errno.EIO], maxretry=IORETRY_MAX, period=IORETRY_PERIOD, **ignored):
    retries = 0
    while True:
        try:
            return f()
        except OSError as ose:
            err = int(ose.errno)
            if not err in errlist:
                raise CommandException(err, str(f), "OSError")
        except CommandException as ce:
            if not int(ce.code) in errlist:
                raise

        retries += 1
        if retries >= maxretry:
            break

        time.sleep(period)

    raise CommandException(errno.ETIMEDOUT, str(f), "Timeout")


def ioretry_stat(path, maxretry=IORETRY_MAX):
    # this ioretry is similar to the previous method, but
    # stat does not raise an error -- so check its return
    retries = 0
    while retries < maxretry:
        stat = os.statvfs(path)
        if stat.f_blocks != -1:
            return stat
        time.sleep(1)
        retries += 1
    raise CommandException(errno.EIO, "os.statvfs")


def sr_get_capability(sr_uuid, session=None):
    result = []
    local_session = None
    if session is None:
        local_session = get_localAPI_session()
        session = local_session

    try:
        sr_ref = session.xenapi.SR.get_by_uuid(sr_uuid)
        sm_type = session.xenapi.SR.get_record(sr_ref)['type']
        sm_rec = session.xenapi.SM.get_all_records_where(
            "field \"type\" = \"%s\"" % sm_type)

        # SM expects at least one entry of any SR type
        if len(sm_rec) > 0:
            result = list(sm_rec.values())[0]['capabilities']

        return result
    finally:
        if local_session:
            local_session.xenapi.session.logout()

def sr_get_driver_info(driver_info):
    results = {}
    # first add in the vanilla stuff
    for key in ['name', 'description', 'vendor', 'copyright', \
                 'driver_version', 'required_api_version']:
        results[key] = driver_info[key]
    # add the capabilities (xmlrpc array)
    # enforcing activate/deactivate for blktap2
    caps = driver_info['capabilities']
    if "ATOMIC_PAUSE" in caps:
        for cap in ("VDI_ACTIVATE", "VDI_DEACTIVATE"):
            if not cap in caps:
                caps.append(cap)
    elif "VDI_ACTIVATE" in caps or "VDI_DEACTIVATE" in caps:
        SMlog("Warning: vdi_[de]activate present for %s" % driver_info["name"])

    results['capabilities'] = caps
    # add in the configuration options
    options = []
    for option in driver_info['configuration']:
        options.append({'key': option[0], 'description': option[1]})
    results['configuration'] = options
    return xmlrpc.client.dumps((results, ), "", True)


def return_nil():
    return xmlrpc.client.dumps((None, ), "", True, allow_none=True)


def SRtoXML(SRlist):
    dom = xml.dom.minidom.Document()
    driver = dom.createElement("SRlist")
    dom.appendChild(driver)

    for key in SRlist.keys():
        dict = SRlist[key]
        entry = dom.createElement("SR")
        driver.appendChild(entry)

        e = dom.createElement("UUID")
        entry.appendChild(e)
        textnode = dom.createTextNode(key)
        e.appendChild(textnode)

        if 'size' in dict:
            e = dom.createElement("Size")
            entry.appendChild(e)
            textnode = dom.createTextNode(str(dict['size']))
            e.appendChild(textnode)

        if 'storagepool' in dict:
            e = dom.createElement("StoragePool")
            entry.appendChild(e)
            textnode = dom.createTextNode(str(dict['storagepool']))
            e.appendChild(textnode)

        if 'aggregate' in dict:
            e = dom.createElement("Aggregate")
            entry.appendChild(e)
            textnode = dom.createTextNode(str(dict['aggregate']))
            e.appendChild(textnode)

    return dom.toprettyxml()


def pathexists(path):
    try:
        os.lstat(path)
        return True
    except OSError as inst:
        if inst.errno == errno.EIO:
            time.sleep(1)
            try:
                listdir(os.path.realpath(os.path.dirname(path)))
                os.lstat(path)
                return True
            except:
                pass
            raise CommandException(errno.EIO, "os.lstat(%s)" % path, "failed")
        return False


def force_unlink(path):
    try:
        os.unlink(path)
    except OSError as e:
        if e.errno != errno.ENOENT:
            raise


def create_secret(session, secret):
    ref = session.xenapi.secret.create({'value': secret})
    return session.xenapi.secret.get_uuid(ref)


def get_secret(session, uuid):
    try:
        ref = session.xenapi.secret.get_by_uuid(uuid)
        return session.xenapi.secret.get_value(ref)
    except:
        raise xs_errors.XenError('InvalidSecret', opterr='Unable to look up secret [%s]' % uuid)


def wait_for_path(path, timeout):
    for i in range(0, timeout):
        if len(glob.glob(path)):
            return True
        time.sleep(1)
    return False


def wait_for_nopath(path, timeout):
    for i in range(0, timeout):
        if not os.path.exists(path):
            return True
        time.sleep(1)
    return False


def wait_for_path_multi(path, timeout):
    for i in range(0, timeout):
        paths = glob.glob(path)
        SMlog("_wait_for_paths_multi: paths = %s" % paths)
        if len(paths):
            SMlog("_wait_for_paths_multi: return first path: %s" % paths[0])
            return paths[0]
        time.sleep(1)
    return ""


def isdir(path):
    try:
        st = os.stat(path)
        return stat.S_ISDIR(st.st_mode)
    except OSError as inst:
        if inst.errno == errno.EIO:
            raise CommandException(errno.EIO, "os.stat(%s)" % path, "failed")
        return False


def get_single_entry(path):
    f = open(path, 'r')
    line = f.readline()
    f.close()
    return line.rstrip()


def get_fs_size(path):
    st = ioretry_stat(path)
    return st.f_blocks * st.f_frsize


def get_fs_utilisation(path):
    st = ioretry_stat(path)
    return (st.f_blocks - st.f_bfree) * \
            st.f_frsize


def ismount(path):
    """Test whether a path is a mount point"""
    try:
        s1 = os.stat(path)
        s2 = os.stat(os.path.join(path, '..'))
    except OSError as inst:
        raise CommandException(inst.errno, "os.stat")
    dev1 = s1.st_dev
    dev2 = s2.st_dev
    if dev1 != dev2:
        return True     # path/.. on a different device as path
    ino1 = s1.st_ino
    ino2 = s2.st_ino
    if ino1 == ino2:
        return True     # path/.. is the same i-node as path
    return False


def makedirs(name, mode=0o777):
    head, tail = os.path.split(name)
    if not tail:
        head, tail = os.path.split(head)
    if head and tail and not pathexists(head):
        makedirs(head, mode)
        if tail == os.curdir:
            return
    try:
        os.mkdir(name, mode)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(name):
            if mode:
                os.chmod(name, mode)
            pass
        else:
            raise


def zeroOut(path, fromByte, bytes):
    """write 'bytes' zeros to 'path' starting from fromByte (inclusive)"""
    blockSize = 4096

    fromBlock = fromByte // blockSize
    if fromByte % blockSize:
        fromBlock += 1
        bytesBefore = fromBlock * blockSize - fromByte
        if bytesBefore > bytes:
            bytesBefore = bytes
        bytes -= bytesBefore
        cmd = [CMD_DD, "if=/dev/zero", "of=%s" % path, "bs=1",
               "seek=%s" % fromByte, "count=%s" % bytesBefore]
        try:
            pread2(cmd)
        except CommandException:
            return False

    blocks = bytes // blockSize
    bytes -= blocks * blockSize
    fromByte = (fromBlock + blocks) * blockSize
    if blocks:
        cmd = [CMD_DD, "if=/dev/zero", "of=%s" % path, "bs=%s" % blockSize,
               "seek=%s" % fromBlock, "count=%s" % blocks]
        try:
            pread2(cmd)
        except CommandException:
            return False

    if bytes:
        cmd = [CMD_DD, "if=/dev/zero", "of=%s" % path, "bs=1",
               "seek=%s" % fromByte, "count=%s" % bytes]
        try:
            pread2(cmd)
        except CommandException:
            return False

    return True


def match_rootdev(s):
    regex = re.compile("^PRIMARY_DISK")
    return regex.search(s, 0)


def getrootdev():
    filename = '/etc/xensource-inventory'
    try:
        f = open(filename, 'r')
    except:
        raise xs_errors.XenError('EIO', \
              opterr="Unable to open inventory file [%s]" % filename)
    rootdev = ''
    for line in filter(match_rootdev, f.readlines()):
        rootdev = line.split("'")[1]
    if not rootdev:
        raise xs_errors.XenError('NoRootDev')
    return rootdev


def getrootdevID():
    rootdev = getrootdev()
    try:
        rootdevID = scsiutil.getSCSIid(rootdev)
    except:
        SMlog("util.getrootdevID: Unable to verify serial or SCSIid of device: %s" \
                   % rootdev)
        return ''

    if not len(rootdevID):
        SMlog("util.getrootdevID: Unable to identify scsi device [%s] via scsiID" \
                   % rootdev)

    return rootdevID


def get_localAPI_session():
    # First acquire a valid session
    session = XenAPI.xapi_local()
    try:
        session.xenapi.login_with_password('root', '', '', 'SM')
    except:
        raise xs_errors.XenError('APISession')
    return session


def get_this_host():
    uuid = None
    f = open("/etc/xensource-inventory", 'r')
    for line in f.readlines():
        if line.startswith("INSTALLATION_UUID"):
            uuid = line.split("'")[1]
    f.close()
    return uuid


def is_master(session):
    pools = session.xenapi.pool.get_all()
    master = session.xenapi.pool.get_master(pools[0])
    return get_this_host_ref(session) == master


def get_localhost_ref(session):
    filename = '/etc/xensource-inventory'
    try:
        f = open(filename, 'r')
    except:
        raise xs_errors.XenError('EIO', \
              opterr="Unable to open inventory file [%s]" % filename)
    domid = ''
    for line in filter(match_domain_id, f.readlines()):
        domid = line.split("'")[1]
    if not domid:
        raise xs_errors.XenError('APILocalhost')

    vms = session.xenapi.VM.get_all_records_where('field "uuid" = "%s"' % domid)
    for vm in vms:
        record = vms[vm]
        if record["uuid"] == domid:
            hostid = record["resident_on"]
            return hostid
    raise xs_errors.XenError('APILocalhost')


def match_domain_id(s):
    regex = re.compile("^CONTROL_DOMAIN_UUID")
    return regex.search(s, 0)


def get_hosts_attached_on(session, vdi_uuids):
    host_refs = {}
    for vdi_uuid in vdi_uuids:
        try:
            vdi_ref = session.xenapi.VDI.get_by_uuid(vdi_uuid)
        except XenAPI.Failure:
            SMlog("VDI %s not in db, ignoring" % vdi_uuid)
            continue
        sm_config = session.xenapi.VDI.get_sm_config(vdi_ref)
        for key in [x for x in sm_config.keys() if x.startswith('host_')]:
            host_refs[key[len('host_'):]] = True
    return host_refs.keys()


def get_this_host_ref(session):
    host_uuid = get_this_host()
    host_ref = session.xenapi.host.get_by_uuid(host_uuid)
    return host_ref


def get_slaves_attached_on(session, vdi_uuids):
    "assume this host is the SR master"
    host_refs = get_hosts_attached_on(session, vdi_uuids)
    master_ref = get_this_host_ref(session)
    return [x for x in host_refs if x != master_ref]


def get_online_hosts(session):
    online_hosts = []
    hosts = session.xenapi.host.get_all_records()
    for host_ref, host_rec in hosts.items():
        metricsRef = host_rec["metrics"]
        metrics = session.xenapi.host_metrics.get_record(metricsRef)
        if metrics["live"]:
            online_hosts.append(host_ref)
    return online_hosts


def get_all_slaves(session):
    "assume this host is the SR master"
    host_refs = get_online_hosts(session)
    master_ref = get_this_host_ref(session)
    return [x for x in host_refs if x != master_ref]


def is_attached_rw(sm_config):
    for key, val in sm_config.items():
        if key.startswith("host_") and val == "RW":
            return True
    return False


def attached_as(sm_config):
    for key, val in sm_config.items():
        if key.startswith("host_") and (val == "RW" or val == "RO"):
            return val


def find_my_pbd_record(session, host_ref, sr_ref):
    try:
        pbds = session.xenapi.PBD.get_all_records()
        for pbd_ref in pbds.keys():
            if pbds[pbd_ref]['host'] == host_ref and pbds[pbd_ref]['SR'] == sr_ref:
                return [pbd_ref, pbds[pbd_ref]]
        return None
    except Exception as e:
        SMlog("Caught exception while looking up PBD for host %s SR %s: %s" % (str(host_ref), str(sr_ref), str(e)))
        return None


def find_my_pbd(session, host_ref, sr_ref):
    ret = find_my_pbd_record(session, host_ref, sr_ref)
    if ret is not None:
        return ret[0]
    else:
        return None


def test_hostPBD_devs(session, sr_uuid, devs):
    host = get_localhost_ref(session)
    sr = session.xenapi.SR.get_by_uuid(sr_uuid)
    try:
        pbds = session.xenapi.PBD.get_all_records()
    except:
        raise xs_errors.XenError('APIPBDQuery')
    for dev in devs.split(','):
        for pbd in pbds:
            record = pbds[pbd]
            # it's ok if it's *our* PBD
            if record["SR"] == sr:
                break
            if record["host"] == host:
                devconfig = record["device_config"]
                if 'device' in devconfig:
                    for device in devconfig['device'].split(','):
                        if os.path.realpath(device) == os.path.realpath(dev):
                            return True
    return False


def test_hostPBD_lun(session, targetIQN, LUNid):
    host = get_localhost_ref(session)
    try:
        pbds = session.xenapi.PBD.get_all_records()
    except:
        raise xs_errors.XenError('APIPBDQuery')
    for pbd in pbds:
        record = pbds[pbd]
        if record["host"] == host:
            devconfig = record["device_config"]
            if 'targetIQN' in devconfig and 'LUNid' in devconfig:
                if devconfig['targetIQN'] == targetIQN and \
                       devconfig['LUNid'] == LUNid:
                    return True
    return False


def test_SCSIid(session, sr_uuid, SCSIid):
    if sr_uuid is not None:
        sr = session.xenapi.SR.get_by_uuid(sr_uuid)
    try:
        pbds = session.xenapi.PBD.get_all_records()
    except:
        raise xs_errors.XenError('APIPBDQuery')
    for pbd in pbds:
        record = pbds[pbd]
        # it's ok if it's *our* PBD
        # During FC SR creation, devscan.py passes sr_uuid as None
        if sr_uuid is not None:
            if record["SR"] == sr:
                break
        devconfig = record["device_config"]
        sm_config = session.xenapi.SR.get_sm_config(record["SR"])
        if 'SCSIid' in devconfig and devconfig['SCSIid'] == SCSIid:
            return True
        elif 'SCSIid' in sm_config and sm_config['SCSIid'] == SCSIid:
            return True
        elif 'scsi-' + SCSIid in sm_config:
            return True
    return False


class TimeoutException(SMException):
    pass


def timeout_call(timeoutseconds, function, *arguments):
    def handler(signum, frame):
        raise TimeoutException()
    signal.signal(signal.SIGALRM, handler)
    signal.alarm(timeoutseconds)
    try:
        return function(*arguments)
    finally:
        signal.alarm(0)


def _incr_iscsiSR_refcount(targetIQN, uuid):
    if not os.path.exists(ISCSI_REFDIR):
        os.mkdir(ISCSI_REFDIR)
    filename = os.path.join(ISCSI_REFDIR, targetIQN)
    try:
        f = open(filename, 'a+')
    except:
        raise xs_errors.XenError('LVMRefCount', \
                                 opterr='file %s' % filename)

    f.seek(0)
    found = False
    refcount = 0
    for line in filter(match_uuid, f.readlines()):
        refcount += 1
        if line.find(uuid) != -1:
            found = True
    if not found:
        f.write("%s\n" % uuid)
        refcount += 1
    f.close()
    return refcount


def _decr_iscsiSR_refcount(targetIQN, uuid):
    filename = os.path.join(ISCSI_REFDIR, targetIQN)
    if not os.path.exists(filename):
        return 0
    try:
        f = open(filename, 'a+')
    except:
        raise xs_errors.XenError('LVMRefCount', \
                                 opterr='file %s' % filename)

    f.seek(0)
    output = []
    refcount = 0
    for line in filter(match_uuid, f.readlines()):
        if line.find(uuid) == -1:
            output.append(line.rstrip())
            refcount += 1
    if not refcount:
        os.unlink(filename)
        return refcount

    # Re-open file and truncate
    f.close()
    f = open(filename, 'w')
    for i in range(0, refcount):
        f.write("%s\n" % output[i])
    f.close()
    return refcount


# The agent enforces 1 PBD per SR per host, so we
# check for active SR entries not attached to this host
def test_activePoolPBDs(session, host, uuid):
    try:
        pbds = session.xenapi.PBD.get_all_records()
    except:
        raise xs_errors.XenError('APIPBDQuery')
    for pbd in pbds:
        record = pbds[pbd]
        if record["host"] != host and record["SR"] == uuid \
               and record["currently_attached"]:
            return True
    return False


def remove_mpathcount_field(session, host_ref, sr_ref, SCSIid):
    try:
        pbdref = find_my_pbd(session, host_ref, sr_ref)
        if pbdref is not None:
            key = "mpath-" + SCSIid
            session.xenapi.PBD.remove_from_other_config(pbdref, key)
    except:
        pass


def kickpipe_mpathcount():
    """
    Issue a kick to the mpathcount service. This will ensure that mpathcount runs
    shortly to update the multipath config records, if it was not already activated
    by a UDEV event.
    """
    cmd = [CMD_KICKPIPE, "mpathcount"]
    (rc, stdout, stderr) = doexec(cmd)
    return (rc == 0)


def _testHost(hostname, port, errstring, timeout=5):
    SMlog("_testHost: Testing host/port: %s,%d" % (hostname, port))
    try:
        sockinfo = socket.getaddrinfo(hostname, int(port))[0]
    except:
        logException('Exception occured getting IP for %s' % hostname)
        raise xs_errors.XenError('DNSError')

    sock = socket.socket(sockinfo[0], socket.SOCK_STREAM)
    # Only allow the connect to block for up to timeout seconds
    sock.settimeout(timeout)
    try:
        sock.connect(sockinfo[4])
        # Fix for MS storage server bug
        sock.send(b'\n')
        sock.close()
    except socket.error as reason:
        SMlog("_testHost: Connect failed after %d seconds (%s) - %s" \
                   % (timeout, hostname, reason))
        raise xs_errors.XenError(errstring)

def testHost(hostname, port):
    """
    Wrapper for _testHost which returns boolean. This also uses the
    f_exceptions wrapper which serves as a test.
    """
    try:
        # The third argument doesn't really matter here because we always
        # swallow the exception and return False. It just needs to be a
        # valid argument to xs_errors.XenError()
        _testHost(hostname, port, "ISCSITarget(SMAPIv3)", timeout=10)
    except f_exceptions.XenError:
        return False
    return True

def match_scsiID(s, id):
    regex = re.compile(id)
    return regex.search(s, 0)


def _isSCSIid(s):
    regex = re.compile("^scsi-")
    return regex.search(s, 0)


def test_scsiserial(session, device):
    device = os.path.realpath(device)
    if not scsiutil._isSCSIdev(device):
        SMlog("util.test_scsiserial: Not a serial device: %s" % device)
        return False
    serial = ""
    try:
        serial += scsiutil.getserial(device)
    except:
        # Error allowed, SCSIid is the important one
        pass

    try:
        scsiID = scsiutil.getSCSIid(device)
    except:
        SMlog("util.test_scsiserial: Unable to verify serial or SCSIid of device: %s" \
                   % device)
        return False
    if not len(scsiID):
        SMlog("util.test_scsiserial: Unable to identify scsi device [%s] via scsiID" \
                   % device)
        return False

    try:
        SRs = session.xenapi.SR.get_all_records()
    except:
        raise xs_errors.XenError('APIFailure')
    for SR in SRs:
        record = SRs[SR]
        conf = record["sm_config"]
        if 'devserial' in conf:
            for dev in conf['devserial'].split(','):
                if _isSCSIid(dev):
                    if match_scsiID(dev, scsiID):
                        return True
                elif len(serial) and dev == serial:
                    return True
    return False


def default(self, field, thunk):
    try:
        return getattr(self, field)
    except:
        return thunk()


def list_VDI_records_in_sr(sr):
    """Helper function which returns a list of all VDI records for this SR
    stored in the XenAPI server, useful for implementing SR.scan"""
    sr_ref = sr.session.xenapi.SR.get_by_uuid(sr.uuid)
    vdis = sr.session.xenapi.VDI.get_all_records_where("field \"SR\" = \"%s\"" % sr_ref)
    return vdis


# Given a partition (e.g. sda1), get a disk name:
def diskFromPartition(partition):
    # check whether this is a device mapper device (e.g. /dev/dm-0)
    m = re.match('(/dev/)?(dm-[0-9]+)(p[0-9]+)?$', partition)
    if m is not None:
        return m.group(2)

    numlen = 0  # number of digit characters
    m = re.match("\D+(\d+)", partition)
    if m is not None:
        numlen = len(m.group(1))

    # is it a cciss?
    if True in [partition.startswith(x) for x in ['cciss', 'ida', 'rd']]:
        numlen += 1  # need to get rid of trailing 'p'

    # is it a mapper path?
    if partition.startswith("mapper"):
        if re.search("p[0-9]*$", partition):
            numlen = len(re.match("\d+", partition[::-1]).group(0)) + 1
            SMlog("Found mapper part, len %d" % numlen)
        else:
            numlen = 0

    # is it /dev/disk/by-id/XYZ-part<k>?
    if partition.startswith("disk/by-id"):
        return partition[:partition.rfind("-part")]

    return partition[:len(partition) - numlen]


def dom0_disks():
    """Disks carrying dom0, e.g. ['/dev/sda']"""
    disks = []
    with open("/etc/mtab", 'r') as f:
        for line in f:
            (dev, mountpoint, fstype, opts, freq, passno) = line.split(' ')
            if mountpoint == '/':
                disk = diskFromPartition(dev)
                if not (disk in disks):
                    disks.append(disk)
    SMlog("Dom0 disks: %s" % disks)
    return disks


def set_scheduler_sysfs_node(node, scheds):
    """
    Set the scheduler for a sysfs node (e.g. '/sys/block/sda')
    according to prioritized list schedulers
    Try to set the first item, then fall back to the next on failure
    """

    path = os.path.join(node, "queue", "scheduler")
    if not os.path.exists(path):
        SMlog("no path %s" % path)
        return

    stored_error = None
    for sched in scheds:
        try:
            with open(path, 'w') as file:
                file.write("%s\n" % sched)
            SMlog("Set scheduler to [%s] on [%s]" % (sched, node))
            return
        except (OSError, IOError) as err:
            stored_error = err

    SMlog("Error setting schedulers to [%s] on [%s], %s" % (scheds, node, str(stored_error)))


def set_scheduler(dev, schedulers=None):
    if schedulers is None:
        schedulers = ["none", "noop"]

    devices = []
    if not scsiutil.match_dm(dev):
        # Remove partition numbers
        devices.append(diskFromPartition(dev).replace('/', '!'))
    else:
        rawdev = diskFromPartition(dev)
        devices = [os.path.realpath(x)[5:] for x in scsiutil._genReverseSCSIidmap(rawdev.split('/')[-1])]

    for d in devices:
        set_scheduler_sysfs_node("/sys/block/%s" % d, schedulers)


# This function queries XAPI for the existing VDI records for this SR
def _getVDIs(srobj):
    VDIs = []
    try:
        sr_ref = getattr(srobj, 'sr_ref')
    except AttributeError:
        return VDIs

    refs = srobj.session.xenapi.SR.get_VDIs(sr_ref)
    for vdi in refs:
        ref = srobj.session.xenapi.VDI.get_record(vdi)
        ref['vdi_ref'] = vdi
        VDIs.append(ref)
    return VDIs


def _getVDI(srobj, vdi_uuid):
    vdi = srobj.session.xenapi.VDI.get_by_uuid(vdi_uuid)
    ref = srobj.session.xenapi.VDI.get_record(vdi)
    ref['vdi_ref'] = vdi
    return ref


def _convertDNS(name):
    addr = socket.getaddrinfo(name, None)[0][4][0]
    return addr


def _containsVDIinuse(srobj):
    VDIs = _getVDIs(srobj)
    for vdi in VDIs:
        if not vdi['managed']:
            continue
        sm_config = vdi['sm_config']
        if 'SRRef' in sm_config:
            try:
                PBDs = srobj.session.xenapi.SR.get_PBDs(sm_config['SRRef'])
                for pbd in PBDs:
                    record = PBDs[pbd]
                    if record["host"] == srobj.host_ref and \
                       record["currently_attached"]:
                        return True
            except:
                pass
    return False


def isVDICommand(cmd):
    if cmd is None or cmd in ["vdi_attach", "vdi_detach",
                              "vdi_activate", "vdi_deactivate",
                              "vdi_epoch_begin", "vdi_epoch_end"]:
        return True
    else:
        return False


#########################
# Daemon helper functions
def p_id_fork():
    try:
        p_id = os.fork()
    except OSError as e:
        print("Fork failed: %s (%d)" % (e.strerror, e.errno))
        sys.exit(-1)

    if (p_id == 0):
        os.setsid()
        try:
            p_id = os.fork()
        except OSError as e:
            print("Fork failed: %s (%d)" % (e.strerror, e.errno))
            sys.exit(-1)
        if (p_id == 0):
            os.chdir('/')
            os.umask(0)
        else:
            os._exit(0)
    else:
        os._exit(0)


def daemon():
    p_id_fork()
    # Query the max file descriptor parameter for this process
    maxfd = resource.getrlimit(resource.RLIMIT_NOFILE)[1]

    # Close any fds that are open
    for fd in range(0, maxfd):
        try:
            os.close(fd)
        except:
            pass

    # Redirect STDIN to STDOUT and STDERR
    os.open('/dev/null', os.O_RDWR)
    os.dup2(0, 1)
    os.dup2(0, 2)

################################################################################
#
#  Fist points
#

# * The global variable 'fistpoint' define the list of all possible fistpoints;
#
# * To activate a fistpoint called 'name', you need to create the file '/tmp/fist_name'
#   on the SR master;
#
# * At the moment, activating a fist point can lead to two possible behaviors:
#   - if '/tmp/fist_LVHDRT_exit' exists, then the function called during the fistpoint is _exit;
#   - otherwise, the function called is _pause.

def _pause(secs, name):
    SMlog("Executing fist point %s: sleeping %d seconds ..." % (name, secs))
    time.sleep(secs)
    SMlog("Executing fist point %s: done" % name)


def _exit(name):
    SMlog("Executing fist point %s: exiting the current process ..." % name)
    raise xs_errors.XenError('FistPoint', opterr='%s' % name)


class FistPoint:
    def __init__(self, points):
        #SMlog("Fist points loaded")
        self.points = points

    def is_legal(self, name):
        return (name in self.points)

    def is_active(self, name):
        return os.path.exists("/tmp/fist_%s" % name)

    def mark_sr(self, name, sruuid, started):
        session = get_localAPI_session()
        try:
            sr = session.xenapi.SR.get_by_uuid(sruuid)

            if started:
                session.xenapi.SR.add_to_other_config(sr, name, "active")
            else:
                session.xenapi.SR.remove_from_other_config(sr, name)
        finally:
            session.xenapi.session.logout()

    def activate(self, name, sruuid):
        if name in self.points:
            if self.is_active(name):
                self.mark_sr(name, sruuid, True)
                if self.is_active("LVHDRT_exit"):
                    self.mark_sr(name, sruuid, False)
                    _exit(name)
                else:
                    _pause(FIST_PAUSE_PERIOD, name)
                self.mark_sr(name, sruuid, False)
        else:
            SMlog("Unknown fist point: %s" % name)

    def activate_custom_fn(self, name, fn):
        if name in self.points:
            if self.is_active(name):
                SMlog("Executing fist point %s: starting ..." % name)
                fn()
                SMlog("Executing fist point %s: done" % name)
        else:
            SMlog("Unknown fist point: %s" % name)


def list_find(f, seq):
    for item in seq:
        if f(item):
            return item

GCPAUSE_FISTPOINT = "GCLoop_no_pause"

fistpoint = FistPoint(["LVHDRT_finding_a_suitable_pair",
                        "LVHDRT_inflating_the_parent",
                        "LVHDRT_resizing_while_vdis_are_paused",
                        "LVHDRT_coalescing_VHD_data",
                        "LVHDRT_coalescing_before_inflate_grandparent",
                        "LVHDRT_relinking_grandchildren",
                        "LVHDRT_before_create_relink_journal",
                        "LVHDRT_xapiSM_serialization_tests",
                        "LVHDRT_clone_vdi_after_create_journal",
                        "LVHDRT_clone_vdi_after_shrink_parent",
                        "LVHDRT_clone_vdi_after_first_snap",
                        "LVHDRT_clone_vdi_after_second_snap",
                        "LVHDRT_clone_vdi_after_parent_hidden",
                        "LVHDRT_clone_vdi_after_parent_ro",
                        "LVHDRT_clone_vdi_before_remove_journal",
                        "LVHDRT_clone_vdi_after_lvcreate",
                        "LVHDRT_clone_vdi_before_undo_clone",
                        "LVHDRT_clone_vdi_after_undo_clone",
                        "LVHDRT_inflate_after_create_journal",
                        "LVHDRT_inflate_after_setSize",
                        "LVHDRT_inflate_after_zeroOut",
                        "LVHDRT_inflate_after_setSizePhys",
                        "LVHDRT_inflate_after_setSizePhys",
                        "LVHDRT_coaleaf_before_coalesce",
                        "LVHDRT_coaleaf_after_coalesce",
                        "LVHDRT_coaleaf_one_renamed",
                        "LVHDRT_coaleaf_both_renamed",
                        "LVHDRT_coaleaf_after_vdirec",
                        "LVHDRT_coaleaf_before_delete",
                        "LVHDRT_coaleaf_after_delete",
                        "LVHDRT_coaleaf_before_remove_j",
                        "LVHDRT_coaleaf_undo_after_rename",
                        "LVHDRT_coaleaf_undo_after_rename2",
                        "LVHDRT_coaleaf_undo_after_refcount",
                        "LVHDRT_coaleaf_undo_after_deflate",
                        "LVHDRT_coaleaf_undo_end",
                        "LVHDRT_coaleaf_stop_after_recovery",
                        "LVHDRT_coaleaf_finish_after_inflate",
                        "LVHDRT_coaleaf_finish_end",
                        "LVHDRT_coaleaf_delay_1",
                        "LVHDRT_coaleaf_delay_2",
                        "LVHDRT_coaleaf_delay_3",
                        "testsm_clone_allow_raw",
                        "xenrt_default_vdi_type_legacy",
                        "blktap_activate_inject_failure",
                        "blktap_activate_error_handling",
                        GCPAUSE_FISTPOINT,
                        "cleanup_coalesceVHD_inject_failure",
                        "cleanup_tracker_no_progress",
                        "FileSR_fail_hardlink",
                        "FileSR_fail_snap1",
                        "FileSR_fail_snap2",
                        "LVM_journaler_exists",
                        "LVM_journaler_none",
                        "LVM_journaler_badname",
                        "LVM_journaler_readfail",
                        "LVM_journaler_writefail"])


def set_dirty(session, sr):
    try:
        session.xenapi.SR.add_to_other_config(sr, "dirty", "")
        SMlog("set_dirty %s succeeded" % (repr(sr)))
    except:
        SMlog("set_dirty %s failed (flag already set?)" % (repr(sr)))


def doesFileHaveOpenHandles(fileName):
    SMlog("Entering doesFileHaveOpenHandles with file: %s" % fileName)
    (retVal, processAndPidTuples) = \
        findRunningProcessOrOpenFile(fileName, False)

    if not retVal:
        SMlog("Failed to determine if file %s has open handles." % \
                   fileName)
        # err on the side of caution
        return True
    else:
        if len(processAndPidTuples) > 0:
            return True
        else:
            return False


# extract SR uuid from the passed in devmapper entry and return
# /dev/mapper/VG_XenStorage--c3d82e92--cb25--c99b--b83a--482eebab4a93-MGT
def extractSRFromDevMapper(path):
    try:
        path = os.path.basename(path)
        path = path[len('VG_XenStorage-') + 1:]
        path = path.replace('--', '/')
        path = path[0:path.rfind('-')]
        return path.replace('/', '-')
    except:
        return ''


def pid_is_alive(pid):
    """
    Try to kill PID with signal 0.
    If we succeed, the PID is alive, so return True.
    If we get an EPERM error, the PID is alive but we are not allowed to
    signal it. Still return true.
    Any other error (e.g. ESRCH), return False
    """
    try:
        os.kill(pid, 0)
        return True
    except OSError as e:
        if e.errno == errno.EPERM:
            return True
        return False


# Looks at /proc and figures either
#   If a process is still running (default), returns open file names
#   If any running process has open handles to the given file (process = False)
#       returns process names and pids
def findRunningProcessOrOpenFile(name, process=True):
    retVal = True
    links = []
    processandpids = []
    sockets = set()
    try:
        SMlog("Entering findRunningProcessOrOpenFile with params: %s" % \
                   [name, process])

        # Look at all pids
        pids = [pid for pid in os.listdir('/proc') if pid.isdigit()]
        for pid in sorted(pids):
            try:
                try:
                    f = None
                    f = open(os.path.join('/proc', pid, 'cmdline'), 'r')
                    prog = f.read()[:-1]
                    if prog:
                        # Just want the process name
                        argv = prog.split('\x00')
                        prog = argv[0]
                except IOError as e:
                    if e.errno in (errno.ENOENT, errno.ESRCH):
                        SMlog("ERROR %s reading %s, ignore" % (e.errno, pid))
                    continue
            finally:
                if f is not None:
                    f.close()

            try:
                fd_dir = os.path.join('/proc', pid, 'fd')
                files = os.listdir(fd_dir)
            except OSError as e:
                if e.errno in (errno.ENOENT, errno.ESRCH):
                    SMlog("ERROR %s reading fds for %s, ignore" % (e.errno, pid))
                    # Ignore pid that are no longer valid
                    continue
                else:
                    raise

            for file in files:
                try:
                    link = os.readlink(os.path.join(fd_dir, file))
                except OSError:
                    continue

                if process:
                    if name == prog:
                        links.append(link)
                else:
                    # need to return process name and pid tuples
                    if link == name:
                        processandpids.append((prog, pid))

            # Get the connected sockets
            if name == prog:
                sockets.update(get_connected_sockets(pid))

        # We will only have a non-empty processandpids if some fd entries were found.
        # Before returning them, verify that all the PIDs in question are properly alive.
        # There is no specific guarantee of when a PID's /proc directory will disappear
        # when it exits, particularly relative to filedescriptor cleanup, so we want to
        # make sure we're not reporting a false positive.
        processandpids = [x for x in processandpids if pid_is_alive(int(x[1]))]
        for pp in processandpids:
            SMlog(f"File {name} has an open handle with process {pp[0]} with pid {pp[1]}")

    except Exception as e:
        SMlog("Exception checking running process or open file handles. " \
                   "Error: %s" % str(e))
        retVal = False

    if process:
        return retVal, links, sockets
    else:
        return retVal, processandpids


def get_connected_sockets(pid):
    sockets = set()
    try:
        # Lines in /proc/<pid>/net/unix are formatted as follows
        # (see Linux source net/unix/af_unix.c, unix_seq_show() )
        # - Pointer address to socket (hex)
        # - Refcount (HEX)
        # - 0
        # - State (HEX, 0 or __SO_ACCEPTCON)
        # - Type (HEX - but only 0001 of interest)
        # - Connection state (HEX - but only 03, SS_CONNECTED  of interest)
        # - Inode number
        # - Path (optional)
        open_sock_matcher = re.compile(
            r'^[0-9a-f]+: [0-9A-Fa-f]+ [0-9A-Fa-f]+ [0-9A-Fa-f]+ 0001 03 \d+ (.*)$')
        with open(
                os.path.join('/proc', str(pid), 'net', 'unix'), 'r') as f:
            lines = f.readlines()
            for line in lines:
                match = open_sock_matcher.match(line)
                if match:
                    sockets.add(match[1])
    except OSError as e:
        if e.errno in (errno.ENOENT, errno.ESRCH):
            # Ignore pid that are no longer valid
            SMlog("ERROR %s reading sockets for %s, ignore" %
                  (e.errno, pid))
        else:
            raise
    return sockets


def retry(f, maxretry=20, period=3):
    retries = 0
    while True:
        try:
            return f()
        except Exception as e:
            SMlog("Got exception: %s. Retry number: %s" % (str(e), retries))

        retries += 1
        if retries >= maxretry:
            break

        time.sleep(period)

    return f()


def getCslDevPath(svid):
    basepath = "/dev/disk/by-csldev/"
    if svid.startswith("NETAPP_"):
        # special attention for NETAPP SVIDs
        svid_parts = svid.split("__")
        globstr = basepath + "NETAPP__LUN__" + "*" + svid_parts[2] + "*" + svid_parts[-1] + "*"
    else:
        globstr = basepath + svid + "*"

    return globstr


# Use device in /dev pointed to by cslg path which consists of svid
def get_scsiid_from_svid(md_svid):
    cslg_path = getCslDevPath(md_svid)
    abs_path = glob.glob(cslg_path)
    if abs_path:
        real_path = os.path.realpath(abs_path[0])
        return scsiutil.getSCSIid(real_path)
    else:
        return None


def get_isl_scsiids(session):
    # Get cslg type SRs
    SRs = session.xenapi.SR.get_all_records_where('field "type" = "cslg"')

    # Iterate through the SR to get the scsi ids
    scsi_id_ret = []
    for SR in SRs:
        sr_rec = SRs[SR]
        # Use the md_svid to get the scsi id
        scsi_id = get_scsiid_from_svid(sr_rec['sm_config']['md_svid'])
        if scsi_id:
            scsi_id_ret.append(scsi_id)

        # Get the vdis in the SR and do the same procedure
        vdi_recs = session.xenapi.VDI.get_all_records_where('field "SR" = "%s"' % SR)
        for vdi_rec in vdi_recs:
            vdi_rec = vdi_recs[vdi_rec]
            scsi_id = get_scsiid_from_svid(vdi_rec['sm_config']['SVID'])
            if scsi_id:
                scsi_id_ret.append(scsi_id)

    return scsi_id_ret


def get_scsi_id(path):
    """
    Compatibility wrapper for sm-core-libs which had its own copy
    of scsiutil.getSCSIid(). Converts any CommandException raised
    to a XenError as consumers of sm-core-libs are expecting.
    """
    try:
        return scsiutil.getSCSIid(path)
    except CommandException as e:
        raise f_exceptions.XenError("Command", e.reason)


class extractXVA:
    # streams files as a set of file and checksum, caller should remove
    # the files, if not needed. The entire directory (Where the files
    # and checksum) will only be deleted as part of class cleanup.
    HDR_SIZE = 512
    BLOCK_SIZE = 512
    SIZE_LEN = 12 - 1  # To remove \0 from tail
    SIZE_OFFSET = 124
    ZERO_FILLED_REC = 2
    NULL_IDEN = '\x00'
    DIR_IDEN = '/'
    CHECKSUM_IDEN = '.checksum'
    OVA_FILE = 'ova.xml'

    # Init gunzips the file using a subprocess, and reads stdout later
    # as and when needed
    def __init__(self, filename):
        self.__extract_path = ''
        self.__filename = filename
        cmd = 'gunzip -cd %s' % filename
        try:
            self.spawn_p = subprocess.Popen(
                            cmd, shell=True, \
                            stdin=subprocess.PIPE, stdout=subprocess.PIPE, \
                            stderr=subprocess.PIPE, close_fds=True)
        except Exception as e:
            SMlog("Error: %s. Uncompress failed for %s" % (str(e), filename))
            raise Exception(str(e))

        # Create dir to extract the files
        self.__extract_path = tempfile.mkdtemp()

    def __del__(self):
        shutil.rmtree(self.__extract_path)

    # Class supports Generator expression. 'for f_name, checksum in getTuple()'
    #   returns filename, checksum content. Returns filename, '' in case
    #   of checksum file missing. e.g. ova.xml
    def getTuple(self):
        zerod_record = 0
        ret_f_name = ''
        ret_base_f_name = ''

        try:
            # Read tar file as sets of file and checksum.
            while True:
                # Read the output of spawned process, or output of gunzip
                f_hdr = self.spawn_p.stdout.read(self.HDR_SIZE)

                # Break out in case of end of file
                if f_hdr == '':
                    if zerod_record == extractXVA.ZERO_FILLED_REC:
                        break
                    else:
                        SMlog('Error. Expects %d zero records', \
                               extractXVA.ZERO_FILLED_REC)
                        raise Exception('Unrecognized end of file')

                # Watch out for zero records, two zero records
                # denote end of file.
                if f_hdr == extractXVA.NULL_IDEN * extractXVA.HDR_SIZE:
                    zerod_record += 1
                    continue

                f_name = f_hdr[:f_hdr.index(extractXVA.NULL_IDEN)]
                # File header may be for a folder, if so ignore the header
                if not f_name.endswith(extractXVA.DIR_IDEN):
                    f_size_octal = f_hdr[extractXVA.SIZE_OFFSET: \
                                 extractXVA.SIZE_OFFSET + extractXVA.SIZE_LEN]
                    f_size = int(f_size_octal, 8)
                    if f_name.endswith(extractXVA.CHECKSUM_IDEN):
                        if f_name.rstrip(extractXVA.CHECKSUM_IDEN) == \
                                                        ret_base_f_name:
                            checksum = self.spawn_p.stdout.read(f_size)
                            yield(ret_f_name, checksum)
                        else:
                            # Expects file followed by its checksum
                            SMlog('Error. Sequence mismatch starting with %s', \
                                     ret_f_name)
                            raise Exception( \
                                    'Files out of sequence starting with %s', \
                                    ret_f_name)
                    else:
                        # In case of ova.xml, read the contents into a file and
                        # return the file name to the caller. For other files,
                        # read the contents into a file, it will
                        # be used when a .checksum file is encountered.
                        ret_f_name = '%s/%s' % (self.__extract_path, f_name)
                        ret_base_f_name = f_name

                        # Check if the folder exists on the target location,
                        # else create it.
                        folder_path = ret_f_name[:ret_f_name.rfind('/')]
                        if not os.path.exists(folder_path):
                            os.mkdir(folder_path)

                        # Store the file to the tmp folder, strip the tail \0
                        f = open(ret_f_name, 'w')
                        f.write(self.spawn_p.stdout.read(f_size))
                        f.close()
                        if f_name == extractXVA.OVA_FILE:
                            yield(ret_f_name, '')

                    # Skip zero'd portion of data block
                    round_off = f_size % extractXVA.BLOCK_SIZE
                    if round_off != 0:
                        zeros = self.spawn_p.stdout.read(
                                extractXVA.BLOCK_SIZE - round_off)
        except Exception as e:
            SMlog("Error: %s. File set extraction failed %s" % (str(e), \
                                                     self.__filename))

            # Kill and Drain stdout of the gunzip process,
            # else gunzip might block on stdout
            os.kill(self.spawn_p.pid, signal.SIGTERM)
            self.spawn_p.communicate()
            raise Exception(str(e))

illegal_xml_chars = [(0x00, 0x08), (0x0B, 0x1F), (0x7F, 0x84), (0x86, 0x9F),
                (0xD800, 0xDFFF), (0xFDD0, 0xFDDF), (0xFFFE, 0xFFFF),
                (0x1FFFE, 0x1FFFF), (0x2FFFE, 0x2FFFF), (0x3FFFE, 0x3FFFF),
                (0x4FFFE, 0x4FFFF), (0x5FFFE, 0x5FFFF), (0x6FFFE, 0x6FFFF),
                (0x7FFFE, 0x7FFFF), (0x8FFFE, 0x8FFFF), (0x9FFFE, 0x9FFFF),
                (0xAFFFE, 0xAFFFF), (0xBFFFE, 0xBFFFF), (0xCFFFE, 0xCFFFF),
                (0xDFFFE, 0xDFFFF), (0xEFFFE, 0xEFFFF), (0xFFFFE, 0xFFFFF),
                (0x10FFFE, 0x10FFFF)]

illegal_ranges = ["%s-%s" % (chr(low), chr(high))
        for (low, high) in illegal_xml_chars
        if low < sys.maxunicode]

illegal_xml_re = re.compile(u'[%s]' % u''.join(illegal_ranges))


def isLegalXMLString(s):
    """Tells whether this is a valid XML string (i.e. it does not contain
    illegal XML characters specified in
    http://www.w3.org/TR/2004/REC-xml-20040204/#charsets).
    """

    if len(s) > 0:
        return re.search(illegal_xml_re, s) is None
    else:
        return True


def unictrunc(string, max_bytes):
    """
    Given a string, returns the largest number of elements for a prefix
    substring of it, such that the UTF-8 encoding of this substring takes no
    more than the given number of bytes.

    The string may be given as a unicode string or a UTF-8 encoded byte
    string, and the number returned will be in characters or bytes
    accordingly.  Note that in the latter case, the substring will still be a
    valid UTF-8 encoded string (which is to say, it won't have been truncated
    part way through a multibyte sequence for a unicode character).

    string: the string to truncate
    max_bytes: the maximum number of bytes the truncated string can be
    """
    if isinstance(string, str):
        return_chars = True
    else:
        return_chars = False
        string = string.decode('UTF-8')

    cur_chars = 0
    cur_bytes = 0
    for char in string:
        charsize = len(char.encode('UTF-8'))
        if cur_bytes + charsize > max_bytes:
            break
        else:
            cur_chars += 1
            cur_bytes += charsize
    return cur_chars if return_chars else cur_bytes


def hideValuesInPropMap(propmap, propnames):
    """
    Worker function: input simple map of prop name/value pairs, and
    a list of specific propnames whose values we want to hide.
    Loop through the "hide" list, and if any are found, hide the
    value and return the altered map.
    If none found, return the original map
    """
    matches = []
    for propname in propnames:
        if propname in propmap:
            matches.append(propname)

    if matches:
        deepCopyRec = copy.deepcopy(propmap)
        for match in matches:
            deepCopyRec[match] = '******'
        return deepCopyRec

    return propmap
# define the list of propnames whose value we want to hide

PASSWD_PROP_KEYS = ['password', 'cifspassword', 'chappassword', 'incoming_chappassword']
DEFAULT_SEGMENT_LEN = 950


def hidePasswdInConfig(config):
    """
    Function to hide passwd values in a simple prop map,
    for example "device_config"
    """
    return hideValuesInPropMap(config, PASSWD_PROP_KEYS)


def hidePasswdInParams(params, configProp):
    """
    Function to hide password values in a specified property which
    is a simple map of prop name/values, and is itself an prop entry
    in a larger property map.
    For example, param maps containing "device_config", or
    "sm_config", etc
    """
    params[configProp] = hideValuesInPropMap(params[configProp], PASSWD_PROP_KEYS)
    return params


def hideMemberValuesInXmlParams(xmlParams, propnames=PASSWD_PROP_KEYS):
    """
    Function to hide password values in XML params, specifically
    for the XML format of incoming params to SR modules.
    Uses text parsing: loop through the list of specific propnames
    whose values we want to hide, and:
    - Assemble a full "prefix" containing each property name, e.g.,
        "<member><name>password</name><value>"
    - Test the XML if it contains that string, save the index.
    - If found, get the index of the ending tag
    - Truncate the return string starting with the password value.
    - Append the substitute "*******" value string.
    - Restore the rest of the original string starting with the end tag.
    """
    findStrPrefixHead = "<member><name>"
    findStrPrefixTail = "</name><value>"
    findStrSuffix = "</value>"
    strlen = len(xmlParams)

    for propname in propnames:
        findStrPrefix = findStrPrefixHead + propname + findStrPrefixTail
        idx = xmlParams.find(findStrPrefix)
        if idx != -1:                           # if found any of them
            idx += len(findStrPrefix)
            idx2 = xmlParams.find(findStrSuffix, idx)
            if idx2 != -1:
                retStr = xmlParams[0:idx]
                retStr += "******"
                retStr += xmlParams[idx2:strlen]
                return retStr
            else:
                return xmlParams
    return xmlParams


def splitXmlText(xmlData, segmentLen=DEFAULT_SEGMENT_LEN, showContd=False):
    """
    Split xml string data into substrings small enough for the
    syslog line length limit. Split at tag end markers ( ">" ).
    Usage:
        strList = []
        strList = splitXmlText( longXmlText, maxLineLen )   # maxLineLen is optional
    """
    remainingData = str(xmlData)

    # "Un-pretty-print"
    remainingData = remainingData.replace('\n', '')
    remainingData = remainingData.replace('\t', '')

    remainingChars = len(remainingData)
    returnData = ''

    thisLineNum = 0
    while remainingChars > segmentLen:
        thisLineNum = thisLineNum + 1
        index = segmentLen
        tmpStr = remainingData[:segmentLen]
        tmpIndex = tmpStr.rfind('>')
        if tmpIndex != -1:
            index = tmpIndex + 1

        tmpStr = tmpStr[:index]
        remainingData = remainingData[index:]
        remainingChars = len(remainingData)

        if showContd:
            if thisLineNum != 1:
                tmpStr = '(Cont\'d): ' + tmpStr
            tmpStr = tmpStr + ' (Cont\'d):'

        returnData += tmpStr + '\n'

    if showContd and thisLineNum > 0:
        remainingData = '(Cont\'d): ' + remainingData
    returnData += remainingData

    return returnData


def inject_failure():
    raise Exception('injected failure')


def open_atomic(path, mode=None):
    """Atomically creates a file if, and only if it does not already exist.
    Leaves the file open and returns the file object.

    path: the path to atomically open
    mode: "r" (read), "w" (write), or "rw" (read/write)
    returns: an open file object"""

    assert path

    flags = os.O_CREAT | os.O_EXCL
    modes = {'r': os.O_RDONLY, 'w': os.O_WRONLY, 'rw': os.O_RDWR}
    if mode:
        if mode not in modes:
            raise Exception('invalid access mode ' + mode)
        flags |= modes[mode]
    fd = os.open(path, flags)
    try:
        if mode:
            return os.fdopen(fd, mode)
        else:
            return os.fdopen(fd)
    except:
        os.close(fd)
        raise


def isInvalidVDI(exception):
    return exception.details[0] == "HANDLE_INVALID" or \
            exception.details[0] == "UUID_INVALID"


def get_pool_restrictions(session):
    """Returns pool restrictions as a map, @session must be already
    established."""
    return list(session.xenapi.pool.get_all_records().values())[0]['restrictions']


def read_caching_is_restricted(session):
    """Tells whether read caching is restricted."""
    if session is None:
        return True
    restrictions = get_pool_restrictions(session)
    if 'restrict_read_caching' in restrictions and \
            restrictions['restrict_read_caching'] == "true":
        return True
    return False


def sessions_less_than_targets(other_config, device_config):
    if 'multihomelist' in device_config and 'iscsi_sessions' in other_config:
        sessions = int(other_config['iscsi_sessions'])
        targets = len(device_config['multihomelist'].split(','))
        SMlog("Targets %d and iscsi_sessions %d" % (targets, sessions))
        return (sessions < targets)
    else:
        return False
