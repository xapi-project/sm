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
# blktap2: blktap/tapdisk management layer
#
import grp
import os
import re
import stat
import time
import copy
import xmlrpc.client
import http.client
import errno
import signal
import subprocess
import syslog as _syslog
import glob
import json
from syslog import openlog, syslog
from stat import *  # S_ISBLK(), ...

import XenAPI # pylint: disable=import-error
from sm.core.lock import Lock
from sm.core import util
from sm.core import xs_errors
from sm.core import scsiutil
from sm import nfs
from sm import resetvdis
from sm import vhdutil
from sm import lvhdutil
from sm import VDI as sm

# For RRDD Plugin Registration
from xmlrpc.client import ServerProxy, Transport
from socket import socket, AF_UNIX, SOCK_STREAM

PLUGIN_TAP_PAUSE = "tapdisk-pause"

SOCKPATH = "/var/xapi/xcp-rrdd"

NUM_PAGES_PER_RING = 32 * 11
MAX_FULL_RINGS = 8

ENABLE_MULTIPLE_ATTACH = "/etc/xensource/allow_multiple_vdi_attach"
NO_MULTIPLE_ATTACH = not (os.path.exists(ENABLE_MULTIPLE_ATTACH))


def locking(excType, override=True):
    def locking2(op):
        def wrapper(self, *args):
            self.lock.acquire()
            try:
                try:
                    ret = op(self, * args)
                except (util.CommandException, util.SMException, XenAPI.Failure) as e:
                    util.logException("BLKTAP2:%s" % op)
                    msg = str(e)
                    if isinstance(e, util.CommandException):
                        msg = "Command %s failed (%s): %s" % \
                                (e.cmd, e.code, e.reason)
                    if override:
                        raise xs_errors.XenError(excType, opterr=msg)
                    else:
                        raise
                except:
                    util.logException("BLKTAP2:%s" % op)
                    raise
            finally:
                self.lock.release()
            return ret
        return wrapper
    return locking2


class RetryLoop(object):

    def __init__(self, backoff, limit):
        self.backoff = backoff
        self.limit = limit

    def __call__(self, f):

        def loop(*__t, **__d):
            attempt = 0

            while True:
                attempt += 1

                try:
                    return f( * __t, ** __d)

                except self.TransientFailure as e:
                    e = e.exception

                    if attempt >= self.limit:
                        raise e

                    time.sleep(self.backoff)

        return loop

    class TransientFailure(Exception):
        def __init__(self, exception):
            self.exception = exception


def retried(**args):
    return RetryLoop( ** args)


class TapCtl(object):
    """Tapdisk IPC utility calls."""

    PATH = "/usr/sbin/tap-ctl"

    def __init__(self, cmd, p):
        self.cmd = cmd
        self._p = p
        self.stdout = p.stdout

    class CommandFailure(Exception):
        """TapCtl cmd failure."""

        def __init__(self, cmd, **info):
            self.cmd = cmd
            self.info = info

        def __str__(self):
            items = self.info.items()
            info = ", ".join("%s=%s" % item
                             for item in items)
            return "%s failed: %s" % (self.cmd, info)

        # Trying to get a non-existent attribute throws an AttributeError
        # exception
        def __getattr__(self, key):
            if key in self.info:
                return self.info[key]
            return object.__getattribute__(self, key)

        @property
        def has_status(self):
            return 'status' in self.info

        @property
        def has_signal(self):
            return 'signal' in self.info

        # Retrieves the error code returned by the command. If the error code
        # was not supplied at object-construction time, zero is returned.
        def get_error_code(self):
            key = 'status'
            if key in self.info:
                return self.info[key]
            else:
                return 0

    @classmethod
    def __mkcmd_real(cls, args):
        return [cls.PATH] + [str(x) for x in args]

    __next_mkcmd = __mkcmd_real

    @classmethod
    def _mkcmd(cls, args):

        __next_mkcmd = cls.__next_mkcmd
        cls.__next_mkcmd = cls.__mkcmd_real

        return __next_mkcmd(args)

    @classmethod
    def _call(cls, args, quiet=False, input=None, text_mode=True):
        """
        Spawn a tap-ctl process. Return a TapCtl invocation.
        Raises a TapCtl.CommandFailure if subprocess creation failed.
        """
        cmd = cls._mkcmd(args)

        if not quiet:
            util.SMlog(cmd)
        try:
            p = subprocess.Popen(cmd,
                                 stdin=subprocess.PIPE,
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE,
                                 close_fds=True,
                                 universal_newlines=text_mode)
            if input:
                p.stdin.write(input)
                p.stdin.close()
        except OSError as e:
            raise cls.CommandFailure(cmd, errno=e.errno)

        return cls(cmd, p)

    def _errmsg(self):
        output = map(str.rstrip, self._p.stderr)
        return "; ".join(output)

    def _wait(self, quiet=False):
        """
        Reap the child tap-ctl process of this invocation.
        Raises a TapCtl.CommandFailure on non-zero exit status.
        """
        status = self._p.wait()
        if not quiet:
            util.SMlog(" = %d" % status)

        if status == 0:
            return

        info = {'errmsg': self._errmsg(),
                 'pid': self._p.pid}

        if status < 0:
            info['signal'] = -status
        else:
            info['status'] = status

        raise self.CommandFailure(self.cmd, ** info)

    @classmethod
    def _pread(cls, args, quiet=False, input=None, text_mode=True):
        """
        Spawn a tap-ctl invocation and read a single line.
        """
        tapctl = cls._call(args=args, quiet=quiet, input=input,
                           text_mode=text_mode)

        output = tapctl.stdout.readline().rstrip()

        tapctl._wait(quiet)
        return output

    @staticmethod
    def _maybe(opt, parm):
        if parm is not None:
            return [opt, parm]
        return []

    @classmethod
    def __list(cls, minor=None, pid=None, _type=None, path=None):
        args = ["list"]
        args += cls._maybe("-m", minor)
        args += cls._maybe("-p", pid)
        args += cls._maybe("-t", _type)
        args += cls._maybe("-f", path)

        tapctl = cls._call(args, True)

        for stdout_line in tapctl.stdout:
            # FIXME: tap-ctl writes error messages to stdout and
            # confuses this parser
            if stdout_line == "blktap kernel module not installed\n":
                # This isn't pretty but (a) neither is confusing stdout/stderr
                # and at least causes the error to describe the fix
                raise Exception("blktap kernel module not installed: try 'modprobe blktap'")
            row = {}

            for field in stdout_line.rstrip().split(' ', 3):
                bits = field.split('=')
                if len(bits) == 2:
                    key, val = field.split('=')

                    if key in ('pid', 'minor'):
                        row[key] = int(val, 10)

                    elif key in ('state'):
                        row[key] = int(val, 0x10)

                    else:
                        row[key] = val
                else:
                    util.SMlog("Ignoring unexpected tap-ctl output: %s" % repr(field))
            yield row

        tapctl._wait(True)

    @classmethod
    @retried(backoff=.5, limit=10)
    def list(cls, **args):

        # FIXME. We typically get an EPROTO when uevents interleave
        # with SM ops and a tapdisk shuts down under our feet. Should
        # be fixed in SM.

        try:
            return list(cls.__list( ** args))

        except cls.CommandFailure as e:
            transient = [errno.EPROTO, errno.ENOENT]
            if e.has_status and e.status in transient:
                raise RetryLoop.TransientFailure(e)
            raise

    @classmethod
    def allocate(cls, devpath=None):
        args = ["allocate"]
        args += cls._maybe("-d", devpath)
        return cls._pread(args)

    @classmethod
    def free(cls, minor):
        args = ["free", "-m", minor]
        cls._pread(args)

    @classmethod
    @retried(backoff=.5, limit=10)
    def spawn(cls):
        args = ["spawn"]
        try:
            pid = cls._pread(args)
            return int(pid)
        except cls.CommandFailure as ce:
            # intermittent failures to spawn. CA-292268
            if ce.status == 1:
                raise RetryLoop.TransientFailure(ce)
            raise

    @classmethod
    def attach(cls, pid, minor):
        args = ["attach", "-p", pid, "-m", minor]
        cls._pread(args)

    @classmethod
    def detach(cls, pid, minor):
        args = ["detach", "-p", pid, "-m", minor]
        cls._pread(args)

    @classmethod
    def _load_key(cls, key_hash, vdi_uuid):
        from sm import plugins

        return plugins.load_key(key_hash, vdi_uuid)

    @classmethod
    def open(cls, pid, minor, _type, _file, options):
        params = Tapdisk.Arg(_type, _file)
        args = ["open", "-p", pid, "-m", minor, '-a', str(params)]
        text_mode = True
        input = None
        if options.get("rdonly"):
            args.append('-R')
        if options.get("lcache"):
            args.append("-r")
        if options.get("existing_prt") is not None:
            args.append("-e")
            args.append(str(options["existing_prt"]))
        if options.get("secondary"):
            args.append("-2")
            args.append(options["secondary"])
        if options.get("standby"):
            args.append("-s")
        if options.get("timeout"):
            args.append("-t")
            args.append(str(options["timeout"]))
        if not options.get("o_direct", True):
            args.append("-D")
        if options.get('cbtlog'):
            args.extend(['-C', options['cbtlog']])
        if options.get('key_hash'):
            key_hash = options['key_hash']
            vdi_uuid = options['vdi_uuid']
            key = cls._load_key(key_hash, vdi_uuid)

            if not key:
                raise util.SMException("No key found with key hash {}".format(key_hash))
            input = key
            text_mode = False
            args.append('-E')

        cls._pread(args=args, input=input, text_mode=text_mode)

    @classmethod
    def close(cls, pid, minor, force=False):
        args = ["close", "-p", pid, "-m", minor, "-t", "120"]
        if force:
            args += ["-f"]
        cls._pread(args)

    @classmethod
    def pause(cls, pid, minor):
        args = ["pause", "-p", pid, "-m", minor]
        cls._pread(args)

    @classmethod
    def unpause(cls, pid, minor, _type=None, _file=None, mirror=None,
                cbtlog=None):
        args = ["unpause", "-p", pid, "-m", minor]
        if mirror:
            args.extend(["-2", mirror])
        if _type and _file:
            params = Tapdisk.Arg(_type, _file)
            args += ["-a", str(params)]
        if cbtlog:
            args.extend(["-c", cbtlog])
        cls._pread(args)

    @classmethod
    def shutdown(cls, pid):
        # TODO: This should be a real tap-ctl command
        os.kill(pid, signal.SIGTERM)
        os.waitpid(pid, 0)

    @classmethod
    def stats(cls, pid, minor):
        args = ["stats", "-p", pid, "-m", minor]
        return cls._pread(args, quiet=True)

    @classmethod
    def major(cls):
        args = ["major"]
        major = cls._pread(args)
        return int(major)


class TapdiskExists(Exception):
    """Tapdisk already running."""

    def __init__(self, tapdisk):
        self.tapdisk = tapdisk

    def __str__(self):
        return "%s already running" % self.tapdisk


class TapdiskNotRunning(Exception):
    """No such Tapdisk."""

    def __init__(self, **attrs):
        self.attrs = attrs

    def __str__(self):
        items = iter(self.attrs.items())
        attrs = ", ".join("%s=%s" % attr
                          for attr in items)
        return "No such Tapdisk(%s)" % attrs


class TapdiskNotUnique(Exception):
    """More than one tapdisk on one path."""

    def __init__(self, tapdisks):
        self.tapdisks = tapdisks

    def __str__(self):
        tapdisks = map(str, self.tapdisks)
        return "Found multiple tapdisks: %s" % tapdisks


class TapdiskFailed(Exception):
    """Tapdisk launch failure."""

    def __init__(self, arg, err):
        self.arg = arg
        self.err = err

    def __str__(self):
        return "Tapdisk(%s): %s" % (self.arg, self.err)

    def get_error(self):
        return self.err


class TapdiskInvalidState(Exception):
    """Tapdisk pause/unpause failure"""

    def __init__(self, tapdisk):
        self.tapdisk = tapdisk

    def __str__(self):
        return str(self.tapdisk)


def mkdirs(path, mode=0o777):
    if not os.path.exists(path):
        parent, subdir = os.path.split(path)
        assert parent != path
        try:
            if parent:
                mkdirs(parent, mode)
            if subdir:
                os.mkdir(path, mode)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise


class KObject(object):

    SYSFS_CLASSTYPE = None

    def sysfs_devname(self):
        raise NotImplementedError("sysfs_devname is undefined")


class Attribute(object):

    SYSFS_NODENAME = None

    def __init__(self, path):
        self.path = path

    @classmethod
    def from_kobject(cls, kobj):
        path = "%s/%s" % (kobj.sysfs_path(), cls.SYSFS_NODENAME)
        return cls(path)

    class NoSuchAttribute(Exception):
        def __init__(self, name):
            self.name = name

        def __str__(self):
            return "No such attribute: %s" % self.name

    def _open(self, mode='r'):
        try:
            return open(self.path, mode)
        except IOError as e:
            if e.errno == errno.ENOENT:
                raise self.NoSuchAttribute(self)
            raise

    def readline(self):
        f = self._open('r')
        s = f.readline().rstrip()
        f.close()
        return s

    def writeline(self, val):
        f = self._open('w')
        f.write(val)
        f.close()


class ClassDevice(KObject):

    @classmethod
    def sysfs_class_path(cls):
        return "/sys/class/%s" % cls.SYSFS_CLASSTYPE

    def sysfs_path(self):
        return "%s/%s" % (self.sysfs_class_path(),
                          self.sysfs_devname())


class Blktap(ClassDevice):

    DEV_BASEDIR = '/dev/xen/blktap-2'
    TAP_MINOR_BASE = '/run/blktap-control/tapdisk'

    SYSFS_CLASSTYPE = "blktap2"

    def __init__(self, minor):
        self.minor = minor
        self._task = None

    @classmethod
    def allocate(cls):
        # FIXME. Should rather go into init.
        mkdirs(cls.DEV_BASEDIR)

        devname = TapCtl.allocate()
        minor = Tapdisk._parse_minor(devname)
        return cls(minor)

    def free(self):
        TapCtl.free(self.minor)

    def __str__(self):
        return "%s(minor=%d)" % (self.__class__.__name__, self.minor)

    def sysfs_devname(self):
        return "blktap!blktap%d" % self.minor

    class Task(Attribute):
        SYSFS_NODENAME = "task"

    def get_task_attr(self):
        if not self._task:
            self._task = self.Task.from_kobject(self)
        return self._task

    def get_task_pid(self):
        pid = self.get_task_attr().readline()
        try:
            return int(pid)
        except ValueError:
            return None

    def find_tapdisk(self):
        pid = self.get_task_pid()
        if pid is None:
            return None

        return Tapdisk.find(pid=pid, minor=self.minor)

    def get_tapdisk(self):
        tapdisk = self.find_tapdisk()
        if not tapdisk:
            raise TapdiskNotRunning(minor=self.minor)
        return tapdisk


class Tapdisk(object):

    TYPES = ['aio', 'vhd']

    def __init__(self, pid, minor, _type, path, state):
        self.pid = pid
        self.minor = minor
        self.type = _type
        self.path = path
        self.state = state
        self._dirty = False
        self._blktap = None

    def __str__(self):
        state = self.pause_state()
        return "Tapdisk(%s, pid=%d, minor=%s, state=%s)" % \
            (self.get_arg(), self.pid, self.minor, state)

    @classmethod
    def list(cls, **args):

        for row in TapCtl.list( ** args):

            args = {'pid': None,
                      'minor': None,
                      'state': None,
                      '_type': None,
                      'path': None}

            for key, val in row.items():
                if key in args:
                    args[key] = val

            if 'args' in row:
                image = Tapdisk.Arg.parse(row['args'])
                args['_type'] = image.type
                args['path'] = image.path

            if None in args.values():
                continue

            yield Tapdisk( ** args)

    @classmethod
    def find(cls, **args):

        found = list(cls.list( ** args))

        if len(found) > 1:
            raise TapdiskNotUnique(found)

        if found:
            return found[0]

        return None

    @classmethod
    def find_by_path(cls, path):
        return cls.find(path=path)

    @classmethod
    def find_by_minor(cls, minor):
        return cls.find(minor=minor)

    @classmethod
    def get(cls, **attrs):

        tapdisk = cls.find( ** attrs)

        if not tapdisk:
            raise TapdiskNotRunning( ** attrs)

        return tapdisk

    @classmethod
    def from_path(cls, path):
        return cls.get(path=path)

    @classmethod
    def from_minor(cls, minor):
        return cls.get(minor=minor)

    @classmethod
    def __from_blktap(cls, blktap):
        tapdisk = cls.from_minor(minor=blktap.minor)
        tapdisk._blktap = blktap
        return tapdisk

    def get_blktap(self):
        if not self._blktap:
            self._blktap = Blktap(self.minor)
        return self._blktap

    class Arg:

        def __init__(self, _type, path):
            self.type = _type
            self.path = path

        def __str__(self):
            return "%s:%s" % (self.type, self.path)

        @classmethod
        def parse(cls, arg):

            try:
                _type, path = arg.split(":", 1)
            except ValueError:
                raise cls.InvalidArgument(arg)

            if _type not in Tapdisk.TYPES:
                raise cls.InvalidType(_type)

            return cls(_type, path)

        class InvalidType(Exception):
            def __init__(self, _type):
                self.type = _type

            def __str__(self):
                return "Not a Tapdisk type: %s" % self.type

        class InvalidArgument(Exception):
            def __init__(self, arg):
                self.arg = arg

            def __str__(self):
                return "Not a Tapdisk image: %s" % self.arg

    def get_arg(self):
        return self.Arg(self.type, self.path)

    def get_devpath(self):
        return "%s/tapdev%d" % (Blktap.DEV_BASEDIR, self.minor)

    @classmethod
    def launch_from_arg(cls, arg):
        arg = cls.Arg.parse(arg)
        return cls.launch(arg.path, arg.type, False)

    @staticmethod
    def cgclassify(pid):

        # We dont provide any <controllers>:<path>
        # so cgclassify uses /etc/cgrules.conf which
        # we have configured in the spec file.
        cmd = ["cgclassify", str(pid)]
        try:
            util.pread2(cmd)
        except util.CommandException as e:
            util.logException(e)

    @classmethod
    def launch_on_tap(cls, blktap, path, _type, options):

        tapdisk = cls.find_by_path(path)
        if tapdisk:
            raise TapdiskExists(tapdisk)

        minor = blktap.minor
        try:
            pid = TapCtl.spawn()
            cls.cgclassify(pid)
            try:
                TapCtl.attach(pid, minor)

                try:
                    TapCtl.open(pid, minor, _type, path, options)
                    try:
                        return cls.__from_blktap(blktap)
                    except:
                        TapCtl.close(pid, minor)
                        raise

                except:
                    TapCtl.detach(pid, minor)
                    raise

            except:
                try:
                    TapCtl.shutdown(pid)
                except:
                    # Best effort to shutdown
                    pass
                raise

        except TapCtl.CommandFailure as ctl:
            util.logException(ctl)
            if ((path.startswith('/dev/xapi/cd/') or path.startswith('/dev/sr')) and
                    ctl.has_status and ctl.get_error_code() == 123):  # ENOMEDIUM (No medium found)
                raise xs_errors.XenError('TapdiskDriveEmpty')
            else:
                raise TapdiskFailed(cls.Arg(_type, path), ctl)

    @classmethod
    def launch(cls, path, _type, rdonly):
        blktap = Blktap.allocate()
        try:
            return cls.launch_on_tap(blktap, path, _type, {"rdonly": rdonly})
        except:
            blktap.free()
            raise

    def shutdown(self, force=False):

        TapCtl.close(self.pid, self.minor, force)

        TapCtl.detach(self.pid, self.minor)

        self.get_blktap().free()

    def pause(self):

        if not self.is_running():
            raise TapdiskInvalidState(self)

        TapCtl.pause(self.pid, self.minor)

        self._set_dirty()

    def unpause(self, _type=None, path=None, mirror=None, cbtlog=None):

        if not self.is_paused():
            raise TapdiskInvalidState(self)

        # FIXME: should the arguments be optional?
        if _type is None:
            _type = self.type
        if  path is None:
            path = self.path

        TapCtl.unpause(self.pid, self.minor, _type, path, mirror=mirror,
                       cbtlog=cbtlog)

        self._set_dirty()

    def stats(self):
        return json.loads(TapCtl.stats(self.pid, self.minor))
    #
    # NB. dirty/refresh: reload attributes on next access
    #

    def _set_dirty(self):
        self._dirty = True

    def _refresh(self, __get):
        t = self.from_minor(__get('minor'))
        self.__init__(t.pid, t.minor, t.type, t.path, t.state)

    def __getattribute__(self, name):
        def __get(name):
            # NB. avoid(rec(ursion)
            return object.__getattribute__(self, name)

        if __get('_dirty') and \
                name in ['minor', 'type', 'path', 'state']:
            self._refresh(__get)
            self._dirty = False

        return __get(name)

    class PauseState:
        RUNNING = 'R'
        PAUSING = 'r'
        PAUSED = 'P'

    class Flags:
        DEAD = 0x0001
        CLOSED = 0x0002
        QUIESCE_REQUESTED = 0x0004
        QUIESCED = 0x0008
        PAUSE_REQUESTED = 0x0010
        PAUSED = 0x0020
        SHUTDOWN_REQUESTED = 0x0040
        LOCKING = 0x0080
        RETRY_NEEDED = 0x0100
        LOG_DROPPED = 0x0200

        PAUSE_MASK = PAUSE_REQUESTED | PAUSED

    def is_paused(self):
        return not not (self.state & self.Flags.PAUSED)

    def is_running(self):
        return not (self.state & self.Flags.PAUSE_MASK)

    def pause_state(self):
        if self.state & self.Flags.PAUSED:
            return self.PauseState.PAUSED

        if self.state & self.Flags.PAUSE_REQUESTED:
            return self.PauseState.PAUSING

        return self.PauseState.RUNNING

    @staticmethod
    def _parse_minor(devpath):
        regex = r'%s/tapdisk-(\d+)$' % Blktap.TAP_MINOR_BASE
        pattern = re.compile(regex)
        groups = pattern.search(devpath)
        if not groups:
            raise Exception("malformed tap device: '%s' (%s) " % (devpath, regex))

        minor = int(groups.group(1))
        return minor

    _major = None

    @classmethod
    def major(cls):
        if cls._major:
            return cls._major

        devices = open("/proc/devices")
        for line in devices:

            row = line.rstrip().split(' ')
            if len(row) != 2:
                continue

            major, name = row
            if name != 'tapdev':
                continue

            cls._major = int(major)
            break

        devices.close()
        return cls._major


class VDI(object):
    """SR.vdi driver decorator for blktap2"""

    CONF_KEY_ALLOW_CACHING = "vdi_allow_caching"
    CONF_KEY_MODE_ON_BOOT = "vdi_on_boot"
    CONF_KEY_CACHE_SR = "local_cache_sr"
    CONF_KEY_O_DIRECT = "o_direct"
    LOCK_CACHE_SETUP = "cachesetup"

    ATTACH_DETACH_RETRY_SECS = 120

    def __init__(self, uuid, target, driver_info):
        self.target = self.TargetDriver(target, driver_info)
        self._vdi_uuid = uuid
        self._session = target.session
        self.xenstore_data = scsiutil.update_XS_SCSIdata(uuid, scsiutil.gen_synthetic_page_data(uuid))
        self.__o_direct = None
        self.__o_direct_reason = None
        self.lock = Lock("vdi", uuid)
        self.tap = None

    def get_o_direct_capability(self, options):
        """Returns True/False based on licensing and caching_params"""
        if self.__o_direct is not None:
            return self.__o_direct, self.__o_direct_reason

        if util.read_caching_is_restricted(self._session):
            self.__o_direct = True
            self.__o_direct_reason = "LICENSE_RESTRICTION"
        elif not ((self.target.vdi.sr.handles("nfs") or self.target.vdi.sr.handles("ext") or self.target.vdi.sr.handles("smb"))):
            self.__o_direct = True
            self.__o_direct_reason = "SR_NOT_SUPPORTED"
        elif options.get("rdonly") and not self.target.vdi.parent:
            self.__o_direct = True
            self.__o_direct_reason = "RO_WITH_NO_PARENT"
        elif options.get(self.CONF_KEY_O_DIRECT):
            self.__o_direct = True
            self.__o_direct_reason = "SR_OVERRIDE"

        if self.__o_direct is None:
            self.__o_direct = False
            self.__o_direct_reason = ""

        return self.__o_direct, self.__o_direct_reason

    @classmethod
    def from_cli(cls, uuid):
        import VDI as sm

        session = XenAPI.xapi_local()
        session.xenapi.login_with_password('root', '', '', 'SM')

        target = sm.VDI.from_uuid(session, uuid)
        driver_info = target.sr.srcmd.driver_info

        session.xenapi.session.logout()

        return cls(uuid, target, driver_info)

    @staticmethod
    def _tap_type(vdi_type):
        """Map a VDI type (e.g. 'raw') to a tapdisk driver type (e.g. 'aio')"""
        return {
            'raw': 'aio',
            'vhd': 'vhd',
            'iso': 'aio',  # for ISO SR
            'aio': 'aio',  # for LVHD
            'file': 'aio',
            'phy': 'aio'
            }[vdi_type]

    def get_tap_type(self):
        vdi_type = self.target.get_vdi_type()
        return VDI._tap_type(vdi_type)

    def get_phy_path(self):
        return self.target.get_vdi_path()

    class UnexpectedVDIType(Exception):

        def __init__(self, vdi_type, target):
            self.vdi_type = vdi_type
            self.target = target

        def __str__(self):
            return \
                "Target %s has unexpected VDI type '%s'" % \
                (type(self.target), self.vdi_type)

    VDI_PLUG_TYPE = {'phy': 'phy',  # for NETAPP
                      'raw': 'phy',
                      'aio': 'tap',  # for LVHD raw nodes
                      'iso': 'tap',  # for ISOSR
                      'file': 'tap',
                      'vhd': 'tap'}

    def tap_wanted(self):
        # 1. Let the target vdi_type decide

        vdi_type = self.target.get_vdi_type()

        try:
            plug_type = self.VDI_PLUG_TYPE[vdi_type]
        except KeyError:
            raise self.UnexpectedVDIType(vdi_type,
                                         self.target.vdi)

        if plug_type == 'tap':
            return True
        elif self.target.vdi.sr.handles('udev'):
            return True
        # 2. Otherwise, there may be more reasons
        #
        # .. TBD

        return False

    class TargetDriver:
        """Safe target driver access."""
        # NB. *Must* test caps for optional calls. Some targets
        # actually implement some slots, but do not enable them. Just
        # try/except would risk breaking compatibility.

        def __init__(self, vdi, driver_info):
            self.vdi = vdi
            self._caps = driver_info['capabilities']

        def has_cap(self, cap):
            """Determine if target has given capability"""
            return cap in self._caps

        def attach(self, sr_uuid, vdi_uuid):
            #assert self.has_cap("VDI_ATTACH")
            return self.vdi.attach(sr_uuid, vdi_uuid)

        def detach(self, sr_uuid, vdi_uuid):
            #assert self.has_cap("VDI_DETACH")
            self.vdi.detach(sr_uuid, vdi_uuid)

        def activate(self, sr_uuid, vdi_uuid):
            if self.has_cap("VDI_ACTIVATE"):
                return self.vdi.activate(sr_uuid, vdi_uuid)

        def deactivate(self, sr_uuid, vdi_uuid):
            if self.has_cap("VDI_DEACTIVATE"):
                self.vdi.deactivate(sr_uuid, vdi_uuid)
        #def resize(self, sr_uuid, vdi_uuid, size):
        #    return self.vdi.resize(sr_uuid, vdi_uuid, size)

        def get_vdi_type(self):
            _type = self.vdi.vdi_type
            if not _type:
                _type = self.vdi.sr.sr_vditype
            if not _type:
                raise VDI.UnexpectedVDIType(_type, self.vdi)
            return _type

        def get_vdi_path(self):
            return self.vdi.path

    class Link(object):
        """Relink a node under a common name"""
        # NB. We have to provide the device node path during
        # VDI.attach, but currently do not allocate the tapdisk minor
        # before VDI.activate. Therefore those link steps where we
        # relink existing devices under deterministic path names.

        BASEDIR = None

        def _mklink(self, target):
            raise NotImplementedError("_mklink is not defined")

        def _equals(self, target):
            raise NotImplementedError("_equals is not defined")

        def __init__(self, path):
            self._path = path

        @classmethod
        def from_name(cls, name):
            path = "%s/%s" % (cls.BASEDIR, name)
            return cls(path)

        @classmethod
        def from_uuid(cls, sr_uuid, vdi_uuid):
            name = "%s/%s" % (sr_uuid, vdi_uuid)
            return cls.from_name(name)

        def path(self):
            return self._path

        def stat(self):
            return os.stat(self.path())

        def mklink(self, target):

            path = self.path()
            util.SMlog("%s -> %s" % (self, target))

            mkdirs(os.path.dirname(path))
            try:
                self._mklink(target)
            except OSError as e:
                # We do unlink during teardown, but have to stay
                # idempotent. However, a *wrong* target should never
                # be seen.
                if e.errno != errno.EEXIST:
                    raise
                assert self._equals(target), "'%s' not equal to '%s'" % (path, target)

        def unlink(self):
            try:
                os.unlink(self.path())
            except OSError as e:
                if e.errno != errno.ENOENT:
                    raise

        def __str__(self):
            path = self.path()
            return "%s(%s)" % (self.__class__.__name__, path)

    class SymLink(Link):
        """Symlink some file to a common name"""

        def readlink(self):
            return os.readlink(self.path())

        def symlink(self):
            return self.path()

        def _mklink(self, target):
            os.symlink(target, self.path())

        def _equals(self, target):
            return self.readlink() == target


    class PhyLink(SymLink):
        BASEDIR = "/dev/sm/phy"


    class NBDLink(SymLink):

        BASEDIR = "/run/blktap-control/nbd"

        def read_minor_from_path(self):
            regex = r'.*/nbd\d+.(\d+)$'
            match = re.search(regex, os.path.realpath(self._path))
            if not match:
                raise Exception("Failed to parse minor from %s" % self._path)

            return int(match.group(1))


    @staticmethod
    def _tap_activate(phy_path, vdi_type, sr_uuid, options):

        tapdisk = Tapdisk.find_by_path(phy_path)
        if not tapdisk:
            blktap = Blktap.allocate()

            try:
                tapdisk = \
                    Tapdisk.launch_on_tap(blktap,
                                          phy_path,
                                          VDI._tap_type(vdi_type),
                                          options)
            except:
                blktap.free()
                raise
            util.SMlog("tap.activate: Launched %s" % tapdisk)

        else:
            util.SMlog("tap.activate: Found %s" % tapdisk)

        return tapdisk.get_devpath(), tapdisk

    @staticmethod
    def _tap_deactivate(minor):

        try:
            tapdisk = Tapdisk.from_minor(minor)
        except TapdiskNotRunning as e:
            util.SMlog("tap.deactivate: Warning, %s" % e)
            # NB. Should not be here unless the agent refcount
            # broke. Also, a clean shutdown should not have leaked
            # the recorded minor.
        else:
            tapdisk.shutdown()
            util.SMlog("tap.deactivate: Shut down %s" % tapdisk)

    @classmethod
    def tap_pause(cls, session, sr_uuid, vdi_uuid, failfast=False):
        """
        Pauses the tapdisk.

        session: a XAPI session
        sr_uuid: the UUID of the SR on which VDI lives
        vdi_uuid: the UUID of the VDI to pause
        failfast: controls whether the VDI lock should be acquired in a
            non-blocking manner
        """
        util.SMlog("Pause request for %s" % vdi_uuid)
        vdi_ref = session.xenapi.VDI.get_by_uuid(vdi_uuid)
        session.xenapi.VDI.add_to_sm_config(vdi_ref, 'paused', 'true')
        sm_config = session.xenapi.VDI.get_sm_config(vdi_ref)
        for key in [x for x in sm_config.keys() if x.startswith('host_')]:
            host_ref = key[len('host_'):]
            util.SMlog("Calling tap-pause on host %s" % host_ref)
            if not cls.call_pluginhandler(session, host_ref,
                    sr_uuid, vdi_uuid, "pause", failfast=failfast):
                # Failed to pause node
                session.xenapi.VDI.remove_from_sm_config(vdi_ref, 'paused')
                return False
        return True

    @classmethod
    def tap_unpause(cls, session, sr_uuid, vdi_uuid, secondary=None,
                    activate_parents=False):
        util.SMlog("Unpause request for %s secondary=%s" % (vdi_uuid, secondary))
        vdi_ref = session.xenapi.VDI.get_by_uuid(vdi_uuid)
        sm_config = session.xenapi.VDI.get_sm_config(vdi_ref)
        for key in [x for x in sm_config.keys() if x.startswith('host_')]:
            host_ref = key[len('host_'):]
            util.SMlog("Calling tap-unpause on host %s" % host_ref)
            if not cls.call_pluginhandler(session, host_ref,
                    sr_uuid, vdi_uuid, "unpause", secondary, activate_parents):
                # Failed to unpause node
                return False
        session.xenapi.VDI.remove_from_sm_config(vdi_ref, 'paused')
        return True

    @classmethod
    def tap_refresh(cls, session, sr_uuid, vdi_uuid, activate_parents=False):
        util.SMlog("Refresh request for %s" % vdi_uuid)
        vdi_ref = session.xenapi.VDI.get_by_uuid(vdi_uuid)
        sm_config = session.xenapi.VDI.get_sm_config(vdi_ref)
        for key in [x for x in sm_config.keys() if x.startswith('host_')]:
            host_ref = key[len('host_'):]
            util.SMlog("Calling tap-refresh on host %s" % host_ref)
            if not cls.call_pluginhandler(session, host_ref,
                       sr_uuid, vdi_uuid, "refresh", None,
                       activate_parents=activate_parents):
                # Failed to refresh node
                return False
        return True

    @classmethod
    def tap_status(cls, session, vdi_uuid):
        """Return True if disk is attached, false if it isn't"""
        util.SMlog("Disk status request for %s" % vdi_uuid)
        vdi_ref = session.xenapi.VDI.get_by_uuid(vdi_uuid)
        sm_config = session.xenapi.VDI.get_sm_config(vdi_ref)
        for key in [x for x in sm_config.keys() if x.startswith('host_')]:
            return True
        return False

    @classmethod
    def call_pluginhandler(cls, session, host_ref, sr_uuid, vdi_uuid, action,
            secondary=None, activate_parents=False, failfast=False):
        """Optionally, activate the parent LV before unpausing"""
        try:
            args = {"sr_uuid": sr_uuid, "vdi_uuid": vdi_uuid,
                    "failfast": str(failfast)}
            if secondary:
                args["secondary"] = secondary
            if activate_parents:
                args["activate_parents"] = "true"
            ret = session.xenapi.host.call_plugin(
                    host_ref, PLUGIN_TAP_PAUSE, action,
                    args)
            return ret == "True"
        except Exception as e:
            util.logException("BLKTAP2:call_pluginhandler %s" % e)
            return False

    def _add_tag(self, vdi_uuid, writable):
        util.SMlog("Adding tag to: %s" % vdi_uuid)
        attach_mode = "RO"
        if writable:
            attach_mode = "RW"
        vdi_ref = self._session.xenapi.VDI.get_by_uuid(vdi_uuid)
        host_ref = self._session.xenapi.host.get_by_uuid(util.get_this_host())
        sm_config = self._session.xenapi.VDI.get_sm_config(vdi_ref)
        attached_as = util.attached_as(sm_config)
        if NO_MULTIPLE_ATTACH and (attached_as == "RW" or \
                (attached_as == "RO" and attach_mode == "RW")):
            util.SMlog("need to reset VDI %s" % vdi_uuid)
            if not resetvdis.reset_vdi(self._session, vdi_uuid, force=False,
                    term_output=False, writable=writable):
                raise util.SMException("VDI %s not detached cleanly" % vdi_uuid)
            sm_config = self._session.xenapi.VDI.get_sm_config(vdi_ref)
        if 'relinking' in sm_config:
            util.SMlog("Relinking key found, back-off and retry" % sm_config)
            return False
        if 'paused' in sm_config:
            util.SMlog("Paused or host_ref key found [%s]" % sm_config)
            return False
        try:
            self._session.xenapi.VDI.add_to_sm_config(
                vdi_ref, 'activating', 'True')
        except XenAPI.Failure as e:
            if e.details[0] == 'MAP_DUPLICATE_KEY' and not writable:
                # Someone else is activating - a retry might succeed
                return False
            raise
        host_key = "host_%s" % host_ref
        assert host_key not in sm_config
        self._session.xenapi.VDI.add_to_sm_config(vdi_ref, host_key,
                                                  attach_mode)
        sm_config = self._session.xenapi.VDI.get_sm_config(vdi_ref)
        if 'paused' in sm_config or 'relinking' in sm_config:
            util.SMlog("Found %s key, aborting" % (
                'paused' if 'paused' in sm_config else 'relinking'))
            self._session.xenapi.VDI.remove_from_sm_config(vdi_ref, host_key)
            self._session.xenapi.VDI.remove_from_sm_config(
                vdi_ref, 'activating')
            return False
        util.SMlog("Activate lock succeeded")
        return True

    def _check_tag(self, vdi_uuid):
        vdi_ref = self._session.xenapi.VDI.get_by_uuid(vdi_uuid)
        sm_config = self._session.xenapi.VDI.get_sm_config(vdi_ref)
        if 'paused' in sm_config:
            util.SMlog("Paused key found [%s]" % sm_config)
            return False
        return True

    def _remove_tag(self, vdi_uuid):
        vdi_ref = self._session.xenapi.VDI.get_by_uuid(vdi_uuid)
        host_ref = self._session.xenapi.host.get_by_uuid(util.get_this_host())
        sm_config = self._session.xenapi.VDI.get_sm_config(vdi_ref)
        host_key = "host_%s" % host_ref
        if host_key in sm_config:
            self._session.xenapi.VDI.remove_from_sm_config(vdi_ref, host_key)
            util.SMlog("Removed host key %s for %s" % (host_key, vdi_uuid))
        else:
            util.SMlog("_remove_tag: host key %s not found, ignore" % host_key)

    def linkNBD(self, sr_uuid, vdi_uuid):
        if self.tap:
            nbd_path = '/run/blktap-control/nbd%d.%d' % (int(self.tap.pid),
                                                         int(self.tap.minor))
            VDI.NBDLink.from_uuid(sr_uuid, vdi_uuid).mklink(nbd_path)

    def attach(self, sr_uuid, vdi_uuid, writable, activate=False, caching_params={}):
        """Return attach details to allow access to storage datapath"""
        if not self.target.has_cap("ATOMIC_PAUSE") or activate:
            util.SMlog("Attach & activate")
            self._attach(sr_uuid, vdi_uuid)
            dev_path = self._activate(sr_uuid, vdi_uuid,
                    {"rdonly": not writable})
            self.linkNBD(sr_uuid, vdi_uuid)

        # Return NBD link
        if self.tap_wanted():
            # Only have NBD if we also have a tap
            nbd_path = "nbd:unix:{}:exportname={}".format(
                VDI.NBDLink.from_uuid(sr_uuid, vdi_uuid).path(),
                vdi_uuid)
        else:
            nbd_path = ""

        options = {"rdonly": not writable}
        options.update(caching_params)
        o_direct, o_direct_reason = self.get_o_direct_capability(options)
        struct = {'params_nbd': nbd_path,
                  'o_direct': o_direct,
                  'o_direct_reason': o_direct_reason,
                  'xenstore_data': self.xenstore_data}
        util.SMlog('result: %s' % struct)

        return xmlrpc.client.dumps((struct, ), "", True)

    def activate(self, sr_uuid, vdi_uuid, writable, caching_params):
        util.SMlog("blktap2.activate")
        options = {"rdonly": not writable}
        options.update(caching_params)

        sr_ref = self.target.vdi.sr.srcmd.params.get('sr_ref')
        sr_other_config = self._session.xenapi.SR.get_other_config(sr_ref)
        for i in range(self.ATTACH_DETACH_RETRY_SECS):
            try:
                if self._activate_locked(sr_uuid, vdi_uuid, options):
                    return
            except util.SRBusyException:
                util.SMlog("SR locked, retrying")
            time.sleep(1)
        raise util.SMException("VDI %s locked" % vdi_uuid)

    @locking("VDIUnavailable")
    def _activate_locked(self, sr_uuid, vdi_uuid, options):
        """Wraps target.activate and adds a tapdisk"""

        #util.SMlog("VDI.activate %s" % vdi_uuid)
        refresh = False
        if self.tap_wanted():
            if not self._add_tag(vdi_uuid, not options["rdonly"]):
                return False
            refresh = True

        try:
            if refresh:
                # it is possible that while the VDI was paused some of its
                # attributes have changed (e.g. its size if it was inflated; or its
                # path if it was leaf-coalesced onto a raw LV), so refresh the
                # object completely
                params = self.target.vdi.sr.srcmd.params
                target = sm.VDI.from_uuid(self.target.vdi.session, vdi_uuid)
                target.sr.srcmd.params = params
                driver_info = target.sr.srcmd.driver_info
                self.target = self.TargetDriver(target, driver_info)

            util.fistpoint.activate_custom_fn(
                    "blktap_activate_inject_failure",
                    lambda: util.inject_failure())

            # Attach the physical node
            if self.target.has_cap("ATOMIC_PAUSE"):
                self._attach(sr_uuid, vdi_uuid)

            vdi_type = self.target.get_vdi_type()

            # Take lvchange-p Lock before running
            # tap-ctl open
            # Needed to avoid race with lvchange -p which is
            # now taking the same lock
            # This is a fix for CA-155766
            if hasattr(self.target.vdi.sr, 'DRIVER_TYPE') and \
               self.target.vdi.sr.DRIVER_TYPE == 'lvhd' and \
               vdi_type == vhdutil.VDI_TYPE_VHD:
                lock = Lock("lvchange-p", lvhdutil.NS_PREFIX_LVM + sr_uuid)
                lock.acquire()

            # When we attach a static VDI for HA, we cannot communicate with
            # xapi, because has not started yet. These VDIs are raw.
            if vdi_type != vhdutil.VDI_TYPE_RAW:
                session = self.target.vdi.session
                vdi_ref = session.xenapi.VDI.get_by_uuid(vdi_uuid)
                # pylint: disable=used-before-assignment
                sm_config = session.xenapi.VDI.get_sm_config(vdi_ref)
                if 'key_hash' in sm_config:
                    key_hash = sm_config['key_hash']
                    options['key_hash'] = key_hash
                    options['vdi_uuid'] = vdi_uuid
                    util.SMlog('Using key with hash {} for VDI {}'.format(key_hash, vdi_uuid))
            # Activate the physical node
            dev_path = self._activate(sr_uuid, vdi_uuid, options)

            if hasattr(self.target.vdi.sr, 'DRIVER_TYPE') and \
               self.target.vdi.sr.DRIVER_TYPE == 'lvhd' and \
               self.target.get_vdi_type() == vhdutil.VDI_TYPE_VHD:
                lock.release()
        except:
            util.SMlog("Exception in activate/attach")
            if self.tap_wanted():
                util.fistpoint.activate_custom_fn(
                        "blktap_activate_error_handling",
                        lambda: time.sleep(30))
                while True:
                    try:
                        self._remove_tag(vdi_uuid)
                        break
                    except xmlrpc.client.ProtocolError as e:
                        # If there's a connection error, keep trying forever.
                        if e.errcode == http.HTTPStatus.INTERNAL_SERVER_ERROR.value:
                            continue
                        else:
                            util.SMlog('failed to remove tag: %s' % e)
                            break
                    except Exception as e:
                        util.SMlog('failed to remove tag: %s' % e)
                        break
            raise
        finally:
            vdi_ref = self._session.xenapi.VDI.get_by_uuid(vdi_uuid)
            self._session.xenapi.VDI.remove_from_sm_config(
                vdi_ref, 'activating')
            util.SMlog("Removed activating flag from %s" % vdi_uuid)

        # Link result to backend/
        self.linkNBD(sr_uuid, vdi_uuid)
        return True

    def _activate(self, sr_uuid, vdi_uuid, options):
        vdi_options = self.target.activate(sr_uuid, vdi_uuid)

        dev_path = self.setup_cache(sr_uuid, vdi_uuid, options)
        if not dev_path:
            phy_path = self.PhyLink.from_uuid(sr_uuid, vdi_uuid).readlink()
            # Maybe launch a tapdisk on the physical link
            if self.tap_wanted():
                vdi_type = self.target.get_vdi_type()
                options["o_direct"] = self.get_o_direct_capability(options)[0]
                if vdi_options:
                    options.update(vdi_options)
                dev_path, self.tap = self._tap_activate(phy_path, vdi_type,
                        sr_uuid, options)
            else:
                dev_path = phy_path  # Just reuse phy

        return dev_path

    def _attach(self, sr_uuid, vdi_uuid):
        attach_info = xmlrpc.client.loads(self.target.attach(sr_uuid, vdi_uuid))[0][0]
        params = attach_info['params']
        xenstore_data = attach_info['xenstore_data']
        phy_path = util.to_plain_string(params)
        self.xenstore_data.update(xenstore_data)
        # Save it to phy/
        self.PhyLink.from_uuid(sr_uuid, vdi_uuid).mklink(phy_path)

    def deactivate(self, sr_uuid, vdi_uuid, caching_params):
        util.SMlog("blktap2.deactivate")
        for i in range(self.ATTACH_DETACH_RETRY_SECS):
            try:
                if self._deactivate_locked(sr_uuid, vdi_uuid, caching_params):
                    return
            except util.SRBusyException as e:
                util.SMlog("SR locked, retrying")
            time.sleep(1)
        raise util.SMException("VDI %s locked" % vdi_uuid)

    @locking("VDIUnavailable")
    def _deactivate_locked(self, sr_uuid, vdi_uuid, caching_params):
        """Wraps target.deactivate and removes a tapdisk"""

        #util.SMlog("VDI.deactivate %s" % vdi_uuid)
        if self.tap_wanted() and not self._check_tag(vdi_uuid):
            return False

        self._deactivate(sr_uuid, vdi_uuid, caching_params)
        if self.target.has_cap("ATOMIC_PAUSE"):
            self._detach(sr_uuid, vdi_uuid)
        if self.tap_wanted():
            self._remove_tag(vdi_uuid)

        return True

    def _resetPhylink(self, sr_uuid, vdi_uuid, path):
        self.PhyLink.from_uuid(sr_uuid, vdi_uuid).mklink(path)

    def detach(self, sr_uuid, vdi_uuid, deactivate=False, caching_params={}):
        if not self.target.has_cap("ATOMIC_PAUSE") or deactivate:
            util.SMlog("Deactivate & detach")
            self._deactivate(sr_uuid, vdi_uuid, caching_params)
            self._detach(sr_uuid, vdi_uuid)
        else:
            pass  # nothing to do

    def _deactivate(self, sr_uuid, vdi_uuid, caching_params):
        # Shutdown tapdisk
        nbd_link = self.NBDLink.from_uuid(sr_uuid, vdi_uuid)

        if not util.pathexists(nbd_link.path()):
            util.SMlog("Nbd path %s does not exist" % nbd_link.path())
            return

        minor = nbd_link.read_minor_from_path()
        self._tap_deactivate(minor)
        self.remove_cache(sr_uuid, vdi_uuid, caching_params)

        # Remove the backend link
        nbd_link.unlink()

        # Deactivate & detach the physical node
        if self.tap_wanted() and self.target.vdi.session is not None:
            # it is possible that while the VDI was paused some of its
            # attributes have changed (e.g. its size if it was inflated; or its
            # path if it was leaf-coalesced onto a raw LV), so refresh the
            # object completely
            target = sm.VDI.from_uuid(self.target.vdi.session, vdi_uuid)
            driver_info = target.sr.srcmd.driver_info
            self.target = self.TargetDriver(target, driver_info)

        self.target.deactivate(sr_uuid, vdi_uuid)

    def _detach(self, sr_uuid, vdi_uuid):
        self.target.detach(sr_uuid, vdi_uuid)

        # Remove phy/
        self.PhyLink.from_uuid(sr_uuid, vdi_uuid).unlink()

    def _updateCacheRecord(self, session, vdi_uuid, on_boot, caching):
        # Remove existing VDI.sm_config fields
        vdi_ref = session.xenapi.VDI.get_by_uuid(vdi_uuid)
        for key in ["on_boot", "caching"]:
            session.xenapi.VDI.remove_from_sm_config(vdi_ref, key)
        if not on_boot is None:
            session.xenapi.VDI.add_to_sm_config(vdi_ref, 'on_boot', on_boot)
        if not caching is None:
            session.xenapi.VDI.add_to_sm_config(vdi_ref, 'caching', caching)

    def setup_cache(self, sr_uuid, vdi_uuid, params):
        if params.get(self.CONF_KEY_ALLOW_CACHING) != "true":
            return

        util.SMlog("Requested local caching")
        if not self.target.has_cap("SR_CACHING"):
            util.SMlog("Error: local caching not supported by this SR")
            return

        scratch_mode = False
        if params.get(self.CONF_KEY_MODE_ON_BOOT) == "reset":
            scratch_mode = True
            util.SMlog("Requested scratch mode")
            if not self.target.has_cap("VDI_RESET_ON_BOOT/2"):
                util.SMlog("Error: scratch mode not supported by this SR")
                return

        dev_path = None
        local_sr_uuid = params.get(self.CONF_KEY_CACHE_SR)
        if not local_sr_uuid:
            util.SMlog("ERROR: Local cache SR not specified, not enabling")
            return
        dev_path = self._setup_cache(self._session, sr_uuid, vdi_uuid,
                local_sr_uuid, scratch_mode, params)

        if dev_path:
            self._updateCacheRecord(self._session, self.target.vdi.uuid,
                    params.get(self.CONF_KEY_MODE_ON_BOOT),
                    params.get(self.CONF_KEY_ALLOW_CACHING))

        return dev_path

    def alert_no_cache(self, session, vdi_uuid, cache_sr_uuid, err):
        vm_uuid = None
        vm_label = ""
        try:
            cache_sr_ref = session.xenapi.SR.get_by_uuid(cache_sr_uuid)
            cache_sr_rec = session.xenapi.SR.get_record(cache_sr_ref)
            cache_sr_label = cache_sr_rec.get("name_label")

            host_ref = session.xenapi.host.get_by_uuid(util.get_this_host())
            host_rec = session.xenapi.host.get_record(host_ref)
            host_label = host_rec.get("name_label")

            vdi_ref = session.xenapi.VDI.get_by_uuid(vdi_uuid)
            vbds = session.xenapi.VBD.get_all_records_where( \
                    "field \"VDI\" = \"%s\"" % vdi_ref)
            for vbd_rec in vbds.values():
                vm_ref = vbd_rec.get("VM")
                vm_rec = session.xenapi.VM.get_record(vm_ref)
                vm_uuid = vm_rec.get("uuid")
                vm_label = vm_rec.get("name_label")
        except:
            util.logException("alert_no_cache")

        alert_obj = "SR"
        alert_uuid = str(cache_sr_uuid)
        alert_str = "No space left in Local Cache SR %s" % cache_sr_uuid
        if vm_uuid:
            alert_obj = "VM"
            alert_uuid = vm_uuid
            reason = ""
            if err == errno.ENOSPC:
                reason = "because there is no space left"
            alert_str = "The VM \"%s\" is not using IntelliCache %s on the Local Cache SR (\"%s\") on host \"%s\"" % \
                    (vm_label, reason, cache_sr_label, host_label)

        util.SMlog("Creating alert: (%s, %s, \"%s\")" % \
                (alert_obj, alert_uuid, alert_str))
        session.xenapi.message.create("No space left in local cache", "3",
                alert_obj, alert_uuid, alert_str)

    def _setup_cache(self, session, sr_uuid, vdi_uuid, local_sr_uuid,
            scratch_mode, options):
        from sm import SR
        from sm.drivers import EXTSR
        from sm.drivers import NFSSR
        from sm.core.lock import Lock
        from sm.drivers.FileSR import FileVDI

        parent_uuid = vhdutil.getParent(self.target.vdi.path,
                FileVDI.extractUuid)
        if not parent_uuid:
            util.SMlog("ERROR: VDI %s has no parent, not enabling" % \
                    self.target.vdi.uuid)
            return

        util.SMlog("Setting up cache")
        parent_uuid = parent_uuid.strip()
        shared_target = NFSSR.NFSFileVDI(self.target.vdi.sr, parent_uuid)

        if shared_target.parent:
            util.SMlog("ERROR: Parent VDI %s has parent, not enabling" %
                       shared_target.uuid)
            return

        SR.registerSR(EXTSR.EXTSR)
        local_sr = SR.SR.from_uuid(session, local_sr_uuid)

        lock = Lock(self.LOCK_CACHE_SETUP, parent_uuid)
        lock.acquire()

        # read cache
        read_cache_path = "%s/%s.vhdcache" % (local_sr.path, shared_target.uuid)
        if util.pathexists(read_cache_path):
            util.SMlog("Read cache node (%s) already exists, not creating" % \
                    read_cache_path)
        else:
            try:
                vhdutil.snapshot(read_cache_path, shared_target.path, False)
            except util.CommandException as e:
                util.SMlog("Error creating parent cache: %s" % e)
                self.alert_no_cache(session, vdi_uuid, local_sr_uuid, e.code)
                return None

        # local write node
        leaf_size = vhdutil.getSizeVirt(self.target.vdi.path)
        local_leaf_path = "%s/%s.vhdcache" % \
                (local_sr.path, self.target.vdi.uuid)
        if util.pathexists(local_leaf_path):
            util.SMlog("Local leaf node (%s) already exists, deleting" % \
                    local_leaf_path)
            os.unlink(local_leaf_path)
        try:
            vhdutil.snapshot(local_leaf_path, read_cache_path, False,
                    msize=leaf_size // 1024 // 1024, checkEmpty=False)
        except util.CommandException as e:
            util.SMlog("Error creating leaf cache: %s" % e)
            self.alert_no_cache(session, vdi_uuid, local_sr_uuid, e.code)
            return None

        local_leaf_size = vhdutil.getSizeVirt(local_leaf_path)
        if leaf_size > local_leaf_size:
            util.SMlog("Leaf size %d > local leaf cache size %d, resizing" %
                    (leaf_size, local_leaf_size))
            vhdutil.setSizeVirtFast(local_leaf_path, leaf_size)

        vdi_type = self.target.get_vdi_type()

        prt_tapdisk = Tapdisk.find_by_path(read_cache_path)
        if not prt_tapdisk:
            parent_options = copy.deepcopy(options)
            parent_options["rdonly"] = False
            parent_options["lcache"] = True

            blktap = Blktap.allocate()
            try:
                prt_tapdisk = \
                    Tapdisk.launch_on_tap(blktap, read_cache_path,
                            'vhd', parent_options)
            except:
                blktap.free()
                raise

        secondary = "%s:%s" % (self.target.get_vdi_type(),
                self.PhyLink.from_uuid(sr_uuid, vdi_uuid).readlink())

        util.SMlog("Parent tapdisk: %s" % prt_tapdisk)
        leaf_tapdisk = Tapdisk.find_by_path(local_leaf_path)
        if not leaf_tapdisk:
            blktap = Blktap.allocate()
            child_options = copy.deepcopy(options)
            child_options["rdonly"] = False
            child_options["lcache"] = (not scratch_mode)
            child_options["existing_prt"] = prt_tapdisk.minor
            child_options["secondary"] = secondary
            child_options["standby"] = scratch_mode
            try:
                leaf_tapdisk = \
                    Tapdisk.launch_on_tap(blktap, local_leaf_path,
                            'vhd', child_options)
            except:
                blktap.free()
                raise

        lock.release()

        util.SMlog("Local read cache: %s, local leaf: %s" % \
                (read_cache_path, local_leaf_path))

        self.tap = leaf_tapdisk
        return leaf_tapdisk.get_devpath()

    def remove_cache(self, sr_uuid, vdi_uuid, params):
        if not self.target.has_cap("SR_CACHING"):
            return

        caching = params.get(self.CONF_KEY_ALLOW_CACHING) == "true"

        local_sr_uuid = params.get(self.CONF_KEY_CACHE_SR)
        if caching and not local_sr_uuid:
            util.SMlog("ERROR: Local cache SR not specified, ignore")
            return

        if caching:
            self._remove_cache(self._session, local_sr_uuid)

        if self._session is not None:
            self._updateCacheRecord(self._session, self.target.vdi.uuid, None, None)

    def _is_tapdisk_in_use(self, minor):
        retVal, links, sockets = util.findRunningProcessOrOpenFile("tapdisk")
        if not retVal:
            # err on the side of caution
            return True

        for link in links:
            if link.find("tapdev%d" % minor) != -1:
                return True

        socket_re = re.compile(r'^/.*/nbd\d+\.%d' % minor)
        for s in sockets:
            if socket_re.match(s):
                return True

        return False

    def _remove_cache(self, session, local_sr_uuid):
        from sm import SR
        from sm.drivers import EXTSR
        from sm.drivers import NFSSR
        from sm.core.lock import Lock
        from sm.drivers.FileSR import FileVDI

        parent_uuid = vhdutil.getParent(self.target.vdi.path,
                FileVDI.extractUuid)
        if not parent_uuid:
            util.SMlog("ERROR: No parent for VDI %s, ignore" % \
                    self.target.vdi.uuid)
            return

        util.SMlog("Tearing down the cache")

        parent_uuid = parent_uuid.strip()
        shared_target = NFSSR.NFSFileVDI(self.target.vdi.sr, parent_uuid)

        SR.registerSR(EXTSR.EXTSR)
        local_sr = SR.SR.from_uuid(session, local_sr_uuid)

        lock = Lock(self.LOCK_CACHE_SETUP, parent_uuid)
        lock.acquire()

        # local write node
        local_leaf_path = "%s/%s.vhdcache" % \
                (local_sr.path, self.target.vdi.uuid)
        if util.pathexists(local_leaf_path):
            util.SMlog("Deleting local leaf node %s" % local_leaf_path)
            os.unlink(local_leaf_path)

        read_cache_path = "%s/%s.vhdcache" % (local_sr.path, shared_target.uuid)
        prt_tapdisk = Tapdisk.find_by_path(read_cache_path)
        if not prt_tapdisk:
            util.SMlog("Parent tapdisk not found")
        elif not self._is_tapdisk_in_use(prt_tapdisk.minor):
            util.SMlog("Parent tapdisk not in use: shutting down %s" % \
                    read_cache_path)
            try:
                prt_tapdisk.shutdown()
            except:
                util.logException("shutting down parent tapdisk")
        else:
            util.SMlog("Parent tapdisk still in use: %s" % read_cache_path)
        # the parent cache files are removed during the local SR's background
        # GC run

        lock.release()

PythonKeyError = KeyError


class UEventHandler(object):

    def __init__(self):
        self._action = None

    class KeyError(PythonKeyError):
        def __init__(self, args):
            super().__init__(args)
            self.key = args[0]

        def __str__(self):
            return \
                "Key '%s' missing in environment. " % self.key + \
                "Not called in udev context?"

    @classmethod
    def getenv(cls, key):
        try:
            return os.environ[key]
        except KeyError as e:
            raise cls.KeyError(e.args[0])

    def get_action(self):
        if not self._action:
            self._action = self.getenv('ACTION')
        return self._action

    class UnhandledEvent(Exception):

        def __init__(self, event, handler):
            self.event = event
            self.handler = handler

        def __str__(self):
            return "Uevent '%s' not handled by %s" % \
                (self.event, self.handler.__class__.__name__)

    ACTIONS = {}

    def run(self):

        action = self.get_action()
        try:
            fn = self.ACTIONS[action]
        except KeyError:
            raise self.UnhandledEvent(action, self)

        return fn(self)

    def __str__(self):
        try:
            action = self.get_action()
        except:
            action = None
        return "%s[%s]" % (self.__class__.__name__, action)


class __BlktapControl(ClassDevice):
    SYSFS_CLASSTYPE = "misc"

    def __init__(self):
        ClassDevice.__init__(self)

    def sysfs_devname(self):
        return "blktap!control"


BlktapControl = __BlktapControl()


class BusDevice(KObject):

    SYSFS_BUSTYPE = None

    @classmethod
    def sysfs_bus_path(cls):
        return "/sys/bus/%s" % cls.SYSFS_BUSTYPE

    def sysfs_path(self):
        path = "%s/devices/%s" % (self.sysfs_bus_path(),
                                  self.sysfs_devname())

        return path


class XenbusDevice(BusDevice):
    """Xenbus device, in XS and sysfs"""

    XBT_NIL = ""

    XENBUS_DEVTYPE = None

    def __init__(self, domid, devid):
        self.domid = int(domid)
        self.devid = int(devid)
        self._xbt = XenbusDevice.XBT_NIL

        import xen.lowlevel.xs  # pylint: disable=import-error
        self.xs = xen.lowlevel.xs.xs()

    def xs_path(self, key=None):
        path = "backend/%s/%d/%d" % (self.XENBUS_DEVTYPE,
                                     self.domid,
                                     self.devid)
        if key is not None:
            path = "%s/%s" % (path, key)

        return path

    def _log(self, prio, msg):
        syslog(prio, msg)

    def info(self, msg):
        self._log(_syslog.LOG_INFO, msg)

    def warn(self, msg):
        self._log(_syslog.LOG_WARNING, "WARNING: " + msg)

    def _xs_read_path(self, path):
        val = self.xs.read(self._xbt, path)
        #self.info("read %s = '%s'" % (path, val))
        return val

    def _xs_write_path(self, path, val):
        self.xs.write(self._xbt, path, val)
        self.info("wrote %s = '%s'" % (path, val))

    def _xs_rm_path(self, path):
        self.xs.rm(self._xbt, path)
        self.info("removed %s" % path)

    def read(self, key):
        return self._xs_read_path(self.xs_path(key))

    def has_xs_key(self, key):
        return self.read(key) is not None

    def write(self, key, val):
        self._xs_write_path(self.xs_path(key), val)

    def rm(self, key):
        self._xs_rm_path(self.xs_path(key))

    def exists(self):
        return self.has_xs_key(None)

    def begin(self):
        assert(self._xbt == XenbusDevice.XBT_NIL)
        self._xbt = self.xs.transaction_start()

    def commit(self):
        ok = self.xs.transaction_end(self._xbt, 0)
        self._xbt = XenbusDevice.XBT_NIL
        return ok

    def abort(self):
        ok = self.xs.transaction_end(self._xbt, 1)
        assert(ok == True)
        self._xbt = XenbusDevice.XBT_NIL

    def create_physical_device(self):
        """The standard protocol is: toolstack writes 'params', linux hotplug
        script translates this into physical-device=%x:%x"""
        if self.has_xs_key("physical-device"):
            return
        try:
            params = self.read("params")
            frontend = self.read("frontend")
            is_cdrom = self._xs_read_path("%s/device-type") == "cdrom"
            # We don't have PV drivers for CDROM devices, so we prevent blkback
            # from opening the physical-device
            if not(is_cdrom):
                major_minor = os.stat(params).st_rdev
                major, minor = divmod(major_minor, 256)
                self.write("physical-device", "%x:%x" % (major, minor))
        except:
            util.logException("BLKTAP2:create_physical_device")

    def signal_hotplug(self, online=True):
        xapi_path = "/xapi/%d/hotplug/%s/%d/hotplug" % (self.domid,
                                                   self.XENBUS_DEVTYPE,
                                                   self.devid)
        upstream_path = self.xs_path("hotplug-status")
        if online:
            self._xs_write_path(xapi_path, "online")
            self._xs_write_path(upstream_path, "connected")
        else:
            self._xs_rm_path(xapi_path)
            self._xs_rm_path(upstream_path)

    def sysfs_devname(self):
        return "%s-%d-%d" % (self.XENBUS_DEVTYPE,
                             self.domid, self.devid)

    def __str__(self):
        return self.sysfs_devname()

    @classmethod
    def find(cls):
        pattern = "/sys/bus/%s/devices/%s*" % (cls.SYSFS_BUSTYPE,
                                               cls.XENBUS_DEVTYPE)
        for path in glob.glob(pattern):

            name = os.path.basename(path)
            (_type, domid, devid) = name.split('-')

            yield cls(domid, devid)


class XenBackendDevice(XenbusDevice):
    """Xenbus backend device"""
    SYSFS_BUSTYPE = "xen-backend"

    @classmethod
    def from_xs_path(cls, _path):
        (_backend, _type, domid, devid) = _path.split('/')

        assert _backend == 'backend'
        assert _type == cls.XENBUS_DEVTYPE

        domid = int(domid)
        devid = int(devid)

        return cls(domid, devid)


class Blkback(XenBackendDevice):
    """A blkback VBD"""

    XENBUS_DEVTYPE = "vbd"

    def __init__(self, domid, devid):
        XenBackendDevice.__init__(self, domid, devid)
        self._phy = None
        self._vdi_uuid = None
        self._q_state = None
        self._q_events = None

    class XenstoreValueError(Exception):
        KEY = None

        def __init__(self, vbd, _str):
            self.vbd = vbd
            self.str = _str

        def __str__(self):
            return "Backend %s " % self.vbd + \
                "has %s = %s" % (self.KEY, self.str)

    class PhysicalDeviceError(XenstoreValueError):
        KEY = "physical-device"

    class PhysicalDevice(object):

        def __init__(self, major, minor):
            self.major = int(major)
            self.minor = int(minor)

        @classmethod
        def from_xbdev(cls, xbdev):

            phy = xbdev.read("physical-device")

            try:
                major, minor = phy.split(':')
                major = int(major, 0x10)
                minor = int(minor, 0x10)
            except Exception as e:
                raise xbdev.PhysicalDeviceError(xbdev, phy)

            return cls(major, minor)

        def makedev(self):
            return os.makedev(self.major, self.minor)

        def is_tap(self):
            return self.major == Tapdisk.major()

        def __str__(self):
            return "%s:%s" % (self.major, self.minor)

        def __eq__(self, other):
            return \
                self.major == other.major and \
                self.minor == other.minor

    def get_physical_device(self):
        if not self._phy:
            self._phy = self.PhysicalDevice.from_xbdev(self)
        return self._phy

    class QueueEvents(Attribute):
        """Blkback sysfs node to select queue-state event
        notifications emitted."""

        SYSFS_NODENAME = "queue_events"

        QUEUE_RUNNING = (1 << 0)
        QUEUE_PAUSE_DONE = (1 << 1)
        QUEUE_SHUTDOWN_DONE = (1 << 2)
        QUEUE_PAUSE_REQUEST = (1 << 3)
        QUEUE_SHUTDOWN_REQUEST = (1 << 4)

        def get_mask(self):
            return int(self.readline(), 0x10)

        def set_mask(self, mask):
            self.writeline("0x%x" % mask)

    def get_queue_events(self):
        if not self._q_events:
            self._q_events = self.QueueEvents.from_kobject(self)
        return self._q_events

    def get_vdi_uuid(self):
        if not self._vdi_uuid:
            self._vdi_uuid = self.read("sm-data/vdi-uuid")
        return self._vdi_uuid

    def pause_requested(self):
        return self.has_xs_key("pause")

    def shutdown_requested(self):
        return self.has_xs_key("shutdown-request")

    def shutdown_done(self):
        return self.has_xs_key("shutdown-done")

    def running(self):
        return self.has_xs_key('queue-0/kthread-pid')

    @classmethod
    def find_by_physical_device(cls, phy):
        for dev in cls.find():
            try:
                _phy = dev.get_physical_device()
            except cls.PhysicalDeviceError:
                continue

            if _phy == phy:
                yield dev

    @classmethod
    def find_by_tap_minor(cls, minor):
        phy = cls.PhysicalDevice(Tapdisk.major(), minor)
        return cls.find_by_physical_device(phy)

    @classmethod
    def find_by_tap(cls, tapdisk):
        return cls.find_by_tap_minor(tapdisk.minor)

    def has_tap(self):

        if not self.can_tap():
            return False

        phy = self.get_physical_device()
        if phy:
            return phy.is_tap()

        return False

    def is_bare_hvm(self):
        """File VDIs for bare HVM. These are directly accessible by Qemu."""
        try:
            self.get_physical_device()

        except self.PhysicalDeviceError as e:
            vdi_type = self.read("type")

            self.info("HVM VDI: type=%s" % vdi_type)

            if e.str is not None or vdi_type != 'file':
                raise

            return True

        return False

    def can_tap(self):
        return not self.is_bare_hvm()


class BlkbackEventHandler(UEventHandler):

    LOG_FACILITY = _syslog.LOG_DAEMON

    def __init__(self, ident=None, action=None):
        if not ident:
            ident = self.__class__.__name__

        self.ident = ident
        self._vbd = None
        self._tapdisk = None

        UEventHandler.__init__(self)

    def run(self):

        self.xs_path = self.getenv('XENBUS_PATH')
        openlog(str(self), 0, self.LOG_FACILITY)

        UEventHandler.run(self)

    def __str__(self):

        try:
            path = self.xs_path
        except:
            path = None

        try:
            action = self.get_action()
        except:
            action = None

        return "%s[%s](%s)" % (self.ident, action, path)

    def _log(self, prio, msg):
        syslog(prio, msg)
        util.SMlog("%s: " % self + msg)

    def info(self, msg):
        self._log(_syslog.LOG_INFO, msg)

    def warn(self, msg):
        self._log(_syslog.LOG_WARNING, "WARNING: " + msg)

    def error(self, msg):
        self._log(_syslog.LOG_ERR, "ERROR: " + msg)

    def get_vbd(self):
        if not self._vbd:
            self._vbd = Blkback.from_xs_path(self.xs_path)
        return self._vbd

    def get_tapdisk(self):
        if not self._tapdisk:
            minor = self.get_vbd().get_physical_device().minor
            self._tapdisk = Tapdisk.from_minor(minor)
        return self._tapdisk
    #
    # Events
    #

    def __add(self):
        vbd = self.get_vbd()
        # Manage blkback transitions
        # self._manage_vbd()

        vbd.create_physical_device()

        vbd.signal_hotplug()

    @retried(backoff=.5, limit=10)
    def add(self):
        try:
            self.__add()
        except Attribute.NoSuchAttribute as e:
            #
            # FIXME: KOBJ_ADD is racing backend.probe, which
            # registers device attributes. So poll a little.
            #
            self.warn("%s, still trying." % e)
            raise RetryLoop.TransientFailure(e)

    def __change(self):
        vbd = self.get_vbd()

        # 1. Pause or resume tapdisk (if there is one)

        if vbd.has_tap():
            pass
            #self._pause_update_tap()

            # 2. Signal Xapi.VBD.pause/resume completion

        self._signal_xapi()

    def change(self):
        vbd = self.get_vbd()

        # NB. Beware of spurious change events between shutdown
        # completion and device removal. Also, Xapi.VM.migrate will
        # hammer a couple extra shutdown-requests into the source VBD.

        while True:
            vbd.begin()

            if not vbd.exists() or \
                    vbd.shutdown_done():
                break

            self.__change()

            if vbd.commit():
                return

        vbd.abort()
        self.info("spurious uevent, ignored.")

    def remove(self):
        vbd = self.get_vbd()

        vbd.signal_hotplug(False)

    ACTIONS = {'add': add,
                'change': change,
                'remove': remove}
    #
    # VDI.pause
    #

    def _tap_should_pause(self):
        """Enumerate all VBDs on our tapdisk. Returns true iff any was
        paused"""

        tapdisk = self.get_tapdisk()
        TapState = Tapdisk.PauseState

        PAUSED = 'P'
        RUNNING = 'R'
        PAUSED_SHUTDOWN = 'P,S'
        # NB. Shutdown/paused is special. We know it's not going
        # to restart again, so it's a RUNNING. Still better than
        # backtracking a removed device during Vbd.unplug completion.

        next = TapState.RUNNING
        vbds = {}

        for vbd in Blkback.find_by_tap(tapdisk):
            name = str(vbd)

            pausing = vbd.pause_requested()
            closing = vbd.shutdown_requested()
            running = vbd.running()

            if pausing:
                if closing and not running:
                    vbds[name] = PAUSED_SHUTDOWN
                else:
                    vbds[name] = PAUSED
                    next = TapState.PAUSED

            else:
                vbds[name] = RUNNING

        self.info("tapdev%d (%s): %s -> %s"
                  % (tapdisk.minor, tapdisk.pause_state(),
                     vbds, next))

        return next == TapState.PAUSED

    def _pause_update_tap(self):
        vbd = self.get_vbd()

        if self._tap_should_pause():
            self._pause_tap()
        else:
            self._resume_tap()

    def _pause_tap(self):
        tapdisk = self.get_tapdisk()

        if not tapdisk.is_paused():
            self.info("pausing %s" % tapdisk)
            tapdisk.pause()

    def _resume_tap(self):
        tapdisk = self.get_tapdisk()

        # NB. Raw VDI snapshots. Refresh the physical path and
        # type while resuming.
        vbd = self.get_vbd()
        vdi_uuid = vbd.get_vdi_uuid()

        if tapdisk.is_paused():
            self.info("loading vdi uuid=%s" % vdi_uuid)
            vdi = VDI.from_cli(vdi_uuid)
            _type = vdi.get_tap_type()
            path = vdi.get_phy_path()
            self.info("resuming %s on %s:%s" % (tapdisk, _type, path))
            tapdisk.unpause(_type, path)
    #
    # VBD.pause/shutdown
    #

    def _manage_vbd(self):
        vbd = self.get_vbd()
        # NB. Hook into VBD state transitions.

        events = vbd.get_queue_events()

        mask = 0
        mask |= events.QUEUE_PAUSE_DONE    # pause/unpause
        mask |= events.QUEUE_SHUTDOWN_DONE  # shutdown
        # TODO: mask |= events.QUEUE_SHUTDOWN_REQUEST, for shutdown=force
        # TODO: mask |= events.QUEUE_RUNNING, for ionice updates etc

        events.set_mask(mask)
        self.info("wrote %s = %#02x" % (events.path, mask))

    def _signal_xapi(self):
        vbd = self.get_vbd()

        pausing = vbd.pause_requested()
        closing = vbd.shutdown_requested()
        running = vbd.running()

        handled = 0

        if pausing and not running:
            if 'pause-done' not in vbd:
                vbd.write('pause-done', '')
                handled += 1

        if not pausing:
            if 'pause-done' in vbd:
                vbd.rm('pause-done')
                handled += 1

        if closing and not running:
            if 'shutdown-done' not in vbd:
                vbd.write('shutdown-done', '')
                handled += 1

        if handled > 1:
            self.warn("handled %d events, " % handled +
                      "pausing=%s closing=%s running=%s" % \
                          (pausing, closing, running))

