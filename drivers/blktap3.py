
#!/usr/bin/env python
#
# Copyright (C) Citrix Systems Inc.
# This program is free software; you can redistribute it and/or modify it under
# the terms of the GNU Lesser General Public License as published by the Free
# Software Foundation; version 2.1 only.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
# details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
#
# blktap3: tapdisk management layer
#


import os
import re
import time
import copy
from lock import Lock
import util
import xmlrpclib
import httplib
import errno
import subprocess
import syslog as _syslog
import glob
import xs_errors
import XenAPI
import scsiutil
from syslog import openlog, syslog
from stat import * # S_ISBLK(), ...
from SR import SROSError
import traceback

import resetvdis
import vhdutil

PLUGIN_TAP_PAUSE = "tapdisk-pause"

ENABLE_MULTIPLE_ATTACH = "/etc/xensource/allow_multiple_vdi_attach"
NO_MULTIPLE_ATTACH = not (os.path.exists(ENABLE_MULTIPLE_ATTACH)) 

# FIXME description
def locking(excType, override=True):
    def locking2(op):
        def wrapper(self, *args):
            self.lock.acquire()
            try:
                try:
                    ret = op(self, *args)
                except (util.SMException, XenAPI.Failure), e:
                    util.logException("BLKTAP3:%s" % op)
                    msg = str(e)
                    if isinstance(e, util.CommandException):
                        msg = "Command %s failed (%s): %s" % \
                                (e.cmd, e.code, e.reason)
                    if override:
                        raise xs_errors.XenError(excType, opterr=msg)
                    else:
                        raise
                except:
                    util.logException("BLKTAP3:%s" % op)
                    raise
            finally:
                self.lock.release()
            return ret
        return wrapper
    return locking2

# FIXME description
class RetryLoop(object):

    def __init__(self, backoff, limit):
        self.backoff = backoff
        self.limit   = limit

    def __call__(self, f):

        def loop(*__t, **__d):
            attempt = 0

            while True:
                attempt += 1

                try:
                    return f(*__t, **__d)

                except self.TransientFailure, e:
                    e = e.exception

                    if attempt >= self.limit: raise e

                    time.sleep(self.backoff)

        return loop

    class TransientFailure(Exception):
        def __init__(self, exception):
            self.exception = exception

def retried(**args): return RetryLoop(**args)

class TapCtl(object):
    """Tapdisk IPC utility calls."""

    PATH = "/usr/sbin/tap-ctl"

    def __init__(self, cmd, p):
        self.cmd    = cmd
        self._p     = p
        self.stdout = p.stdout

    class CommandFailure(Exception):
        """TapCtl cmd failure."""

        def __init__(self, cmd, **info):
            self.cmd  = cmd
            self.info = info

        def __str__(self):
            items = self.info.iteritems()
            info  = ", ".join("%s=%s" % item
                              for item in items)
            return "%s failed: %s" % (self.cmd, info)

        # Trying to get a non-existent attribute throws an AttributeError
        # exception
        def __getattr__(self, key):
            if self.info.has_key(key): return self.info[key]
            return object.__getattribute__(self, key)

        # Retrieves the error code returned by the command. If the error code
        # was not supplied at object-construction time, zero is returned.
        def get_error_code(self):
            key = 'status'
            if self.info.has_key(key):
                return self.info[key]
            else:
                return 0

    @classmethod
    def __mkcmd_real(cls, args):
        return [ cls.PATH ] + map(str, args)

    __next_mkcmd = __mkcmd_real

    @classmethod
    def _mkcmd(cls, args):

        __next_mkcmd     = cls.__next_mkcmd
        cls.__next_mkcmd = cls.__mkcmd_real

        return __next_mkcmd(args)

    # TODO unused?
    @classmethod
    def failwith(cls, status, prev=False):
        """
        Fail next invocation with @status. If @prev is true, execute
        the original command
        """

        __prev_mkcmd = cls.__next_mkcmd

        @classmethod
        def __mkcmd(cls, args):
            if prev:
                cmd = __prev_mkcmd(args)
                cmd = "'%s' && exit %d" % ("' '".join(cmd), status)
            else:
                cmd = "exit %d" % status

            return [ '/bin/sh', '-c', cmd  ]

        cls.__next_mkcmd = __mkcmd

    __strace_n = 0

    # TODO unused?
    @classmethod
    def strace(cls):
        """
        Run next invocation through strace.
        Output goes to /tmp/tap-ctl.<sm-pid>.<n>; <n> counts invocations.
        """

        __prev_mkcmd = cls.__next_mkcmd

        @classmethod
        def __next_mkcmd(cls, args):
            cmd = __prev_mkcmd(args)

            tracefile = "/tmp/%s.%d.%d" % (os.path.basename(cls.PATH),
                                           os.getpid(),
                                           cls.__strace_n)
            cls.__strace_n += 1

            return \
                [ '/usr/bin/strace', '-o', tracefile, '--'] + cmd

        cls.__next_mkcmd = __next_mkcmd

    @classmethod
    def _call(cls, args, quiet = False):
        """
        Spawn a tap-ctl process. Return a TapCtl invocation.
        Raises a TapCtl.CommandFailure if subprocess creation failed.
        """
        cmd = cls._mkcmd(args)

        if not quiet:
            util.SMlog(cmd)
        try:
            p = subprocess.Popen(cmd,
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE)
        except OSError, e:
            raise cls.CommandFailure(cmd, errno=e.errno)

        return cls(cmd, p)

    def _errmsg(self):
        output = map(str.rstrip, self._p.stderr)
        return "; ".join(output)

    def _wait(self, quiet = False):
        """
        Reap the child tap-ctl process of this invocation.
        Raises a TapCtl.CommandFailure on non-zero exit status.
        """
        status = self._p.wait()
        if not quiet:
            util.SMlog(" = %d" % status)

        if status == 0: return

        info = { 'errmsg'   : self._errmsg(),
                 'pid'      : self._p.pid }

        if status < 0:
            info['signal'] = -status
        else:
            info['status'] = status

        raise self.CommandFailure(self.cmd, **info)

    @classmethod
    def _pread(cls, args, quiet = False):
        """
        Spawn a tap-ctl invocation and read a single line.
        """
        tapctl = cls._call(args, quiet)

        output = tapctl.stdout.readline().rstrip()

        tapctl._wait(quiet)
        return output

    @staticmethod
    def _maybe(opt, parm):
        if parm is not None: return [ opt, parm ]
        return []

    @classmethod
    def __list(cls, uuid = None, pid = None, _type = None, path = None):
        args = [ "list" ]
        args += cls._maybe("-n", uuid)
        args += cls._maybe("-p", pid)
        args += cls._maybe("-t", _type)
        args += cls._maybe("-f", path)

        # list the tapdisks
        tapctl = cls._call(args, True)

        # parse the output
        for line in tapctl.stdout:
            row = {}

            for field in line.rstrip().split(' ', 3):
                # TODO Is the '=' due to tap-ctl-list dictionary output?
                bits = field.split('=')
                if len(bits) == 2:
                    key, val = field.split('=')

                    if key == 'pid':
                        row[key] = int(val, 10)

                    elif key in ('state'):
                        row[key] = int(val, 0x10)

                    else:
                        row[key] = val
                else:
                    util.SMlog("Ignoring unexpected tap-ctl output: %s" \
                            % repr(field))
            yield row

        tapctl._wait(True)

    @classmethod
    @retried(backoff=.5, limit=10)
    def list(cls, **args):

        # FIXME. We typically get an EPROTO when uevents interleave
        # with SM ops and a tapdisk shuts down under our feet. Should
        # be fixed in SM.

        try:
            return list(cls.__list(**args))

        except cls.CommandFailure, e:
            transient = [ errno.EPROTO, errno.ENOENT ]
            if e.status in transient:
                raise RetryLoop.TransientFailure(e)
            raise

    @classmethod
    def spawn(cls):
        args = [ "spawn" ]
        pid = cls._pread(args)
        return int(pid)

    @classmethod
    def open(cls, pid, uuid, _type, _file, options):
        params = Tapdisk.Arg(_type, _file)
        args = [ "open", "-p", pid, '-n', uuid, '-a', str(params) ]
        if options.get("rdonly"):
            args.append('-R')
        if options.get("lcache"):
            args.append("-r")
        if options.get("existing_prt") != None:
            args.append("-e")
            args.append(options["existing_prt"])
        if options.get("secondary"):
            args.append("-2")
            args.append(options["secondary"])
        if options.get("standby"):
            args.append("-s")
        if options.get("timeout"):
            args.append("-t")
            args.append(str(options["timeout"]))
        cls._pread(args)

    @classmethod
    def close(cls, pid, uuid, force = False):
        args = [ "close", "-p", pid, "-n", uuid ]
        if force: args += [ "-f" ]
        cls._pread(args)

    @classmethod
    def pause(cls, pid, uuid):
        args = [ "pause", "-p", pid, "-n", uuid ]
        cls._pread(args)

    @classmethod
    def unpause(cls, pid, uuid, _type = None, _file = None, mirror = None):
        args = [ "unpause", "-p", pid, "-n", uuid ]
        if mirror:
            args.extend(["-2", mirror])
        if _type and _file:
            params = Tapdisk.Arg(_type, _file)
            args  += [ "-a", str(params) ]
        cls._pread(args)

    @classmethod
    def stats(cls, pid, uuid):
        args = [ "stats", "-p", pid, "-n", uuid ]
        return cls._pread(args, quiet = True)

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
        items = self.attrs.iteritems()
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
        self.arg   = arg
        self.err   = err

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

def mkdirs(path, mode=0777):
    if not os.path.exists(path):
        parent, subdir = os.path.split(path)
        assert parent != path
        try:
            if parent:
                mkdirs(parent, mode)
            if subdir:
                os.mkdir(path, mode)
        except OSError, e:
            if e.errno != errno.EEXIST:
                raise


class Tapdisk(object):

    TYPES = [ 'aio', 'vhd' ]

    def __init__(self, pid, uuid, _type, path, state):
        self.pid     = pid
        self.uuid    = uuid
        self.type    = _type
        self.path    = path
        self.state   = state
        self._dirty  = False

    def __str__(self):
        state = self.pause_state()
        return "Tapdisk(%s, pid=%d, uuid=%s, state=%s)" % \
            (self.get_arg(), self.pid, self.uuid, state)

    @classmethod
    def list(cls, **args):

        for row in TapCtl.list(**args):

            args =  { 'pid'     : None,
                      'uuid'    : None,
                      'state'   : None,
                      '_type'   : None,
                      'path'    : None }

            for key, val in row.iteritems():
                if key in args:
                    args[key] = val

            if 'args' in row:
                image = Tapdisk.Arg.parse(row['args'])
                args['_type'] = image.type
                args['path']  = image.path

            if None in args.values():
                continue

            yield Tapdisk(**args)

    @classmethod
    def find(cls, **args):

        found = list(cls.list(**args))

        if len(found) > 1:
            raise TapdiskNotUnique(found)

        if found:
            return found[0]

        return None

    @classmethod
    def find_by_path(cls, path):
        return cls.find(path=path)

    @classmethod
    def find_by_uuid(cls, uuid):
        return cls.find(uuid=uuid)

    @classmethod
    def get(cls, **attrs):

        tapdisk = cls.find(**attrs)

        if not tapdisk:
            raise TapdiskNotRunning(**attrs)

        return tapdisk

    @classmethod
    def from_path(cls, path):
        return cls.get(path=path)

    @classmethod
    def from_uuid(cls, uuid):
        return cls.get(uuid=uuid)


    class Arg:

        def __init__(self, _type, path):
            self.type = _type
            self.path =  path

        def __str__(self):
            return "%s:%s" % (self.type, self.path)

        @classmethod
        def parse(cls, arg):

            try:
                _type, path = arg.split(":", 1)
            except ValueError:
                raise cls.InvalidArgument(arg)

            if _type not in Tapdisk.TYPES:
                raise cls.InvalidType(_type, path)

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
        return '/var/run/blktap-control/nbd%s.%s' % (self.pid, self.uuid)

    @classmethod
    def launch_from_arg(cls, arg):
        arg = cls.Arg.parse(arg)
        return cls.launch(arg.path, arg.type, False)

    @classmethod
    def launch_on_tap(cls, uuid, path, _type, options):

        tapdisk = cls.find_by_path(path)
        if tapdisk:
            raise TapdiskExists(tapdisk)

        # FIXME replace spanw/open with create
        try:
            pid = TapCtl.spawn()

            try:
                TapCtl.open(pid, uuid, _type, path, options)
                return cls.from_uuid(uuid)

            except:
                # FIXME: Should be tap-ctl shutdown.
                try:
                    import signal
                    os.kill(pid, signal.SIGTERM)
                    os.waitpid(pid, 0)
                finally:
                    raise

        except TapCtl.CommandFailure, ctl:
            util.logException(ctl)
            raise TapdiskFailed(cls.Arg(_type, path), ctl)

    @classmethod
    def launch(cls, path, _type, rdonly, uuid):
        return cls.launch_on_tap(uuid, path, _type, {"rdonly": rdonly})

    def shutdown(self, force = False):
        TapCtl.close(self.pid, self.uuid, force)

    def pause(self):

        if not self.is_running():
            raise TapdiskInvalidState(self)

        TapCtl.pause(self.pid, self.uuid)

        self._set_dirty()

    def unpause(self, _type=None, path=None, mirror=None):

        if not self.is_paused():
            raise TapdiskInvalidState(self)

        # FIXME: should the arguments be optional?
        if _type is None: _type = self.type
        if  path is None:  path = self.path

        TapCtl.unpause(self.pid, self.uuid, _type, path, mirror=mirror)

        self._set_dirty()

    def stats(self):
        import simplejson
        json = TapCtl.stats(self.pid, self.uuid)
        return simplejson.loads(json)

    #
    # NB. dirty/refresh: reload attributes on next access
    #

    def _set_dirty(self):
        self._dirty = True

    def _refresh(self, __get):
        t = self.from_uuid(__get('uuid'))
        self.__init__(t.pid, t.uuid, t.type, t.path, t.state)

    def __getattribute__(self, name):
        def __get(name):
            # NB. avoid(rec(ursion)
            return object.__getattribute__(self, name)

        if __get('_dirty') and \
                name in ['uuid', 'type', 'path', 'state']:
            self._refresh(__get)
            self._dirty = False

        return __get(name)

    class PauseState:
        RUNNING             = 'R'
        PAUSING             = 'r'
        PAUSED              = 'P'

    class Flags:
        DEAD                 = 0x0001
        CLOSED               = 0x0002
        QUIESCE_REQUESTED    = 0x0004
        QUIESCED             = 0x0008
        PAUSE_REQUESTED      = 0x0010
        PAUSED               = 0x0020
        SHUTDOWN_REQUESTED   = 0x0040
        LOCKING              = 0x0080
        RETRY_NEEDED         = 0x0100
        LOG_DROPPED          = 0x0200

        PAUSE_MASK           = PAUSE_REQUESTED|PAUSED

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


class VDI(object):
    """SR.vdi driver decorator for blktap3"""

    CONF_KEY_ALLOW_CACHING = "vdi_allow_caching"
    CONF_KEY_MODE_ON_BOOT = "vdi_on_boot"
    CONF_KEY_CACHE_SR = "local_cache_sr"
    LOCK_CACHE_SETUP = "cachesetup"

    ATTACH_DETACH_RETRY_SECS = 120

    # number of seconds on top of NFS timeo mount option the tapdisk should 
    # wait before reporting errors. This is to allow a retry to succeed in case 
    # packets were lost the first time around, which prevented the NFS client 
    # from returning before the timeo is reached even if the NFS server did 
    # come back earlier
    TAPDISK_TIMEOUT_MARGIN = 30

    def __init__(self, uuid, target, driver_info):
        self.target      = self.TargetDriver(target, driver_info)
        self._vdi_uuid   = uuid
        self._session    = target.session
        self.xenstore_data = scsiutil.update_XS_SCSIdata(uuid,scsiutil.gen_synthetic_page_data(uuid))
        self.lock        = Lock("vdi", uuid)

    @classmethod
    def from_cli(cls, uuid):
        import VDI as sm
        import XenAPI

        session = XenAPI.xapi_local()
        session.xenapi.login_with_password('root', '')

        target = sm.VDI.from_uuid(session, uuid)
        driver_info = target.sr.srcmd.driver_info

        return cls(uuid, target, driver_info)

    @staticmethod
    def _tap_type(vdi_type):
        """Map a VDI type (e.g. 'raw') to a tapdisk driver type (e.g. 'aio')"""
        return {
            'raw'  : 'aio',
            'vhd'  : 'vhd',
            'iso'  : 'aio', # for ISO SR
            'aio'  : 'aio', # for LVHD
            'file' : 'aio',
            } [vdi_type]

    def get_tap_type(self):
        vdi_type = self.target.get_vdi_type()
        return VDI._tap_type(vdi_type)

    def get_phy_path(self):
        return self.target.get_vdi_path()

    class UnexpectedVDIType(Exception):

        def __init__(self, vdi_type, target):
            self.vdi_type = vdi_type
            self.target   = target

        def __str__(self):
            return \
                "Target %s has unexpected VDI type '%s'" % \
                (type(self.target), self.vdi_type)

    VDI_PLUG_TYPE = { 'phy'  : 'phy',  # for NETAPP
                      'raw'  : 'phy',
                      'aio'  : 'tap',  # for LVHD raw nodes
                      'iso'  : 'tap', # for ISOSR
                      'file' : 'tap',
                      'vhd'  : 'tap' }

    def tap_wanted(self):

        # 1. Let the target vdi_type decide

        vdi_type = self.target.get_vdi_type()

        try:
            plug_type = self.VDI_PLUG_TYPE[vdi_type]
        except KeyError:
            raise self.UnexpectedVDIType(vdi_type,
                                         self.target.vdi)

        if plug_type == 'tap': return True

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
            self.vdi    = vdi
            self._caps  = driver_info['capabilities']

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
                self.vdi.activate(sr_uuid, vdi_uuid)

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
            except OSError, e:
                # We do unlink during teardown, but have to stay
                # idempotent. However, a *wrong* target should never
                # be seen.
                if e.errno != errno.EEXIST: raise
                assert self._equals(target)

        def unlink(self):
            try:
                os.unlink(self.path())
            except OSError, e:
                if e.errno != errno.ENOENT: raise

        def __str__(self):
            path = self.path()
            return "%s(%s)" % (self.__class__.__name__, path)

    class SymLink(Link):
        """Symlink some file to a common name"""

        def readlink(self):
            return os.readlink(self.path())

        def symlink(self):
            return self.path

        def _mklink(self, target):
            os.symlink(target, self.path())

        def _equals(self, target):
            return self.readlink() == target

    class DeviceNode(Link):
        """Relink a block device node to a common name"""

        @classmethod
        def _real_stat(cls, target):
            """stat() not on @target, but its realpath()"""
            _target = os.path.realpath(target)
            return os.stat(_target)

        @classmethod
        def is_block(cls, target):
            """Whether @target refers to a block device."""
            return S_ISBLK(cls._real_stat(target).st_mode)

        def _mklink(self, target):

            st = self._real_stat(target)
            if not S_ISBLK(st.st_mode):
                raise cls.NotABlockDevice(target, st)

            os.mknod(self.path(), st.st_mode, st.st_rdev)

        def _equals(self, target):
            target_rdev = self._real_stat(target).st_rdev
            return self.stat().st_rdev == target_rdev

        class NotABlockDevice(Exception):

            def __init__(self, path, st):
                self.path = path
                self.st   = st

            def __str__(self):
                return "%s is not a block device: %s" % (self.path, self.st)

    class Hybrid(Link):

        def __init__(self, path):
            VDI.Link.__init__(self, path)
            self._devnode = VDI.DeviceNode(path)
            self._symlink = VDI.SymLink(path)

        def mklink(self, target):
            if self._devnode.is_block(target):
                self._obj = self._devnode
            else:
                self._obj = self._symlink
            self._obj.mklink(target)

        def _equals(self, target):
            return self._obj._equals(target)

    class PhyLink(SymLink): BASEDIR = "/dev/sm/phy"
    # NB. Cannot use DeviceNodes, e.g. FileVDIs aren't bdevs.

    class BackendLink(Hybrid): BASEDIR = "/dev/sm/backend"
    # NB. Could be SymLinks as well, but saving major,minor pairs in
    # Links enables neat state capturing when managing Tapdisks.  Note
    # that we essentially have a tap-ctl list replacement here. For
    # now make it a 'Hybrid'. Likely to collapse into a DeviceNode as
    # soon as ISOs are tapdisks.

    # FIXME What does this do?
    @staticmethod
    def _tap_activate(phy_path, vdi_type, sr_uuid, options, uuid):

        # TODO Shouldn't this be find_by_uuid?
        tapdisk = Tapdisk.find_by_path(phy_path)
        if not tapdisk:
            tapdisk = Tapdisk.launch_on_tap(uuid, phy_path,
                    VDI._tap_type(vdi_type), options)
            util.SMlog("tap.activate: Launched %s" % tapdisk)
        else:
            util.SMlog("tap.activate: Found %s" % tapdisk)
        return tapdisk.get_devpath()

    @staticmethod
    def _tap_deactivate(uuid):

        try:
            tapdisk = Tapdisk.from_uuid(uuid)
        except TapdiskNotRunning, e:
            util.SMlog("tap.deactivate: Warning, %s" % e)
            # NB. Should not be here unless the agent refcount
            # broke. Also, a clean shutdown should not have leaked
            # the recorded minor.
        else:
            tapdisk.shutdown()
            util.SMlog("tap.deactivate: Shut down %s" % tapdisk)

    @classmethod
    def tap_pause(cls, session, sr_uuid, vdi_uuid):
        util.SMlog("Pause request for %s" % vdi_uuid)
        vdi_ref = session.xenapi.VDI.get_by_uuid(vdi_uuid)
        session.xenapi.VDI.add_to_sm_config(vdi_ref, 'paused', 'true')
        sm_config = session.xenapi.VDI.get_sm_config(vdi_ref)
        for key in filter(lambda x: x.startswith('host_'), sm_config.keys()):
            host_ref = key[len('host_'):]
            util.SMlog("Calling tap-pause on host %s" % host_ref)
            if not cls.call_pluginhandler(session, host_ref,
                    sr_uuid, vdi_uuid, "pause"):
                # Failed to pause node
                session.xenapi.VDI.remove_from_sm_config(vdi_ref, 'paused')
                return False
        return True

    @classmethod
    def tap_unpause(cls, session, sr_uuid, vdi_uuid, secondary = None,
            activate_parents = False):
        util.SMlog("Unpause request for %s secondary=%s" % (vdi_uuid, secondary))
        vdi_ref = session.xenapi.VDI.get_by_uuid(vdi_uuid)
        sm_config = session.xenapi.VDI.get_sm_config(vdi_ref)
        for key in filter(lambda x: x.startswith('host_'), sm_config.keys()):
            host_ref = key[len('host_'):]
            util.SMlog("Calling tap-unpause on host %s" % host_ref)
            if not cls.call_pluginhandler(session, host_ref,
                    sr_uuid, vdi_uuid, "unpause", secondary, activate_parents):
                # Failed to unpause node
                return False
        session.xenapi.VDI.remove_from_sm_config(vdi_ref, 'paused')
        return True

    @classmethod
    def tap_refresh(cls, session, sr_uuid, vdi_uuid, activate_parents = False):
        util.SMlog("Refresh request for %s" % vdi_uuid)
        vdi_ref = session.xenapi.VDI.get_by_uuid(vdi_uuid)
        sm_config = session.xenapi.VDI.get_sm_config(vdi_ref)
        for key in filter(lambda x: x.startswith('host_'), sm_config.keys()):
            host_ref = key[len('host_'):]
            util.SMlog("Calling tap-refresh on host %s" % host_ref)
            if not cls.call_pluginhandler(session, host_ref,
                    sr_uuid, vdi_uuid, "refresh", None, activate_parents):
                # Failed to refresh node
                return False
        return True

    @classmethod
    def call_pluginhandler(cls, session, host_ref, sr_uuid, vdi_uuid, action,
            secondary = None, activate_parents = False):
        """Optionally, activate the parent LV before unpausing"""
        try:
            args = {"sr_uuid":sr_uuid,"vdi_uuid":vdi_uuid}
            if secondary:
                args["secondary"] = secondary
            if activate_parents:
                args["activate_parents"] = "true"
            ret = session.xenapi.host.call_plugin(
                    host_ref, PLUGIN_TAP_PAUSE, action,
                    args)
            return ret == "True"
        except:
            util.logException("BLKTAP3:call_pluginhandler")
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
        if sm_config.has_key('paused'):
            util.SMlog("Paused or host_ref key found [%s]" % sm_config)
            return False
        host_key = "host_%s" % host_ref
        if sm_config.has_key(host_key):
            util.SMlog("WARNING: host key %s (%s) already there!" % (host_key,
                    sm_config[host_key]))
        else:
            self._session.xenapi.VDI.add_to_sm_config(vdi_ref, host_key,
                    attach_mode)
        sm_config = self._session.xenapi.VDI.get_sm_config(vdi_ref)
        if sm_config.has_key('paused'):
            util.SMlog("Found paused key, aborting")
            self._session.xenapi.VDI.remove_from_sm_config(vdi_ref, host_key)
            return False
        util.SMlog("Activate lock succeeded")
        return True

    def _check_tag(self, vdi_uuid):
        vdi_ref = self._session.xenapi.VDI.get_by_uuid(vdi_uuid)
        sm_config = self._session.xenapi.VDI.get_sm_config(vdi_ref)
        if sm_config.has_key('paused'):
            util.SMlog("Paused key found [%s]" % sm_config)
            return False
        return True

    def _remove_tag(self, vdi_uuid):
        vdi_ref = self._session.xenapi.VDI.get_by_uuid(vdi_uuid)
        host_ref = self._session.xenapi.host.get_by_uuid(util.get_this_host())
        sm_config = self._session.xenapi.VDI.get_sm_config(vdi_ref)
        host_key = "host_%s" % host_ref
        if sm_config.has_key(host_key):
            self._session.xenapi.VDI.remove_from_sm_config(vdi_ref, host_key)
            util.SMlog("Removed host key %s for %s" % (host_key, vdi_uuid))
        else:
            util.SMlog("WARNING: host key %s not found!" % host_key)

    def attach(self, sr_uuid, vdi_uuid, writable, activate = False):
        if not self.target.has_cap("ATOMIC_PAUSE") or activate:
            util.SMlog("Attach & activate")
            self._attach(sr_uuid, vdi_uuid)
            dev_path = self._activate(sr_uuid, vdi_uuid,
                    {"rdonly": not writable})
            self.BackendLink.from_uuid(sr_uuid, vdi_uuid).mklink(dev_path)

        # Return backend/ link
        back_path = self.BackendLink.from_uuid(sr_uuid, vdi_uuid).path()
        struct = { 'params': back_path,
                   'xenstore_data': self.xenstore_data}
        util.SMlog('result: %s' % struct)

        try:
            f=open("%s.attach_info" % back_path, 'a')
            f.write(xmlrpclib.dumps((struct,), "", True))
            f.close()
        except Exception, e:
            # FIXME Is it safe to ignore such exceptions?
            util.SMlog('ignoring exception %s (%s)' \
                    % (e, traceback.format_exc()))

        return xmlrpclib.dumps((struct,), "", True)

    def activate(self, sr_uuid, vdi_uuid, writable, caching_params):
        util.SMlog("blktap3.activate")
        options = {"rdonly": not writable}
        options.update(caching_params)
        timeout = util.get_nfs_timeout(self.target.vdi.session, sr_uuid)
        if timeout:
            options["timeout"] = timeout + self.TAPDISK_TIMEOUT_MARGIN
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
        import VDI as sm

        #util.SMlog("VDI.activate %s" % vdi_uuid)
        if self.tap_wanted():
            if not self._add_tag(vdi_uuid, not options["rdonly"]):
                return False
            # it is possible that while the VDI was paused some of its
            # attributes have changed (e.g. its size if it was inflated; or its 
            # path if it was leaf-coalesced onto a raw LV), so refresh the
            # object completely
            params = self.target.vdi.sr.srcmd.params
            target = sm.VDI.from_uuid(self.target.vdi.session, vdi_uuid)
            target.sr.srcmd.params = params
            driver_info = target.sr.srcmd.driver_info
            self.target = self.TargetDriver(target, driver_info)

        try:
            util.fistpoint.activate_custom_fn(
                    "blktap_activate_inject_failure",
                    lambda: util.inject_failure())

            # Attach the physical node
            if self.target.has_cap("ATOMIC_PAUSE"):
                self._attach(sr_uuid, vdi_uuid)

            # Activate the physical node
            dev_path = self._activate(sr_uuid, vdi_uuid, options)
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
                    except xmlrpclib.ProtocolError, e:
                        # If there's a connection error, keep trying forever.
                        if e.errcode == httplib.INTERNAL_SERVER_ERROR:
                            continue
                        else:
                            util.SMlog('failed to remove tag: %s' % e)
                            break
                    except Exception, e:
                        util.SMlog('failed to remove tag: %s' % e)
                        break
            raise

        # FIXME
        # Link result to backend/
        self.BackendLink.from_uuid(sr_uuid, vdi_uuid).mklink(dev_path)
        return True

    def _activate(self, sr_uuid, vdi_uuid, options):
        self.target.activate(sr_uuid, vdi_uuid)

        # FIXME what's this cache thing?
        dev_path = self.setup_cache(sr_uuid, vdi_uuid, options)
        if not dev_path:
            phy_path = self.PhyLink.from_uuid(sr_uuid, vdi_uuid).readlink()
            # Maybe launch a tapdisk on the physical link
            if self.tap_wanted():
                vdi_type = self.target.get_vdi_type()
                dev_path = self._tap_activate(phy_path, vdi_type, sr_uuid,
                        options, vdi_uuid)
            else:
                dev_path = phy_path # Just reuse phy

        return dev_path

    def _attach(self, sr_uuid, vdi_uuid):
        attach_info = xmlrpclib.loads(self.target.attach(sr_uuid, vdi_uuid))[0][0]
        params = attach_info['params']
        xenstore_data = attach_info['xenstore_data']
        phy_path = util.to_plain_string(params)
        self.xenstore_data.update(xenstore_data)
        # Save it to phy/
        self.PhyLink.from_uuid(sr_uuid, vdi_uuid).mklink(phy_path)

    def deactivate(self, sr_uuid, vdi_uuid, caching_params):
        util.SMlog("blktap3.deactivate")
        for i in range(self.ATTACH_DETACH_RETRY_SECS):
            try:
                if self._deactivate_locked(sr_uuid, vdi_uuid, caching_params):
                    return
            except util.SRBusyException, e:
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

    # FIXME
    def _resetPhylink(self, sr_uuid, vdi_uuid, path):
        self.PhyLink.from_uuid(sr_uuid, vdi_uuid).mklink(path)

    def detach(self, sr_uuid, vdi_uuid):
        if not self.target.has_cap("ATOMIC_PAUSE"):
            util.SMlog("Deactivate & detach")
            self._deactivate(sr_uuid, vdi_uuid, {})
            self._detach(sr_uuid, vdi_uuid)
        else:
            pass # nothing to do
    
    def _deactivate(self, sr_uuid, vdi_uuid, caching_params):
        import VDI as sm

        # Shutdown tapdisk
        back_link = self.BackendLink.from_uuid(sr_uuid, vdi_uuid)
        if not util.pathexists(back_link.path()):
            util.SMlog("Backend path %s does not exist" % back_link.path())
            return

        try:
            attach_info_path = "%s.attach_info" % (back_link.path())
            os.unlink(attach_info_path)
        except:
            util.SMlog("unlink of attach_info failed")

        self._tap_deactivate(vdi_uuid)
        self.remove_cache(sr_uuid, vdi_uuid, caching_params)

        # Remove the backend link
        back_link.unlink()

        # Deactivate & detach the physical node
        if self.tap_wanted():
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

        # FIXME
        # Remove phy/
        self.PhyLink.from_uuid(sr_uuid, vdi_uuid).unlink()

    def _updateCacheRecord(self, session, vdi_uuid, on_boot, caching):
        # Remove existing VDI.sm_config fields
        vdi_ref = session.xenapi.VDI.get_by_uuid(vdi_uuid)
        for key in ["on_boot", "caching"]:
            session.xenapi.VDI.remove_from_sm_config(vdi_ref,key)
        if not on_boot is None:
            session.xenapi.VDI.add_to_sm_config(vdi_ref,'on_boot',on_boot)
        if not caching is None:
            session.xenapi.VDI.add_to_sm_config(vdi_ref,'caching',caching)

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

        session = XenAPI.xapi_local()
        session.xenapi.login_with_password('root', '')

        dev_path = None
        local_sr_uuid = params.get(self.CONF_KEY_CACHE_SR)
        if not local_sr_uuid:
            util.SMlog("ERROR: Local cache SR not specified, not enabling")
            return
        dev_path = self._setup_cache(session, sr_uuid, vdi_uuid,
                local_sr_uuid, scratch_mode, params)

        if dev_path:
            self._updateCacheRecord(session, self.target.vdi.uuid,
                    params.get(self.CONF_KEY_MODE_ON_BOOT),
                    params.get(self.CONF_KEY_ALLOW_CACHING))

        session.xenapi.session.logout()
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
        import SR
        import EXTSR
        import NFSSR
        import XenAPI
        from lock import Lock
        from FileSR import FileVDI

        parent_uuid = vhdutil.getParent(self.target.vdi.path,
                FileVDI.extractUuid)
        if not parent_uuid:
            util.SMlog("ERROR: VDI %s has no parent, not enabling" % \
                    self.target.vdi.uuid)
            return

        util.SMlog("Setting up cache")
        parent_uuid = parent_uuid.strip()
        shared_target = NFSSR.NFSFileVDI(self.target.vdi.sr, parent_uuid)

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
            except util.CommandException, e:
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
                    msize = leaf_size / 1024 / 1024, checkEmpty = False)
        except util.CommandException, e:
            util.SMlog("Error creating leaf cache: %s" % e)
            self.alert_no_cache(session, vdi_uuid, local_sr_uuid, e.code)
            return None

        local_leaf_size = vhdutil.getSizeVirt(local_leaf_path)
        if leaf_size > local_leaf_size:
            util.SMlog("Leaf size %d > local leaf cache size %d, resizing" %
                    (leaf_size, local_leaf_size))
            vhdutil.setSizeVirtFast(local_leaf_path, leaf_size)

        vdi_type = self.target.get_vdi_type()

        prt_tapdisk_created = False
        prt_tapdisk = Tapdisk.find_by_path(read_cache_path)
        if not prt_tapdisk:
            parent_options = copy.deepcopy(options)
            parent_options["rdonly"] = False
            parent_options["lcache"] = True

            # FIXME find a better name for the parent UUID
            prt_tapdisk = Tapdisk.launch_on_tap("parent_of_%s" % vdi_uuid,
                    read_cache_path, 'vhd', parent_options)
            prt_tapdisk_created = True

        secondary = "%s:%s" % (self.target.get_vdi_type(),
                self.PhyLink.from_uuid(sr_uuid, vdi_uuid).readlink())

        util.SMlog("Parent tapdisk: %s" % prt_tapdisk)
        leaf_tapdisk = Tapdisk.find_by_path(local_leaf_path)
        if not leaf_tapdisk:
            child_options = copy.deepcopy(options)
            child_options["rdonly"] = False
            child_options["lcache"] = False
            child_options["existing_prt"] = prt_tapdisk.get_devpath()
            child_options["secondary"] = secondary
            child_options["standby"] = scratch_mode
            try:
                leaf_tapdisk = Tapdisk.launch_on_tap(vdi_uuid,
                        local_leaf_path, 'vhd', child_options)
            except:
                if prt_tapdisk_created:
                    prt_tapdisk.shutdown()
                raise

        lock.release()

        util.SMlog("Local read cache: %s, local leaf: %s" % \
                (read_cache_path, local_leaf_path))

        return leaf_tapdisk.get_devpath()

    def remove_cache(self, sr_uuid, vdi_uuid, params):
        if not self.target.has_cap("SR_CACHING"):
            return

        caching = params.get(self.CONF_KEY_ALLOW_CACHING) == "true"

        local_sr_uuid = params.get(self.CONF_KEY_CACHE_SR)
        if caching and not local_sr_uuid:
            util.SMlog("ERROR: Local cache SR not specified, ignore")
            return

        session = XenAPI.xapi_local()
        session.xenapi.login_with_password('root', '')

        if caching:
            self._remove_cache(session, local_sr_uuid)

        self._updateCacheRecord(session, self.target.vdi.uuid, None, None)
        session.xenapi.session.logout()

    def _is_tapdisk_in_use(self, uuid):
        (retVal, links) = util.findRunningProcessOrOpenFile("tapdisk")
        if not retVal:
            # err on the side of caution
            return True

        # FIXME Assuming that tapdisk process is using
        # /dev/sm/backend/<SR UUID>/<VDI UUID>
        for link in links:
            if link.find(uuid) != -1:
                return True
        return False

    def _remove_cache(self, session, local_sr_uuid):
        import SR
        import EXTSR
        import NFSSR
        import XenAPI
        from lock import Lock
        from FileSR import FileVDI

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
        elif not self._is_tapdisk_in_use(prt_tapdisk.uuid):
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


if __name__ == '__main__':

    import sys
    prog  = os.path.basename(sys.argv[0])

    #
    # Simple CLI interface for manual operation
    #
    #  tap.*  level calls go down to local Tapdisk()s (by physical path)
    #  vdi.*  level calls run the plugin calls across host boundaries.
    #

    def usage(stream):
        print >>stream, \
            "usage: %s tap.{list}" % prog
        print >>stream, \
            "       %s tap.{launch|find|get|pause|" % prog + \
            "unpause|shutdown|stats} {[<tt>:]<path>} | [uuid=]<str> | .. }"
        print >>stream, \
            "       %s vbd.uevent" % prog

    try:
        cmd = sys.argv[1]
    except IndexError:
        usage(sys.stderr)
        sys.exit(1)

    try:
        _class, method = cmd.split('.')
    except:
        usage(sys.stderr)
        sys.exit(1)

    #
    # Local Tapdisks
    #
    from pprint import pprint

    if cmd == 'tap.launch':

        tapdisk = Tapdisk.launch_from_arg(sys.argv[2])
        print >> sys.stderr, "Launched %s" % tapdisk

    elif _class == 'tap':

        attrs = {}
        for item in sys.argv[2:]:
            try:
                key, val = item.split('=')
                attrs[key] = val
                continue
            except ValueError:
                pass

            try:
                attrs['uuid'] = item
                continue
            except ValueError:
                pass

            try:
                arg = Tapdisk.Arg.parse(item)
                attrs['_type'] = arg.type
                attrs['path']  = arg.path
                continue
            except Tapdisk.Arg.InvalidArgument:
                pass

            attrs['path'] = item

        if cmd == 'tap.list':

            # FIXME If some script uses this output it must be updated
            # accordingly.
            for tapdisk in Tapdisk.list(**attrs):
                print tapdisk

        else:
            
            if not attrs:
                usage(sys.stderr)
                sys.exit(1)

            try:
                tapdisk = Tapdisk.get(**attrs)
            except TypeError:
                usage(sys.stderr)
                sys.exit(1)

            if cmd == 'tap.shutdown':
                # Shutdown a running tapdisk, or raise
                tapdisk.shutdown()
                print >> sys.stderr, "Shut down %s" % tapdisk

            elif cmd == 'tap.pause':
                # Pause an unpaused tapdisk, or raise
                tapdisk.pause()
                print >> sys.stderr, "Paused %s" % tapdisk

            elif cmd == 'tap.unpause':
                # Unpause a paused tapdisk, or raise
                tapdisk.unpause()
                print >> sys.stderr, "Unpaused %s" % tapdisk

            elif cmd == 'tap.stats':
                # Gather tapdisk status
                stats = tapdisk.stats()
                print "%s:" % tapdisk
                pprint(stats)

            else:
                usage(sys.stderr)
                sys.exit(1)
    else:
        usage(sys.stderr)
        sys.exit(1)
