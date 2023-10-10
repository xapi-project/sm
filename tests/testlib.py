import re
import unittest.mock as mock
import os
import io
import fnmatch
import string
import random
import textwrap
import errno

from xml.dom.minidom import parseString

PATHSEP = '/'


class ContextSetupError(Exception):
    pass


def get_error_codes():
    this_dir = os.path.dirname(__file__)
    drivers_dir = os.path.join(this_dir, '..', 'drivers')
    error_codes_path = os.path.join(drivers_dir, 'XE_SR_ERRORCODES.xml')
    error_code_catalog = open(error_codes_path, 'r')
    contents = error_code_catalog.read()
    error_code_catalog.close()
    return contents.encode('utf-8')


class SCSIDisk(object):
    def __init__(self, adapter):
        self.adapter = adapter
        self.long_id = ''.join(
            random.choice(string.digits) for _ in range(33))

    def disk_device_paths(self, host_id, disk_id, actual_disk_letter):
        yield '/sys/class/scsi_disk/%s:0:%s:0' % (host_id, disk_id)
        yield '/sys/class/scsi_disk/%s:0:%s:0/device/block/sd%s' % (
            host_id, disk_id, actual_disk_letter)
        yield '/sys/block/sd%s/device' % (actual_disk_letter)
        yield '/sys/block/sd%s/device/rescan' % (actual_disk_letter)
        yield '/dev/disk/by-scsibus/%s-%s:0:%s:0' % (
            self.adapter.long_id, host_id, disk_id)
        yield '/dev/disk/by-id/%s' % (self.long_id)


class SCSIAdapter(object):
    def __init__(self):
        self.disks = []
        self.long_id = ''.join(
            random.choice(string.digits) for _ in range(33))
        self.parameters = []

    def add_disk(self):
        disk = SCSIDisk(self)
        self.disks.append(disk)
        return disk

    def add_parameter(self, host_class, values):
        self.parameters.append((host_class, values))

    def adapter_device_paths(self, host_id):
        yield '/sys/class/scsi_host/host%s' % host_id


class AdapterWithNonBlockDevice(SCSIAdapter):
    def adapter_device_paths(self, host_id):
        for adapter_device_path in super(AdapterWithNonBlockDevice,
                                         self).adapter_device_paths(host_id):
            yield adapter_device_path
        yield '/sys/class/fc_transport/target7:0:0/device/7:0:0:0'


class Executable(object):
    def __init__(self, function_to_call):
        self.function_to_call = function_to_call

    def run(self, args, stdin):
        (return_code, stdout, stderr) = self.function_to_call(args, stdin)
        return (return_code, stdout, stderr)


class Subprocess(object):
    def __init__(self, executable, args):
        self.executable = executable
        self.args = args

    def communicate(self, data):
        self.returncode, out, err = self.executable.run(self.args, data)
        return out, err


class TestContext(object):
    def __init__(self):
        self.patchers = []
        self.error_codes = get_error_codes()
        self.inventory = {
            'PRIMARY_DISK': '/dev/disk/by-id/primary'
        }
        self.scsi_adapters = []
        self.kernel_version = '3.1'
        self.executables = {}
        self._created_directories = []
        self._path_content = {}
        self._next_fileno = 0
        self.mock_fcntl = None

    def _get_inc_fileno(self):
        result = self._next_fileno
        self._next_fileno += 1
        return result

    def add_executable(self, fpath, funct):
        self.executables[fpath] = Executable(funct)

    def generate_inventory_contents(self):
        return '\n'.join(
            [
                '='.join(
                    [k, v.join(2 * ["'"])]) for k, v in self.inventory.items()
            ]
        )

    def patch(self, *args, **kwargs):
        patcher = mock.patch(*args, **kwargs)
        self.patchers.append(patcher)
        patcher.start()

    def start(self):
        self.patch('builtins.open', new=self.fake_open)
        self.patch('fcntl.fcntl', new=self.fake_fcntl)
        self.patch('os.path.exists', new=self.fake_exists)
        self.patch('os.makedirs', new=self.fake_makedirs)
        self.patch('os.listdir', new=self.fake_listdir)
        self.patch('glob.glob', new=self.fake_glob)
        self.patch('os.uname', new=self.fake_uname)
        self.patch('subprocess.Popen', new=self.fake_popen)
        self.patch('os.rmdir', new=self.fake_rmdir)
        self.patch('os.stat', new=self.fake_stat)

        self.setup_modinfo()

    def fake_fcntl(self, fd, cmd, arg):
        assert(self.mock_fcntl)
        return self.mock_fcntl(fd, cmd, arg)

    def fake_rmdir(self, path):
        if path not in self.get_filesystem():
            raise OSError(errno.ENOENT, 'No such file %s' % path)

        if self.fake_glob(os.path.join(path, '*')):
            raise OSError(errno.ENOTEMPTY, 'Directory is not empty %s' % path)

        assert path in self._created_directories
        self._created_directories = [
            d for d in self._created_directories if d != path]

    def fake_stat(self, path):
        if not self.fake_exists(path):
            raise OSError()

    def fake_makedirs(self, path):
        if path in self.get_filesystem():
            raise OSError(path + " Already exists")
        self._created_directories.append(path)
        self.log("Recursively created directory", path)

    def setup_modinfo(self):
        self.add_executable('/sbin/modinfo', self.fake_modinfo)

    def setup_error_codes(self):
        self._path_content['/opt/xensource/sm/XE_SR_ERRORCODES.xml'] = (
            self.error_codes
        )

    def fake_modinfo(self, args, stdin_data):
        assert len(args) == 3
        assert args[1] == '-d'
        return 0, (args[2] + '-description'), ''

    def fake_popen(self, args, stdin, stdout, stderr, close_fds, env=None, universal_newlines=None):
        import subprocess
        assert stdin == subprocess.PIPE
        assert stdout == subprocess.PIPE
        assert stderr == subprocess.PIPE
        assert close_fds is True

        path_to_executable = args[0]

        if path_to_executable not in self.executables:
            raise ContextSetupError(
                path_to_executable
                + ' was not found. Set it up using add_executable.'
                + ' was called with: ' + str(args))

        executable = self.executables[path_to_executable]
        return Subprocess(executable, args)

    def fake_uname(self):
        return (
            'Linux',
            'testbox',
            self.kernel_version,
            '#1 SMP Thu May 8 09:50:50 EDT 2014',
            'x86_64'
        )

    def fake_open(self, fname, mode='r'):
        if fname == '/etc/xensource-inventory':
            return io.StringIO(self.generate_inventory_contents())

        for fpath, contents in self.generate_path_content():
            if fpath == fname:
                if not self.is_binary(mode):
                    return io.StringIO(contents)
                else:
                    return io.BytesIO(contents)

        if 'w' in mode:
            paths_to_check = list(self.get_created_directories()) + list(self.generate_device_paths())
            if os.path.dirname(fname) in paths_to_check:
                self._path_content[fname] = ''
                return WriteableFile(self, fname, self._get_inc_fileno(),
                                     binary=self.is_binary(mode))
            error = IOError('No such file %s' % fname)
            error.errno = errno.ENOENT
            raise error

        self.log('tried to open file', fname)
        raise IOError(fname)

    def fake_exists(self, fname):
        for existing_fname in self.get_filesystem():
            if fname == existing_fname:
                return True

        self.log('not exists', fname)
        return False

    def fake_listdir(self, path):
        assert '*' not in path
        glob_pattern = path + '/*'
        glob_matches = self.fake_glob(glob_pattern)
        return [match[len(path) + 1:] for match in glob_matches]

    def get_filesystem(self):
        result = set(['/'])
        for devpath in self.generate_device_paths():
            for path in filesystem_for(devpath):
                result.add(path)

        for executable_path in self.executables:
            for path in filesystem_for(executable_path):
                result.add(path)

        for directory in self.get_created_directories():
            result.add(directory)

        return sorted(result)

    def get_created_directories(self):
        result = set(['/'])
        for created_directory in self._created_directories:
            for path in filesystem_for(created_directory):
                result.add(path)
        return sorted(result)

    def generate_path_content(self):
        for host_id, adapter in enumerate(self.scsi_adapters):
            for host_class, values in adapter.parameters:
                for key, value in values.items():
                    path = '/sys/class/%s/host%s/%s' % (
                        host_class, host_id, key)
                    yield (path, value)

        for path, value in self._path_content.items():
            yield (path, value)

    def generate_device_paths(self):
        actual_disk_letter = 'a'
        for host_id, adapter in enumerate(self.scsi_adapters):
            for adapter_device_path in adapter.adapter_device_paths(host_id):
                yield adapter_device_path
            for disk_id, disk in enumerate(adapter.disks):
                for path in disk.disk_device_paths(host_id, disk_id,
                                                   actual_disk_letter):
                    yield path
                actual_disk_letter = chr(ord(actual_disk_letter) + 1)

        for path, _content in self.generate_path_content():
            yield path

    def fake_glob(self, pattern):
        result = []
        pattern_parts = pattern.split(PATHSEP)
        for fname in self.get_filesystem():
            fname_parts = fname.split(PATHSEP)
            if len(fname_parts) != len(pattern_parts):
                continue

            found = True
            for pattern_part, fname_part in zip(pattern_parts, fname_parts):
                if not fnmatch.fnmatch(fname_part, pattern_part):
                    found = False
            if found:
                result.append(fname)

        if not result:
            self.log('no glob', pattern)
        return list(set(result))

    def log(self, *args):
        WARNING = '\033[93m'
        ENDC = '\033[0m'
        import sys
        sys.stdout.write(
            WARNING
            + ' '.join(str(arg) for arg in args)
            + ENDC
            + '\n')

    def stop(self):
        list(map(lambda patcher: patcher.stop(), self.patchers))

    def add_adapter(self, adapter):
        self.scsi_adapters.append(adapter)
        return adapter

    def get_error_code(self, error_name):
        xml = parseString(self.error_codes)
        for code in xml.getElementsByTagName('code'):
            name = code.getElementsByTagName('name')[0].firstChild.nodeValue
            if name == error_name:
                return int(code.getElementsByTagName('value')[0].firstChild.nodeValue)
        return None

    @staticmethod
    def is_binary(mode):
        return 'b' in mode


def with_custom_context(context_class):
    def _with_context(func):
        def decorated(self, *args, **kwargs):
            context = context_class()
            context.start()
            try:
                return func(self, context, * args, ** kwargs)
            finally:
                context.stop()

        decorated.__name__ = func.__name__
        return decorated
    return _with_context


def with_context(func):
    decorator = with_custom_context(TestContext)
    return decorator(func)


def xml_string(text):
    dedented = textwrap.dedent(text).strip()
    lines = []
    for line in dedented.split('\n'):
        lines.append(re.sub(r'^ *', '', line))

    return ''.join(lines)


def marshalled(dom):
    text = dom.toxml()
    result = text.replace('\n', '')
    result = result.replace('\t', '')
    return result


def filesystem_for(path):
    result = [PATHSEP]
    assert path.startswith(PATHSEP)
    segments = [seg for seg in path.split(PATHSEP) if seg]
    for i in range(len(segments)):
        result.append(PATHSEP + PATHSEP.join(segments[:i + 1]))
    return result


class XmlMixIn(object):
    def assertXML(self, expected, actual):
        import xml

        expected_dom = xml.dom.minidom.parseString(
            xml_string(expected))

        actual_dom = xml.dom.minidom.parseString(actual)

        self.assertEqual(
            marshalled(expected_dom),
            marshalled(actual_dom)
        )


class WriteableFile(object):
    def __init__(self, context, fname, fileno, data=None, binary=False):
        self._context = context
        self._fname = fname
        if not binary:
            self._file = io.StringIO(data)
        else:
            self._file = io.BytesIO(data)
        self._fileno = fileno

    def fileno(self):
        return self._fileno

    def write(self, data):
        return self._file.write(data)

    def close(self):
        self._context._path_content[self._fname] = self._file.getvalue()
        self._file.close()

    def seek(self, offset, whence=io.SEEK_SET):
        self._file.seek(offset, whence)

    def read(self, size):
        return self._file.read(size)

    def __enter__(self):
        return self

    def __exit__(self,exc_type, exc_value, traceback):
        self.close()
