import json
import os
import re
import shutil
import socket
import stat
import subprocess
import sys
import tempfile
import unittest
from collections import defaultdict


INSTALLATION_UUID = "uuid-for-installation"
POOL_UUID = "uuid-for-pool"


class TestStorageInit(unittest.TestCase):
    """
    Unit test for storage-init. Since that is a shell script, simulating the
    environment it expects is a bit involved. The basic idea is that (a
    tweaked version of) the script is executed from within a test, but with
    PATH set only to include a directory that the test controls; any command
    that the script needs to run needs to be available in some way, and for
    ones that the test is interested in are given implementations that connect
    via a socket to the test - which can then capture their arguments and
    determine their behaviour.

    This is a mite awkward, and the code is skewed towards the motivating
    case, which was avoiding regressions in the handling creation of local
    storage after installation.
    """

    def setUp(self):
        self.test_dir = tempfile.TemporaryDirectory()

        # There are tweaks we need to make the to storage-init:
        # - Change the location of various key files
        # - Replace the uses of the shell's built-in '[' operator to a command
        #   we can mock
        # - Ensure that the script calls a special command when it exits, so
        #   that the test knows when it is done.

        self.storage_config = os.path.join(self.test_dir.name,
                                           "default-storage.conf")

        inv_path = self.write_file("xensource-inventory",
                                   f"INSTALLATION_UUID={INSTALLATION_UUID}\n")
        self.write_file("sr-multipathing.conf",
                        "MULTIPATHING_ENABLED='False'\n")

        storage_init_path = os.path.join(os.path.dirname(__file__),
                                         "..",
                                         "scripts",
                                         "storage-init")
        with open(storage_init_path) as f:
            script = f.read()

        script = re.sub(r"/etc/firstboot.d/data\b", self.test_dir.name, script)
        script = re.sub(r"/etc/xensource-inventory\b", inv_path, script)
        script = re.sub(r"/var/lib/misc/ran-storage-init\b",
                        os.path.join(self.test_dir.name, "ran-storage-init"),
                        script)
        script = re.sub(r"(?<!\[)\[\s+([^]]*)\s+\]", r"mock_test \1", script)
        script = f"trap on_exit EXIT\n{script}"

        self.script_path = self.write_file("storage-init", script)

        os.chmod(self.script_path, stat.S_IRWXU)

        # The test directory will contain all the commands we expect
        # storage-init to run. There are three ways these are handled:
        # - symlinked to the real implementation (copy_command)
        # - symlinked to 'true', for commands where we need no output,
        #   we just want them to succeed (create_dummy_command)
        # - create a script that will connect back to test via a socket
        #   (create_mock_command)
        # For the last of these, the communication is a single JSON request
        # and response. The script implementing the command sends its argv,
        # and gets back an exit code, and content for stdout and stderr.

        self.socket_path = os.path.join(self.test_dir.name, "socket.s")
        self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.socket.bind(self.socket_path)
        self.socket.listen(1)

        self.create_mock_command("on_exit")
        self.create_mock_command("mock_test")
        self.create_mock_command("xe")

        self.create_dummy_command("dd")
        self.create_dummy_command("logger")
        self.create_dummy_command("pvremove")
        self.create_dummy_command("pvs")
        self.create_dummy_command("sleep")
        self.create_dummy_command("vgchange")
        self.create_dummy_command("vgreduce")

        self.copy_command("awk")
        self.copy_command("basename")
        self.copy_command("cut")
        self.copy_command("sed")
        self.copy_command("touch")

        self.script_exited = False
        self.created_srs = defaultdict(list)
        self.misc_xe_calls = []
        self.unanticipated_xe_calls = []

    def tearDown(self):
        self.socket.close()
        self.test_dir.cleanup()

    def write_file(self, name, content):
        path = os.path.join(self.test_dir.name, name)
        with open(path, "wt") as f:
            f.write(content)
        return path

    def make_env(self):
        return {
            "PATH": self.test_dir.name
        }

    def copy_command(self, cmd):
        path = shutil.which(cmd)
        assert path
        os.symlink(path, os.path.join(self.test_dir.name, cmd))

    def create_dummy_command(self, cmd):
        path = shutil.which("true")
        assert path
        os.symlink(path, os.path.join(self.test_dir.name, cmd))

    def create_mock_command(self, cmd):
        cmd_path = self.write_file(cmd, f"""\
#!{sys.executable}

import json
import socket
import sys

req = dict(argv=sys.argv)
s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
s.connect("{self.socket_path}")
s.send(json.dumps(req).encode())
resp = json.loads(s.recv(1000000).decode())
sys.stdout.write(resp.get("stdout", ""))
sys.stderr.write(resp.get("stderr", ""))
sys.exit(resp.get("returncode", 0))
        """)

        os.chmod(cmd_path, stat.S_IRWXU)

    def test_install_with_lvm(self):
        with open(self.storage_config, "wt") as f:
            f.write("TYPE=lvm\n")
            f.write("PARTITIONS='/dev/sda'")

        p = subprocess.Popen(["/bin/bash", self.script_path],
                             env=self.make_env())

        self.run_script_commands()

        returncode = p.wait()

        self.assertListEqual(self.unanticipated_xe_calls, [])
        self.assertTrue(os.path.isfile(os.path.join(self.test_dir.name,
                                                    "ran-storage-init")))
        self.assertEqual(returncode, 0)

        self.assertEqual(self.created_srs.keys(), {"lvm", "udev"})
        self.assertEqual(1, len(self.created_srs["lvm"]))
        self.assertEqual(self.created_srs["lvm"][0], {
            "content-type": "user",
            "device-config:device": "/dev/sda",
            "host-uuid": INSTALLATION_UUID,
            "name-label": "Local storage",
            "type": "lvm"
        })

        # At this point this becomes even more of a characterization test,
        # with the expecations being merely what behaviour has been observed
        # in the past.

        self.assertEqual(2, len(self.created_srs["udev"]))
        self.assertEqual(self.created_srs["udev"][0], {
            'content-type': 'disk',
            'device-config-location': '/dev/xapi/block',
            'host-uuid': INSTALLATION_UUID,
            'name-label': 'Removable storage',
            'sm-config:type': 'block',
            'type': 'udev'
        })
        self.assertEqual(self.created_srs["udev"][1], {
            'content-type': 'iso',
            'device-config-location': '/dev/xapi/cd',
            'host-uuid': INSTALLATION_UUID,
            'name-label': 'DVD drives',
            'sm-config:type': 'cd',
            'type': 'udev'
        })
        expected_xe_calls = [
            "sr-param-set uuid=uuid-for-sr-lvm-0 other-config:i18n-key=local-storage",
            "sr-param-set uuid=uuid-for-sr-lvm-0 other-config:i18n-original-value-name_label=Local storage",
            "pool-param-set uuid=uuid-for-pool default-SR=uuid-for-sr-lvm-0",
            "host-param-set uuid=uuid-for-installation crash-dump-sr-uuid=uuid-for-sr-lvm-0",
            "host-param-set uuid=uuid-for-installation suspend-image-sr-uuid=uuid-for-sr-lvm-0",
            "sr-param-set uuid=uuid-for-sr-udev-0 other-config:i18n-key=local-hotplug-disk",
            "sr-param-set uuid=uuid-for-sr-udev-0 other-config:i18n-original-value-name_label=Removable storage",
            "sr-param-set uuid=uuid-for-sr-udev-1 other-config:i18n-key=local-hotplug-cd",
            "sr-param-set uuid=uuid-for-sr-udev-1 other-config:i18n-original-value-name_label=DVD drives",
            "sr-param-set uuid=uuid-for-sr-udev-1 name-description=Physical DVD drives",
            "sr-param-set uuid=uuid-for-sr-udev-1 other-config:i18n-original-value-name_description=Physical DVD drives",
            "pool-sync-database"
        ]

        self.assertListEqual([" ".join(call) for call in self.misc_xe_calls],
                             expected_xe_calls)

    def test_install_with_ext(self):
        with open(self.storage_config, "wt") as f:
            f.write("TYPE=ext\n")
            f.write("PARTITIONS='/dev/sda4 /dev/sdb'")

        p = subprocess.Popen(["/bin/bash", self.script_path],
                             env=self.make_env())

        self.run_script_commands()

        returncode = p.wait()

        self.assertListEqual(self.unanticipated_xe_calls, [])
        self.assertTrue(os.path.isfile(os.path.join(self.test_dir.name,
                                                    "ran-storage-init")))
        self.assertEqual(returncode, 0)

        self.assertEqual(self.created_srs.keys(), {"ext", "udev"})
        self.assertEqual(1, len(self.created_srs["ext"]))
        self.assertEqual(self.created_srs["ext"][0], {
            "content-type": "user",
            "device-config:device": "/dev/sda4,/dev/sdb",
            "host-uuid": INSTALLATION_UUID,
            "name-label": "Local storage",
            "type": "ext"
        })

        # At this point this becomes even more of a characterization test,
        # with the expecations being merely what behaviour has been observed
        # in the past.

        self.assertEqual(2, len(self.created_srs["udev"]))
        self.assertEqual(self.created_srs["udev"][0], {
            'content-type': 'disk',
            'device-config-location': '/dev/xapi/block',
            'host-uuid': INSTALLATION_UUID,
            'name-label': 'Removable storage',
            'sm-config:type': 'block',
            'type': 'udev'
        })
        self.assertEqual(self.created_srs["udev"][1], {
            'content-type': 'iso',
            'device-config-location': '/dev/xapi/cd',
            'host-uuid': INSTALLATION_UUID,
            'name-label': 'DVD drives',
            'sm-config:type': 'cd',
            'type': 'udev'
        })

        expected_xe_calls = [
            "sr-param-set uuid=uuid-for-sr-ext-0 other-config:i18n-key=local-storage",
            "sr-param-set uuid=uuid-for-sr-ext-0 other-config:i18n-original-value-name_label=Local storage",
            "pool-param-set uuid=uuid-for-pool default-SR=uuid-for-sr-ext-0",
            "host-param-set uuid=uuid-for-installation crash-dump-sr-uuid=uuid-for-sr-ext-0",
            "host-param-set uuid=uuid-for-installation suspend-image-sr-uuid=uuid-for-sr-ext-0",
            "event-wait class=host uuid=uuid-for-installation enabled=true",
            "host-disable uuid=uuid-for-installation",
            "host-enable-local-storage-caching uuid=uuid-for-installation sr-uuid=uuid-for-sr-ext-0",
            "host-enable uuid=uuid-for-installation",
            "sr-param-set uuid=uuid-for-sr-udev-0 other-config:i18n-key=local-hotplug-disk",
            "sr-param-set uuid=uuid-for-sr-udev-0 other-config:i18n-original-value-name_label=Removable storage",
            "sr-param-set uuid=uuid-for-sr-udev-1 other-config:i18n-key=local-hotplug-cd",
            "sr-param-set uuid=uuid-for-sr-udev-1 other-config:i18n-original-value-name_label=DVD drives",
            "sr-param-set uuid=uuid-for-sr-udev-1 name-description=Physical DVD drives",
            "sr-param-set uuid=uuid-for-sr-udev-1 other-config:i18n-original-value-name_description=Physical DVD drives",
            "pool-sync-database"
        ]
        self.assertListEqual([" ".join(call) for call in self.misc_xe_calls],
                             expected_xe_calls)

    def test_install_with_other_sr_type(self):
        with open(self.storage_config, "wt") as f:
            f.write("TYPE=wtf\n")
            f.write("PARTITIONS='/dev/sda4 /dev/sdb /dev/sdc'")

        p = subprocess.Popen(["/bin/bash", self.script_path],
                             env=self.make_env())

        self.run_script_commands()

        returncode = p.wait()

        self.assertListEqual(self.unanticipated_xe_calls, [])
        self.assertTrue(os.path.isfile(os.path.join(self.test_dir.name,
                                                    "ran-storage-init")))
        self.assertEqual(returncode, 0)

        self.assertEqual(self.created_srs.keys(), {"wtf", "udev"})
        self.assertEqual(1, len(self.created_srs["wtf"]))
        self.assertEqual(self.created_srs["wtf"][0], {
            "content-type": "user",
            "device-config:device": "/dev/sda4,/dev/sdb,/dev/sdc",
            "host-uuid": INSTALLATION_UUID,
            "name-label": "Local storage",
            "type": "wtf"
        })

    def run_script_commands(self):
        while not self.script_exited:
            c, _ = self.socket.accept()

            req = json.loads(c.recv(1000000).decode())
            argv = req["argv"]
            cmd = os.path.basename(argv[0])
            handler = getattr(self, f"_{cmd}_command")
            resp = handler(argv[1:])
            c.send(json.dumps(resp.to_json()).encode())

            c.close()

    def _on_exit_command(self, args):
        self.script_exited = True
        return CmdResult()

    def _mock_test_command(self, args):
        combined_args = " ".join(args)

        # We need to pretend that disks exist, and are block devices, but
        # aren't symlinks (otherwise we'd have to implement readlink).
        if re.match(r"-L /dev/", combined_args):
            returncode = 1
        elif re.match(r"! -b /dev/", combined_args):
            returncode = 1
        else:
            proc = subprocess.run(["test"] + args)
            returncode = proc.returncode

        return CmdResult(returncode)

    def _xe_command(self, args): # pragma: no cover
        if len(args) == 0:
            return CmdResult(1)

        subcmd = args[0]
        combined_args = " ".join(sorted(args[1:]))

        if subcmd == "sm-list":
            m = re.match("--minimal params=uuid type=(\S+)$", combined_args)
            if m:
                sm_uuid = "uuid-for-sr-type-" + m.group(1)
                return CmdResult(stdout=f"{sm_uuid}\n")

        if subcmd == "sr-list":
            if len(args) == 2 and args[1].startswith("name-label="):
                if not self.created_srs:
                    return CmdResult()

            m = re.match("--minimal params=uuid type=(\S+)$", combined_args)
            if m:
                sr_type = m.group(1)
                num_srs = len(self.created_srs[sr_type])
                uuids = [f"uuid-for-sr-{sr_type}-{sr_num}"
                         for sr_num in range(num_srs)]
                return CmdResult(stdout=",".join(uuids))

        if subcmd == "pbd-list":
            return CmdResult()

        if subcmd == "pool-list" and combined_args == "--minimal params=uuid":
            return CmdResult(stdout=f"{POOL_UUID}\n")

        if subcmd == "sr-create":
            params = {}
            for arg in args:
                m = re.match(r"([^=]*)=(.*)", arg)
                if m:
                    params[m.group(1)] = m.group(2)
            sr_type = params.get("type", "unknown")
            sr_num = len(self.created_srs[sr_type])
            sr_uuid = f"uuid-for-sr-{sr_type}-{sr_num}"

            self.created_srs[sr_type].append(params)
            return CmdResult(stdout=f"{sr_uuid}\n")

        if subcmd.endswith("param-set"):
            self.misc_xe_calls.append(args)
            return CmdResult()

        if re.match(r"host-(en|dis)able", subcmd):
            self.misc_xe_calls.append(args)
            return CmdResult()

        if subcmd in ("pool-sync-database", "event-wait"):
            self.misc_xe_calls.append(args)
            return CmdResult()

        self.unanticipated_xe_calls.append(args)
        return CmdResult(1, stderr=f"Unanticipated: {args}")


class CmdResult:
    def __init__(self, returncode=0, stdout="", stderr=""):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr

    def to_json(self):
        return {
            "returncode": self.returncode,
            "stdout": self.stdout,
            "stderr": self.stderr
        }
