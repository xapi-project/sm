import unittest
import testlib
import mock

import os
import util
import subprocess


class fake_proc:

    def __init__(self):
        self.returncode = 0

    def communicate(self, inputtext):
        return "hello", "hello"


class TestCreate(unittest.TestCase):

    @mock.patch('subprocess.Popen', autospec=True)
    def test_env_concatenated(self, popen):
        new_env = {"NewVar1": "yadayada", "NewVar2": "blah"}
        popen.return_value = fake_proc()
        with mock.patch.dict('os.environ', {'hello': 'world'}, clear=True):
            self.assertEqual(os.environ.get('hello'), 'world')
            util.pread(['mount.cifs', '\\aServer',
                        '/var/run/sr-mount/asr_uuid',
                        '-o', 'cache=loose,vers=3.0,actimeo=0,domain=citrix'],
                       new_env=new_env)
            expected_cmd = ['mount.cifs', '\\aServer',
                            '/var/run/sr-mount/asr_uuid', '-o',
                            'cache=loose,vers=3.0,actimeo=0,domain=citrix']
            popen.assert_called_with(expected_cmd,
                                     close_fds=True, stdin=-1, stderr=-1,
                                     env={'hello': 'world', 'NewVar2': 'blah',
                                          'NewVar1': 'yadayada'}, stdout=-1)
