import errno
import json
from StringIO import StringIO
import subprocess
import unittest
import mock
import os

import blktap2
import testlib
import util

class TestVDI(unittest.TestCase):
    # This can't use autospec as vdi is created in __init__
    # See https://docs.python.org/3/library/unittest.mock.html#autospeccing
    @mock.patch('blktap2.VDI.TargetDriver')
    @mock.patch('blktap2.Lock', autospec=True)
    def setUp(self, mock_lock, mock_target):
        mock_target.get_vdi_type.return_value = 'phy'

        def mock_handles(type_str):
            return type_str == 'udev'

        mock_target.vdi.sr.handles.side_effect = mock_handles

        self.vdi = blktap2.VDI('uuid', mock_target, None)
        self.vdi.target = mock_target

    def test_tap_wanted_returns_true_for_udev_device(self):
        result = self.vdi.tap_wanted()

        self.assertEquals(True, result)

    def test_get_tap_type_returns_aio_for_udev_device(self):
        result = self.vdi.get_tap_type()

        self.assertEquals('aio', result)

    class NBDLinkForTest(blktap2.VDI.NBDLink):
        __name__ = "bob"

    @mock.patch('blktap2.VDI.NBDLink', autospec=NBDLinkForTest)
    @mock.patch('blktap2.VDI.NBDLink', autospec=NBDLinkForTest)
    def test_linknbd_not_called_for_no_tap(self, nbd_link2, nbd_link):
        self.vdi.linkNBD("blahblah", "yadayada")
        self.assertEquals(nbd_link.from_uuid.call_count, 0)

    @mock.patch('blktap2.VDI.NBDLink', autospec=NBDLinkForTest)
    @mock.patch('blktap2.VDI.NBDLink', autospec=NBDLinkForTest)
    def test_linknbd(self, nbd_link2, nbd_link):
        self.vdi.tap = blktap2.Tapdisk(123, 456, "blah", "blah", "blah")
        nbd_link.from_uuid.return_value = nbd_link2
        self.vdi.linkNBD("blahblah", "yadayada")
        expected_path = '/run/blktap-control/nbd%d.%d' % (123, 456)
        nbd_link.from_uuid.assert_called_with("blahblah", "yadayada")
        nbd_link2.mklink.assert_called_with(expected_path)


class TestTapCtl(unittest.TestCase):

    def setUp(self):
        subprocess_patcher = mock.patch("blktap2.subprocess")
        self.mock_subprocess = subprocess_patcher.start()

        log_patcher = mock.patch('blktap2.util.SMlog', autospec=True)
        self.mock_log = log_patcher.start()
        self.mock_log.side_effect = self.log

        self.addCleanup(mock.patch.stopall)

    def log(self, message):
        print message

    def test_list_no_args(self):
        """
        TapCtl list no args
        """
        mock_process = mock.MagicMock(autospec='subprocess.Popen')
        mock_process.stdout = StringIO(
            "pid=705 minor=0 state=0 args=vhd:/dev/VG_XenStorage-2eeb9fd5-6545-8f0b-cf72-0378e413a31c/VHD-a7c0f37e-b7fb-4a44-a6fe-05067fb84c09")
        mock_process.wait.return_value = 0
        self.mock_subprocess.Popen.return_value = mock_process

        results = blktap2.TapCtl.list()

        self.mock_subprocess.Popen.assert_called_with(
            ['/usr/sbin/tap-ctl', 'list'],
            close_fds=True, stdin=mock.ANY,
            stdout=mock.ANY, stderr=mock.ANY)
        self.assertEqual(1, len(results))
        self.assertEqual(705, results[0]['pid'])
        self.assertEqual(0, results[0]['minor'])
        self.assertEqual(0, results[0]['state'])
        self.assertEqual('vhd:/dev/VG_XenStorage-2eeb9fd5-6545-8f0b-cf72-0378e413a31c/VHD-a7c0f37e-b7fb-4a44-a6fe-05067fb84c09',
                         results[0]['args'])

    def test_list_pid_arg(self):
        """
        TapCtl list pid arg
        """
        mock_process = mock.MagicMock(autospec='subprocess.Popen')
        mock_process.stdout = StringIO("")
        mock_process.wait.return_value = 0
        self.mock_subprocess.Popen.return_value = mock_process

        attrs = {"pid": 705}

        results = blktap2.TapCtl.list(**attrs)

        self.assertEqual(0, len(results))
        self.assertEqual(1, self.mock_subprocess.Popen.call_count)
        self.assertIn('-p 705', ' '.join(
            self.mock_subprocess.Popen.call_args[0][0]))

    def test_list_retry_eproto(self):
        """
        TapCtl list retry on eproto
        """
        mock_process1 = mock.MagicMock(autospec='subprocess.Popen')
        mock_process1.stdout = StringIO("")
        mock_process1.wait.return_value = errno.EPROTO
        mock_process2 = mock.MagicMock(autospec='subprocess.Popen')
        mock_process2.stdout = StringIO("")
        mock_process2.wait.return_value = 0

        self.mock_subprocess.Popen.side_effect = [
            mock_process1, mock_process2]

        results = blktap2.TapCtl.list()

        self.assertEqual(0, len(results))
        self.assertEqual(2, self.mock_subprocess.Popen.call_count)

    def test_list_eperm_failure(self):
        """
        TapCtl list failure on eperm
        """
        mock_process1 = mock.MagicMock(autospec='subprocess.Popen')
        mock_process1.stdout = StringIO("")
        mock_process1.wait.return_value = errno.EPERM

        self.mock_subprocess.Popen.side_effect = [
            mock_process1]

        with self.assertRaises(blktap2.TapCtl.CommandFailure) as cf:
            blktap2.TapCtl.list()

        self.assertTrue(cf.exception.has_status)

    def test_list_signalled(self):
        """
        TapCtl list, exited signalled
        """
        mock_process1 = mock.MagicMock(autospec='subprocess.Popen')
        mock_process1.stdout = StringIO("")
        mock_process1.wait.return_value = -11

        self.mock_subprocess.Popen.side_effect = [
            mock_process1]

        with self.assertRaises(blktap2.TapCtl.CommandFailure) as cf:
            blktap2.TapCtl.list()

        self.assertTrue(cf.exception.has_signal)

    def test_allocate_no_path(self):
        """
        TapCtl allocate
        """
        mock_process = mock.MagicMock(autospec='subprocess.Popen')
        mock_process.stdout = StringIO('/dev/xen/blktap-2/tapdev1')
        mock_process.wait.return_value = 0
        self.mock_subprocess.Popen.side_effect = [mock_process]

        results = blktap2.TapCtl.allocate()

        self.mock_subprocess.Popen.assert_called_with(
            ['/usr/sbin/tap-ctl', 'allocate'],
            close_fds=True, stdin=mock.ANY,
            stdout=mock.ANY, stderr=mock.ANY)
        self.assertEqual('/dev/xen/blktap-2/tapdev1',
                         results)

    def test_free(self):
        """
        TapCtl free
        """
        mock_process = mock.MagicMock(autospec='subprocess.Popen')
        mock_process.stdout = StringIO('')
        mock_process.wait.return_value = 0
        self.mock_subprocess.Popen.side_effect = [mock_process]

        blktap2.TapCtl.free(1)

        self.mock_subprocess.Popen.assert_called_with(
            ['/usr/sbin/tap-ctl', 'free', '-m', '1'],
            close_fds=True, stdin=mock.ANY,
            stdout=mock.ANY, stderr=mock.ANY)

    def test_spawn(self):
        """
        TapCtl spawn
        """
        mock_process = mock.MagicMock(autospec='subprocess.Popen')
        mock_process.stdout = StringIO('22127')
        mock_process.wait.return_value = 0
        self.mock_subprocess.Popen.side_effect = [mock_process]

        pid = blktap2.TapCtl.spawn()

        self.mock_subprocess.Popen.assert_called_with(
            ['/usr/sbin/tap-ctl', 'spawn'],
            close_fds=True, stdin=mock.ANY,
            stdout=mock.ANY, stderr=mock.ANY)

        self.assertEqual(22127, pid)

    def test_spawn_retry_on_eperm(self):
        """
        TapCtl spawn, retry (CA-292268)
        """
        mock_process1 = mock.MagicMock(autospec='subprocess.Popen')
        mock_process1.stdout = StringIO('')
        mock_process1.wait.return_value = errno.EPERM
        mock_process2 = mock.MagicMock(autospec='subprocess.Popen')
        mock_process2.stdout = StringIO('22127')
        mock_process2.wait.return_value = 0
        self.mock_subprocess.Popen.side_effect = [
            mock_process1, mock_process2]

        pid = blktap2.TapCtl.spawn()

        self.mock_subprocess.Popen.assert_called_with(
            ['/usr/sbin/tap-ctl', 'spawn'],
            close_fds=True, stdin=mock.ANY,
            stdout=mock.ANY, stderr=mock.ANY)

        self.assertEqual(22127, pid)

    def test_spawn_fail_on_error(self):
        """
        TapCtl spawn, command failure
        """
        mock_process = mock.MagicMock(autospec='subprocess.Popen')
        mock_process.stdout = StringIO('')
        mock_process.wait.return_value = errno.EIO
        self.mock_subprocess.Popen.side_effect = [mock_process]

        with self.assertRaises(blktap2.TapCtl.CommandFailure) as cf:
            blktap2.TapCtl.spawn()

        self.assertEqual(errno.EIO, cf.exception.status)

    def test_attach(self):
        """
        TapCtl attach
        """
        mock_process = mock.MagicMock(autospec='subprocess.Popen')
        mock_process.stdout = StringIO('')
        mock_process.wait.return_value = 0
        self.mock_subprocess.Popen.side_effect = [mock_process]

        blktap2.TapCtl.attach(22127, 2)

        self.assertEqual(1, self.mock_subprocess.Popen.call_count)
        proc_args = ' '.join(self.mock_subprocess.Popen.call_args[0][0])
        self.assertIn('/usr/sbin/tap-ctl attach', proc_args)
        self.assertIn('-p 22127', proc_args)
        self.assertIn('-m 2', proc_args)

    def test_detach(self):
        """
        TapCtl detach
        """
        mock_process = mock.MagicMock(autospec='subprocess.Popen')
        mock_process.stdout = StringIO('')
        mock_process.wait.return_value = 0
        self.mock_subprocess.Popen.side_effect = [mock_process]

        blktap2.TapCtl.detach(22127, 2)

        self.assertEqual(1, self.mock_subprocess.Popen.call_count)
        proc_args = ' '.join(self.mock_subprocess.Popen.call_args[0][0])
        self.assertIn('/usr/sbin/tap-ctl detach', proc_args)
        self.assertIn('-p 22127', proc_args)
        self.assertIn('-m 2', proc_args)

    def test_close(self):
        """
        Tapctl close
        """
        mock_process = mock.MagicMock(autospec='subprocess.Popen')
        mock_process.stdout = StringIO('')
        mock_process.wait.return_value = 0
        self.mock_subprocess.Popen.side_effect = [mock_process]

        blktap2.TapCtl.close(22127, 2)

        self.assertEqual(1, self.mock_subprocess.Popen.call_count)
        proc_args = ' '.join(self.mock_subprocess.Popen.call_args[0][0])
        self.assertIn('/usr/sbin/tap-ctl close', proc_args)
        self.assertIn('-p 22127', proc_args)
        self.assertIn('-m 2', proc_args)
        # Close should have a timeout
        self.assertIn('-t 30', proc_args)
        # Not forced
        self.assertNotIn('-f', proc_args)

    def test_close_force(self):
        """
        Tapctl close, forced
        """
        mock_process = mock.MagicMock(autospec='subprocess.Popen')
        mock_process.stdout = StringIO('')
        mock_process.wait.return_value = 0
        self.mock_subprocess.Popen.side_effect = [mock_process]

        blktap2.TapCtl.close(22127, 2, force=True)

        self.assertEqual(1, self.mock_subprocess.Popen.call_count)
        proc_args = ' '.join(self.mock_subprocess.Popen.call_args[0][0])
        self.assertIn('/usr/sbin/tap-ctl close', proc_args)
        self.assertIn('-p 22127', proc_args)
        self.assertIn('-m 2', proc_args)
        # Close should have a timeout
        self.assertIn('-t 30', proc_args)
        # Forced
        self.assertIn('-f', proc_args)

    def test_pause(self):
        """
        TapCtl pause
        """
        mock_process = mock.MagicMock(autospec='subprocess.Popen')
        mock_process.stdout = StringIO('')
        mock_process.wait.return_value = 0
        self.mock_subprocess.Popen.side_effect = [mock_process]

        blktap2.TapCtl.pause(22127, 2)

        self.assertEqual(1, self.mock_subprocess.Popen.call_count)
        proc_args = ' '.join(self.mock_subprocess.Popen.call_args[0][0])
        self.assertIn('/usr/sbin/tap-ctl pause', proc_args)
        self.assertIn('-p 22127', proc_args)
        self.assertIn('-m 2', proc_args)

    def test_unpause(self):
        """
        TapCtl unpause
        """
        mock_process = mock.MagicMock(autospec='subprocess.Popen')
        mock_process.stdout = StringIO('')
        mock_process.wait.return_value = 0
        self.mock_subprocess.Popen.side_effect = [mock_process]

        blktap2.TapCtl.unpause(
            22127, 2, _type='vhd',
            _file='/dev/VG_XenStorage-2eeb9fd5-6545-8f0b-cf72-'
            '0378e413a31c/VHD-a7c0f37e-b7fb-4a44-a6fe-05067fb84c09.vhd')

        self.assertEqual(1, self.mock_subprocess.Popen.call_count)
        proc_args = ' '.join(self.mock_subprocess.Popen.call_args[0][0])
        self.assertIn('/usr/sbin/tap-ctl unpause', proc_args)
        self.assertIn('-p 22127', proc_args)
        self.assertIn('-m 2', proc_args)
        self.assertIn('-a vhd:/dev/VG_XenStorage-2eeb9fd5-6545-8f0b-cf72-'
                      '0378e413a31c/VHD-a7c0f37e-b7fb-4a44-a6fe-'
                      '05067fb84c09.vhd',
                      proc_args)

    def test_unpause_mirror(self):
        """
        TapCtl unpause, mirroring
        """
        mock_process = mock.MagicMock(autospec='subprocess.Popen')
        mock_process.stdout = StringIO('')
        mock_process.wait.return_value = 0
        self.mock_subprocess.Popen.side_effect = [mock_process]

        blktap2.TapCtl.unpause(22127, 2, mirror='nbd:mirror_vbd/10/xvda')

        self.assertEqual(1, self.mock_subprocess.Popen.call_count)
        proc_args = ' '.join(self.mock_subprocess.Popen.call_args[0][0])
        self.assertIn('/usr/sbin/tap-ctl unpause', proc_args)
        self.assertIn('-p 22127', proc_args)
        self.assertIn('-m 2', proc_args)
        self.assertIn('-2 nbd:mirror_vbd/10/xvda', proc_args)

    def test_unpause_cbtlog(self):
        """
        TapCtl unpause, CBT logging
        """
        mock_process = mock.MagicMock(autospec='subprocess.Popen')
        mock_process.stdout = StringIO('')
        mock_process.wait.return_value = 0
        self.mock_subprocess.Popen.side_effect = [mock_process]

        blktap2.TapCtl.unpause(
            22127, 2,
            cbtlog='/dev/VG_XenStorage-9bf5335b-7fef-298c-109c-'
            '1d12e931edfd/b76f0618-4dad-4b15-825f-b0b0fb006d67.cbtlog')

        self.assertEqual(1, self.mock_subprocess.Popen.call_count)
        proc_args = ' '.join(self.mock_subprocess.Popen.call_args[0][0])
        self.assertIn('/usr/sbin/tap-ctl unpause', proc_args)
        self.assertIn('-p 22127', proc_args)
        self.assertIn('-m 2', proc_args)
        self.assertIn('-c /dev/VG_XenStorage-9bf5335b-7fef-298c-109c-'
                      '1d12e931edfd/b76f0618-4dad-4b15-825f'
                      '-b0b0fb006d67.cbtlog',
                      proc_args)

    def test_open(self):
        """
        TapCtl open
        """
        mock_process = mock.MagicMock(autospec='subprocess.Popen')
        mock_process.stdout = StringIO('')
        mock_process.wait.return_value = 0
        self.mock_subprocess.Popen.side_effect = [mock_process]

        options = {'timeout': 40}

        blktap2.TapCtl.open(
            22127, 2, 'vhd', '/dev/VG_XenStorage-2eeb9fd5-6545-8f0b-cf72-'
            '0378e413a31c/VHD-a7c0f37e-b7fb-4a44-a6fe-05067fb84c09.vhd', options)

        self.assertEqual(1, self.mock_subprocess.Popen.call_count)
        proc_args = ' '.join(self.mock_subprocess.Popen.call_args[0][0])
        self.assertIn('/usr/sbin/tap-ctl open', proc_args)
        self.assertIn('-p 22127', proc_args)
        self.assertIn('-m 2', proc_args)
        self.assertIn('-a vhd:/dev/VG_XenStorage-2eeb9fd5-6545-8f0b-cf72-'
            '0378e413a31c/VHD-a7c0f37e-b7fb-4a44-a6fe-05067fb84c09.vhd',
                      proc_args)
        self.assertIn('-t 40', proc_args)

    def test_open_readonly(self):
        """
        TapCtl open, readonly
        """
        mock_process = mock.MagicMock(autospec='subprocess.Popen')
        mock_process.stdout = StringIO('')
        mock_process.wait.return_value = 0
        self.mock_subprocess.Popen.side_effect = [mock_process]

        options = {'rdonly': True}

        blktap2.TapCtl.open(
            22127, 2, 'vhd', '/dev/VG_XenStorage-2eeb9fd5-6545-8f0b-cf72-'
            '0378e413a31c/VHD-a7c0f37e-b7fb-4a44-a6fe-05067fb84c09.vhd', options)

        self.assertEqual(1, self.mock_subprocess.Popen.call_count)
        proc_args = ' '.join(self.mock_subprocess.Popen.call_args[0][0])
        self.assertIn('/usr/sbin/tap-ctl open', proc_args)
        self.assertIn('-R', proc_args)

    def test_open_secondary(self):
        """
        TapCtl open, readonly
        """
        mock_process = mock.MagicMock(autospec='subprocess.Popen')
        mock_process.stdout = StringIO('')
        mock_process.wait.return_value = 0
        self.mock_subprocess.Popen.side_effect = [mock_process]

        options = {'secondary': 'nbd:mirror_vbd/10/xvda'}

        blktap2.TapCtl.open(
            22127, 2, 'vhd', '/dev/VG_XenStorage-2eeb9fd5-6545-8f0b-cf72-'
            '0378e413a31c/VHD-a7c0f37e-b7fb-4a44-a6fe-05067fb84c09.vhd', options)

        self.assertEqual(1, self.mock_subprocess.Popen.call_count)
        proc_args = ' '.join(self.mock_subprocess.Popen.call_args[0][0])
        self.assertIn('/usr/sbin/tap-ctl open', proc_args)
        self.assertIn('-2 nbd:mirror_vbd/10/xvda', proc_args)

    def test_open_read_cache(self):
        """
        TapCtl open, read cache
        """
        mock_process = mock.MagicMock(autospec='subprocess.Popen')
        mock_process.stdout = StringIO('')
        mock_process.wait.return_value = 0
        self.mock_subprocess.Popen.side_effect = [mock_process]

        options = {'o_direct': False}

        blktap2.TapCtl.open(
            22127, 2, 'vhd', '/dev/VG_XenStorage-2eeb9fd5-6545-8f0b-cf72-'
            '0378e413a31c/VHD-a7c0f37e-b7fb-4a44-a6fe-05067fb84c09.vhd', options)

        self.assertEqual(1, self.mock_subprocess.Popen.call_count)
        proc_args = ' '.join(self.mock_subprocess.Popen.call_args[0][0])
        self.assertIn('/usr/sbin/tap-ctl open', proc_args)
        self.assertIn('-D', proc_args)

    def test_open_intellicache_leaf(self):
        """
        TapCtl open, intellicache leaf
        """
        mock_process = mock.MagicMock(autospec='subprocess.Popen')
        mock_process.stdout = StringIO('')
        mock_process.wait.return_value = 0
        self.mock_subprocess.Popen.side_effect = [mock_process]

        options = {
            'rdonly': False,
            'lcache': False,
            'existing_prt': 4,
            'secondary': None,
            'standby': False
        }

        blktap2.TapCtl.open(
            22127, 2, 'vhd', '/dev/VG_XenStorage-2eeb9fd5-6545-8f0b-cf72-'
            '0378e413a31c/VHD-a7c0f37e-b7fb-4a44-a6fe-05067fb84c09.vhd', options)

        self.assertEqual(1, self.mock_subprocess.Popen.call_count)
        proc_args = ' '.join(self.mock_subprocess.Popen.call_args[0][0])
        self.assertIn('/usr/sbin/tap-ctl open', proc_args)
        self.assertIn('-e 4', proc_args)

    def test_open_intellicache_leaf_non_persist(self):
        """
        TapCtl open, intellicache leaf, non-persistent
        """
        mock_process = mock.MagicMock(autospec='subprocess.Popen')
        mock_process.stdout = StringIO('')
        mock_process.wait.return_value = 0
        self.mock_subprocess.Popen.side_effect = [mock_process]

        options = {
            'rdonly': False,
            'lcache': False,
            'existing_prt': 4,
            'secondary': None,
            'standby': True
        }

        blktap2.TapCtl.open(
            22127, 2, 'vhd', '/dev/VG_XenStorage-2eeb9fd5-6545-8f0b-cf72-'
            '0378e413a31c/VHD-a7c0f37e-b7fb-4a44-a6fe-05067fb84c09.vhd', options)

        self.assertEqual(1, self.mock_subprocess.Popen.call_count)
        proc_args = ' '.join(self.mock_subprocess.Popen.call_args[0][0])
        self.assertIn('/usr/sbin/tap-ctl open', proc_args)
        self.assertIn('-e 4', proc_args)
        self.assertIn('-s', proc_args)

    def test_open_intellicache_parent(self):
        """
        TapCtl open, intellicache parent
        """
        mock_process = mock.MagicMock(autospec='subprocess.Popen')
        mock_process.stdout = StringIO('')
        mock_process.wait.return_value = 0
        self.mock_subprocess.Popen.side_effect = [mock_process]

        options = {
            'rdonly': False,
            'lcache': True,
        }

        blktap2.TapCtl.open(
            22127, 2, 'vhd', '/dev/VG_XenStorage-2eeb9fd5-6545-8f0b-cf72-'
            '0378e413a31c/VHD-a7c0f37e-b7fb-4a44-a6fe-05067fb84c09.vhd', options)

        self.assertEqual(1, self.mock_subprocess.Popen.call_count)
        proc_args = ' '.join(self.mock_subprocess.Popen.call_args[0][0])
        self.assertIn('/usr/sbin/tap-ctl open', proc_args)
        self.assertIn('-r', proc_args)

    def test_open_cbt_log(self):
        """
        TapCtl open, CBT logging
        """
        mock_process = mock.MagicMock(autospec='subprocess.Popen')
        mock_process.stdout = StringIO('')
        mock_process.wait.return_value = 0
        self.mock_subprocess.Popen.side_effect = [mock_process]

        options = {
            'cbtlog': ('/dev/VG_XenStorage-9bf5335b-7fef-298c-109c-'
                       '1d12e931edfd/b76f0618-4dad-4b15-825f'
                       '-b0b0fb006d67.cbtlog')
        }

        blktap2.TapCtl.open(
            22127, 2, 'vhd', '/dev/VG_XenStorage-2eeb9fd5-6545-8f0b-cf72-'
            '0378e413a31c/VHD-a7c0f37e-b7fb-4a44-a6fe-05067fb84c09.vhd', options)

        self.assertEqual(1, self.mock_subprocess.Popen.call_count)
        proc_args = ' '.join(self.mock_subprocess.Popen.call_args[0][0])
        self.assertIn('/usr/sbin/tap-ctl open', proc_args)
        self.assertIn('-C /dev/VG_XenStorage-9bf5335b-7fef-298c-109c-'
                      '1d12e931edfd/b76f0618-4dad-4b15-825f'
                      '-b0b0fb006d67.cbtlog',
                      proc_args)

    @mock.patch('blktap2.TapCtl._load_key')
    def test_open_encryption(self, mock_load_key):
        """
        TapCtl open, with encryption key
        """
        mock_process = mock.MagicMock(autospec='subprocess.Popen')
        mock_process.stdout = StringIO('')
        mock_process.wait.return_value = 0
        self.mock_subprocess.Popen.side_effect = [mock_process]

        options = {'key_hash':
                   '02ebc80e3322161efa5c6b3abe46acc'
                   '0ba1703c469b6d84424e1768e86a652e0',
                   'vdi_uuid': 'a7c0f37e-b7fb-4a44-a6fe-05067fb84c09'}

        rand_data = bytearray(os.urandom(1000000))
        mock_load_key.return_value = rand_data

        blktap2.TapCtl.open(
            22127, 2, 'vhd', '/dev/VG_XenStorage-2eeb9fd5-6545-8f0b-cf72-'
            '0378e413a31c/VHD-a7c0f37e-b7fb-4a44-a6fe-05067fb84c09.vhd', options)

        mock_load_key.assert_called_with(
            '02ebc80e3322161efa5c6b3abe46acc'
            '0ba1703c469b6d84424e1768e86a652e0',
            'a7c0f37e-b7fb-4a44-a6fe-05067fb84c09')
        self.assertEqual(1, self.mock_subprocess.Popen.call_count)
        proc_args = ' '.join(self.mock_subprocess.Popen.call_args[0][0])
        self.assertIn('/usr/sbin/tap-ctl open', proc_args)
        self.assertIn('-p 22127', proc_args)
        self.assertIn('-m 2', proc_args)
        self.assertIn('-a vhd:/dev/VG_XenStorage-2eeb9fd5-6545-8f0b-cf72-'
            '0378e413a31c/VHD-a7c0f37e-b7fb-4a44-a6fe-05067fb84c09.vhd',
                      proc_args)
        self.assertIn('-E', proc_args)

    @mock.patch('blktap2.TapCtl._load_key')
    def test_open_encryption_nokey(self, mock_load_key):
        """
        TapCtl open, with encryption no key found
        """
        options = {'key_hash':
                   '02ebc80e3322161efa5c6b3abe46acc'
                   '0ba1703c469b6d84424e1768e86a652e0',
                   'vdi_uuid': 'a7c0f37e-b7fb-4a44-a6fe-05067fb84c09'}

        mock_load_key.return_value = None

        with self.assertRaises(util.SMException):
            blktap2.TapCtl.open(
                22127, 2, 'vhd', '/dev/VG_XenStorage-2eeb9fd5-6545-8f0b-cf72-'
                '0378e413a31c/VHD-a7c0f37e-b7fb-4a44-a6fe-05067fb84c09.vhd', options)

    def test_stats(self):
        """
        TapCtl stats
        """
        mock_process = mock.MagicMock(autospec='subprocess.Popen')
        mock_process.stdout = StringIO('{ "name": "vhd:/dev/VG_XenStorage-2eeb9fd5-6545-8f0b-cf72-0378e413a31c/VHD-a7c0f37e-b7fb-4a44-a6fe-05067fb84c09", "secs": [ 688, 0 ], "images": [ { "name": "/dev/VG_XenStorage-2eeb9fd5-6545-8f0b-cf72-0378e413a31c/VHD-a7c0f37e-b7fb-4a44-a6fe-05067fb84c09", "hits": [ 688, 0 ], "fail": [ 0, 0 ], "driver": { "type": 4, "name": "vhd", "status": null } } ], "tap": { "minor": 0, "reqs": [ 35, 35 ], "kicks": [ 33, 28 ] }, "FIXME_enospc_redirect_count": 0, "nbd_mirror_failed": 0, "reqs_outstanding": 0, "read_caching": "false" }')
        mock_process.wait.return_value = 0
        self.mock_subprocess.Popen.side_effect = [mock_process]

        results = blktap2.TapCtl.stats(705, 0)

        self.assertIsNotNone(results)
        results_dict = json.loads(results)
        self.assertIn('name', results_dict)
        self.assertIn('secs', results_dict)
        self.assertIn('images', results_dict)
        self.assertIn('hits', results_dict['images'][0])
        self.assertIn('fail', results_dict['images'][0])

    def test_major(self):
        """
        TapCtl major
        """
        mock_process = mock.MagicMock(autospec='subprocess.Popen')
        mock_process.stdout = StringIO('254')
        mock_process.wait.return_value = 0
        self.mock_subprocess.Popen.side_effect = [mock_process]

        results = blktap2.TapCtl.major()

        self.assertEqual(254, results)
