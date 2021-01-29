import mock
import MooseFSSR
import unittest


class FakeMooseFSSR(MooseFSSR.MooseFSSR):
    uuid = None
    sr_ref = None
    srcmd = None
    other_config = {}

    def __init__(self, srcmd, none):
        self.dconf = srcmd.dconf
        self.srcmd = srcmd


class TestMooseFSSR(unittest.TestCase):

    def create_moosefssr(self, masterhost='aServer', rootpath='/aServerpath',
                     sr_uuid='asr_uuid', useroptions=''):
        srcmd = mock.Mock()
        srcmd.dconf = {
            'masterhost': masterhost,
            'rootpath': rootpath
        }
        if useroptions:
            srcmd.dconf.update({'options': useroptions})
        srcmd.params = {
            'command': 'some_command',
            'device_config': {}
        }
        moosefssr = FakeMooseFSSR(srcmd, None)
        moosefssr.load(sr_uuid)
        return moosefssr

    @mock.patch('MooseFSSR.MooseFSSR._is_moosefs_available', mock.MagicMock(return_value="mfsmount"))
    @mock.patch('MooseFSSR.Lock', autospec=True)
    def test_load(self, Lock):
        self.create_moosefssr()

    @mock.patch('MooseFSSR.MooseFSSR._is_moosefs_available', mock.MagicMock(return_value="mfsmount"))
    @mock.patch('MooseFSSR.MooseFSSR.checkmount', autospec=True)
    @mock.patch('MooseFSSR.Lock', autospec=True)
    def test_attach_if_mounted_then_attached(self, mock_lock, mock_checkmount):
        mfssr = self.create_moosefssr()
        mock_checkmount.return_value=True
        mfssr.attach('asr_uuid')
        self.assertTrue(mfssr.attached)

    @mock.patch('MooseFSSR.MooseFSSR._is_moosefs_available', mock.MagicMock(return_value="mfsmount"))
    @mock.patch('MooseFSSR.Lock', autospec=True)
    def test_mount_mountpoint_empty_string(self, mock_lock):
        mfssr = self.create_moosefssr()
        self.assertRaises(MooseFSSR.MooseFSException, mfssr.mount)

    @mock.patch('MooseFSSR.MooseFSSR._is_moosefs_available', mock.MagicMock(return_value="mfsmount"))
    @mock.patch('MooseFSSR.MooseFSSR.checkmount',return_value=False, autospec=True)
    @mock.patch('MooseFSSR.Lock', autospec=True)
    def test_detach_not_detached_if_not_mounted(self, mock_lock, mock_checkmount):
        mfssr = self.create_moosefssr()
        mfssr.attached = True
        mock_checkmount.return_value=False
        mfssr.detach('asr_uuid')
        self.assertTrue(mfssr.attached)
