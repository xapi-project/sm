import iscsilib
import mock
import unittest


class Test_iscsilib(unittest.TestCase):

    @mock.patch('iscsilib.get_rootdisk_IQNs')
    @mock.patch('util.doexec')
    def test_save_rootdisk_nodes(self, doexec, get_rootdisk_iqns):
        get_rootdisk_iqns.return_value = ['iqn.2003-01.com.bla:00.ecd28.mo121']

        iscsilib.save_rootdisk_nodes()

        doexec.assert_called_with(['/bin/cp', '-a',
                                   '/var/lib/iscsi/nodes/iqn.2003-01.com.bla:'
                                   '00.ecd28.mo121',
                                   '/tmp'])

    @mock.patch('iscsilib.get_rootdisk_IQNs')
    @mock.patch('util.doexec')
    def test_restore_rootdisk_nodes(self, doexec, get_rootdisk_iqns):
        get_rootdisk_iqns.return_value = ['iqn.2003-01.com.bla:00.ecd28.mo121']

        iscsilib.restore_rootdisk_nodes()

        doexec.assert_called_with(['/bin/cp', '-a',
                                   '/tmp/iqn.2003-01.com.bla:00.ecd28.mo121',
                                   '/var/lib/iscsi/nodes'])

    @mock.patch('iscsilib.stop_daemon', mock.Mock())
    @mock.patch('iscsilib.exn_on_failure', mock.Mock())
    @mock.patch('util.doexec', mock.Mock())
    @mock.patch('os.path.exists')
    @mock.patch('shutil.rmtree')
    def test_restart_daemon(self, rmtree, exists):
        exists.return_value = True

        iscsilib.restart_daemon()

        rmtree.assert_has_calls([mock.call('/var/lib/iscsi/nodes'),
                                 mock.call('/var/lib/iscsi/send_targets')])
