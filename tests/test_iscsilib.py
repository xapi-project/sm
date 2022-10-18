import iscsilib
import unittest.mock as mock
import unittest


TEST_IQN = 'iqn.2003-01.com.bla:00.ecd28.mo121'


class Test_iscsilib(unittest.TestCase):

    @mock.patch('iscsilib.get_rootdisk_IQNs')
    @mock.patch('util.doexec')
    def test_save_rootdisk_nodes(self, doexec, get_rootdisk_iqns):
        get_rootdisk_iqns.return_value = [TEST_IQN]

        iscsilib.save_rootdisk_nodes("/my/safe/tempdir")

        doexec.assert_called_with(['/bin/cp', '-a',
                                   '/var/lib/iscsi/nodes/iqn.2003-01.com.bla:'
                                   '00.ecd28.mo121',
                                   '/my/safe/tempdir'])

    @mock.patch('iscsilib.get_rootdisk_IQNs')
    @mock.patch('util.doexec')
    def test_restore_rootdisk_nodes(self, doexec, get_rootdisk_iqns):
        get_rootdisk_iqns.return_value = [TEST_IQN]

        iscsilib.restore_rootdisk_nodes("/my/safe/tempdir")

        doexec.assert_called_with(['/bin/cp', '-a',
                                   '/my/safe/tempdir/iqn.2003-01.com.bla:00.ecd28.mo121',
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


    @mock.patch('util.doexec', mock.Mock())
    @mock.patch('iscsilib.exn_on_failure')
    @mock.patch('iscsilib.tempfile', autospec=True)
    @mock.patch('shutil.rmtree', autospec=True)
    def test_discovery_success(self, rmtree, mock_tempfile, mock_exc):
        mock_exc.return_value = ("test-target,1000 " + TEST_IQN, "")
        iscsilib.discovery('test-target', 3260, "", "")

        print('Call {}'.format(mock_exc.mock_calls))
        mock_exc.assert_called_with(
            ["iscsiadm", "-m", "discovery", "-t", "st", "-p", "test-target:3260",
             "-I", "default"],
            mock.ANY)

    @mock.patch('util.doexec', mock.Mock())
    @mock.patch('iscsilib.exn_on_failure')
    @mock.patch('iscsilib.tempfile', autospec=True)
    @mock.patch('shutil.rmtree', autospec=True)
    def test_discovery_chap_success(self, rmtree, mock_tempfile, mock_exc):
        mock_exc.side_effect = [
            ("New discovery record for [test-target:3260] added", ""),
            ("",""),
            ("test-target,1000 " + TEST_IQN, "")]
        iscsilib.discovery('test-target', 3260, "chapuser", "chapppass")

        print('Call {}'.format(mock_exc.mock_calls))
        mock_exc.assert_called_with(
            ["iscsiadm", "-m", "discoverydb", "-t", "st", "-p", "test-target:3260",
             "-I", "default", "--discover"],
            mock.ANY)
