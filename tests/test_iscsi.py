from sm.core import iscsi
import unittest.mock as mock
import unittest


TEST_IQN = 'iqn.2003-01.com.bla:00.ecd28.mo121'


class Test_iscsi(unittest.TestCase):

    @mock.patch('sm.core.iscsi.get_rootdisk_IQNs')
    @mock.patch('sm.core.iscsi.util.doexec', return_value=(0, '', ''))
    def test_save_rootdisk_nodes(self, doexec, get_rootdisk_iqns):
        get_rootdisk_iqns.return_value = [TEST_IQN]

        iscsi.save_rootdisk_nodes("/my/safe/tempdir")

        doexec.assert_called_with(['/bin/cp', '-a',
                                   '/var/lib/iscsi/nodes/iqn.2003-01.com.bla:'
                                   '00.ecd28.mo121',
                                   '/my/safe/tempdir'])

    @mock.patch('sm.core.iscsi.util.doexec', return_value=(0, '', ''))
    @mock.patch('sm.core.iscsi.get_rootdisk_IQNs')
    def test_restore_rootdisk_nodes(self, get_rootdisk_iqns, doexec):
        get_rootdisk_iqns.return_value = [TEST_IQN]

        iscsi.restore_rootdisk_nodes("/my/safe/tempdir")

        doexec.assert_called_with(['/bin/cp', '-a',
                                   '/my/safe/tempdir/iqn.2003-01.com.bla:00.ecd28.mo121',
                                   '/var/lib/iscsi/nodes'])

    @mock.patch('sm.core.iscsi._clear_dir')
    @mock.patch('sm.core.iscsi.stop_daemon', mock.Mock())
    @mock.patch('sm.core.iscsi.exn_on_failure', mock.Mock())
    @mock.patch('sm.core.iscsi.saved_rootdisk_nodes')
    def test_restart_daemon(self, _saved, clear_dir):
        """restart_daemon must clear nodes/ and send_targets/ (keeping the
        directories), via _clear_dir, before starting iscsid."""
        iscsi.restart_daemon()

        clear_dir.assert_any_call('/var/lib/iscsi/nodes')
        clear_dir.assert_any_call('/var/lib/iscsi/send_targets')

    @mock.patch('sm.core.iscsi.restart_daemon')
    @mock.patch('sm.core.iscsi.set_current_initiator_name')
    @mock.patch('sm.core.iscsi.is_iscsi_daemon_running', return_value=False)
    def test_ensure_daemon_running_ok_not_running(
            self, _is_running, set_iqn, restart):
        """When iscsid is not running, set initiator name and restart."""
        iscsi.ensure_daemon_running_ok(TEST_IQN)

        set_iqn.assert_called_once_with(TEST_IQN)
        restart.assert_called_once()

    @mock.patch('sm.core.iscsi.restart_daemon')
    @mock.patch('sm.core.iscsi.set_current_initiator_name')
    @mock.patch('sm.core.iscsi._checkAnyTGT', return_value=False)
    @mock.patch('sm.core.iscsi.get_current_initiator_name', return_value='other-iqn')
    @mock.patch('sm.core.iscsi.is_iscsi_daemon_running', return_value=True)
    def test_ensure_daemon_running_ok_iqn_mismatch(
            self, _is_running, _get_iqn, _check_tgt, set_iqn, restart):
        """IQN mismatch with no non-root sessions: set initiator name and restart."""
        iscsi.ensure_daemon_running_ok(TEST_IQN)

        set_iqn.assert_called_once_with(TEST_IQN)
        restart.assert_called_once()

    @mock.patch('sm.core.iscsi.restart_daemon')
    @mock.patch('sm.core.iscsi.get_current_initiator_name', return_value=TEST_IQN)
    @mock.patch('sm.core.iscsi.is_iscsi_daemon_running', return_value=True)
    def test_ensure_daemon_running_ok_no_restart_needed(
            self, _is_running, _get_iqn, restart):
        """When iscsid is running with the correct IQN, no restart occurs."""
        iscsi.ensure_daemon_running_ok(TEST_IQN)

        restart.assert_not_called()

    @mock.patch('sm.core.iscsi.restart_daemon', side_effect=Exception("start failed"))
    @mock.patch('sm.core.iscsi.set_current_initiator_name')
    @mock.patch('sm.core.iscsi.is_iscsi_daemon_running', return_value=False)
    def test_ensure_daemon_running_ok_restart_fails_propagates(
            self, _is_running, _set_iqn, _restart):
        """If restart_daemon raises, the exception propagates out."""
        with self.assertRaises(Exception):
            iscsi.ensure_daemon_running_ok(TEST_IQN)

    @mock.patch('sm.core.iscsi.util.doexec', mock.Mock())
    @mock.patch('sm.core.iscsi.exn_on_failure')
    @mock.patch('sm.core.iscsi.tempfile', autospec=True)
    def test_discovery_success(self, mock_tempfile, mock_exc):
        mock_exc.return_value = ("test-target,1000 " + TEST_IQN, "")
        iscsi.discovery('test-target', 3260, "", "")

        print('Call {}'.format(mock_exc.mock_calls))
        mock_exc.assert_called_with(
            ["iscsiadm", "-m", "discovery", "-t", "st", "-p", "test-target:3260",
             "-I", "default"],
            mock.ANY)

    @mock.patch('sm.core.iscsi.util.doexec', mock.Mock())
    @mock.patch('sm.core.iscsi.exn_on_failure', autospec=True)
    @mock.patch('sm.core.iscsi.tempfile', autospec=True)
    def test_discovery_chap_success(self, mock_tempfile, mock_exc):
        mock_exc.side_effect = [
            ("New discovery record for [test-target:3260] added", ""),
            ("",""),
            ("test-target,1000 " + TEST_IQN, "")]
        iscsi.discovery('test-target', 3260, "chapuser", "chapppass")

        print('Call {}'.format(mock_exc.mock_calls))
        mock_exc.assert_called_with(
            ["iscsiadm", "-m", "discoverydb", "-t", "st", "-p", "test-target:3260",
             "-I", "default", "--discover"],
            mock.ANY)
