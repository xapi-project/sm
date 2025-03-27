import unittest
from unittest import mock

from sm.core import iscsi
from SRCommand import SRCommand


class ISCSITestCase(unittest.TestCase):

    def setUp(self):
        iscsi_patcher = mock.patch(f'{self.TEST_CLASS}.iscsi',
                                      autospec=True)
        self.mock_iscsi = iscsi_patcher.start()
        self.mock_iscsi.discovery.side_effect = self.discovery
        self.mock_iscsi._checkTGT.side_effect = self._checkTGT
        self.mock_iscsi.login.side_effect = self.iscsi_login
        self.mock_iscsi.parse_IP_port = iscsi.parse_IP_port
        self.discovery_data = {}
        self.sessions = []

        sleep_patcher = mock.patch(f'{self.TEST_CLASS}.time.sleep',
                                   autospec=True)
        self.mock_sleep = sleep_patcher.start()

    def _checkTGT(self, tgtIQN, tgt=''):
        all_sessions = '\n'.join(self.sessions)
        matched = iscsi._compare_sessions_to_tgt(all_sessions, tgtIQN, tgt)
        return matched

    def discovery(self, target, port, chapuser, chappassword,
                  targetIQN="any", interface_array=["default"]):
        return self.discovery_data.get(target, [])

    def iscsi_login(self, target, target_iqn, chauser, chappassword,
                    incoming_user, incoming_password, mpath):
        session_count = len(self.sessions)
        self.sessions.append(f'tcp: [{session_count}] {target},1 {target_iqn}')

    def create_sr_command(
            self, additional_dconf=None, cmd=None,
            target_iqn='iqn.2009-01.example.test:iscsi085e938a',
            multihomelist='tgt1:3260,tgt2:3260', target="10.70.89.34"):

        sr_cmd = mock.create_autospec(SRCommand)
        sr_cmd.dconf = {
            'SCSIid': '3600a098038313577792450384a4a6275',
            'multihomelist': multihomelist,
            'target': target,
            'targetIQN': target_iqn,
            'localIQN': 'iqn.2018-05.com.example:0d312804'
        }
        if additional_dconf:
            sr_cmd.dconf.update(additional_dconf)

        sr_cmd.params = {
            'command': 'nop',
            'session_ref': 'test_session',
            'host_ref': 'test_host',
            'sr_ref': 'sr_ref'
        }
        sr_cmd.cmd = cmd
        return sr_cmd

