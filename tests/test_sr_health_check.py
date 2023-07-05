import mock
import unittest

import sr_health_check
from SR import SR

TEST_HOST = 'test_host'

SR_UUID = 'sr uuid'


class TestSrHealthCheck(unittest.TestCase):

    def setUp(self):
        util_patcher = mock.patch('sr_health_check.util')
        self.mock_util = util_patcher.start()
        self.mock_session = mock.MagicMock()
        self.mock_util.get_localAPI_session.return_value = self.mock_session
        sr_patcher = mock.patch('sr_health_check.SR.SR', autospec=True)
        self.mock_sr = sr_patcher.start()

        self.addCleanup(mock.patch.stopall)

    def expect_good_sr_record(self):
        self.mock_session.xenapi.SR.get_all_records_where.return_value = {
            "iscsi_ref": {'uuid': SR_UUID, 'host': TEST_HOST}
        }

    def expect_good_localhost(self):
        self.mock_util.get_localhost_uuid.return_value = TEST_HOST

    def expect_good_sm_types(self):
        self.mock_session.xenapi.SM.get_all_records_where.return_value = {
            'lvmoiscsi_type_ref': {'type': 'lvmoiscsi'}
        }

    def test_health_check_no_srs(self):
        # Arrange
        self.expect_good_sm_types()
        self.mock_session.xenapi.SR.get_all_records_where.return_value = {}

        # Act
        sr_health_check.main()

        # Assert
        self.mock_session.xenapi.SR.get_all_records_where.assert_called()

    def test_health_check_no_local_pbd(self):
        # Arrange
        self.expect_good_localhost()
        self.expect_good_sm_types()
        self.expect_good_sr_record()
        self.mock_session.xenapi.PBD.get_all_records_where.return_value = {}

        # Act
        sr_health_check.main()

        # Assert
        self.mock_session.xenapi.PBD.get_all_records_where.assert_called_with(
            'field "SR" = "iscsi_ref" and field "host" = "{TEST_HOST}"'.format(
                TEST_HOST=TEST_HOST))

    def test_health_check_sr_not_plugged(self):
        # Arrange
        self.expect_good_localhost()
        self.expect_good_sm_types()
        self.expect_good_sr_record()
        self.mock_session.xenapi.PBD.get_all_records_where.return_value = {
            'pbd_ref': {'currently_attached': False}
        }

        # Act
        sr_health_check.main()

        # Assert
        self.mock_session.xenapi.PBD.get_all_records_where.assert_called_with(
            'field "SR" = "iscsi_ref" and field "host" = "{TEST_HOST}"'.format(
                TEST_HOST=TEST_HOST))

    def test_health_check_run_sr_check(self):
        # Arrange
        self.expect_good_localhost()
        self.expect_good_sm_types()
        self.expect_good_sr_record()
        self.mock_session.xenapi.PBD.get_all_records_where.return_value = {
            'pbd_ref': {'currently_attached': True}
        }
        mock_sr = mock.create_autospec(SR)
        self.mock_sr.from_uuid.return_value = mock_sr

        # Act
        sr_health_check.main()

        # Assert
        mock_sr.check_sr.assert_called_with(SR_UUID)
