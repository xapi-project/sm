import unittest
import mock
import uuid

import importlib

import on_slave

class Test_on_slave_is_open(unittest.TestCase):

    MOCK_IMPORTS = ['SRCommand', 'SR', 'NFSSR', 'EXTSR', 'LVHDSR', 'blktap2']

    def fake_import(self, name, *args):
        print 'Asked to import {}'.format(name)
        if name in Test_on_slave_is_open.MOCK_IMPORTS:
            if name not in self.mocks:
                self.mocks[name] = mock.MagicMock()

            return self.mocks[name]
        else:
            return self.real_import(name)

    def setUp(self):
        self.mocks = {}

        self.real_import = __import__

        import_patcher = mock.patch('__builtin__.__import__')
        self.addCleanup(import_patcher.stop)
        self.mock_import = import_patcher.start()
        self.mock_import.side_effect = self.fake_import

        self.mock_sr = mock.MagicMock()
        self.mocks['SR'] = self.mock_sr
        self.mock_blktap2 = mock.MagicMock()
        self.mocks['blktap2'] = self.mock_blktap2

    def test_is_open_nfssr_success(self):
        """
        VDI is open
        """
        vdi_uuid = uuid.uuid4()
        mock_session = mock.MagicMock()

        sr_uuid = uuid.uuid4()
        mock_session.xenapi.SR.get_record.return_value = {
            'type': 'nfssr',
            'uuid': sr_uuid}

        is_open = on_slave.is_open(mock_session,
                                   {
                                       'vdiUuid': vdi_uuid,
                                       'srRef': 'opaqueref:sr_mine'
                                   })
        self.mock_sr.driver.assert_called_once_with('nfssr')
        self.assertEqual('True', is_open)

    def test_is_open_lvm_success(self):
        """
        LVM srs are uplifted to lvhd
        """
        vdi_uuid = uuid.uuid4()
        mock_session = mock.MagicMock()

        sr_uuid = uuid.uuid4()
        mock_session.xenapi.SR.get_record.return_value = {
            'type': 'lvmoiscsisr',
            'uuid': sr_uuid}

        is_open = on_slave.is_open(mock_session,
                                   {
                                       'vdiUuid': vdi_uuid,
                                       'srRef': 'opaqueref:sr_mine'
                                   })
        self.mock_sr.driver.assert_called_once_with('lvhd')
        self.assertEqual('True', is_open)

    def test_is_open_false(self):
        """
        VDI is not open
        """
        self.mock_blktap2.Tapdisk.find_by_path.return_value = None

        vdi_uuid = uuid.uuid4()
        mock_session = mock.MagicMock()

        is_open = on_slave.is_open(mock_session,
                                   {
                                       'vdiUuid': vdi_uuid,
                                       'srRef': 'opaqueref:sr_mine'
                                   })
        self.assertEqual('False', is_open)

    @mock.patch('on_slave.util')
    def test_is_open_xapi_exception(self, mock_util):
        """
        Exceptions from is_open are logged
        """
        mock_log_exception = mock.MagicMock()
        mock_util.logException.side_effect = mock_log_exception
        mock_blktap2 = mock.MagicMock()
        self.mocks['blktap2'] = mock_blktap2

        mock_blktap2.Tapdisk.find_by_path.return_value = None

        vdi_uuid = uuid.uuid4()
        mock_session = mock.MagicMock()

        mock_session.xenapi.SR.get_record.side_effect = Exception('Failed')

        with self.assertRaises(Exception):
            is_open = on_slave.is_open(mock_session,
                                       {
                                           'vdiUuid': vdi_uuid,
                                           'srRef': 'opaqueref:sr_mine'
                                       })

        self.assertTrue(mock_log_exception.called)
        mock_log_exception.assert_called_once_with('is_open')


class Test_on_slave_refresh_lun(unittest.TestCase):
    """
    Tests for refresh_lun_size_by_SCSIid
    """

    def setUp(self):
        self.mock_session = mock.MagicMock()

    @mock.patch('on_slave.scsiutil')
    def test_refresh_success(self, mock_scsiutil):
        """
        Successfully refresh scsi lun size
        """
        mock_scsiutil.refresh_lun_size_by_SCSIid.return_value = True

        refreshed = on_slave.refresh_lun_size_by_SCSIid(self.mock_session, {'SCSIid': 'fake_id'})

        self.assertEqual('True', refreshed)
        mock_scsiutil.refresh_lun_size_by_SCSIid.assert_called_once_with('fake_id')

    @mock.patch('on_slave.scsiutil')
    def test_refresh_failed(self, mock_scsiutil):
        """
        Refresh scsi lun size fails
        """
        mock_scsiutil.refresh_lun_size_by_SCSIid.return_value = False

        refreshed = on_slave.refresh_lun_size_by_SCSIid(self.mock_session, {'SCSIid': 'fake_id'})

        self.assertEqual('False', refreshed)
        mock_scsiutil.refresh_lun_size_by_SCSIid.assert_called_once_with('fake_id')
