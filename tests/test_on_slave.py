import errno
import unittest
import unittest.mock as mock
import uuid

import lvhdutil
import lvmcache
import util
import vhdutil

import on_slave


class Test_on_slave_is_open(unittest.TestCase):

    MOCK_IMPORTS = ['SRCommand', 'SR', 'NFSSR', 'EXTSR', 'LVHDSR', 'blktap2']

    def fake_import(self, name, *args):
        print('Asked to import {}'.format(name))
        return self.mocks.get(name, self.real_import(name))

    def setUp(self):
        self.addCleanup(mock.patch.stopall)
        self.mocks = {x: mock.MagicMock() for x in self.MOCK_IMPORTS}

        self.real_import = __import__

        import_patcher = mock.patch('builtins.__import__')
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


class Test_on_slave_multi(unittest.TestCase):

    TMP_RENAME_PREFIX = "TEST_OLD_"

    def setUp(self):
        self.session = mock.MagicMock()

        lvmcache_patcher = mock.patch('on_slave.LVMCache', autospec=True)
        self.addCleanup(lvmcache_patcher.stop)
        patched_lvmcache = lvmcache_patcher.start()
        self.mock_lvmcache = mock.MagicMock(lvmcache.LVMCache)
        patched_lvmcache.return_value = self.mock_lvmcache

    @mock.patch('refcounter.RefCounter')
    def test_multi_vdi_inactive(self, mock_refcount):
        vgName = "test_vg"
        sr_uuid = str(uuid.uuid4())
        vdi_uuid = str(uuid.uuid4())
        vdi_fileName = "test-vdi.vhd"
        lock_ref = lvhdutil.NS_PREFIX_LVM + sr_uuid

        args = {"vgName": vgName,
                "action1": "deactivateNoRefcount",
                "lvName1": vdi_fileName,
                "action2": "cleanupLockAndRefcount",
                "uuid2": vdi_uuid,
                "ns2": lock_ref}

        on_slave.multi(self.session, args)

        self.mock_lvmcache.deactivateNoRefcount.assert_called_once_with(
            vdi_fileName)
        mock_refcount.reset.assert_called_once_with(vdi_uuid, lock_ref)

    def test_multi_undo_leaf_coalesce(self):
        vgName = "test_vg"
        sr_uuid = str(uuid.uuid4())
        child_uuid = str(uuid.uuid4())
        child_fileName = "child-vdi.vhd"
        parent_fileName = "parent-vdi.vhd"
        tmpName = lvhdutil.LV_PREFIX[vhdutil.VDI_TYPE_VHD] + \
                self.TMP_RENAME_PREFIX + child_uuid

        args = {"vgName": vgName,
                "action1": "deactivateNoRefcount",
                "lvName1": tmpName,
                "action2": "deactivateNoRefcount",
                "lvName2": child_fileName,
                "action3": "refresh",
                "lvName3": child_fileName,
                "action4": "refresh",
                "lvName4": parent_fileName}

        on_slave.multi(self.session, args)

        self.mock_lvmcache.deactivateNoRefcount.assert_has_calls(
            [mock.call(tmpName), mock.call(child_fileName)])
        self.mock_lvmcache.activateNoRefcount.assert_has_calls(
            [mock.call(child_fileName, True), mock.call(parent_fileName, True)])

    @mock.patch('refcounter.RefCounter')
    def test_multi_update_slave_rename(self, mock_refcount):
        vgName = "test_vg"
        old_name_lv = "old-lv-name"
        vdi_fileName = "test-vdi.vhd"
        origParentUuid = str(uuid.uuid4())
        vdi_uuid = str(uuid.uuid4())

        lock_ref = lvhdutil.NS_PREFIX_LVM + vdi_uuid

        args = {"vgName": vgName,
                "action1": "deactivateNoRefcount",
                "lvName1": old_name_lv,
                "action2": "refresh",
                "lvName2": vdi_fileName,
                "action3": "cleanupLockAndRefcount",
                "uuid3": origParentUuid,
                "ns3": lock_ref}

        on_slave.multi(self.session, args)
        self.mock_lvmcache.deactivateNoRefcount.assert_called_once_with(
            old_name_lv)
        self.mock_lvmcache.activateNoRefcount.assert_called_once_with(
            vdi_fileName, True)
        mock_refcount.reset.assert_called_once_with(origParentUuid, lock_ref)

    def test_multi_refresh_on_slaves(self):
        vgName = "test_vg"
        sr_uuid = str(uuid.uuid4())
        vdi_uuid = str(uuid.uuid4())
        lv_name = 'test_lv'

        lock_ref = lvhdutil.NS_PREFIX_LVM + sr_uuid

        args = {"vgName": vgName,
                "action1": "activate",
                "uuid1": vdi_uuid,
                "ns1": lock_ref,
                "lvName1": lv_name,
                "action2": "refresh",
                "lvName2": lv_name,
                "action3": "deactivate",
                "uuid3": vdi_uuid,
                "ns3": lock_ref,
                "lvName3": lv_name}

        on_slave.multi(self.session, args)

        self.mock_lvmcache.activate.assert_called_once_with(
            lock_ref, vdi_uuid, lv_name, False)
        self.mock_lvmcache.activateNoRefcount.assert_called_once_with(
            lv_name, True)
        self.mock_lvmcache.deactivate.assert_called_once_with(
            lock_ref, vdi_uuid, lv_name, False)

    @mock.patch('refcounter.RefCounter')
    def test_multi_rename_deactivate_error(self, mock_refcount):
        vgName = "test_vg"
        old_name_lv = "old-lv-name"
        vdi_fileName = "test-vdi.vhd"
        origParentUuid = str(uuid.uuid4())
        vdi_uuid = str(uuid.uuid4())

        lock_ref = lvhdutil.NS_PREFIX_LVM + vdi_uuid

        self.mock_lvmcache.deactivateNoRefcount.side_effect = util.CommandException(errno.EIO, 'activate')

        args = {"vgName": vgName,
                "action1": "deactivateNoRefcount",
                "lvName1": old_name_lv,
                "action2": "refresh",
                "lvName2": vdi_fileName,
                "action3": "cleanupLockAndRefcount",
                "uuid3": origParentUuid,
                "ns3": lock_ref}

        with self.assertRaises(util.CommandException):
            on_slave.multi(self.session, args)

        self.mock_lvmcache.deactivateNoRefcount.assert_called_once_with(
            old_name_lv)
        self.assertEqual(0, self.mock_lvmcache.activateNoRefcount.call_count)
        self.assertEqual(0, mock_refcount.reset.call_count)

    @mock.patch('refcounter.RefCounter')
    def test_multi_rename_refresh_error(self, mock_refcount):
        vgName = "test_vg"
        old_name_lv = "old-lv-name"
        vdi_fileName = "test-vdi.vhd"
        origParentUuid = str(uuid.uuid4())
        vdi_uuid = str(uuid.uuid4())

        lock_ref = lvhdutil.NS_PREFIX_LVM + vdi_uuid

        self.mock_lvmcache.activateNoRefcount.side_effect = util.CommandException(errno.EIO, 'activate')

        args = {"vgName": vgName,
                "action1": "deactivateNoRefcount",
                "lvName1": old_name_lv,
                "action2": "refresh",
                "lvName2": vdi_fileName,
                "action3": "cleanupLockAndRefcount",
                "uuid3": origParentUuid,
                "ns3": lock_ref}

        with self.assertRaises(util.CommandException):
            on_slave.multi(self.session, args)

        self.mock_lvmcache.deactivateNoRefcount.assert_called_once_with(
            old_name_lv)
        self.assertEqual(0, mock_refcount.reset.call_count)

    def test_multi_refresh_on_slaves_activate_error(self):
        vgName = "test_vg"
        sr_uuid = str(uuid.uuid4())
        vdi_uuid = str(uuid.uuid4())
        lv_name = 'test_lv'

        lock_ref = lvhdutil.NS_PREFIX_LVM + sr_uuid

        self.mock_lvmcache.activate.side_effect = util.CommandException(errno.EIO, 'activate')

        args = {"vgName": vgName,
                "action1": "activate",
                "uuid1": vdi_uuid,
                "ns1": lock_ref,
                "lvName1": lv_name,
                "action2": "refresh",
                "lvName2": lv_name,
                "action3": "deactivate",
                "uuid3": vdi_uuid,
                "ns3": lock_ref,
                "lvName3": lv_name}

        with self.assertRaises(util.CommandException):
            on_slave.multi(self.session, args)

        self.mock_lvmcache.activate.assert_called_once_with(
            lock_ref, vdi_uuid, lv_name, False)
        self.assertEqual(0, self.mock_lvmcache.activateNoRefcount.call_count)
        self.assertEqual(0, self.mock_lvmcache.deactivate.call_count)

    def test_multi_refresh_on_slaves_refresh_error(self):
        vgName = "test_vg"
        sr_uuid = str(uuid.uuid4())
        vdi_uuid = str(uuid.uuid4())
        lv_name = 'test_lv'

        lock_ref = lvhdutil.NS_PREFIX_LVM + sr_uuid

        self.mock_lvmcache.activateNoRefcount.side_effect = util.CommandException(errno.EIO, 'activate')

        args = {"vgName": vgName,
                "action1": "activate",
                "uuid1": vdi_uuid,
                "ns1": lock_ref,
                "lvName1": lv_name,
                "action2": "refresh",
                "lvName2": lv_name,
                "action3": "deactivate",
                "uuid3": vdi_uuid,
                "ns3": lock_ref,
                "lvName3": lv_name}

        with self.assertRaises(util.CommandException):
            on_slave.multi(self.session, args)

        self.mock_lvmcache.activate.assert_called_once_with(
            lock_ref, vdi_uuid, lv_name, False)
        self.mock_lvmcache.activateNoRefcount.assert_called_once_with(
            lv_name, True)
        self.assertEqual(0, self.mock_lvmcache.deactivate.call_count)

    def test_multi_refresh_on_slaves_deactivate_error(self):
        vgName = "test_vg"
        sr_uuid = str(uuid.uuid4())
        vdi_uuid = str(uuid.uuid4())
        lv_name = 'test_lv'

        lock_ref = lvhdutil.NS_PREFIX_LVM + sr_uuid

        self.mock_lvmcache.deactivate.side_effect = util.CommandException(errno.EIO, 'activate')

        args = {"vgName": vgName,
                "action1": "activate",
                "uuid1": vdi_uuid,
                "ns1": lock_ref,
                "lvName1": lv_name,
                "action2": "refresh",
                "lvName2": lv_name,
                "action3": "deactivate",
                "uuid3": vdi_uuid,
                "ns3": lock_ref,
                "lvName3": lv_name}

        with self.assertRaises(util.CommandException):
            on_slave.multi(self.session, args)

        self.mock_lvmcache.activate.assert_called_once_with(
            lock_ref, vdi_uuid, lv_name, False)
        self.mock_lvmcache.activateNoRefcount.assert_called_once_with(
            lv_name, True)
        self.mock_lvmcache.deactivate.assert_called_once_with(
            lock_ref, vdi_uuid, lv_name, False)

    def test_multi_bad_operation(self):
        args = {"vgName": 'test-vg',
                "action1": "bad_operation"}

        with self.assertRaises(util.SMException):
            on_slave.multi(self.session, args)
