import copy
import os
import unittest
import unittest.mock as mock

import uuid

from sm import cleanup
from sm.drivers import LVHDSR
from sm import lvhdutil
from sm import lvutil
from sm import vhdutil

import testlib

PV_FOR_VG_DATA = "/dev/mapper/3600a098038314650465d523777417142"

VHD_UTIL = '/usr/bin/vhd-util'

TEST_VHD_HEADER = """
VHD Footer Summary:
-------------------
Cookie              : conectix
Features            : (0x00000002) <RESV>
File format version : Major: 1, Minor: 0
Data offset         : 512
Timestamp           : Mon Apr 14 09:32:51 2025
Creator Application : 'tap'
Creator version     : Major: 1, Minor: 3
Creator OS          : Unknown!
Original disk size  : 49096 MB (51480887296 Bytes)
Current disk size   : 49096 MB (51480887296 Bytes)
Geometry            : Cyl: 24644, Hds: 16, Sctrs: 255
                    : = 49095 MB (51480330240 Bytes)
Disk type           : Dynamic hard disk
Checksum            : 0xffffed34|0xffffed34 (Good!)
UUID                : e21541f1-0daf-4394-9d80-3c0bfe957b1b
Saved state         : No
Hidden              : 0

VHD Header Summary:
-------------------
Cookie              : cxsparse
Data offset (unusd) : 18446744073709551615
Table offset        : 1536
Header version      : 0x00010000
Max BAT size        : 24548
Block size          : 2097152 (2 MB)
Parent name         :
Parent UUID         : 00000000-0000-0000-0000-000000000000
Parent timestamp    : Sat Jan  1 00:00:00 2000
Checksum            : 0xfffff334|0xfffff334 (Good!)

VHD Batmap Summary:
-------------------
Batmap offset       : 100352
Batmap size (secs)  : 6
Batmap version      : 0x00010002
Checksum            : 0xfffeb525|0xfffeb525 (Good!)
"""



class SMLog(object):
    def __call__(self, *args):
        print(args)


class Stubs(object):
    def init_stubs(self):
        self._stubs = []

    def stubout(self, *args, **kwargs):
        patcher = mock.patch( * args, ** kwargs)
        self._stubs.append(patcher)
        return patcher.start()

    def remove_stubs(self):
        for patcher in self._stubs:
            patcher.stop()


class TestLVHDSR(unittest.TestCase, Stubs):

    def setUp(self):
        self.init_stubs()

    def tearDown(self):
        self.remove_stubs()

    def create_LVHDSR(self, master=False, command='foo', sr_uuid=None, extra_params={}):
        srcmd = mock.Mock()
        srcmd.dconf = {'device': '/dev/bar'}
        if master:
            srcmd.dconf.update({"SRmaster": "true"})
        srcmd.params = {
            'command': command,
            'session_ref': 'some session ref',
            'sr_ref': 'test_sr_ref'}
        srcmd.params.update(extra_params)
        if sr_uuid is None:
            sr_uuid = str(uuid.uuid4())
        return LVHDSR.LVHDSR(srcmd, sr_uuid)

    @mock.patch('sm.drivers.LVHDSR.lvutil.Fairlock', autospec=True)
    @mock.patch('sm.drivers.LVHDSR.lvhdutil.getVDIInfo', autospec=True)
    @mock.patch('sm.drivers.LVHDSR.Lock', autospec=True)
    @mock.patch('sm.drivers.LVHDSR.SR.XenAPI')
    def test_loadvids(self, mock_xenapi, mock_lock, mock_getVDIInfo, mock_lvlock):
        """sr.allVDIs populated by _loadvdis"""

        vdi_uuid = 'some VDI UUID'
        mock_getVDIInfo.return_value = {vdi_uuid: lvhdutil.VDIInfo(vdi_uuid)}
        sr = self.create_LVHDSR()

        sr._loadvdis()

        self.assertEqual([vdi_uuid], list(sr.allVDIs.keys()))

    @mock.patch('sm.drivers.LVHDSR.lvutil.Fairlock', autospec=True)
    @mock.patch('sm.drivers.LVHDSR.lvhdutil.lvRefreshOnAllSlaves', autospec=True)
    @mock.patch('sm.drivers.LVHDSR.lvhdutil.getVDIInfo', autospec=True)
    @mock.patch('sm.journaler.Journaler.getAll', autospec=True)
    @mock.patch('sm.drivers.LVHDSR.Lock', autospec=True)
    @mock.patch('sm.drivers.LVHDSR.SR.XenAPI')
    def test_undoAllInflateJournals(
            self,
            mock_xenapi,
            mock_lock,
            mock_getAll,
            mock_getVDIInfo,
            mock_lvhdutil_lvRefreshOnAllSlaves,
            mock_lvutil_fairlock):
        """No LV refresh on slaves when Cleaning up local LVHD SR's journal"""

        self.stubout('sm.journaler.Journaler.remove')
        self.stubout('sm.drivers.LVHDSR.util.zeroOut')
        self.stubout('sm.drivers.LVHDSR.lvhdutil.deflate')
        self.stubout('sm.drivers.LVHDSR.util.SMlog', new_callable=SMLog)
        self.stubout('sm.drivers.LVHDSR.lvmcache.LVMCache')

        vdi_uuid = 'some VDI UUID'

        mock_getAll.return_value = {vdi_uuid: '0'}
        mock_getVDIInfo.return_value = {vdi_uuid: lvhdutil.VDIInfo(vdi_uuid)}

        sr = self.create_LVHDSR()

        sr._undoAllInflateJournals()
        self.assertEqual(0, mock_lvhdutil_lvRefreshOnAllSlaves.call_count)

    @mock.patch('sm.drivers.LVHDSR.cleanup', autospec=True)
    @mock.patch('sm.drivers.LVHDSR.IPCFlag', autospec=True)
    @mock.patch('sm.drivers.LVHDSR.Lock', autospec=True)
    @mock.patch('sm.drivers.LVHDSR.SR.XenAPI')
    @testlib.with_context
    def test_srlifecycle_success(self,
                            context,
                            mock_xenapi,
                            mock_lock,
                            mock_ipc,
                            mock_cleanup):
        sr_uuid = str(uuid.uuid4())
        self.stubout('sm.drivers.LVHDSR.lvutil._checkVG')
        mock_lvm_cache = self.stubout('sm.drivers.LVHDSR.lvmcache.LVMCache')
        mock_get_vg_stats = self.stubout('sm.drivers.LVHDSR.lvutil._getVGstats')
        mock_scsi_get_size = self.stubout('sm.drivers.LVHDSR.scsiutil.getsize')

        device_size = 100 * 1024 * 1024
        device_free = 10 * 1024 * 1024
        mock_get_vg_stats.return_value = {
            'physical_size': device_size,
            'physical_utilisation': device_free}
        mock_scsi_get_size.return_value = device_size
        mock_lvm_cache.return_value.checkLV.return_value = False

        mock_session = mock_xenapi.xapi_local.return_value
        mock_session.xenapi.SR.get_sm_config.return_value = {
            'allocation': 'thick',
            'use_vhd': 'true'
        }
        vdi_data = {
            'vdi1_ref': {
                'uuid': str(uuid.uuid4()),
                'name_label': "VDI1",
                'name_description': "First VDI",
                'is_a_snapshot': False,
                'snapshot_of': None,
                'snapshot_time': None,
                'type': 'User',
                'metadata-of-pool': None,
                'sm-config': {
                    'vdi_type': 'vhd'
                }
            },
            'vdi2_ref': {
                'uuid': str(uuid.uuid4()),
                'name_label': "VDI2",
                'name_description': "Second VDI",
                'is_a_snapshot': False,
                'snapshot_of': None,
                'snapshot_time': None,
                'type': 'User',
                'metadata-of-pool': None,
                'sm-config': {
                    'vdi_type': 'vhd'
                }
            }
        }
        mock_session.xenapi.SR.get_VDIs.return_value = list(vdi_data.keys())

        def get_vdi_data(vdi_key, vdi_ref):
            return vdi_data[vdi_ref][vdi_key]

        def get_vdi_by_uuid(vdi_uuid):
            return [v for v in vdi_data if v['uuid'] == vdi_uuid][0]

        mock_session.xenapi.VDI.get_uuid.side_effect = (
            lambda x: get_vdi_data('uuid', x))
        mock_session.xenapi.VDI.get_name_label.side_effect = (
            lambda x: get_vdi_data('name_label', x))
        mock_session.xenapi.VDI.get_name_description.side_effect = (
            lambda x: get_vdi_data('name_description', x))
        mock_session.xenapi.VDI.get_is_a_snapshot.side_effect = (
            lambda x: get_vdi_data('is_a_snapshot', x))
        mock_session.xenapi.VDI.get_snapshot_time.side_effect = (
            lambda x: get_vdi_data('snapshot_time', x))
        mock_session.xenapi.VDI.get_type.side_effect = (
            lambda x: get_vdi_data('type', x))
        mock_session.xenapi.VDI.get_metadata_of_pool.side_effect = (
            lambda x: get_vdi_data('metadata-of-pool', x))
        mock_session.xenapi.VDI.get_sm_config.side_effect = (
            lambda x: get_vdi_data('sm-config', x))
        mock_session.xenapi.VDI.get_by_uuid.side_effect = get_vdi_by_uuid

        sr = self.create_LVHDSR(master=True, command='sr_attach',
                                sr_uuid=sr_uuid)
        os.makedirs(sr.path)

        # Act (1)
        # This introduces the metadata volume
        sr.attach(sr.uuid)

        # Arrange (2)
        sr = self.create_LVHDSR(master=True, command='sr_detach',
                                sr_uuid=sr_uuid)

        # Arrange for detach
        self.stubout('sm.drivers.LVHDSR.Fairlock')
        mock_remove_device = self.stubout(
            'sm.drivers.LVHDSR.lvutil.removeDevMapperEntry')
        mock_glob = self.stubout('glob.glob')
        mock_vdi_uuid = "72101dbd-bd62-4a14-a03c-afca8cceec86"
        mock_filepath = os.path.join(
            '/dev/mapper/', 'VG_XenStorage'
            f'--{sr_uuid.replace("-", "--")}-'
            f'{mock_vdi_uuid.replace("-", "--")}')
        mock_glob.return_value = [mock_filepath]
        mock_open_handles = self.stubout(
            'sm.drivers.LVHDSR.util.doesFileHaveOpenHandles')

        # Act (Detach)
        with self.assertRaises(Exception):
            # Fail the first one with busy handles
            mock_open_handles.return_value = True
            sr.detach(sr.uuid)

        # Now succeed
        mock_open_handles.return_value = False
        sr.detach(sr.uuid)

        # Assert for detach
        mock_remove_device.assert_called_once_with(mock_filepath, False)

        # Create new SR
        mock_lvm_cache.return_value.checkLV.return_value = True
        sr = self.create_LVHDSR(master=True, command='sr_attach',
                                sr_uuid=sr_uuid)

        # Act (2)
        # This syncs the already existing metadata volume
        print("Doing second attach")
        sr.attach(sr.uuid)

        # Now resize
        mock_cmd_lvm = self.stubout('sm.drivers.LVHDSR.lvutil.cmd_lvm')
        lvm_cmds = {
            "pvs": PV_FOR_VG_DATA,
            "pvresize": ""
        }
        def cmd(args):
            return lvm_cmds[args[0]]

        mock_cmd_lvm.side_effect = cmd
        mock_scsi_get_size.return_value = device_size + (2 * 1024 * 1024 * 1024)
        sr.scan(sr.uuid)

        # Find new VDI during scan
        extended_vdi_data = copy.deepcopy(vdi_data)
        extended_vdi_data.update({
            'vdi3_ref': {
                'uuid': str(uuid.uuid4()),
                'name_label': "VDI3",
                'name_description': "Third  VDI",
                'is_a_snapshot': False,
                'snapshot_of': None,
                'snapshot_time': None,
                'type': 'User',
                'metadata-of-pool': None,
                'sm-config': {
                    'vdi_type': 'vhd'
                }
            }})
        with mock.patch('sm.drivers.LVHDSR.LVMMetadataHandler', autospec=True) as m, \
             mock.patch('sm.drivers.LVHDSR.vhdutil', autotspec=True) as v:
            m.return_value.getMetadata.return_value = [
                None, self.convert_vdi_to_meta(extended_vdi_data)]
            v._getVHDParentNoCheck.return_value = None
            sr.scan(sr.uuid)

            lvm_cache = mock_lvm_cache.return_value
            self.assertEqual(1, lvm_cache.activate.call_count)
            self.assertEqual(1, lvm_cache.deactivate.call_count)

        # Act (3)
        # This tests SR metadata updates
        sr.updateSRMetadata('thick')

        # Test that removing vdi_type on a vdi does crash properly
        del vdi_data['vdi2_ref']['sm-config']['vdi_type']
        with self.assertRaises(Exception):
            # Fail on vdi2_ref
            sr.updateSRMetadata('thick')

    def convert_vdi_to_meta(self, vdi_data):
        metadata = {}
        for item in vdi_data.items():
            metadata[item[0]] = {
                'uuid': item[1]['uuid'],
                'is_a_snapshot': item[1]['is_a_snapshot'],
                'snapshot_of': item[1]['snapshot_of'],
                'vdi_type': item[1]['sm-config']['vdi_type'],
                'name_label': item[1]['name_label'],
                'name_description': item[1]['name_description'],
                'type': item[1]['type'],
                'read_only': False,
                'managed': True,
            }
        return metadata

    @mock.patch('sm.drivers.LVHDSR.cleanup', autospec=True)
    @mock.patch('sm.drivers.LVHDSR.IPCFlag', autospec=True)
    @mock.patch('sm.drivers.LVHDSR.Lock', autospec=True)
    @mock.patch('sm.drivers.LVHDSR.lvhdutil.getLVInfo', autospec=True)
    @mock.patch('sm.drivers.LVHDSR.lvhdutil.getVDIInfo', autospec=True)
    @mock.patch('sm.drivers.LVHDSR.SR.XenAPI')
    @testlib.with_context
    def test_snapshotof_success(self,
                            context,
                            mock_xenapi,
                            mock_getVDIInfo,
                            mock_getLVInfo,
                            mock_lock,
                            mock_ipc,
                            mock_cleanup):

        def test_function(args, inp):
            assert args[1] == "read"
            return 0, TEST_VHD_HEADER, ""

        context.add_executable(VHD_UTIL, test_function)
        sr_uuid = str(uuid.uuid4())
        self.stubout('sm.drivers.LVHDSR.lvutil._checkVG', autospec=True)
        mock_lvm_cache = self.stubout('sm.drivers.LVHDSR.lvmcache.LVMCache')
        mock_get_vg_stats = self.stubout('sm.drivers.LVHDSR.lvutil._getVGstats', autospec=True)
        mock_scsi_get_size = self.stubout('sm.drivers.LVHDSR.scsiutil.getsize', autospec=True)
        mock_vhdutil_getAllVHDs = self.stubout('sm.drivers.LVHDSR.vhdutil.getAllVHDs', autospec=True)
        mock_sr_util_pathexists = self.stubout('sm.drivers.LVHDSR.util.pathexists', autospec=True)
        mock_sr_util_gen_uuid = self.stubout('sm.drivers.LVHDSR.util.gen_uuid', autospec=True)
        mock_cleanup.SR.TMP_RENAME_PREFIX = cleanup.SR.TMP_RENAME_PREFIX

        device_size = 100 * 1024 * 1024
        device_free = 10 * 1024 * 1024
        mock_get_vg_stats.return_value = {
            'physical_size': device_size,
            'physical_utilisation': device_free,
            'freespace': device_size - device_free}
        mock_scsi_get_size.return_value = device_size
        mock_lvm_cache.return_value.checkLV.return_value = False
        mock_lvm_cache.return_value.getSize.return_value = 10240

        mock_session = mock_xenapi.xapi_local.return_value
        mock_session.xenapi.SR.get_sm_config.return_value = {
            'allocation': 'thick',
            'use_vhd': 'true'
        }
        vdi_data = {
            'vdi1_ref': {
                'uuid': str(uuid.uuid4()),
                'name_label': "VDI1",
                'name_description': "First VDI",
                'is_a_snapshot': False,
                'snapshot_of': None,
                'snapshot_time': None,
                'type': 'User',
                'metadata-of-pool': None,
                'sm-config': {
                    'vdi_type': 'vhd'
                }
            }
        }
        metadata = {}

        def get_vdis(sr_ref):
            return list(vdi_data.keys())

        def get_vdi_data(vdi_key, vdi_ref):
            return vdi_data[vdi_ref][vdi_key]

        def get_vdi_by_uuid(vdi_uuid):
            return [v for v in vdi_data if vdi_data[v]['uuid'] == vdi_uuid][0]

        def db_introduce(uuid, label, description, sr_ref, ty, shareable, read_only, other_config, location, xenstore_data, sm_config, managed, size, utilisation, metadata_of_pool, is_a_snapshot, snapshot_time, snapshot_of, cbt_enabled):
            vdi_data.update({
                'vdi3_ref': {
                    'uuid': uuid,
                    'name_label': label,
                    'name_description': description,
                    'is_a_snapshot': is_a_snapshot,
                    'snapshot_of': snapshot_of,
                    'snapshot_time': snapshot_time,
                    'type': ty,
                    'metadata-of-pool': metadata_of_pool,
                    'sm-config': {
                        'vdi_type': 'vhd'
                    }
                }})
            return 'vdi3_ref'

        mock_session.xenapi.VDI.get_uuid.side_effect = (
            lambda x: get_vdi_data('uuid', x))
        mock_session.xenapi.VDI.get_name_label.side_effect = (
            lambda x: get_vdi_data('name_label', x))
        mock_session.xenapi.VDI.get_name_description.side_effect = (
            lambda x: get_vdi_data('name_description', x))
        mock_session.xenapi.VDI.get_is_a_snapshot.side_effect = (
            lambda x: get_vdi_data('is_a_snapshot', x))
        mock_session.xenapi.VDI.get_snapshot_of.side_effect = (
            lambda x: get_vdi_data('snapshot_of', x))
        mock_session.xenapi.VDI.get_snapshot_time.side_effect = (
            lambda x: get_vdi_data('snapshot_time', x))
        mock_session.xenapi.VDI.get_type.side_effect = (
            lambda x: get_vdi_data('type', x))
        mock_session.xenapi.VDI.get_metadata_of_pool.side_effect = (
            lambda x: get_vdi_data('metadata-of-pool', x))
        mock_session.xenapi.VDI.get_sm_config.side_effect = (
            lambda x: get_vdi_data('sm-config', x))
        mock_session.xenapi.SR.get_VDIs.side_effect = get_vdis
        mock_session.xenapi.VDI.get_by_uuid.side_effect = get_vdi_by_uuid
        mock_session.xenapi.VDI.db_introduce.side_effect = db_introduce

        sr = self.create_LVHDSR(master=True, command='sr_attach',
                                sr_uuid=sr_uuid,
                                extra_params={'driver_params': {'type': 'double'}, 'vdi_ref': 'vdi1_ref'})
        os.makedirs(sr.path)

        # Act (1)
        # This introduces the metadata volume
        sr.attach(sr.uuid)

        # Create and check snapshot in metadata
        vdis_info = {}
        def addVdi(vdi_info):
            uuid = vdi_info['uuid']
            metadata[uuid] = {
                'uuid': uuid,
                'is_a_snapshot': vdi_info['is_a_snapshot'],
                'snapshot_of': vdi_info['snapshot_of'],
                'vdi_type':  vdi_info['vdi_type'],
                'name_label': vdi_info['name_label'],
                'name_description': vdi_info['name_description'],
                'type': vdi_info['type'],
                'read_only':  False,
                'managed': True
                }
            vdi_data['vdi3_ref']['snapshot_of'] = 'vdi3_ref'
            vdi_data['vdi3_ref']['is_a_snapshot'] = vdi_info['is_a_snapshot']
            vdis_info.update({uuid: lvhdutil.VDIInfo(uuid)})

        def write_metadata(sr_info, vdi_info):
            for item in vdi_info.items():
                metadata[item[0]] = {
                    'uuid': item[1]['uuid'],
                    'is_a_snapshot': item[1]['is_a_snapshot'],
                    'snapshot_of': item[1]['snapshot_of'],
                    'vdi_type': item[1]['vdi_type'],
                    'name_label': item[1]['name_label'],
                    'name_description': item[1]['name_description'],
                    'type': item[1]['type'],
                    'read_only': False,
                    'managed': True,
                }
            return metadata

        mock_metadata = self.stubout('sm.drivers.LVHDSR.LVMMetadataHandler')
        mock_metadata.return_value.addVdi.side_effect = addVdi
        mock_metadata.return_value.writeMetadata.side_effect = write_metadata

        self.stubout('sm.journaler.Journaler.create')
        self.stubout('sm.journaler.Journaler.remove')
        self.stubout('sm.drivers.LVHDSR.RefCounter.set')
        self.stubout('sm.drivers.LVHDSR.RefCounter.put')

        vdi_uuid = get_vdi_data('uuid', 'vdi1_ref')

        for vdi_meta in vdi_data.values():
            vdis_info.update({vdi_meta['uuid']: lvhdutil.VDIInfo(vdi_meta['uuid'])})
        mock_getVDIInfo.return_value = vdis_info

        mock_lv = lvutil.LVInfo('test-lv')
        mock_lv.size = 10240
        mock_lv.active = True
        mock_lv.hidden = False
        mock_lv.vdiType = vhdutil.VDI_TYPE_VHD

        mock_getLVInfo.return_value = {vdi_uuid: mock_lv}

        vhdInfo = vhdutil.VHDInfo(vdi_uuid)
        vhdInfo.hidden = False

        mock_vhdutil = self.stubout('sm.drivers.LVHDSR.vhdutil', autospec=True)
        mock_vhdutil.VDI_TYPE_VHD = vhdutil.VDI_TYPE_VHD
        mock_vhdutil.VDI_TYPE_RAW = vhdutil.VDI_TYPE_RAW
        mock_vhdutil.MAX_CHAIN_SIZE = vhdutil.MAX_CHAIN_SIZE
        mock_vhdutil.getVHDInfo.return_value = vhdInfo

        mock_vhdutil_getAllVHDs.return_value = {vhdInfo.uuid: vhdInfo}

        vdi = sr.vdi(vdi_uuid)
        vdi.vdi_type = vhdutil.VDI_TYPE_VHD
        mock_sr_util_pathexists.return_value = True
        def gen_uuid():
            return str(uuid.uuid4())
        mock_sr_util_gen_uuid.side_effect = gen_uuid
        mock_vhdutil.getDepth.return_value = 1

        snap = vdi.snapshot(sr.uuid, vdi_uuid)
        snapshot_of = metadata[get_vdi_data('uuid', 'vdi3_ref')]['snapshot_of']
        self.assertEqual(snapshot_of.startswith('OpaqueRef:'), False)

        # Update SR metadata and recheck snapshot field
        metadata = {}
        sr.updateSRMetadata('thick')
        snapshot_of = metadata[get_vdi_data('uuid', 'vdi3_ref')]['snapshot_of']
        self.assertEqual(snapshot_of.startswith('OpaqueRef:'), False)

class TestLVHDVDI(unittest.TestCase, Stubs):

    def setUp(self):
        self.init_stubs()

        lvhdutil_patcher = mock.patch('sm.drivers.LVHDSR.lvhdutil', autospec=True)
        self.mock_lvhdutil = lvhdutil_patcher.start()
        self.mock_lvhdutil.VG_LOCATION = lvhdutil.VG_LOCATION
        self.mock_lvhdutil.VG_PREFIX = lvhdutil.VG_PREFIX
        self.mock_lvhdutil.LV_PREFIX = lvhdutil.LV_PREFIX
        vhdutil_patcher = mock.patch('sm.drivers.LVHDSR.vhdutil', autospec=True)
        self.mock_vhdutil = vhdutil_patcher.start()
        self.mock_vhdutil.getBlockSize.return_value = vhdutil.DEFAULT_VHD_BLOCK_SIZE
        self.mock_vhdutil.VDI_TYPE_VHD = vhdutil.VDI_TYPE_VHD
        self.mock_vhdutil.VDI_TYPE_RAW = vhdutil.VDI_TYPE_RAW
        self.mock_vhdutil.MAX_CHAIN_SIZE = vhdutil.MAX_CHAIN_SIZE
        vdi_vhdutil_patcher = mock.patch('sm.VDI.vhdutil', autospec=True)
        self.mock_vdi_vhdutil = vdi_vhdutil_patcher.start()
        self.mock_vdi_vhdutil.getBlockSize.return_value = vhdutil.DEFAULT_VHD_BLOCK_SIZE
        lvutil_patcher = mock.patch('sm.drivers.LVHDSR.lvutil', autospec=True)
        self.mock_lvutil = lvutil_patcher.start()
        vdi_util_patcher = mock.patch('sm.VDI.util', autospec=True)
        self.mock_vdi_util = vdi_util_patcher.start()
        sr_util_patcher = mock.patch('sm.drivers.LVHDSR.util', autospec=True)
        self.mock_sr_util = sr_util_patcher.start()
        self.mock_sr_util.gen_uuid.side_effect = str(uuid.uuid4())
        xmlrpclib_patcher = mock.patch('sm.VDI.xmlrpc.client', autospec=True)
        self.mock_xmlrpclib = xmlrpclib_patcher.start()
        cbtutil_patcher = mock.patch('sm.VDI.cbtutil', autospec=True)
        self.mock_cbtutil = cbtutil_patcher.start()

        self.stubout('sm.drivers.LVHDSR.lvmcache.LVMCache')
        self.stubout('sm.drivers.LVHDSR.LVHDSR._ensureSpaceAvailable')
        self.stubout('sm.journaler.Journaler.create')
        self.stubout('sm.journaler.Journaler.remove')
        self.stubout('sm.drivers.LVHDSR.RefCounter.set')
        self.stubout('sm.drivers.LVHDSR.RefCounter.put')
        self.stubout('sm.drivers.LVHDSR.LVMMetadataHandler')

        self.addCleanup(mock.patch.stopall)

    def tearDown(self):
        self.remove_stubs()

    def create_LVHDSR(self):
        srcmd = mock.Mock()
        srcmd.dconf = {'device': '/dev/bar'}
        srcmd.params = {'command': 'foo', 'session_ref': 'some session ref'}
        return LVHDSR.LVHDSR(srcmd, "some SR UUID")

    def get_dummy_vdi(self, vdi_uuid):
        self.mock_lvhdutil.getVDIInfo.return_value = {
            vdi_uuid: lvhdutil.VDIInfo(vdi_uuid)}

        mock_lv =  lvutil.LVInfo('test-lv')
        mock_lv.size = 10240
        mock_lv.active = True
        mock_lv.hidden = False
        mock_lv.vdiType = vhdutil.VDI_TYPE_VHD

        self.mock_lvhdutil.getLVInfo.return_value = {
            vdi_uuid: mock_lv}

        return mock_lv

    def get_dummy_vhd(self, vdi_uuid, hidden):
        test_vhdInfo = vhdutil.VHDInfo(vdi_uuid)
        test_vhdInfo.hidden = hidden
        self.mock_vhdutil.getVHDInfo.return_value = test_vhdInfo

    @mock.patch('sm.drivers.LVHDSR.Lock', autospec=True)
    @mock.patch('sm.drivers.LVHDSR.SR.XenAPI')
    def test_clone_success(self, mock_xenapi, mock_lock):
        """
        Successfully create clone
        """

        # Arrange
        xapi_session = mock_xenapi.xapi_local.return_value
        xapi_session.xenapi.VDI.get_sm_config.return_value = {}
        vdi_uuid = 'some VDI UUID'
        mock_lv = self.get_dummy_vdi(vdi_uuid)
        self.get_dummy_vhd(vdi_uuid, False)

        sr = self.create_LVHDSR()
        sr.isMaster = True
        sr.legacyMode = False
        sr.srcmd.params = {'vdi_ref': 'test ref'}

        vdi = sr.vdi('some VDI UUID')
        self.mock_sr_util.pathexists.return_value = True

        self.mock_vhdutil.getDepth.return_value = 1

        # Act
        clone = vdi.clone(sr.uuid, 'some VDI UUID')

        # Assert
        self.assertIsNotNone(clone)

    @mock.patch('sm.drivers.LVHDSR.Lock', autospec=True)
    @mock.patch('sm.drivers.LVHDSR.SR.XenAPI')
    def test_snapshot_attached_success(self, mock_xenapi, mock_lock):
        """
        LVHDSR.snapshot, attached on host, no CBT
        """
        # Arrange
        xapi_session = mock_xenapi.xapi_local.return_value
        xapi_session.xenapi.VDI.get_sm_config.return_value = {}

        vdi_uuid = 'some VDI UUID'
        mock_lv = self.get_dummy_vdi(vdi_uuid)
        self.get_dummy_vhd(vdi_uuid, False)

        sr = self.create_LVHDSR()
        sr.isMaster = True
        sr.legacyMode = False
        sr.srcmd.params = {
            'vdi_ref': 'test ref',
            'driver_params': {
                'type': 'double'}
            }
        sr.cmd = "vdi_snapshot"

        vdi = sr.vdi('some VDI UUID')
        vdi.vdi_type = vhdutil.VDI_TYPE_VHD
        self.mock_sr_util.pathexists.return_value = True
        self.mock_sr_util.get_hosts_attached_on.return_value = ["hostref2"]
        self.mock_sr_util.get_this_host_ref.return_value = ["hostref1"]
        self.mock_vhdutil.getDepth.return_value = 1

        # Act
        snap = vdi.snapshot(sr.uuid, "Dummy UUID")

        # Assert
        self.assertIsNotNone(snap)

    @mock.patch('sm.drivers.LVHDSR.Lock', autospec=True)
    @mock.patch('sm.drivers.LVHDSR.SR.XenAPI')
    def test_snapshot_attached_cbt_success(self, mock_xenapi, mock_lock):
        """
        LVHDSR.snapshot, attached on host, with CBT
        """
        # Arrange
        xapi_session = mock_xenapi.xapi_local.return_value
        xapi_session.xenapi.VDI.get_sm_config.return_value = {}

        vdi_uuid = 'some VDI UUID'
        mock_lv = self.get_dummy_vdi(vdi_uuid)
        self.get_dummy_vhd(vdi_uuid, False)

        sr = self.create_LVHDSR()
        sr.isMaster = True
        sr.legacyMode = False
        sr.srcmd.params = {
            'vdi_ref': 'test ref',
            'driver_params': {
                'type': 'double'}
            }
        sr.cmd = "vdi_snapshot"

        vdi = sr.vdi('some VDI UUID')
        vdi.vdi_type = vhdutil.VDI_TYPE_VHD
        self.mock_sr_util.pathexists.return_value = True
        self.mock_sr_util.get_hosts_attached_on.return_value = ["hostref2"]
        self.mock_sr_util.get_this_host_ref.return_value = ["hostref1"]
        self.mock_vdi_util.sr_get_capability.return_value = {
            'VDI_CONFIG_CBT'}
        self.mock_vhdutil.getDepth.return_value = 1

        # Act
        with mock.patch('sm.VDI.Lock'):
            snap = vdi.snapshot(sr.uuid, "Dummy UUID")

        # Assert
        self.assertIsNotNone(snap)
        self.assertEqual(self.mock_cbtutil.set_cbt_child.call_count, 3)

    @mock.patch('sm.drivers.LVHDSR.Lock', autospec=True)
    @mock.patch('sm.drivers.LVHDSR.SR.XenAPI')
    def test_update_slaves_on_cbt_disable(self, mock_xenapi, mock_lock):
        """
        Ensure we tell the supporter host when we disable CBT for one of its VMs
        """
        # Arrange
        xapi_session = mock_xenapi.xapi_local.return_value

        vdi_uuid = str(uuid.uuid4)
        mock_lv = self.get_dummy_vdi(vdi_uuid)
        self.get_dummy_vhd(vdi_uuid, False)

        sr = self.create_LVHDSR()
        sr.isMaster = True

        vdi = sr.vdi(vdi_uuid)
        vdi.vdi_type = vhdutil.VDI_TYPE_VHD

        self.mock_sr_util.get_this_host_ref.return_value = 'ref1'
        self.mock_sr_util.get_hosts_attached_on.return_value = ['ref2']

        # Act
        log_file_path = "test_log_path"
        vdi.update_slaves_on_cbt_disable(log_file_path)

        # Assert
        self.assertEqual(1, xapi_session.xenapi.host.call_plugin.call_count)
        xapi_session.xenapi.host.call_plugin.assert_has_calls([
            mock.call('ref2', 'on-slave', 'multi', mock.ANY)
        ])

    @mock.patch('sm.drivers.LVHDSR.Lock', autospec=True)
    @mock.patch('sm.drivers.LVHDSR.SR.XenAPI')
    def test_snapshot_secondary_success(self, mock_xenapi, mock_lock):
        """
        LVHDSR.snapshot, attached on host with secondary mirror
        """
        # Arrange
        xapi_session = mock_xenapi.xapi_local.return_value
        xapi_session.xenapi.VDI.get_sm_config.return_value = {}

        vdi_ref = mock.MagicMock()
        xapi_session.xenapi.VDI.get_by_uuid.return_value = vdi_ref
        vdi_uuid = 'some VDI UUID'
        self.get_dummy_vdi(vdi_uuid)
        self.get_dummy_vhd(vdi_uuid, False)

        sr = self.create_LVHDSR()
        sr.isMaster = True
        sr.legacyMode = False
        sr.srcmd.params = {
            'vdi_ref': 'test ref',
            'driver_params': {
                'type': 'double',
                'mirror': 'nbd:mirror_vbd/5/xvda'}
            }
        sr.cmd = "vdi_snapshot"

        vdi = sr.vdi('some VDI UUID')
        vdi.vdi_type = vhdutil.VDI_TYPE_VHD
        self.mock_sr_util.pathexists.return_value = True
        self.mock_sr_util.get_hosts_attached_on.return_value = ["hostref2"]
        self.mock_sr_util.get_this_host_ref.return_value = ["hostref1"]
        self.mock_vhdutil.getDepth.return_value = 1

        # Act
        with mock.patch('sm.core.lock.Lock'):
            snap = vdi.snapshot(sr.uuid, "Dummy UUID")

        # Assert
        self.assertIsNotNone(snap)
        xapi_session.xenapi.VDI.add_to_other_config.assert_called_once_with(
            vdi_ref, cleanup.VDI.DB_LEAFCLSC, cleanup.VDI.LEAFCLSC_DISABLED)
