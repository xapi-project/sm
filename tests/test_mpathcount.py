"""
Unit tests for mpathcount
"""
import errno
import os
import unittest
import mock
import testlib
import mpathcount
import SR
import util

# pylint: disable=W0613; mocks don't need to be accessed
# pylint: disable=R0201; methods must be instance for nose to work
# pylint: disable=W0212; unit tests are permitted to snoop
class TestMpathCount(unittest.TestCase):
    """
    Unit tests for mpathcount
    """

    @mock.patch('mpathcount.os', autospec=True)
    def test_get_root_dev_major(self, mock_os):
        mpathcount.get_root_dev_major()
        assert(mock_os.major.called)

    @mock.patch('mpathcount.mpath_cli', autospec=True)
    def test_get_path_count(self, mpath_cli):
        mpath_cli.get_topology.return_value = [
            "multipathd> show map 3600a098038303973743f486833396d44 topology",
            "3600a098038303973743f486833396d44 dm-1 NETAPP  ,LUN C-Mode",
            "size=200G features='4 queue_if_no_path pg_init_retries 50 retain_attached_hw_handle' hwhandler='1 alua' wp=rw",
            "`-+- policy='service-time 0' prio=50 status=active",
            "  |- 0:0:0:4  sdr  65:16  active ready  running",
            "  |- 0:0:1:4  sdg  8:96   active ready  running",
            "  |- 7:0:0:4  sdab 65:176 failed faulty running",
            "  `- 7:0:1:4  sdam 66:96  failed faulty running"
            ]
        count, total = mpathcount.get_path_count('3600a098038303973743f486833396d44')
        self.assertEqual(4, total, msg='total count incorrect')
        self.assertEqual(2, count, msg='count count incorrect')

    @mock.patch('mpathcount.get_path_count', return_value=(2,4))
    def test_update_config(self, get_path_count):
        store={'fred': ''}
        def remove(key):
            if key in store:
                del store[key]

        def add(key, val):
            store[key] = val

        ## Point this to a place where SCSIid files either do or
        ## don't exist, to exercise different branches
        mpathcount.MAPPER_DIR = "test_support/fake_mapper"

        mpathcount.update_config("fred", "3600a098038303973743f486833396d44", "[2, 4]", remove, add, True)
        self.assertIn('MPPEnabled', store)
        self.assertIn('multipathed', store)
        self.assertEqual('[2, 4]', store['fred'], msg="Store value incorrect for key 'fred'")

        store={'fred': ''}
        mpathcount.update_config("fred", "3600a098038303973743f486833396d44", "[2, hamster]", remove, add, False)
        self.assertIn('multipathed', store)
        self.assertEqual('[2, 4]', store['fred'], msg="Store value incorrect for key 'fred'")

        store={'fred': ''}
        mpathcount.update_config("fred", "3600a098038303973743f486833396d44", "[2, 2]", remove, add, False)
        self.assertIn('multipathed', store)
        self.assertEqual('[2, 4]', store['fred'], msg="Store value incorrect for key 'fred'")

        store={'fred': ''}
        mpathcount.update_config("fred", "3600a098038303973743f486833396d44", "", remove, add, False)
        self.assertIn('multipathed', store)
        self.assertEqual('[2, 4]', store['fred'], msg="Store value incorrect for key 'fred'")

        store={'fred': ''}
        mpathcount.update_config("fred", "NotARealItem", "", remove, add, False)
        self.assertNotIn('multipathed', store)
        self.assertNotIn('fred', store)

    @mock.patch('mpathcount.get_dm_major')
    @mock.patch('mpathcount.get_root_dev_major')
    @mock.patch('mpathcount.update_config', autospec=True)
    def test_check_root_disk(self, update_config, get_root_dev_major, get_dm_major):
        store={}
        def fake_update_config(k, s, v, a, t):
            store[k] = v

        get_root_dev_major.return_value = 4
        get_dm_major.return_value = 4
        update_config.side_effect = fake_update_config
        mpathcount.match_bySCSIid = False
        maps = ["3600a098038303973743f486833396d44", 'name']
        mpathcount.check_root_disk({}, maps, None, None)
        self.assertIn('mpath-boot', store)

        mpathcount.match_bySCSIid = True
        mpathcount.SCSIid = "3600a098038303973743f486833396d44"
        maps = ["3600a098038303973743f486833396d44"]
        store={}
        mpathcount.check_root_disk({'mpath-boot': '[2, 4]'}, maps, None, None)
        self.assertIn('mpath-boot', store, msg="Key 'mpath-boot' not present in store")
        self.assertEqual('[2, 4]', store['mpath-boot'], msg="Store value incorrect for key 'mpath-boot'")

        get_root_dev_major.return_value = 4
        get_dm_major.return_value = 2
        mpathcount.match_bySCSIid = False
        maps = ["3600a098038303973743f486833396d44", 'name']
        store={}
        mpathcount.check_root_disk({}, maps, None, None)
        self.assertNotIn('mpath-boot', store)

    @mock.patch('mpathcount.update_config', autospec=True)
    def test_check_devconfig(self, update_config):
        store={}
        def remove(key):
            if key in store:
                print("del {}".format(key))
                del store[key]

        def fake_update_config(k, s, v, a, t):
            store[k] = v

        update_config.side_effect = fake_update_config

        store={}
        mpathcount.match_bySCSIid = False
        mpathcount.check_devconfig(
            {},
            {},
            {'mpath-3600a098038303973743f486833396d40': '[2, 4]'},
            remove, None)
        self.assertNotIn('mpath-3600a098038303973743f486833396d40', store)

        store={}
        mpathcount.match_bySCSIid = False
        mpathcount.check_devconfig(
            {},
            {'SCSIid': '3600a098038303973743f486833396d40,3600a098038303973743f486833396d41'},
            {'mpath-3600a098038303973743f486833396d40': '[2, 4]'},
            remove, None)
        self.assertIn('mpath-3600a098038303973743f486833396d40', store)
        self.assertIn('mpath-3600a098038303973743f486833396d41', store)
        self.assertEqual('[2, 4]', store['mpath-3600a098038303973743f486833396d40'],
                         msg="Store value incorrect for key 'mpath-3600a098038303973743f486833396d40'")
        self.assertEqual('', store['mpath-3600a098038303973743f486833396d41'],
                         msg="Store value incorrect for key 'mpath-3600a098038303973743f486833396d41'")

        store={}
        mpathcount.match_bySCSIid = False
        mpathcount.check_devconfig(
            {'SCSIid': '3600a098038303973743f486833396d40'},
            {},
            {'mpath-3600a098038303973743f486833396d40': '[2, 4]'},
            remove, None)
        self.assertIn('mpath-3600a098038303973743f486833396d40', store)
        self.assertEqual('[2, 4]', store['mpath-3600a098038303973743f486833396d40'],
                         msg="Store value incorrect for key 'mpath-3600a098038303973743f486833396d40'")

        store={}
        mpathcount.match_bySCSIid = False
        mpathcount.check_devconfig(
            {'provider': 'present', 'ScsiId': '3600a098038303973743f486833396d40'},
            {},
            {'mpath-3600a098038303973743f486833396d40': '[2, 4]'},
            remove, None)
        self.assertIn('mpath-3600a098038303973743f486833396d40', store)
        self.assertEqual('[2, 4]', store['mpath-3600a098038303973743f486833396d40'],
                         msg="Store value incorrect for key 'mpath-3600a098038303973743f486833396d40'")

        store={
            'mpath-3600a098038303973743f486833396d40': '[2, 4]',
            'multipathed': True
            }
        mpathcount.match_bySCSIid = False
        mpathcount.mpath_enabled = False
        mpathcount.check_devconfig(
            {},
            {'SCSIid': '3600a098038303973743f486833396d40,3600a098038303973743f486833396d41'},
            {'mpath-3600a098038303973743f486833396d40': '[2, 4]'},
            remove, None)
        self.assertNotIn('multipathed', store)
        self.assertNotIn('mpath-3600a098038303973743f486833396d40', store)

