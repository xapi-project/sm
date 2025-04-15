import unittest

from sm.blktap2 import Tapdisk
from lcache import CachingTap, LeafCachingTap

class TestLcacheCachingTap(unittest.TestCase):

    def test_from_tapdisk_nbd(self):
        test_tapdisk = Tapdisk(
            234567, 1, 'vhd',
            'vhd:/var/run/sr-mount/21a94b1f-0909-60ee-d838-cad8a90b801d/ae34b288-dd2e-4dbc-899d-f14f21b110b2.vhdcache',
            0)

        stats =  {
            'name': 'vhd:/var/run/sr-mount/21a94b1f-0909-60ee-d838-cad8a90b801d/ae34b288-dd2e-4dbc-899d-f14f21b110b2.vhdcache',
            'secs': [62231944, 76640972],
            'images': [
                {
                    'name': '/var/run/sr-mount/db04b2bb-6541-5da1-9299-0308a1b2daee/ae34b288-dd2e-4dbc-899d-f14f21b110b2.vhd',
                    'hits': [36782760, 75356313],
                    'fail': [0, 0],
                    'driver': {
                        'type': 4, 'name': 'vhd', 'status': None}
                },
                {
                    'name': '/var/run/sr-mount/21a94b1f-0909-60ee-d838-cad8a90b801d/ae34b288-dd2e-4dbc-899d-f14f21b110b2.vhdcache',
                    'hits': [1826692, 2844259],
                    'fail': [0, 0],
                    'driver': {
                        'type': 4, 'name': 'vhd', 'status': None}
                },
                {
                    'name': '/var/run/blktap-control/nbd132558.0',
                    'hits': [23622492, 0],
                    'fail': [0, 0],
                    'driver': {
                        'type': 15, 'name': 'nbd', 'status': None}
                }
            ],
            'tap': {
                'minor': 1,
                'reqs': [0, 0],
                'kicks': [3, 0]
            },
            'xenbus': {
                'pool': 'td-xenio-default',
                'domid': 15,
                'devid': 768,
                'reqs': [2685748, 2685748],
                'kicks': [614279, 1510961],
                'errors': {
                    'msg': 0, 'map': 0, 'vbd': 0, 'img': 0}
            },
            'FIXME_enospc_redirect_count': 75356313,
            'nbd_mirror_failed': 0,
            'reqs_outstanding': 0,
            'read_caching': 'true'
        }

        cachingTap = CachingTap.from_tapdisk(test_tapdisk, stats)

        self.assertIsNotNone(cachingTap)
        self.assertIsInstance(cachingTap, LeafCachingTap)
        self.assertEqual(0, cachingTap.parent_minor)
