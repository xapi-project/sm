"""
Test the "fake" keymanager for testing VHD encryption
"""
import base64
import copy
import io
import json
import unittest
import unittest.mock as mock


import plugins.keymanagerutil as keymanagerutil

from sm.core import util


class TestKeymanagerutil(unittest.TestCase):

    def setUp(self):
        self.addCleanup(mock.patch.stopall)

        log_patcher = mock.patch('plugins.keymanagerutil.util.SMlog', autospec=True)
        self.mock_log = log_patcher.start()
        self.mock_log.side_effect = self.log

        open_patcher = mock.patch('builtins.open', autospec=True)
        self.mock_open = open_patcher.start()
        self.mock_open.side_effect = self.open

        self.files = {}

        isfile_patcher = mock.patch(
            'plugins.keymanagerutil.os.path.isfile', autospec=True)
        self.mock_isfile = isfile_patcher.start()

        json_patcher = mock.patch('plugins.keymanagerutil.json', autospec=True)
        self.mock_json = json_patcher.start()

    def log(self, message, ident="SM", priority=util.LOG_INFO):
        print(f"{priority}:{ident}: {message}")

    def open(self, file, mode='r'):
        if mode == 'r':
            # Reading
            mock_file = mock.MagicMock(spec=io.TextIOBase, name=file)
            mock_file.__enter__ = mock_file
            mock_file.__exit__ = lambda x, y, z, a: None
            return mock_file

        mock_file = mock.MagicMock(spec=io.TextIOBase, name=file)
        mock_file.__enter__ = mock_file
        mock_file.__exit__ = lambda x, y, z, a: None

        return mock_file

    def test_generatekey_strong(self):
        # Arrange
        self.mock_isfile.return_value = False
        file_buffer = io.StringIO()
        self.files['/tmp/keystore.json'] = file_buffer

        # Act
        key_man = keymanagerutil.KeyManager(key_type='strong')
        key_man.generate()

        # Assert
        generated_keystore = self.mock_json.dump.call_args[0][0]
        print(f"Generated {generated_keystore}")
        self.assertEqual(1, len(generated_keystore.keys()))

        # Retrieve key
        self.mock_isfile.return_value = True
        self.mock_json.load.return_value = copy.deepcopy(generated_keystore)
        for key_hash in generated_keystore:
            key = keymanagerutil.KeyManager(key_hash=key_hash).get_key(log_key_info=False)

            self.assertNotEqual(0, len(key))

        # Retrieve key_hash
        self.mock_isfile.return_value = True
        self.mock_json.load.return_value = copy.deepcopy(generated_keystore)
        self.mock_json.dumps = json.dumps
        for key in generated_keystore.values():
            self.mock_log.reset_mock()
            key = base64.b64decode(key)
            keymanagerutil.KeyManager(key=key).get_keyhash()

    def test_get_key_no_hash(self):
        # Act
        key_man = keymanagerutil.KeyManager()
        with self.assertRaises(keymanagerutil.InputError):
            key_man.get_key()

    def test_get_key_unmatched(self):
        # Arrange
        self.mock_json.load.return_value = {}

        key_man = keymanagerutil.KeyManager(key_hash='missing_hash')

        with self.assertRaises(keymanagerutil.KeyLookUpError):
            key_man.get_key()
