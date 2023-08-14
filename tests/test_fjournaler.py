from io import BytesIO as StringIO
import os
import unittest
import unittest.mock as mock

import fjournaler

TEST_DIR_PATH = '/var/lib/sometest'


class FakeFile(object):

    def __init__(self):
        self.content = b''
        self.data = None

    def open(self):
        print('Opening fake file, content = {}'.format(self.content))
        self.data = StringIO(self.content)
        return self

    def close(self):
        self.content = self.data.getvalue()
        self.data.close()
        self.data = None

    def write(self, val):
        self.data.write(str.encode(val))

    def readline(self):
        return self.data.readline()


class TestFjournaler(unittest.TestCase):

    def setUp(self):
        self.files = {}
        self.open_handlers = {TEST_DIR_PATH: self.__fake_open}

        self.subject = fjournaler.Journaler(TEST_DIR_PATH)

        self.real_open = open
        open_patcher = mock.patch("builtins.open", autospec=True)
        self.mock_open = open_patcher.start()
        self.mock_open.side_effect = self.__open_selector

        exists_patcher = mock.patch('fjournaler.util.pathexists',
                                    autospec=True)
        self.mock_exists = exists_patcher.start()
        self.mock_exists.side_effect = self.__fake_exists

        unlink_patcher = mock.patch('fjournaler.os.unlink', autospec=True)
        self.mock_unlink = unlink_patcher.start()
        self.mock_unlink.side_effect = self.__fake_unlink

        listdir_patcher = mock.patch('fjournaler.os.listdir', autospec=True)
        self.mock_listdir = listdir_patcher.start()
        self.mock_listdir.side_effect = self.__fake_listdir

        self.addCleanup(mock.patch.stopall)

    def __fake_exists(self, path):
        return path in self.files

    def __open_selector(self, path, mode):
        handler = self.open_handlers.get(os.path.dirname(path), self.real_open)
        return handler(path, mode)

    def __fake_open(self, path, mode):
        if path not in self.files:
            mock_file = FakeFile()
            self.files[path] = mock_file
        return self.files[path].open()

    def __fake_unlink(self, path):
        del self.files[path]

    def __fake_listdir(self, path):
        assert(path == TEST_DIR_PATH)
        return [os.path.basename(x) for x in list(self.files.keys())]

    def test_non_existing(self):
        self.assertIsNone(self.subject.get('clone', '1'))

    def test_create_and_exists(self):
        self.subject.create('clone', '1', 'a')

        val = self.subject.get('clone', '1')
        self.assertEqual(b'a', val)

        self.subject.remove('clone', '1')

        self.assertIsNone(self.subject.get('clone', '1'))

    def test_create_mulitple(self):
        self.subject.create("modify", "X", "831_3")
        self.subject.create("modify", "Z", "831_4")
        self.subject.create("modify", "Y", "53_0")

        self.assertEqual(b"831_3", self.subject.get("modify", "X"),
                         msg="create underscore_val failed")
        self.assertEqual(b"53_0", self.subject.get("modify", "Y"),
                         msg="create multiple id's failed")

        entries = self.subject.getAll('modify')
        self.assertSetEqual({'X', 'Y', 'Z'}, set(entries.keys()))
        self.assertEqual(b"831_3", entries['X'])
        self.assertEqual(b"53_0", entries['Y'])

        # Check no extra returned
        self.subject.create('clone', '1', 'a')
        entries = self.subject.getAll('modify')
        self.assertSetEqual({'X', 'Y', 'Z'}, set(entries.keys()))

        self.subject.create('modify2', '1', 'z')
        entries = self.subject.getAll('modify')
        self.assertSetEqual({'X', 'Y', 'Z'}, set(entries.keys()))

        # getAll should not return empty value journals
        self.subject.create('modify', 'N', '')
        entries = self.subject.getAll('modify')
        self.assertSetEqual({'X', 'Y', 'Z'}, set(entries.keys()))

        # Remove X
        self.subject.remove('modify', 'X')
        entries = self.subject.getAll('modify')
        self.assertSetEqual({'Y', 'Z'}, set(entries.keys()))
        self.assertEqual(b"53_0", entries['Y'])

    def test_create_existing_error(self):
        self.subject.create('clone', '1', 'a')

        with self.assertRaises(fjournaler.JournalerException):
            self.subject.create('clone', '1', 'a')

    def test_remove_non_existing_error(self):
        with self.assertRaises(fjournaler.JournalerException):
            self.subject.remove('clone', '1')
