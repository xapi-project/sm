import builtins
import copy
import os
import sys
import unittest
import unittest.mock as mock

import lock
import lock_queue

## Instead of saving the process queue to disk the mocks will save it here.
## It needs to be global because it is shared between threads.
saved_queue = []

def mock_pickle_dump_fn(*args):
   global saved_queue
   saved_queue = copy.deepcopy(args[0])

def mock_pickle_load_fn(*args):
   global saved_queue
   return copy.deepcopy(saved_queue)


class Test_LockQueue(unittest.TestCase):
    def setUp(self):
        # Re-initialize queue to empty for each test
        global saved_queue
        saved_queue = []

    def get_lock_name(self):
        return "bacon"

    @mock.patch('lock_queue.pickle.load', side_effect=mock_pickle_load_fn)
    @mock.patch('lock_queue.pickle.dump', side_effect=mock_pickle_dump_fn)
    @mock.patch('lock_queue.os.getpid')
    @mock.patch('lock_queue.get_process_start_time')
    @mock.patch('lock.Lock', autospec=False)
    def test_push_to_queue_3x(self, lock, start_time, getpid, pdump, pload):
        global saved_queue

        lq = lock_queue.LockQueue(self.get_lock_name())
        # Push to queue 3 times using these PID and Start Time combinations
        test_pids = [997, 993, 996]
        test_sts = [360, 430, 458]
        for p, s in zip(test_pids, test_sts):
            start_time.return_value = s
            getpid.return_value = p
            lq.push_into_process_queue()

        # Test the queue includes the PID and Start Time pairs in the order we expect
        self.assertEqual(list(zip(test_pids, test_sts)), saved_queue)

    @mock.patch('lock_queue.pickle.load', side_effect=mock_pickle_load_fn)
    @mock.patch('lock_queue.pickle.dump', side_effect=mock_pickle_dump_fn)
    @mock.patch('lock_queue.os.getpid')
    @mock.patch('lock_queue.get_process_start_time')
    @mock.patch('lock.Lock', autospec=False)
    def test_context_manager(self, lock, start_time, getpid, pdump, pload):
        global saved_queue

        getpid.return_value = 959
        start_time.return_value = 575

        # Queue is empty
        self.assertEqual(saved_queue, [])

        with lock_queue.LockQueue(self.get_lock_name()) as lq:
            # Should have removed from the queue before completing entry to the context manager
            self.assertEqual(saved_queue, [])

    @mock.patch('lock_queue.pickle.load', side_effect=mock_pickle_load_fn)
    @mock.patch('lock_queue.pickle.dump', side_effect=mock_pickle_dump_fn)
    @mock.patch('lock_queue.os.getpid')
    @mock.patch('lock_queue.get_process_start_time')
    @mock.patch('lock.Lock', autospec=False)
    def test_context_manager_bad_entry(self, lock, start_time, getpid, pdump, pload):
        global saved_queue

        # Initialise saved_queue with non-existent pid
        saved_queue = [(0, 67867)]

        getpid.return_value = 959
        start_time.return_value = 575
        with lock_queue.LockQueue(self.get_lock_name()) as lq:
            # Should have removed from the queue before completing entry to the context manager
            self.assertEqual(saved_queue, [])

