# Copyright (C) Cloud Software Group, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published
# by the Free Software Foundation; version 2.1 only.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

import os
import pickle
import sys
import time

import lock
from sm.core import util


DEBUG_LOG = True

def debug_log(msg):
    if DEBUG_LOG:
        util.SMlog("LockQueue: " + msg)

def get_process_start_time(pid):
    proc_file = f"/proc/{pid}/stat"
    with open(proc_file, 'r') as f:
        return f.read().split(')')[-1].split(' ')[20]

def process_is_valid(pid, start_time):
    proc_file = f"/proc/{pid}/stat"

    try:
        if start_time != get_process_start_time(pid):
            debug_log(f"Process {pid} has incorrect start time real:{get_process_start_time(pid)} vs expected:{start_time}")
            return False
    except FileNotFoundError:
        debug_log(f"Process {pid} is dead")
        return False

    return True

class LockQueue:
    def __init__(self, name):
        self.name = name
        self._queue_lock = lock.Lock(name, f"ql-{name}")
        self._action_lock = lock.Lock(name, f"al-{name}")
        # Filename to hold the process queue
        self._mem = f"/tmp/mem-{name}"

    def load_queue(self):
        try:
            with open(self._mem, "rb") as f:
                queue = pickle.load(f)
            debug_log("load_queue {}".format(queue))
        except EOFError:
            queue = []
        except FileNotFoundError:
            queue = []
        return queue

    def save_queue(self, queue):
        with open(self._mem, "w+b") as f:
            pickle.dump(queue, f)
        debug_log("save_queue {}".format(queue))

    def push_into_process_queue(self):
        self._queue_lock.acquire()

        queue = self.load_queue()
        queue.append((os.getpid(), get_process_start_time(os.getpid())))
        self.save_queue(queue)

        self._queue_lock.release()

    def __enter__(self):
        # Add ourselves to the process queue.
        self.push_into_process_queue()

        # Keep reading the process queue until we are at the front
        while True:
            self._queue_lock.acquire()
            queue = self.load_queue()
            front_pid, front_start_time = queue.pop(0)
            debug_log(f"Testing for PID {front_pid}")
            if front_pid == os.getpid():
                # We are at the front, it is now our turn to wait on the action lock
                # and then do our work
                debug_log(f"{front_pid} taking action lock")
                self._action_lock.acquire()
                # When we have the action lock, save the queue (which no longer
                # includes us) and release the queue lock to let others join.
                self.save_queue(queue)
                self._queue_lock.release()
                break

            # Getting here means it was not our turn to do stuff
            # If the process at the front of the queue is not alive then remove it
            if not process_is_valid(front_pid, front_start_time):
                # front pid has already been popped from queue so just save it
                debug_log(f"Removing invalid process {front_pid}")
                self.save_queue(queue)
            # Release the lock and try again later. Most waiting will be on the queue lock,
            # waiting for the single Action lock waiter to release it when it has the action
            # lock. We sleep a short while before our next check to make it easier for new
            # waiters to join the queue without really wasting our own time.
            self._queue_lock.release()
            time.sleep(0.1)

        debug_log("In manager")
        return self

    def __exit__(self, type, value, tbck):
        self._action_lock.release()
