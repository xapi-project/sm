import os
import socket
import inspect
import time

SOCKDIR = "/run/fairlock"
START_SERVICE_TIMEOUT_SECS = 2

class SingletonWithArgs(type):
    _instances = {}
    _init = {}

    def __init__(cls, name, bases, dct):
        cls._init[cls] = dct.get('__init__', None)

    def __call__(cls, *args, **kwargs):
        init = cls._init[cls]
        if init is not None:
            key = (cls, frozenset(
                    inspect.getcallargs(init, None, *args, **kwargs).items()))
        else:
            key = cls

        if key not in cls._instances:
            cls._instances[key] = super(SingletonWithArgs, cls).__call__(*args, **kwargs)
        return cls._instances[key]

class FairlockDeadlock(Exception):
    pass

class FairlockServiceTimeout(Exception):
    pass

class Fairlock(metaclass=SingletonWithArgs):
    def __init__(self, name):
        self.name = name
        self.sockname = os.path.join(SOCKDIR, name)
        self.connected = False
        self.sock = None

    def _ensure_service(self):
        service=f"fairlock@{self.name}.service"
        os.system(f"/usr/bin/systemctl start {service}")
        timeout = time.time() + START_SERVICE_TIMEOUT_SECS
        time.sleep(0.1)
        while os.system(f"/usr/bin/systemctl --quiet is-active {service}") != 0:
            time.sleep(0.1)
            if time.time() > timeout:
                raise FairlockServiceTimeout(f"Timed out starting service {service}")

    def __enter__(self):
        if self.connected:
            raise FairlockDeadlock(f"Deadlock on Fairlock resource '{self.name}'")

        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.setblocking(True)
        try:
            self.sock.connect(self.sockname)
            # Merely being connected is not enough. Read a small blob of data.
            self.sock.recv(10)
        except (FileNotFoundError, ConnectionRefusedError):
            self._ensure_service()
            self.sock.connect(self.sockname)
            # Merely being connected is not enough. Read a small blob of data.
            self.sock.recv(10)

        self.sock.send(f'{os.getpid()} - {time.monotonic()}'.encode())
        self.connected = True
        return self

    def __exit__(self, type, value, traceback):
        self.sock.close()
        self.sock = None
        self.connected = False
        return False

