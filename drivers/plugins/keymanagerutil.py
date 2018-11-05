#!/usr/bin/python

"""
Helper module used for XenRT testing of the VDI encryption feature (REQ-718).
This module implements the key lookup plugin interface, so if it is
installed, SM will use it to retrieve keys based on their hashes.
This key store is backed by a file stored on disk in dom0, and helper
functions are provided to manipulate it.
"""

import base64
import os
import os.path
import hashlib
import json
import argparse


def load_key(key_hash):
    """
    load_key is called by SM plugin when it needs to find the key for
    specified key_hash from the key store
    """
    try:
        key = KeyManager(key_hash=key_hash).get_key(log_key_info=False)
        return key
    except Exception:
        return None


class InputError(Exception):
    def __init__(self, message):
        super(InputError, self).__init__(message)


class KeyLookUpError(Exception):
    def __init__(self, message):
        super(KeyLookUpError, self).__init__(message)


class Logger(object):
    def __init__(self, key=None, key_hash=None):
        self.key = key
        self.key_hash = key_hash

    def log_key_info(self):
        data = {}
        if self.key:
            data['key'] = repr(self.key)
            data['key_base64'] = base64.b64encode(self.key)
        if self.key_hash:
            data['key_hash'] = self.key_hash
        print(json.dumps(data))

    def log_message(self, message):
        print(message)


KEYSTORE_PATH = '/tmp/keystore.json'

def _read_keystore():
    """If the keystore file exists, returns its contents, otherwise returns an empty dictionary."""
    if os.path.isfile(KEYSTORE_PATH):
        with open(KEYSTORE_PATH, "r") as key_store_file:
            key_store = json.load(key_store_file)
            for key_hash in key_store:
                key_base64 = key_store[key_hash]
                key = base64.b64decode(key_base64)
                key_store[key_hash] = key
            return key_store
    else:
        return {}

def _write_keystore(key_store):
    """
    Write the given key store contents to the key store file, which will be
    created if it does not exist.
    """
    for key_hash in key_store:
        key = key_store[key_hash]
        key_base64 = base64.b64encode(key)
        key_store[key_hash] = key_base64
    with open(KEYSTORE_PATH, "w+") as key_store_file:
        json.dump(key_store, key_store_file)
        key_store_file.write("\n")


class KeyManager(object):

    """
     KeyManager is a python utility tool for generating and managing the keys in the jey store.
     One can request KeyManager to generate the keys, passing just the type of
     the key - either strong or weak or even the length of the key.
     One can request KeyManger to get the key from the key store by passing key_hash.
     One can request KeyManager to get the key_hash from the key store by passing encryption key.
     KeyManager maintains the keystore(json record) under /tmp/keystore.json.

     """
    def __init__(self, key_type=None, key_length=None, key=None, key_hash=None):
        self.key_type = key_type
        self.key_length = key_length
        self.key = key
        self.key_hash = key_hash

    def __add_to_keystore(self):
        """
        Update the key and key hash in the key store - requires both hash and
        key.
        """
        if not self.key_hash or not self.key:
            raise InputError("Need both key_hash and key to update into key store")
        key_store = _read_keystore()
        key_store[self.key_hash] = self.key
        _write_keystore(key_store)

    def __hash_key(self):

        #hash the given key - requires key
        if not self.key:
            raise InputError("Need key to hash")

        hash_it = hashlib.new('sha256')
        hash_it.update(b'\0' * 32)
        hash_it.update(self.key)
        self.key_hash = hash_it.hexdigest()
        return self.key_hash

    def generate(self):
        """
        generate the encryption key
        Hash the generated key
        Update the key store with key and hash
        """
        self.key = _get_key_generator(key_length=self.key_length, key_type=self.key_type).generate()
        self.key_hash = self.__hash_key()
        logger = Logger(self.key, self.key_hash)
        logger.log_key_info()
        self.__add_to_keystore()

    def get_key(self, log_key_info=True):
        """Fetch the key from the key store based on the key_hash - requires key hash"""
        if not self.key_hash:
            raise InputError("Need key hash to get the key from the key store")

        key_store = _read_keystore()
        key = key_store.get(self.key_hash, None)
        if key and log_key_info:
            logger = Logger(key=key)
            logger.log_key_info()
        if not key:
            raise KeyLookUpError("No keys in the keystore which matches the given key hash")

        return key

    def get_keyhash(self):
        """Fetch the key hash from the key store based on the key - requires key"""
        if not self.key:
            raise InputError("Need key to get the key hash from the key store")
        key_store = _read_keystore()
        try:
            key_hash = key_store.keys()[key_store.values().index(self.key)]
            logger = Logger(key_hash=key_hash)
            logger.log_key_info()
        except ValueError:
            raise KeyLookUpError("No key hash in the keystore which matches the given key")

    def update_keystore(self):
        """If this key hash is already in the key store, update its corresponding key"""

        if not (self.key_hash and self.key):
            raise InputError("Need key hash and key to update the key store")

        key_store = _read_keystore()
        if self.key_hash in key_store:
            key_store[self.key_hash] = self.key
        else:
            raise InputError("No existing key in the keystore"
                             "with key hash {}".format(self.key_hash))
        _write_keystore(key_store)


def _get_key_generator(key_length=None, key_type=None):
    if key_length:
        return RandomKeyGenerator(key_length=key_length)
    elif key_type == "weak":
        return WeakKeyGenerator()
    elif key_type == "strong":
        return StrongKeyGenerator()
    else:
        raise InputError("Either key_length in byte or key_type(\"strong OR weak\")"
                         " should be specified to generate the key")


class RandomKeyGenerator(object):
    """Generates a completely random key of the specified length"""

    def __init__(self, key_length):
        self.key_length = key_length

    def generate(self):
        """Generate a completely random byte sequence"""
        return os.urandom(self.key_length)


class StrongKeyGenerator(RandomKeyGenerator):
    """Generates a completely random 512-bit key"""

    def __init__(self):
        super(StrongKeyGenerator, self).__init__(key_length=64)


class WeakKeyGenerator(RandomKeyGenerator):
    """Generates a completely random 256-bit key"""

    def __init__(self):
        super(WeakKeyGenerator, self).__init__(key_length=32)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()

    parser.add_argument('--generatekey', action='store_true', dest='generate',
                        default=False, help="Generates the encryption key based on the given either keytype or keylength")

    parser.add_argument('--getkey', action='store_true', dest='get_key',
                        default=False, help="To get the key from the keystore based on the given key hash")

    parser.add_argument('--getkeyhash', action='store_true', dest='get_key_hash',
                        default=False, help="To get the key hash from the keystore based on the given key")

    parser.add_argument('--updatekeystore', action='store_true', dest='update_keystore',
                        default=False, help="If needs to update the already existing key in the keystore pass the keyHash and new key")

    parser.add_argument('--keytype', action='store', dest='key_type', default=None,
                        help='Type of the key: values expected weak or strong')

    parser.add_argument('--keylength', action='store', default=None, type=int,
                        dest='key_length',
                        help='length of the encryption key in byte')

    parser.add_argument('--keyhash', action='store', dest='key_hash', default=None,
                        help='Encryption key')

    parser.add_argument('--key', action='store', dest='key', default=None,
                        help='Base64-encoded encryption key')

    parser_input = parser.parse_args()

    if parser_input.key:
        parser_input.key = base64.b64decode(parser_input.key)

    if parser_input.generate:
        KeyManager(key_type=parser_input.key_type, key_length=parser_input.key_length).generate()
    elif parser_input.get_key:
        KeyManager(key_hash=parser_input.key_hash).get_key()
    elif parser_input.get_key_hash:
        KeyManager(key=parser_input.key).get_keyhash()
    elif parser_input.update_keystore:
        KeyManager(key_hash=parser_input.key_hash, key=parser_input.key).update_keystore()

