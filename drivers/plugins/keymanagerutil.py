#!/usr/bin/python

import os
import string
import hashlib
import json
import argparse
from random import SystemRandom


"load_key is called by SM plugin when it needs to find the key for specified key_hash from the key store"
def load_key(key_hash):
    try:
        key = KeyManager(key_hash=key_hash).get_key()
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

        if self.key and self.key_hash:
            print(json.dumps({"key":self.key, "key_hash":self.key_hash}))
            return

        elif self.key_hash:
            print(json.dumps({"key_hash":self.key_hash}))

        elif self.key:
            print(json.dumps({"key":self.key}))
        
        else:
            print(json.dumps({}))

    def log_message(self, message):
        print(message)

class KeyManager(object):
    
    """
     KeyManager is a python utility tool for generating and managing the keys in the jey store.
     One can request KeyManager to generate the keys, passing just the type of the key - either strong or weak or even the length of the key.
     One can request KeyManger to get the key from the key store by passing key_hash.
     One can request KeyManager to get the key_hash from the key store by passing encryption key.
     KeyManager maintains the keystore(json record) under /tmp/keystore.json.
     
     """
    def __init__(self, key_type=None, key_length=None, key=None, key_hash=None):
        self.key_type = key_type
        self.key_length = key_length
        self.key = key
        self.key_hash = key_hash

    def __update_keystore(self):
        
        #update the key and key hash in the key store - requires both hash and key\
        if not self.key_hash or not self.key:
            raise InputError("Need both key_hash and key to update into key store")

        keyInfo = {self.key_hash:self.key}

        with open("/tmp/keystore.json", "a") as key_store:
            json.dump(keyInfo, key_store)
            key_store.write("\n")

    def __hash_key(self):
        
        #hash the given key - requires key
        if not self.key:
            raise InputError("Need key to hash")

        key_length = len(self.key)
        hash_it = hashlib.new('sha256')
        hash_it.update(b'\0' * 32)
        hash_it.update(self.key.encode('utf-8'))
        self.key_hash = hash_it.hexdigest()
        return self.key_hash

    def generate(self):
        """
        generate the encryption key 
        Hash the generated key
        Update the key store with key and hash
        """
        self.key = KeyGenerator(self.key_length, self.key_type).generate()
        self.key_hash = self.__hash_key()
        self.logger = Logger(self.key, self.key_hash)
        self.logger.log_key_info()
        self.__update_keystore()
    
    def get_key(self):
        
        #fetch the key from the key store based on the key_hash - requires key hash
        if not self.key_hash:
            raise InputError("Need key hash to get the key from the key store")
        
        keyInfo = {}
        with open("/tmp/keystore.json", "r") as key_store:
            for line in key_store:
                keyInfo.update(json.loads(line))
            key = keyInfo.get(self.key_hash, None)
            if key:
                logger = Logger(key=key)
                logger.log_key_info()
            else:
                raise KeyLookUpError("No keys in the keystore which matches the given key hash")

        return key
    
    def get_keyhash(self):
        
        #fetch the key hash from the key store based on the key - requires key
        if not self.key:
            raise InputError("Need key to get the key hash from the key store")
        keyInfo = dict()
        with open("/tmp/keystore.json", "r") as key_store:
            for line in key_store:
                keyInfo.update(json.loads(line))
            try:
                key_hash = keyInfo.keys()[keyInfo.values().index(self.key)]
            except ValueError:
                raise KeyLookUpError("No key hash in the keystore which matches the given key")
            
            if key_hash:
                logger = Logger(key_hash=key_hash)
                logger.log_key_info()
            else:
                raise KeyLookUpError("No keys in the keystore which matches the given key hash")
    
    def update_keystore(self):
        
        if not (self.key_hash and self.key):
            raise InputError("Need key hash and key to update the key store")
        
        keyInfo = {}
        with open("/tmp/keystore.json", "r") as key_store:
            for line in key_store:
                keyInfo.update(json.loads(line))

        for key_hash, key in keyInfo.items():
            if key_hash == self.key_hash:
                keyInfo[key_hash] = self.key

        with open("/tmp/keystore.json", "w") as key_store:
            json.dump(keyInfo, key_store)
            key_store.write("\n")


class KeyGenerator(object):
    
    def __init__(self, key_length=None, key_type=None):
        self.key_length = key_length
        self.key_type = key_type
    
    def generate(self):

        if self.key_length and self.key_type == "strong":
            return StrongKeyGenerator(self.key_length).generate()

        elif self.key_length and self.key_type == "weak":
            return WeakKeyGenerator(self.key_length).generate()

        elif self.key_type == "strong":
            return StrongKeyGenerator().generate()
        
        elif self.key_type == "weak":
            return WeakKeyGenerator().generate()

        elif self.key_length:
            #if there is only key_length specified then we generate strong key with the specified length
            return StrongKeyGenerator(self.key_length).generate()

        else:
            raise InputError("Either key_length in byte or key_type(\"strong OR weak\") should be specified to generate the key")

class StrongKeyGenerator(KeyGenerator):

    def __init__(self, key_length=None):
        self.key_length = key_length if key_length else 64
    
    def generate(self):
        "Generate the key from the ascii letters, digits, whitespaces, punctuations - this is considered strong"
        keys_from = string.ascii_letters + string.digits + string.whitespace + string.punctuation
        return "".join([SystemRandom().choice(keys_from) for _ in range(self.key_length)])
        

class WeakKeyGenerator(KeyGenerator):

    def __init__(self, key_length=None):
        self.key_length = key_length if key_length else 16

    def generate(self):
        "Generate the key from the ascii letters and digits - this is considered weak"
        keys_from = string.ascii_letters + string.digits
        return "".join([SystemRandom().choice(keys_from) for _ in range(self.key_length)])


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

    parser.add_argument('--keyhash', action='store', dest='key_hash',default=None,
                        help='Encryption key')

    parser.add_argument('--key', action='store', dest='key', default=None, help='Encryption key')

    parser_input = parser.parse_args()

    
    if parser_input.generate:
        KeyManager(key_type=parser_input.key_type, key_length=parser_input.key_length).generate()
    elif parser_input.get_key:
        KeyManager(key_hash=parser_input.key_hash).get_key()
    elif parser_input.get_key_hash:
        KeyManager(key=parser_input.key).get_keyhash()
    elif parser_input.update_keystore:
        KeyManager(key_hash=parser_input.key_hash, key=parser_input.key).update_keystore()


