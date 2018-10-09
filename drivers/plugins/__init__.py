import glob
import importlib
import os
import re
import sys

plugindir = os.path.dirname(__file__)

plugins = []

for file_name in glob.glob(os.path.join(plugindir, '*.py')):
    if file_name == __file__:
        continue
    module_name = os.path.splitext(os.path.split(file_name)[-1])[0]
    try:
        module = importlib.import_module('{}.{}'.format(__name__, module_name))
        plugins.append(module)
    except:
        # ignore module import errors
        pass

def load_key(key_hash):
    for plugin in plugins:
        try:
            key = plugin.load_key(key_hash)
            if key:
                return key
        except:
            # ignore plugin failures
            pass

    return None
