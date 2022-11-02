import glob
import importlib
import os
import sys
import traceback

sys.path.append('/opt/xensource/sm/')
import util

plugindir = os.path.dirname(__file__)

plugins = []


def _log_exn_backtrace():
    for line in traceback.format_exc().splitlines():
        util.SMlog(line)

for file_name in glob.glob(os.path.join(plugindir, '*.py')):
    # Avoid recursively loading this module again. The __file__ variable might
    # have a .pyc extension, so we have to compare the filenames without
    # extension:
    if os.path.splitext(file_name)[0] == os.path.splitext(__file__)[0]:
        continue
    module_name = os.path.splitext(os.path.split(file_name)[-1])[0]
    try:
        module = importlib.import_module('{}.{}'.format(__name__, module_name))
        plugins.append(module)
    except:
        # ignore and log module import errors
        util.SMlog('Failed to load key lookup plugin {}'.format(module_name))
        _log_exn_backtrace()

def load_key(key_hash, vdi_uuid):
    for plugin in plugins:
        try:
            key = plugin.load_key(key_hash, vdi_uuid)
            if key:
                return key
        except:
            # ignore and log plugin failures
            util.SMlog('Key lookup plugin {} failed while loading key'
                       ' with hash {} for VDI {}'.format(
                           plugin.__name__, key_hash, vdi_uuid))
            _log_exn_backtrace()

    return None
