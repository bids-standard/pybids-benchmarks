import sys
import os

def _load_pybids_from_path(path):
    # Unload existing pybids
    for mod in list(sys.modules.keys()):
        if mod.startswith('bids'):
            del(sys.modules[mod])

    # Load pybids from path
    sys.path.insert(0,os.path.abspath(path))
    import bids.layout as bids_l
    return bids_l