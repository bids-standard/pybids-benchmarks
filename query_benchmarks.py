import os
import sys
from functools import wraps
from time import time
from pathlib import Path
from collections import defaultdict
import pandas as pd
from datetime import datetime

DATASET_PATH = Path('/home/zorro/datasets/raw/')

RESULTS = []
RESULTS_COLS = ['version', 'function', 'rep', 'time']

def timing(loops=10):
    """ Decorator factory to time a function. """
    def wrap(f):
        def wrapped_f(*args, **kw):

            if 'version' in kw:
                version = kw.pop('version')
            else:
                version = 'unknown'
            
            for rep in range(loops):
                ts = time()
                result = f(*args, **kw)
                te = time()
                ms_diff = round((te - ts) * 1000, 2)
                RESULTS.append([version, f.__name__, rep, ms_diff])

            return result
        return wrapped_f
    return wrap

nndb_b2t = ''

@timing(loops=1)
def load_layouts_no_md(bids_l):
    indexer  = bids_l.BIDSLayoutIndexer(index_metadata=False)
    layouts = {}
    for ds in DATASET_PATH.iterdir():
        if ds.is_dir():
            layouts[ds.stem] = bids_l.BIDSLayout(ds, indexer=indexer)
    return layouts
 
@timing(loops=1)
def load_layouts(bids_l):
    layouts = {}
    for ds in DATASET_PATH.iterdir():
        if ds.is_dir():
            layouts[ds.stem] = bids_l.BIDSLayout(ds)
    return layouts

@timing()
def all_subjects(layouts):
    """ Get all subjects in a layout."""
    result = []
    for ds, layout in layouts.items():
        result.append([ds, layout.get_subjects()])
    return result

@timing()
def all_tasks(layouts):
    """ Get all subjects in a layout."""
    result = []
    for ds, layout in layouts.items():
        result.append(layout.get_tasks())
    return result

@timing()
def subjects_for_task(layouts, tasks):
    """ Get all subjects in a task from all datasets in a layout."""
    result = []
    for ix, (ds, layout) in enumerate(layouts.items()):
        ta = tasks[ix][0]
        result.append([ds, layout.get_subjects(task=ta)])
    return result

@timing()
def print_repr(layouts):
    for ds, layout in layouts.items():
        layout.__repr__()

@timing()
def get_niftis_as_files(layouts):
    results = []
    for ds, layout in layouts.items():
        task = layout.get_tasks()[0]
        results.append(
            layout.get(task=task, extension='.nii.gz', return_type='file')
        )

    return results

@timing()
def get_objects_from_paths(layouts, files):
    results = []
    for ix, layout in enumerate(layouts.values()):
        f_set = files[ix]
        for f in f_set:
            results.append(layout.get_file(f))

@timing()
def get_niftis_as_objects(layouts):
    results = []
    for ds, layout in layouts.items():
        task = layout.get_tasks()[0]
        results.append(
            layout.get(task=task, extension='.nii.gz', return_type='object')
        )

    return results

@timing()
def get_metadata(bids_files):
    results = []
    for files in bids_files:
        for f in files:
            results.append(f.get_metadata())
    return results

@timing()
def get_return_type_dict(layouts):
    results = []
    for ds, layout in layouts.items():
        results.append(
            layout.get(target='subject', return_type='dir')
        )

    return results

# Test bids2table on similar queries
# def b2t_ses_query(ta):
#     return (
#         nndb_b2t['_index']
#         .query(f"task == '{ta}'")
#         .loc[:, ["dataset", "subject"]]
#         .values
#         )


if __name__ == '__main__':

    ## Test "legacy" pybids
    sys.path.insert(0,os.path.abspath('/home/zorro/repos/pybids-legacy'))
    import bids.layout as bids_l

    version = 'legacy'
    
    _ = load_layouts_no_md(bids_l, version=version)
    layouts = load_layouts(bids_l, version=version)
    all_subjects(layouts, version=version)
    tasks = all_tasks(layouts, version=version)
    subjects_for_task(layouts, tasks, version=version)
    print_repr(layouts, version=version)
    get_niftis_as_files(layouts, version=version)
    bids_files = get_niftis_as_objects(layouts, version=version)
    get_metadata(bids_files, version=version)
    get_return_type_dict(layouts, version=version)

    ## Unload module
    for mod in list(sys.modules.keys()):
        if mod.startswith('bids'):
            del(sys.modules[mod])

    ## Test refactored "ancp-bids" pybids
    del(sys.path[0])
    sys.path.insert(0,os.path.abspath('/home/zorro/repos/pybids'))
    import bids.layout as bids_l

    version = 'pybids-refactor'

    _ = load_layouts_no_md(bids_l, version=version)
    layouts = load_layouts(bids_l, version=version)
    all_subjects(layouts, version=version)
    tasks = all_tasks(layouts, version=version)
    subjects_for_task(layouts, tasks, version=version)
    print_repr(layouts, version=version)
    get_niftis_as_files(layouts, version=version)
    bids_files = get_niftis_as_objects(layouts, version=version)
    get_metadata(bids_files, version=version)
    get_return_type_dict(layouts, version=version)

    ## Conver to dataframe
    df = pd.DataFrame(RESULTS, columns=RESULTS_COLS)
    # Save with timestamp
    df.to_csv(f"results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", index=False)

    print("RESULT SUMMARY:")
    means = df.groupby(['version', 'function']).mean().reset_index()
    print(means.pivot(index='function', columns='version', values='time'))





