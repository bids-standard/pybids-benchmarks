from time import time
from pathlib import Path
import pandas as pd
from datetime import datetime

from utils import _load_pybids_from_path

DATASET_PATH = Path('/home/zorro/datasets/raw/')
PYBIDS_LEGACY_PATH = '/home/zorro/repos/pybids-legacy'
PYBIDS_REFACTOR_PATH = '/home/zorro/repos/pybids'
BENCHMARK_STATS = []
BENCHMARK_COLS = ['dataset', 'version', 'function', 'rep', 'time']
N_LOOPS = 5

def _time_and_run(f, loops=N_LOOPS, version='unknown', dataset='N/A', **kwargs):
    """ Time a function call and return the results."""
    for rep in range(loops):
        ts = time()
        result = f(**kwargs)
        te = time()
        ms_diff = round((te - ts) * 1000, 2)
        BENCHMARK_STATS.append([dataset, version, f.__name__, rep, ms_diff])
    return result

def timing(loops=N_LOOPS):
    """ Decorator to time a query. 

    Runs function 'loops' times, and appends the results to the global
    BENCHMARK_STATS list.

    If the 'layouts' argument is given, the decorator will run the query once for
    dataset, and return a dictionary of results, keyed by dataset name. The
    decorator passes a single BIDSLayout to the query as 'layout'.

    Other keyword arguments are passed to the query are assumed to contained the
    same keys as 'layouts', and are passed to the query individually.

    Args:
        loops (int): Number of times to run the query.
    Query Args:
        version (str): Version of pybids to test. Append to results.
    
    The following are optional, and depend on the query being run.
        layouts (dict): Dictionary of BIDSLayout, keyed by dataset name.
                         Decorator passes a single BIDSLayout to the query 
                         as 'layout'.
        **kwargs (dict): Any other keyword arguments are passed to the query are 
                         assumed to contained the same keys as 'layouts', and 
                         are passed to the query individually.
    """
    def wrap(f):
        def wrapped_f(**kw):
            version = kw.pop('version', 'unknown')

            if 'layouts' in kw:
                layouts = kw.pop('layouts')
                results = {}
                for ds, layout in layouts.items():
                    results[ds] = _time_and_run(
                            f, layout=layout, loops=loops, version=version, dataset=ds,
                            **{k: v[ds] for k, v in kw.items()})
                return results
            else:
                return _time_and_run(f, loops=loops, version=version, **kw)

        return wrapped_f
    return wrap

# Load BIDSLayout -- only runs once, since its slower
@timing(loops=1)
def load_layouts_no_md(*, bids_module):
    indexer  = bids_module.BIDSLayoutIndexer(index_metadata=False)
    layouts = {}
    for ds in DATASET_PATH.iterdir():
        if ds.is_dir():
            layouts[ds.stem] = bids_module.BIDSLayout(ds, indexer=indexer)
    return layouts
 
@timing(loops=1)
def load_layouts(*, bids_module):
    layouts = {}
    for ds in DATASET_PATH.iterdir():
        if ds.is_dir():
            layouts[ds.stem] = bids_module.BIDSLayout(ds)
    return layouts

# Query tests
@timing()
def all_subjects(*, layout):
    """ Get all subjects in a layout."""
    return layout.get_subjects()

@timing()
def all_tasks(*, layout):
    """ Get all subjects in a layout."""
    return layout.get_tasks()

@timing()
def subjects_for_task(*, layout, tasks):
    """ Get all subjects in a task in a layout."""
    return layout.get_subjects(task=tasks[0])

@timing()
def print_repr(*, layout):
    return layout.__repr__()

@timing()
def get_niftis_as_files(*, layout, tasks):
    return layout.get(task=tasks[0], extension='.nii.gz', return_type='file')

@timing()
def get_objects_from_paths(*, layout, files):
    results = []
    for f in files:
        results.append(layout.get_file(f.path))
    return results

@timing()
def get_niftis_as_objects(*, layout, tasks):
    return layout.get(task=tasks[0], extension='.nii.gz', return_type='object')

@timing()
def get_return_type_dict(*, layout):
    return layout.get(target='subject', return_type='dir')

@timing()
def get_metadata(*, layout, files):
    results = []
    for f in files:
        results.append(f.get_metadata())
    return results


def _run_pybids_benchmarks(bids_module, version):
    """ Run all benchmarks for a given version of pybids."""
    _ = load_layouts_no_md(bids_module=bids_module, version=version)
    layouts = load_layouts(bids_module=bids_module, version=version)

    # Basic queries
    print_repr(layouts=layouts, version=version)
    get_return_type_dict(layouts=layouts, version=version)
    all_subjects(layouts=layouts, version=version)

    # Related queries
    tasks = all_tasks(layouts=layouts, version=version)
    subjects_for_task(layouts=layouts, tasks=tasks, version=version)
    get_niftis_as_files(layouts=layouts, tasks=tasks, version=version)

    # File queries
    files = get_niftis_as_objects(layouts=layouts, tasks=tasks, version=version)
    get_metadata(layouts=layouts, files=files, version=version)
    # get_objects_from_paths(layouts=layouts, files=files, version=version) # Not working in ancpbids

if __name__ == '__main__':
    # Test "legacy" pybids
    bids_l = _load_pybids_from_path(PYBIDS_LEGACY_PATH)

    _run_pybids_benchmarks(bids_module=bids_l, version='pybids-legacy')
    
    # Test "refactor" pybids
    bids_l_refactor = _load_pybids_from_path(PYBIDS_REFACTOR_PATH)

    _run_pybids_benchmarks(bids_module=bids_l_refactor, version='pybids-refactor')

    # Convert & save results
    df = pd.DataFrame(BENCHMARK_STATS, columns=BENCHMARK_COLS)
    df.to_csv(f"results/results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", index=False)

    print("RESULT SUMMARY:")
    means = df.groupby(['version', 'function']).mean().reset_index()
    print(means.pivot(index='function', columns='version', values='time'))





