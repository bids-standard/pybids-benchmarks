# pybids-benchmarks

Performance based benchmarks on common pybids functionality

The main goal of this repository is to evaluate the peformance improvemets of "legacy" pybids using a sql-lite backend, 
versus a [ancp-bids](https://github.com/ANCPLabOldenburg/ancp-bids) based backend.

Another goal is to compare peformance to [bids2table](https://github.com/FCP-INDI/bids2table), a tool that uses
pandas and Parquet to represents BIDS data, and enable fast cross-dataset queries.

## Usage

Set `DATASET_PATH` to a directory containing multiple BIDS Datasets that you want to benchmark.

The current script is set up to compare two local version of pybids, one with a sql-lite backend, and one with a ancp-bids backend.
The script loads pybids, runs benchmarks, then reloads using the other backend, and runs the same benchmarks.

Set the `PYBIDS_PATH` and `PYBIDS_REFACTOR_PATH` to the path of the pybids repositories you want to use.

To keep track of timing, the `@timing` decorator is used. This decorator will keep track of the time it takes to run a function,
and store it in a global variable. The name of the function, in addition to a kwarg named `version` is used to identify the function.

Run:

`python query_benchmarks.py` 

## TODO

- [ ] Add more benchmarks
- [ ] Add more datasets (ideally bids-examples, or larger dataset)
- [ ] Modify @timing to keep track of results separtely for each dataset
- [ ] Add bids2tables queries
- [ ] Potentially integrate with pytest-benchmark
