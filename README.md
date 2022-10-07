# pynock

![Licence](https://img.shields.io/github/license/DerwenAI/pynock)
![Repo size](https://img.shields.io/github/repo-size/DerwenAI/pynock)
![GitHub commit activity](https://img.shields.io/github/commit-activity/w/DerwenAI/pynock?style=plastic)
[![Checked with mypy](http://www.mypy-lang.org/static/mypy_badge.svg)](http://mypy-lang.org/)
![CI](https://github.com/DerwenAI/pynock/workflows/CI/badge.svg)
![downloads](https://img.shields.io/pypi/dm/pynock)
![sponsor](https://img.shields.io/github/sponsors/ceteri)

The following describes a proposed standard `NOCK` for a Parquet
format that supports efficient distributed serialization of multiple
kinds of graph technologies.

This library `pynock` provides Examples for working with low-level
Parquet read/write efficiently in Python.

Our intent is to serialize graphs in a way which aligns the data
representations required for popular graph technologies and related
data sources:

  * semantic graphs (e.g., W3C formats RDF, TTL, JSON-LD, etc.)
  * labeled property graphs (e.g., openCypher)
  * probabilistic graphs (e.g., PSL)
  * spreadsheet import/export (e.g., CSV)
  * dataframes (e.g., Pandas, Dask, Spark, etc.)
  * edge lists (e.g., NetworkX, cuGraph, etc.)

This approach also efficient distributed partitions based on Parquet,
which can scale on a cluster to very large (+1 T node) graphs.

For details about the proposed format in Parquet files, see the
[`FORMAT.md`](https://github.com/DerwenAI/pynock/blob/main/FORMAT.md)
file.

If you have questions, suggestions, or bug reports, please open
[an issue](https://github.com/DerwenAI/pynock/issues)
on our public GitHub repo.


## Caveats

Note that the `pynock` library does not provide any support for graph
computation or querying, merely for manipulating and validating
serialization formats.

Our intent is to provide examples where others from the broader open
source developer community can help troubleshoot edge cases in
Parquet.


## Dependencies

This code has been tested and validated using Python 3.8, and we make
no guarantees regarding correct behaviors on other versions.

The Parquet file formats depend on Arrow 5.0.x or later.

For the Python dependencies, the library versioning info is listed in the
[`requirements.txt`](https://github.com/DerwenAI/pynock/blob/main/requirements.txt)
file.


## Set up

To install via PIP:

```
python3 -m pip install -U pynock
```

To set up this library locally:

```
python3 -m venv venv
source venv/bin/activate

python3 -m pip install -U pip wheel
python3 -m pip install -r requirements.txt
```

## Usage via CLI

To run examples from CLI:

```
python3 cli.py load-parq --file dat/recipes.parq --debug
```

```
python3 cli.py load-rdf --file dat/tiny.ttl --save-csv foo.csv
```

For further information:

```
python3 cli.py --help
```

## Usage programmatically in Python

To construct a partition file programmatically, see the 
[`examples`](https://github.com/DerwenAI/pynock/blob/main/examples)
for Jupyter notebooks with sample code and debugging.


## Background

For more details about using Arrow and Parquet see:

["Apache Arrow homepage"](https://arrow.apache.org/)

["Finer-grained Reading and Writing"](https://arrow.apache.org/docs/python/parquet.html#finer-grained-reading-and-writing)

["Apache Arrow: Read DataFrame With Zero Memory"](https://towardsdatascience.com/apache-arrow-read-dataframe-with-zero-memory-69634092b1a)  
Dejan Simic  
_Towards Data Science_ (2020-06-25)


## Why the name?

A `nock` is the English word for the end of an arrow opposite its point.

If you must have an acronym, the proposed standard `NOCK` stands for
**N**etwork **O**bjects for **C**onsistent **K**nowledge.

Also, the library name had minimal namespace collisions on GitHub and
PyPi :)


## Developer updates

To set up the build environment locally, also run:
```
python3 -m pip install -U pip setuptools wheel
python3 -m pip install -r requirements-dev.txt
```

Note that we require the use of [`pre-commit` hooks](https://pre-commit.com/)
and to configure that locally:

```
pre-commit install
git config --local core.hooksPath .git/hooks/
```


## Package releases

First, verify that `setup.py` will run correctly for the package
release process:

```
python3 -m pip install -e .
python3 -m pytest -rx tests/
python3 -m pip uninstall pynock
```

Next, update the semantic version number in `setup.py` and create a
release on GitHub, and make sure to update the local repo:

```
git stash
git checkout main
git pull
```

Make sure that you have set up your 2FA authentication for generating
an API token on PyPi: <https://pypi.org/manage/account/token/>

Then run our PyPi push script:

```
./bin/push_pypi.sh
```


## Star History

[![Star History Chart](https://api.star-history.com/svg?repos=derwenai/pynock&type=Date)](https://star-history.com/#derwenai/pynock&Date)
