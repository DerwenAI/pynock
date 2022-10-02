# pynock

This library `pynock` provides Examples for working with low-level
Parquet read/write efficiently in Python.

Our intent is to serialize graphs which align the data representations
required for multiple areas of popular graph technologies:

  * semantic graphs (e.g., W3C)
  * labeled property graphs (e.g., openCypher)
  * probabilistic graphs (e.g., PSL)
  * edge lists (e.g., NetworkX)

This approach also supports distributed partitions based on Parquet
which can scale to very large (+1 T node) graphs.

For details about the formatting required in Parquet files, see the
[`FORMAT.md`](https://github.com/DerwenAI/pynock/blob/main/FORMAT.md)
page.


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

For the Python dependencies, see the `requirements.txt` file.


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
python3 example.py load-parq --file dat/recipes.parq --debug
```

```
python3 example.py load-rdf --file dat/tiny.ttl --save-cvs foo.cvs
```

For further information:

```
python3 example.py --help
```

## Usage programmatically in Python

To construct a partition file programmatically, see the sample code
[`tiny.py`](https://github.com/DerwenAI/pynock/blob/main/tiny.py)
which builds the minimal recipe example as an RDF graph.


## Background

For more details about using Arrow and Parquet see:

["Apache Arrow homepage"](https://arrow.apache.org/)

["Finer-grained Reading and Writing"](https://arrow.apache.org/docs/python/parquet.html#finer-grained-reading-and-writing)

["Apache Arrow: Read DataFrame With Zero Memory"](https://towardsdatascience.com/apache-arrow-read-dataframe-with-zero-memory-69634092b1a)  
Dejan Simic  
_Towards Data Science_ (2020-06-25)


## Why the name?

A `nock` is the English word for the end of an arrow opposite its point.


## Package Release

First, verify that `setup.py` will run correctly for the package
release process:

```
python3 -m pip install -e .
python3 -m pytest tests/
python3 -m pip uninstall pynock
```
