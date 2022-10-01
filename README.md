# pynock

This library `pynock` provides Examples for working with low-level
Parquet read/write efficiently in Python. The intent is to serialize
graphs which can align data representations for multiple areas of
popular graph technologies:

  * semantic graphs (e.g., W3C)
  * labeled property graphs (e.g., openCypher)
  * probabilistic graphs (e.g., PSL)
  * edge lists (e.g., NetworkX)

This approach also supports distributed partitions based on
Arrow/Parquet which can scale to very large (+1 T node) graphs.

Note that the `pynock` library does not provide any support for graph
computation or querying, merely for manipulating and validating
serialization formats.


## Dependencies

This code has been tested and validated using Python 3.8, and we make
no guarantees regarding correct behaviors on other versions.

The Parquet file formats depend on Arrow 5.0.x or later.

For the Python dependencies, see the `requirements.txt` file.


## Getting started

To set up this library locally:

```
python3 -m venv venv
source venv/bin/activate

python3 -m pip install -U pip wheel
python3 -m pip install -r requirements.txt
```

Then to run the example code from CLI:

```
python3 example.py load-parquet --file dat/recipes.parq --debug
```

For further information:

```
python3 example.py --help
```


## Package Release

First, verify that `setup.py` will run correctly for the package
release process:

```
python3 -m pip install -e .
python3 -m pytest tests/
python3 -m pip uninstall pynock
```


## Why the name?

A `nock` is the English word for the end of an arrow opposite its point.


## Background

For more details about using Arrow/Parquet:

["Apache Arrow homepage"](https://arrow.apache.org/)

["Apache Arrow: Read DataFrame With Zero Memory"](https://towardsdatascience.com/apache-arrow-read-dataframe-with-zero-memory-69634092b1a)  
Dejan Simic  
_Towards Data Science_ (2020-06-25)
