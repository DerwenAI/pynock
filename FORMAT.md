# NOCK Open Standard

[Apache Arrow](https://arrow.apache.org/docs/index.html) 
and its [Parquet](https://arrow.apache.org/docs/cpp/parquet.html) format
provide the most efficient means for graph serialization and persistence.

This proposed `NOCK` open standard serializes graphs efficiently at
scale in a way which aligns the data representations required for
popular graph technologies and related data sources:

  * semantic graphs (e.g., W3C formats RDF, TTL, JSON-LD, etc.)
  * labeled property graphs (e.g., openCypher)
  * probabilistic graphs (e.g., PSL)
  * spreadsheet import/export (e.g., CSV)
  * dataframes (e.g., Pandas, Dask, Spark, etc.)
  * edge lists (e.g., NetworkX, cuGraph, etc.)


## Terminology

Graph data has two possible states:

  * _marshalled_: serialized and persisted in storage, i.e., "at rest"
  * _unmarshalled_: dynamic data structures in memory, i.e., "live"


A node may be referenced either as a _source node_, which has directed edges, or as a _destination node_ which is the target of an edge.

When a node from another partition is referenced as a _destination node_, then at least its "shadow" information (i.e., its unique symbol) gets included within the referencing partition. This is called a _shadow node_.

When a shadow node gets unmarshalled, that triggers an `asyncio` _future_ (called an _object reference_ in Ray) to perform a distributed lookup of the node by name across the cluster. Then its partition info replaces the `"edge_id"` value.


## Conventions: Nodes and Edges
Records of type `Node` have always `"edge_id"` field set to `NOT_FOUND` value.
Records of type `Edge` have always `"edge_id"` field set to an integer value greater or equal to`0` (type `pydantic.NonNegativeInt`).

## Conventions: Missing Values, etc.

Data frameworks such as Excel and `pandas` have conflicting rules and default settings for how to handle missing values when marshalling and unmarshalling data. Language differences (Python, C++, SQL) as well as their popular libraries for handling CSV, JSON, dataframes, and so on, impose their own rules in addition. Consequently we encounter a range of possible ways to represent missing values:

  * `""` (empty string)
  * `NA`
  * `NaN`
  * `None`
  * `null`

Therefore to help minimize data quality surprises, `NOCK` uses the following missing values for the sake of improved consistency:

  * integer columns: `-1`
  * string columns: `""`  (including labels and properties)

These values are reserved. So far, there are no known cases where these reserved values conflict with graph use cases.

Missing values for the `truth` column are undefined and will raise an exception.

Note that for CSV files:

  * a header row is expected
  * strings are always quoted, using double quotes

Note that when using `pandas` to read Parquet files in `NOCK` format, to avoid having `NaN` substituted automatically for empty strings, 
be sure to use the `use_nullable_dtypes = True` setting:

```
df_parq = pd.read_parquet(
    "dat/tiny.parq",
    use_nullable_dtypes = True,
).fillna("")
```

Similarly, when using `pandas` to read CSV files in `NOCK` format, use the `DataFrame.fillna("")` filter:

```
df_csv = pd.read_csv(
    "dat/tiny.csv",
).fillna("")
```


## Schema

The Parquet datasets are sharded into multiple `partition` files, which use the following Parquet schema:

| field name | repetition | type | converted type | purpose |
| -- | -- | -- | -- | -- |
| "src_name" | `Repetition::REQUIRED` | `Type::BYTE_ARRAY` | `ConvertedType::UTF8` | unique symbol for a source node (subject) |
| "edge_id" | `Repetition::OPTIONAL` | `Type::INT32` | `ConvertedType::INT_32` | integer identifier for an edge, which does not need to be unique |
| "rel_name" | `Repetition::OPTIONAL` | `Type::BYTE_ARRAY` | `ConvertedType::UTF8` | optional relation symbol for an edge (predicate) |
| "dst_name" | `Repetition::OPTIONAL` | `Type::BYTE_ARRAY` | `ConvertedType::UTF8` | optional unique symbol for a destination node (object) |
| "truth" | `Repetition::OPTIONAL` | `Type::FLOAT` | `ConvertedType::NONE` | "truth" value for a source node |
| "shadow" | `Repetition::OPTIONAL` | `Type::INT32` | `ConvertedType::INT_32` | shadow; use `-1` for local node, or non-negative integer if this node resides on another partition |
| "is_rdf" | `Repetition::OPTIONAL` | `Type::BOOLEAN` | `ConvertedType::NONE` | boolean flag, true if source node was created through W3C stack |
| "labels" |  `Repetition::OPTIONAL` | `Type::BYTE_ARRAY` | `ConvertedType::UTF8` | source node labels, represented as a comma-delimited string |
| "props" | `Repetition::OPTIONAL` | `Type::BYTE_ARRAY` | `ConvertedType::UTF8` | properties, either for source nodes or edges, represented as a JSON string of key/value pairs |


## Row Organization

There are two kinds of rows represented by this schema:

  - _node row_
  - _edge row_

Within a partition, each node gets serialized as one _node row_ in the Parquet file, followed by an _edge row_ for each of its edges. These two cases are distinguished by the `"edge_id"` column values:

  * negative for a _node row_
  * non-negative integer values, unique within a source node for an _edge row_

No specific sort order is required of the node rows. Even so, a sort order may be forced for non-Parquet files during file validation. This allows for row-level comparisons.


## Optimizations

One possible optimization could be to use _nested rows_, where the edge rows get nested in Parquet under their corresponding node rows.

An obvious parallelization is to use multithreading for parsing/building the edge rows for each node row.


## Caveats

1. These field types are intended to make the format independent of system OS and language constraints, e.g., a Parquet dataset could be generated in a SQL query, Excel export, Jupyter notebook, Dask task, Spark job, JavaScript UI, etc., as input into a graph.

2. Additional columns/fields may be added to this organization as needed, such as for _subgraphs_, supporting evidence, etc.

3. Currently the node and edge properties are represented using JSON, although these may become optimized later as Parquet maps instead.
