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


## Conventions: Missing Values, etc.

Data frameworks such as Excel and `pandas` have conflicting rules and default settings for how to handle missing values when marshalling and unmarshalling data. Language differences (Python, C++, SQL) as well as their popular libraries for handling CSV, JSON, dataframes, and so on, impose their own rules in addition. Consequently we encounter a range of possible ways to represent missing values:

  * `""` (empty string)
  * `NA`
  * `NaN`
  * `None`
  * `null`

Note that in some cases, such as the `rel_name` column, an empty string may have special meaning when unmarshalled, other than as a missing value.

To help minimize data quality issues, `NOCK` uses the following missing values for consistency:

  * integer columns: `-1`
  * label columns: `"[]"`
  * property columns: `"{}"`
  * string columns: `"null"`

These values are reserved. So far, there are no known cases where these reserved values conflict with graph use cases.

Strings are always quoted using straight doublequotes.


## Schema

The Parquet datasets are sharded into multiple `partition` files, which use the following Parquet schema:

| field name | repetition | type | converted type | purpose |
| -- | -- | -- | -- | -- |
| "src_name" | `Repetition::REQUIRED` | `Type::BYTE_ARRAY` | `ConvertedType::UTF8` | unique symbol for a source node (subject) |
| "edge_id" | `Repetition::OPTIONAL` | `Type::INT32` | `ConvertedType::INT_32` | integer identifier for an edge |
| "rel_name" | `Repetition::OPTIONAL` | `Type::BYTE_ARRAY` | `ConvertedType::UTF8` | optional relation symbol for an edge (predicate) |
| "dst_name" | `Repetition::OPTIONAL` | `Type::BYTE_ARRAY` | `ConvertedType::UTF8` | optional unique symbol for a destination node (object) |
| "truth" | `Repetition::OPTIONAL` | `Type::FLOAT` | `ConvertedType::NONE` | "truth" value for a source node |
| "shadow" | `Repetition::OPTIONAL` | `Type::INT32` | `ConvertedType::INT_32` | shadow index; `-1` for local node |
| "is_rdf" | `Repetition::OPTIONAL` | `Type::BOOLEAN` | `ConvertedType::NONE` | boolean flag, true if source node was created through W3C stack |
| "labels" |  `Repetition::OPTIONAL` | `Type::BYTE_ARRAY` | `ConvertedType::UTF8` | source node labels, represented as a comma-delimited string |
| "props" | `Repetition::OPTIONAL` | `Type::BYTE_ARRAY` | `ConvertedType::UTF8` | properties, either for source nodes or edges, represented as a JSON string of key/value pairs |


## Row Organization

There are two kinds of rows represented by this schema:

  - _node row_
  - _edge row_

Within a partition, each node gets serialized as one _node row_ in the Parquet file, followed by an _edge row_ for each of its edges. These are distinguished by the `"edge_id"` column values:

  * generally, non-negative integer values, unique within a source node
  * negative in the case of shadow nodes

No specific sort order is required of the node rows. Even so, a sort order may be forced for non-Parquet files during file validation. This allows for row-level comparisons.


## Caveats

1. These field types are intended to make the format independent of system OS and language constraints, e.g., a Parquet dataset could be generated in a Jupyter notebook, Dask task, Spark job, etc., for input into the graph.
1. Additional columns/fields may be added to this organization as needed, such as for _subgraphs_, supporting evidence, etc.
1. Currently the node and edge properties are represented using JSON, although these may become optimized later as Parquet maps instead.
1. One possible optimization could be to use _nested rows_, where the edge rows get nested in Parquet under their corresponding node rows.
1. Another possible parallelization could be to use threads for parsing/building each edge rows for a given node row.
1. Additional GPU acceleration using [`Dask-cuDF`](https://docs.rapids.ai/api/cudf/stable/10min.html) is planned, for fast Parquet loading and parallelization when parsing/building node rows.
