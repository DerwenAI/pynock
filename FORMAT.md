[Apache Arrow](https://arrow.apache.org/docs/index.html) 
and its [Parquet](https://arrow.apache.org/docs/cpp/parquet.html) format
provide the most efficient means for graph serialization and persistence.

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

There are two kinds of rows represented by this schema:

  - _node row_
  - _edge row_

Within a partition, every node will be serialized by one _node row_ in the Parquet file, followed by an _edge row_ for each of its edges.
These get distinguished by the `"edge_id"` column which will is a non-null value in _edge rows_.

Every node referenced by an edge must have at least its "shadow" information (i.e., its unique symbol) included within the serialized Parquet partition file.
Once loaded into memory, each "shadow" nodes will also have its associated `asyncio` _future_ (called an _object reference_ in Ray) that links to the partition in the Ray cluster where that node lives.


## Caveats

1. These field types are intended to make the format independent of system OS and language constraints, e.g., a Parquet dataset could be generated in a Jupyter notebook, Dask task, Spark job, etc., for input into the graph.
1. Additional columns/fields may be added to this organization as needed, such as for _subgraphs_, supporting evidence, etc.
1. Currently the node and edge properties are represented using JSON, although these may become optimized later as Parquet maps instead.
1. A possible optimization could be to use _nested rows_, where the edge rows get nested in Parquet under their corresponding node rows.
1. A possible parallelization could be to use threads for parsing/building each edge rows for a given node row.
1. Additional GPU acceleration using [`Dask-cuDF`](https://docs.rapids.ai/api/cudf/stable/10min.html) is planned, for fast Parquet loading and parallelization when parsing/building node rows.
