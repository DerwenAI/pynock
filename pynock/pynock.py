#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Example graph serialization using low-level Parquet read/write
efficiently in Python.
"""

import ast
import csv
import json
import sys
import typing

from icecream import ic  # type: ignore  # pylint: disable=E0401
from pydantic import BaseModel, confloat, conint, NonNegativeInt, ValidationError  # pylint: disable=E0401,E0611
from rich.progress import track  # pylint: disable=E0401
import cloudpathlib
import pandas as pd
import pyarrow as pa  # type: ignore  # pylint: disable=E0401
import pyarrow.lib  # type: ignore  # pylint: disable=E0401
import pyarrow.parquet as pq  # type: ignore  # pylint: disable=E0401
import rdflib


######################################################################
## non-class definitions

GraphRow = typing.Dict[str, typing.Any]
IndexInts = conint(ge=-1)
PropMap = typing.Dict[str, typing.Any]
TruthType = confloat(ge=0.0, le=1.0)

EMPTY_STRING: str = ""
NOT_FOUND: IndexInts = -1  # type: ignore


######################################################################
## edges

class Edge (BaseModel):  # pylint: disable=R0903
    """
Representing an edge (arc) in the graph.
    """
    BLANK_RELATION: typing.ClassVar[NonNegativeInt] = 0

    rel: NonNegativeInt = BLANK_RELATION
    node_id: IndexInts = NOT_FOUND  # type: ignore
    truth: TruthType = 1.0  # type: ignore
    prop_map: PropMap = {}


######################################################################
## nodes

class Node (BaseModel):  # pylint: disable=R0903
    """
Representing a node (entity) in the graph.
    """
    BASED_LOCAL: typing.ClassVar[int] = -1

    node_id: IndexInts = NOT_FOUND  # type: ignore
    name: str = EMPTY_STRING
    shadow: IndexInts = BASED_LOCAL  # type: ignore
    is_rdf: bool = False
    label_set: typing.Set[str] = set()
    truth: TruthType = 1.0  # type: ignore
    prop_map: PropMap = {}
    edge_map: typing.Dict[IndexInts, list] = {}  # type: ignore


    def add_edge (
        self,
        edge: Edge,
        *,
        debug: bool = False,  # pylint: disable=W0613
        ) -> None:
        """
Add the given edge to its src node.
        """
        if edge.rel not in self.edge_map:
            self.edge_map[edge.rel] = []

        self.edge_map[edge.rel].append(edge)


######################################################################
## partitions

class Partition (BaseModel):  # pylint: disable=R0903
    """
Representing a partition in the graph.
    """
    part_id: IndexInts = NOT_FOUND  # type: ignore
    next_node: NonNegativeInt = 0
    nodes: typing.Dict[NonNegativeInt, Node] = {}
    node_names: typing.Dict[str, NonNegativeInt] = {}
    edge_rels: typing.List[str] = [""]


    def lookup_node (
        self,
        node_name: str,
        *,
        debug: bool = False,  # pylint: disable=W0613
        ) -> typing.Optional[Node]:
        """
Lookup a node, return None if not found.
        """
        if node_name in self.node_names:
            return self.nodes[self.node_names[node_name]]

        return None


    def create_node_name (
        self,
        node_name: str,
        *,
        debug: bool = False,  # pylint: disable=W0613
        ) -> int:
        """
Create a name for a new node in the namespace, looking up first to avoid duplicates.
        """
        node_id: IndexInts = NOT_FOUND  # type: ignore

        if node_name in [None, ""]:
            raise ValueError(f"node name cannot be null |{ node_name }|")
        elif node_name in self.node_names:
            node_id = self.node_names[node_name]
        else:
            node_id = self.next_node
            self.node_names[node_name] = node_id
            self.next_node += 1

        return node_id


    @classmethod
    def _load_props (
        cls,
        props: str,
        *,
        debug: bool = False,  # pylint: disable=W0613
        ) -> PropMap:
        """
Load property pairs from a JSON string.
        """
        prop_map: PropMap = {}

        if props not in (EMPTY_STRING, "null"):
            prop_map = json.loads(props)

        return prop_map


    @classmethod
    def _save_props (
        cls,
        prop_map: PropMap,
        *,
        debug: bool = False,  # pylint: disable=W0613
        ) -> str:
        """
Save property pairs to a JSON string.
        """
        props: str = EMPTY_STRING

        if len(prop_map) > 0:
            props = json.dumps(prop_map)
            props = props.replace("\": ", "\":")
            props = props.replace(", \"", ",\"")

        return props


    def add_node (
        self,
        node: Node,
        *,
        debug: bool = False,  # pylint: disable=W0613
        ) -> None:
        """
Add a node to the partition.
        """
        self.nodes[node.node_id] = node


    @classmethod
    def _validation_error (
        cls,
        row_num: NonNegativeInt,
        row: GraphRow,
        message: str,
        ) -> None:
        """
Print an error message to stderr.
        """
        print(
            f"error at input row { row_num }: { message }",
            file = sys.stderr,
        )

        print(
            row,
            file = sys.stderr,
        )


    def populate_node (
        self,
        row: GraphRow,
        *,
        debug: bool = False,  # pylint: disable=W0613
        ) -> Node:
        """
Populate a Node object from the given Parquet row data.
        """
        # first, lookup to make sure we don't overwrite if there are
        # duplicates for the src node
        src_name: str = row["src_name"]
        src_node: typing.Optional[Node] = self.lookup_node(src_name, debug=debug)

        if src_node is None:
            # create a src node
            src_node = Node(
                node_id = self.create_node_name(src_name, debug=debug),
                name = row["src_name"],
                truth = row["truth"],
                is_rdf = row["is_rdf"],
                shadow = row["shadow"],
                label_set = set(row["labels"].split(",")),
                prop_map = self._load_props(row["props"], debug=debug),
            )

            self.add_node(src_node, debug=debug)  # type: ignore

        return src_node  # type: ignore


    def get_edge_rel (
        self,
        rel_name: str,
        *,
        create: bool = False,  # pylint: disable=W0613
        debug: bool = False,  # pylint: disable=W0613
        ) -> int:
        """
Lookup the integer index for the named edge relation.
        """
        if rel_name not in self.edge_rels:
            if create:
                self.edge_rels.append(rel_name)
            else:
                return NOT_FOUND

        return self.edge_rels.index(rel_name)


    def populate_edge (
        self,
        row: GraphRow,
        node: Node,
        *,
        debug: bool = False,  # pylint: disable=W0613
        ) -> Edge:
        """
Populate an Edge object from the given Parquet row data.
        """
        # first, lookup the dst node and create if needed
        dst_name: str = row["dst_name"]
        dst_node: typing.Optional[Node] = self.lookup_node(dst_name, debug=debug)

        if dst_node is None:
            dst_node = Node(
                node_id = self.create_node_name(dst_name, debug=debug),
                name = dst_name,
                truth = row["truth"],
                is_rdf = row["is_rdf"],
            )

            self.add_node(dst_node, debug=debug)

        # create the edge
        edge: Edge = Edge(
            rel = self.get_edge_rel(row["rel_name"], create=True, debug=debug),
            truth = row["truth"],
            node_id = dst_node.node_id,
            prop_map = self._load_props(row["props"], debug=debug),
        )

        node.add_edge(edge, debug=debug)

        return edge


    def dump_data (
        self,
        ) -> None:
        """
Dump the internal data structures for this partition.
        """
        for _, src_node_id in self.node_names.items():
            src_node: Node = self.nodes[src_node_id]
            self.dump_node(src_node)


    def dump_node (
        self,
        node: Node,
        ) -> None:
        """
Dump the internal data structures for this node.
        """
        ic(node)

        for edge_rel, edge_list in node.edge_map.items():
            for edge in edge_list:
                dst_node: Node = self.nodes[edge.node_id]
                ic(edge_rel, edge, dst_node.name)


    @classmethod
    def dump_parquet (
        cls,
        parq_file: pq.ParquetFile,
        *,
        debug: bool = False,
        ) -> None:
        """
Dump the metadata and content for an input Parquet file.
        """
        ic(parq_file.metadata)
        ic(parq_file.schema)
        ic(parq_file.num_row_groups)

        for batch in range(parq_file.num_row_groups):
            row_group: pyarrow.lib.Table = parq_file.read_row_group(batch)  # pylint: disable=I1101

            if row_group.num_rows > 0:
                ic(row_group)
                ic(row_group.columns)


    @classmethod
    def iter_load_parquet (
        cls,
        parq_file: pq.ParquetFile,
        *,
        debug: bool = False,
        ) -> typing.Iterable[typing.Tuple[int, GraphRow]]:
        """
Iterate through the rows in a Parquet file.
        """
        row_num: NonNegativeInt = 0

        for batch in range(parq_file.num_row_groups):
            row_group: pyarrow.lib.Table = parq_file.read_row_group(batch)  # pylint: disable=I1101

            for r_idx in range(row_group.num_rows):
                row: GraphRow = {}

                for c_idx in range(row_group.num_columns):
                    try:
                        key: str = row_group.column_names[c_idx]
                        col: pyarrow.lib.ChunkedArray = row_group.column(c_idx)  # pylint: disable=I1101
                        val: typing.Any = col[r_idx]
                        row[key] = val.as_py()
                    except IndexError as ex:
                        ic(ex, r_idx, c_idx)
                        sys.exit(-1)

                if debug:
                    print()
                    ic(r_idx, row)

                yield row_num, row
                row_num += 1


    def iter_load_csv (
        self,
        csv_path: cloudpathlib.AnyPath,
        *,
        encoding: str = "utf-8",
        debug: bool = False,
        ) -> typing.Iterable[typing.Tuple[int, GraphRow]]:
        """
Iterate through the rows in a CSV file.
        """
        row_num: NonNegativeInt = 0

        with open(csv_path, encoding=encoding) as fp:
            reader = csv.reader(
                fp,
                delimiter = ",",
                quotechar = '"',
            )

            header = next(reader)

            try:
                for row_val in reader:
                    row: GraphRow = dict(zip(header, row_val))
                    row["edge_id"] = int(row["edge_id"])
                    row["is_rdf"] = bool(ast.literal_eval(row["is_rdf"]))
                    row["shadow"] = int(row["shadow"])
                    row["truth"] = float(row["truth"])

                    yield row_num, row
                    row_num += 1
            except ValueError as ex:
                self._validation_error(row_num, row, str(ex))
                sys.exit(-1)


    def iter_load_rdf (
        self,
        rdf_path: cloudpathlib.AnyPath,
        rdf_format: str,
        *,
        encoding: str = "utf-8",
        debug: bool = False,
        ) -> typing.Iterable[typing.Tuple[int, GraphRow]]:
        """
Iterate through the rows implied by a RDF file.
        """
        row_num: NonNegativeInt = 0
        graph = rdflib.Graph()

        graph.parse(
            rdf_path,
            format = rdf_format,
            encoding = encoding,
        )

        for subj in graph.subjects(unique=True):  # type: ignore
            # node representation for a triple
            row: GraphRow = {}
            row["src_name"] = str(subj)
            row["truth"] = 1.0
            row["edge_id"] = NOT_FOUND
            row["rel_name"] = EMPTY_STRING
            row["dst_name"] = EMPTY_STRING
            row["is_rdf"] = True
            row["shadow"] = Node.BASED_LOCAL
            row["labels"] = EMPTY_STRING
            row["props"] = EMPTY_STRING

            if debug:
                ic("node", subj, row_num, row)

            yield row_num, row
            row_num += 1

            for _, pred, objt in graph.triples((subj, None, None)):
                if debug:
                    ic(subj, pred, objt)

                # edge representation for a triple
                row = {}
                row["src_name"] = str(subj)
                row["truth"] = 1.0
                row["edge_id"] = 1
                row["rel_name"] = str(pred)
                row["dst_name"] = str(objt)
                row["is_rdf"] = True
                row["shadow"] = Node.BASED_LOCAL
                row["labels"] = EMPTY_STRING
                row["props"] = EMPTY_STRING

                if debug:
                    ic("edge", objt, row_num, row)

                yield row_num, row
                row_num += 1


    def parse_rows (
        self,
        iter_load: typing.Iterable[typing.Tuple[int, GraphRow]],
        *,
        debug: bool = False,
        ) -> None:
        """
Parse a stream of rows to construct a graph partition.
        """
        for row_num, row in track(iter_load, description=f"parse rows"):
            # have we reached a row which begins a new node?
            if row["edge_id"] < 0:
                try:
                    node: Node = self.populate_node(row, debug=debug)

                    if debug:
                        print()
                        ic(node)
                except ValidationError as ex:
                    self._validation_error(row_num, row, str(ex))
                    sys.exit(-1)

            # validate the node/edge sequencing and consistency among the rows
            elif row["src_name"] != node.name:
                error_node = row["src_name"]
                message = f"|{ error_node }| out of sequence at row {row_num}"
                raise ValueError(message)

            # otherwise this row is an edge for the most recent node
            else:
                try:
                    edge: Edge = self.populate_edge(row, node, debug=debug)

                    if debug:
                        ic(edge)
                except ValidationError as ex:
                    self._validation_error(row_num, row, str(ex))
                    sys.exit(-1)


    def iter_gen_rows (
        self,
        *,
        sort: bool = False,
        debug: bool = False,
        ) -> typing.Iterable[GraphRow]:
        """
Iterator for generating rows on writes.

Optionally, sort on:
  * src `node.name` in ASC order
  * `edge_id` and dst `node.name` in ASC order
        """
        if sort:
            node_iter = sorted(self.node_names.items())
        else:
            node_iter = self.node_names.items()  # type: ignore

        for _, node_id in node_iter:
            node: Node = self.nodes[node_id]

            row = {
                "src_name": node.name,
                "edge_id": -1,
                "rel_name": None,
                "dst_name": None,
                "truth": node.truth,
                "shadow": node.shadow,
                "is_rdf": node.is_rdf,
                "labels": ",".join(node.label_set),
                "props": self._save_props(node.prop_map, debug=debug),
            }

            yield row

            edge_id: NonNegativeInt = 0

            if sort:
                edge_rel_iter = sorted(node.edge_map.items())
            else:
                edge_rel_iter = node.edge_map.items()  # type: ignore

            for _, edge_list in edge_rel_iter:
                if sort:
                    edge_iter = sorted(edge_list, key=lambda e: self.nodes[e.node_id].name)
                else:
                    edge_iter = edge_list

                for edge in edge_iter:
                    row = {
                        "src_name": node.name,
                        "edge_id": edge_id,
                        "rel_name": self.edge_rels[edge.rel],
                        "dst_name": self.nodes[edge.node_id].name,
                        "truth": edge.truth,
                        "shadow": -1,
                        "is_rdf": node.is_rdf,
                        "labels": None,
                        "props": self._save_props(edge.prop_map, debug=debug),
                    }

                    yield row
                    edge_id += 1


    def to_df (
        self,
        *,
        sort: bool = False,
        debug: bool = False,
        ) -> pd.DataFrame:
        """
Represent the partition as a DataFrame.
        """
        return pd.DataFrame([
            row
            for row in self.iter_gen_rows(
                    sort = sort,
                    debug = debug,
            )
        ])


    def save_file_parquet (
        self,
        save_parq: cloudpathlib.AnyPath,
        *,
        sort: bool = False,
        debug: bool = False,
        ) -> None:
        """
Save a partition to a Parquet file.
        """
        table = pa.Table.from_pandas(
            self.to_df(
                sort = sort,
                debug = debug,
            ),
        )

        writer = pq.ParquetWriter(save_parq.as_posix(), table.schema)
        writer.write_table(table)
        writer.close()


    def save_file_csv (
        self,
        save_csv: cloudpathlib.AnyPath,
        *,
        encoding: str = "utf-8",
        sort: bool = False,
        debug: bool = False,
        ) -> None:
        """
Save a partition to a CSV file.
        """
        self.to_df(
            sort = sort,
            debug = debug,
        ).to_csv(
            save_csv.as_posix(),
            index = False,
            header = True,
            encoding = encoding,
            quoting = csv.QUOTE_NONNUMERIC,
        )


    def save_file_rdf (
        self,
        save_rdf: cloudpathlib.AnyPath,
        *,
        rdf_format: str = "ttl",
        encoding: str = "utf-8",
        sort: bool = False,
        debug: bool = False,
        ) -> None:
        """
Save a partition to an RDF file.
        """
        subj = None
        graph = rdflib.Graph()

        row_iter = self.iter_gen_rows(
            sort = sort,
            debug = debug,
        )

        for row in row_iter:
            if row["is_rdf"]:
                if row["edge_id"] < 0:
                    subj = rdflib.term.URIRef(row["src_name"])
                else:
                    pred = rdflib.term.URIRef(row["rel_name"])
                    objt = rdflib.term.URIRef(row["dst_name"])
                    
                    graph.add((subj, pred, objt))  # type: ignore

                    if debug:
                        ic(subj, pred, objt)

        graph.serialize(
            save_rdf,
            format = rdf_format,
            encoding = encoding,
        )
