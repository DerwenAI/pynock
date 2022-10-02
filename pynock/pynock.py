#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Example graph serialization using low-level Parquet read/write
efficiently in Python.
"""

import csv
import json
import sys
import typing

from icecream import ic  # type: ignore  # pylint: disable=E0401
from pydantic import BaseModel  # pylint: disable=E0401,E0611
from rich.progress import track  # pylint: disable=E0401
import cloudpathlib
import kglab
import pandas as pd
import pyarrow as pa  # type: ignore  # pylint: disable=E0401
import pyarrow.lib  # type: ignore  # pylint: disable=E0401
import pyarrow.parquet as pq  # type: ignore  # pylint: disable=E0401
import rdflib


######################################################################
## non-class definitions

GraphRow = typing.Dict[str, typing.Any]
PropMap = typing.Dict[str, typing.Any]

NOT_FOUND: int = -1


######################################################################
## edges

class Edge (BaseModel):  # pylint: disable=R0903
    """
Representing an edge (arc) in the graph.
    """
    BLANK_RELATION: typing.ClassVar[int] = 0

    rel: int = BLANK_RELATION
    node_id: int = NOT_FOUND
    truth: float = 1.0
    prop_map: PropMap = {}


######################################################################
## nodes

class Node (BaseModel):  # pylint: disable=R0903
    """
Representing a node (entity) in the graph.
    """
    BASED_LOCAL: typing.ClassVar[int] = -1

    node_id: int = NOT_FOUND
    name: str = ""
    shadow: int = BASED_LOCAL
    is_rdf: bool = False
    label_set: typing.Set[str] = set()
    truth: float = 1.0
    prop_map: PropMap = {}
    edge_map: typing.Dict[int, list] = {}


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
    PROPS_NULL: typing.ClassVar[str] = "null"

    part_id: int = NOT_FOUND
    next_node: int = 0
    nodes: typing.Dict[int, Node] = {}
    node_names: typing.Dict[str, int] = {}
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
        node_id: int = NOT_FOUND

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
    def load_props (
        cls,
        props: str,
        *,
        debug: bool = False,  # pylint: disable=W0613
        ) -> PropMap:
        """
Load property pairs from a JSON string.
        """
        prop_map: PropMap = {}

        if props not in (cls.PROPS_NULL, ""):
            prop_map = json.loads(props)

        return prop_map


    @classmethod
    def save_props (
        cls,
        prop_map: PropMap,
        *,
        debug: bool = False,  # pylint: disable=W0613
        ) -> str:
        """
Save property pairs to a JSON string.
        """
        props: str = cls.PROPS_NULL

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


    def populate_node (
        self,
        row: GraphRow,
        *,
        debug: bool = False,  # pylint: disable=W0613
        ) -> Node:
        """
Populate a Node object from the given Parquet row data.
        """
        # create a src node
        node: Node = Node(
            node_id = self.create_node_name(row["src_name"]),
            name = row["src_name"],
            truth = row["truth"],
            is_rdf = row["is_rdf"],
            shadow = row["shadow"],
            label_set = set(row["labels"].split(",")),
            prop_map = self.load_props(row["props"]),
        )

        self.add_node(node)

        return node


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
        dst_node: typing.Optional[Node] = self.lookup_node(dst_name)

        if dst_node is None:
            dst_node = Node(
                node_id = self.create_node_name(dst_name),
                name = dst_name,
                truth = row["truth"],
                is_rdf = row["is_rdf"],
            )

            self.add_node(dst_node)

        # create the edge
        edge: Edge = Edge(
            rel = self.get_edge_rel(row["rel_name"], create=True),
            truth = row["truth"],
            node_id = dst_node.node_id,
            prop_map = self.load_props(row["props"]),
        )

        node.add_edge(edge)

        return edge


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
        row_num: int = 0

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
                        return

                if debug:
                    print()
                    ic(r_idx, row)

                yield row_num, row
                row_num += 1


    def iter_load_csv (
        self,
        csv_path: cloudpathlib.AnyPath,
        *,
        debug: bool = False,
        ) -> typing.Iterable[typing.Tuple[int, GraphRow]]:
        """
Iterate through the rows in a CSV file.
        """
        row_num: int = 0

        with open(csv_path) as fp:
            reader = csv.reader(fp, delimiter=",", quotechar='"')
            header = next(reader)

            for row_val in reader:
                row: GraphRow = dict(zip(header, row_val))
                row["edge_id"] = int(row["edge_id"])
                row["is_rdf"] = bool(row["is_rdf"])
                row["shadow"] = int(row["shadow"])
                row["truth"] = float(row["truth"])

                yield row_num, row
                row_num += 1


    def iter_load_rdf (
        self,
        rdf_path: cloudpathlib.AnyPath,
        rdf_format: str,
        *,
        debug: bool = False,
        ) -> typing.Iterable[typing.Tuple[int, GraphRow]]:
        """
Iterate through the rows implied by a RDF file.
        """
        row_num: int = 0

        kg = kglab.KnowledgeGraph()
        kg.load_rdf(rdf_path, format=rdf_format)

        for subj, pred, objt in kg.rdf_graph():
            if debug:
                ic(subj, pred, objt)

            # node representation for a triple
            row: GraphRow = {}
            row["src_name"] = str(subj)
            row["truth"] = 1.0
            row["edge_id"] = NOT_FOUND
            row["rel_name"] = None
            row["dst_name"] = None
            row["is_rdf"] = True
            row["shadow"] = Node.BASED_LOCAL
            row["labels"] = ""
            row["props"] = self.PROPS_NULL

            if debug:
                ic("node", subj, row_num, row)

            yield row_num, row
            row_num += 1

            # edge representation for a triple
            row = {}
            row["src_name"] = str(subj)
            row["truth"] = 1.0
            row["edge_id"] = 1
            row["rel_name"] = str(pred)
            row["dst_name"] = str(objt)
            row["is_rdf"] = True
            row["shadow"] = Node.BASED_LOCAL
            row["labels"] = ""
            row["props"] = self.PROPS_NULL

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
                node: Node = self.populate_node(row)

                if debug:
                    print()
                    ic(node)

            # validate the node/edge sequencing and consistency among the rows
            elif row["src_name"] != node.name:
                error_node = row["src_name"]
                message = f"|{ error_node }| out of sequence at row {row_num}"
                raise ValueError(message)

            # otherwise this row is an edge for the most recent node
            else:
                edge: Edge = self.populate_edge(row, node)

                if debug:
                    ic(edge)


    def iter_gen_rows (
        self,
        ) -> typing.Iterable[GraphRow]:
        """
Iterator for generating rows on writes.
        """
        for node in self.nodes.values():
            yield {
                "src_name": node.name,
                "edge_id": -1,
                "rel_name": None,
                "dst_name": None,
                "truth": node.truth,
                "shadow": node.shadow,
                "is_rdf": node.is_rdf,
                "labels": ",".join(node.label_set),
                "props": self.save_props(node.prop_map),
            }

            edge_id: int = 0

            for _, edge_list in node.edge_map.items():
                for edge in edge_list:
                    yield {
                        "src_name": node.name,
                        "edge_id": edge_id,
                        "rel_name": self.edge_rels[edge.rel],
                        "dst_name": self.nodes[edge.node_id].name,
                        "truth": edge.truth,
                        "shadow": -1,
                        "is_rdf": node.is_rdf,
                        "labels": None,
                        "props": self.save_props(edge.prop_map),
                    }

                    edge_id += 1


    def to_df (
        self,
        ) -> pd.DataFrame:
        """
Represent the partition as a DataFrame.
        """
        return pd.DataFrame([row for row in self.iter_gen_rows()])


    def save_file_parquet (
        self,
        save_parq: cloudpathlib.AnyPath,
        *,
        debug: bool = False,
        ) -> None:
        """
Save a partition to a Parquet file.
        """
        table = pa.Table.from_pandas(self.to_df())
        writer = pq.ParquetWriter(save_parq.as_posix(), table.schema)
        writer.write_table(table)
        writer.close()


    def save_file_csv (
        self,
        save_csv: cloudpathlib.AnyPath,
        *,
        debug: bool = False,
        ) -> None:
        """
Save a partition to a CSV file.
        """
        self.to_df().to_csv(save_csv.as_posix(), index=False)


    def save_file_rdf (
        self,
        save_rdf: cloudpathlib.AnyPath,
        rdf_format: str,
        *,
        debug: bool = False,
        ) -> None:
        """
Save a partition to an RDF file.
        """
        kg = kglab.KnowledgeGraph()
        subj = None

        for row in self.iter_gen_rows():
            if row["is_rdf"]:

                if row["edge_id"] < 0:
                    subj = rdflib.term.URIRef(row["src_name"])
                else:
                    pred = rdflib.term.URIRef(row["rel_name"])
                    objt = rdflib.term.URIRef(row["dst_name"])
                    kg.add(subj, pred, objt)

                    if debug:
                        ic(subj, pred, objt)

        kg.save_rdf(save_rdf, format=rdf_format)
