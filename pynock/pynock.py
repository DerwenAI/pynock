#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Example graph serialization using low-level Parquet read/write
efficiently in Python.
"""

import json
import typing

from icecream import ic  # type: ignore  # pylint: disable=E0401
from pydantic import BaseModel  # pylint: disable=E0401,E0611
from rich.progress import track  # pylint: disable=E0401
import cloudpathlib
import pandas as pd
import pyarrow as pa  # type: ignore  # pylint: disable=E0401
import pyarrow.lib  # type: ignore  # pylint: disable=E0401
import pyarrow.parquet as pq  # type: ignore  # pylint: disable=E0401


NOT_FOUND: int = -1


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
    is_rdf: bool = True
    label_set: typing.Set[str] = set()
    truth: float = 1.0
    prop_map: typing.Dict[str, str] = {}
    edge_map: typing.Dict[int, list] = {}


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
    prop_map: typing.Dict[str, str] = {}


######################################################################
## partitions

class Partition (BaseModel):  # pylint: disable=R0903
    """
Representing a partition in the graph.
    """
    part_id: int = NOT_FOUND
    next_node: int = 0
    nodes: typing.Dict[int, Node] = {}
    node_names: typing.Dict[str, int] = {}
    edge_rels: typing.List[str] = [ "" ]


    def create_node (
        self,
        node_name: str,
        *,
        debug: bool = False,  # pylint: disable=W0613
        ) -> int:
        """
Create a node, looking up first to avoid duplicates.
        """
        node_id: int = NOT_FOUND

        if node_name in self.node_names:
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
        ) -> typing.Dict[str, str]:
        """
Load property pairs from a JSON string.
        """
        prop_map: typing.Dict[str, str] = {}

        if props not in ("null", ""):
            prop_map = json.loads(props)

        return prop_map


    @classmethod
    def save_props (
        cls,
        prop_map: typing.Dict[str, str],
        *,
        debug: bool = False,  # pylint: disable=W0613
        ) -> str:
        """
Save property pairs to a JSON string.
        """
        props: str = "null"

        if len(prop_map) > 0:
            props = json.dumps(prop_map)
            props = props.replace("\": \"", "\":\"")
            props = props.replace("\", \"", "\",\"")

        return props


    def populate_node (
        self,
        row: dict,
        *,
        debug: bool = False,  # pylint: disable=W0613
        ) -> Node:
        """
Populate a Node object from the given Parquet row data.
        """
        node: Node = Node(
            name = row["src_name"],
            truth = row["truth"],
            is_rdf = row["is_rdf"],
            shadow = row["shadow"],
            label_set = set(row["labels"].split(",")),
            prop_map = self.load_props(row["props"]),
        )

        # add this node to the global list
        node.node_id = self.create_node(node.name)
        self.nodes[node.node_id] = node

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
            self.edge_rels.append(rel_name)

        return self.edge_rels.index(rel_name)


    def populate_edge (
        self,
        row: dict,
        node: Node,
        *,
        debug: bool = False,  # pylint: disable=W0613
        ) -> Edge:
        """
Populate an Edge object from the given Parquet row data.
        """
        edge: Edge = Edge(
            rel = self.get_edge_rel(row["rel_name"], create=True),
            truth = row["truth"],
            node_id = self.create_node(row["dst_name"]),
            prop_map = self.load_props(row["props"]),
        )

        # add this edge to its src node
        if edge.rel not in node.edge_map:
            node.edge_map[edge.rel] = []

        node.edge_map[edge.rel].append(edge)

        return edge


    @classmethod
    def iter_row_group (
        cls,
        row_group: pyarrow.lib.Table,  # pylint: disable=I1101
        *,
        debug: bool = False,  # pylint: disable=W0613
        ) -> typing.Iterable:
        """
Iterate through the rows in a Parquet row group.
        """
        for r_idx in range(row_group.num_rows):
            row: dict = {}

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

            yield row


    def load_rows (
        self,
        pq_file: pq.ParquetFile,
        *,
        debug: bool = False,
        ) -> None:
        """
Load and parse all of the Parquet rows into a graph partition.
        """
        for i in range(pq_file.num_row_groups):
            row_group: pyarrow.lib.Table = pq_file.read_row_group(i)  # pylint: disable=I1101

            for row in track(self.iter_row_group(row_group), description=f"row group {i}"):
                # have we reached a row which begins a new node?
                if row["edge_id"] < 0:
                    node = self.populate_node(row)

                    if debug:
                        print()
                        ic(node)

                # otherwise this row is an edge for the most recent node
                else:
                    assert row["src_name"] == node.name
                    # 'edge_id': 2,

                    edge = self.populate_edge(row, node)

                    if debug:
                        ic(edge)


    def iter_gen_rows (
        self,
        ) -> typing.Iterable:
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

            for _, edge_list in node.edge_map.items():
                for edge_id, edge in enumerate(edge_list):
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


    def save_file_parquet (
        self,
        save_parq: cloudpathlib.AnyPath,
        *,
        debug: bool = False,
        ) -> None:
        """
Save a partition to a Parquet file.
        """
        df = pd.DataFrame([ row for row in self.iter_gen_rows() ])
        table = pa.Table.from_pandas(df)
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
        df = pd.DataFrame([ row for row in self.iter_gen_rows() ])
        df.to_csv(save_csv.as_posix(), index=False)
