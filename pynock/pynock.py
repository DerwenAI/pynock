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
import pyarrow.parquet as pq  # type: ignore  # pylint: disable=E0401
import pyarrow.lib  # type: ignore  # pylint: disable=E0401
from rich.progress import track  # pylint: disable=E0401


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


    def populate_node (
        self,
        dat: dict,
        *,
        debug: bool = False,  # pylint: disable=W0613
        ) -> Node:
        """
Populate a Node object from the given Parquet row data.
        """
        node: Node = Node(
            name = dat["src_name"],
            truth = dat["truth"],
            is_rdf = dat["is_rdf"],
            shadow = dat["shadow"],
            label_set = set(dat["labels"].split(",")),
            prop_map = self.load_props(dat["props"]),
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
        dat: dict,
        node: Node,
        *,
        debug: bool = False,  # pylint: disable=W0613
        ) -> Edge:
        """
Populate an Edge object from the given Parquet row data.
        """
        edge: Edge = Edge(
            rel = self.get_edge_rel(dat["rel_name"], create=True),
            truth = dat["truth"],
            node_id = self.create_node(dat["dst_name"]),
            prop_map = self.load_props(dat["props"]),
        )

        # add this edge to its src node
        if edge.rel not in node.edge_map:
            node.edge_map[edge.rel] = []

        node.edge_map[edge.rel].append(edge)

        return edge


    @classmethod
    def iter_rows (
        cls,
        row_group: pyarrow.lib.Table,  # pylint: disable=I1101
        *,
        debug: bool = False,  # pylint: disable=W0613
        ) -> typing.Iterable:
        """
Iterate through the rows in a Parquet row group.
        """
        for r_idx in range(row_group.num_rows):
            dat: dict = {}

            for c_idx in range(row_group.num_columns):
                try:
                    key: str = row_group.column_names[c_idx]
                    col: pyarrow.lib.ChunkedArray = row_group.column(c_idx)  # pylint: disable=I1101
                    val: typing.Any = col[r_idx]
                    dat[key] = val.as_py()
                except IndexError as ex:
                    ic(ex, r_idx, c_idx)
                    return

            if debug:
                print()
                ic(r_idx, dat)

            yield dat


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

            for dat in track(self.iter_rows(row_group), description=f"row group {i}"):
                # have we reached a row which begins a new node?
                if dat["edge_id"] < 0:
                    node = self.populate_node(dat)

                    if debug:
                        print()
                        ic(node)

                # otherwise this row is an edge for the most recent node
                else:
                    assert dat["src_name"] == node.name
                    # 'edge_id': 2,

                    edge = self.populate_edge(dat, node)

                    if debug:
                        ic(edge)
