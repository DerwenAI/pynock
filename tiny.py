#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
A minimal example using `pynock` to construct a partition
programmatically, based on the graph described in `dat/tiny.rdf`
"""

import typing

from icecream import ic
import cloudpathlib

from pynock import Partition, Node, Edge


if __name__ == "__main__":
    # initialize a partition
    part: Partition = Partition(
        part_id = 0,
    )

    # lookup/create the src node for the recipe
    # NB: this node has properties, which RDF cannot query
    src_name: str = "https://www.food.com/recipe/327593"
    src_node: typing.Optional[Node] = part.lookup_node(src_name)

    if src_node is None:
        src_node = Node(
            node_id = part.create_node_name(src_name),
            name = src_name,
            is_rdf = True,
            label_set = set(["Recipe"]),
            prop_map = {
                "minutes": 8,
                "name": "anytime crepes"
            },
        )

        part.add_node(src_node)

    # lookup/create a dst node for the "Egg" ingredient
    dst_name: str = "http://purl.org/heals/ingredient/ChickenEgg"
    dst_node: typing.Optional[Node] = part.lookup_node(dst_name)

    if dst_node is None:
        dst_node = Node(
            node_id = part.create_node_name(dst_name),
            name = dst_name,
            is_rdf = True,
            label_set = set(["Ingredient"]),
        )

        part.add_node(dst_node)

    # define an edge connecting src => dst for this ingredient
    edge: Edge = Edge(
        rel = part.get_edge_rel("http://purl.org/heals/food/uses_ingredient", create=True),
        node_id = dst_node.node_id,
    )

    src_node.add_edge(edge)

    # define a dst node for the "Milk" ingredient
    dst_name = "http://purl.org/heals/ingredient/CowMilk"

    dst_node = Node(
        node_id = part.create_node_name(dst_name),
        name = dst_name,
        is_rdf = True,
        label_set = set(["Ingredient"]),
    )

    part.add_node(dst_node)

    # define an edge connecting src => dst for this ingredient
    edge = Edge(
        rel = part.get_edge_rel("http://purl.org/heals/food/uses_ingredient", create=True),
        node_id = dst_node.node_id,
    )

    src_node.add_edge(edge)

    # define a dst node for the "Flour" ingredient
    # NB: this node has properties, which RDF cannot query
    dst_name = "http://purl.org/heals/ingredient/WholeWheatFlour"
    dst_node = part.lookup_node(dst_name)

    if dst_node is None:
        dst_node = Node(
            node_id = part.create_node_name(dst_name),
            name = dst_name,
            is_rdf = True,
            label_set = set(["Ingredient"]),
            prop_map = {
                "vegan": True,
            },
        )

        part.add_node(dst_node)

    # define an edge connecting src => dst for this ingredient
    edge = Edge(
        rel = part.get_edge_rel("http://purl.org/heals/food/uses_ingredient", create=True),
        node_id = dst_node.node_id,
    )

    src_node.add_edge(edge)

    # define a dst node for the "wtm:Recipe" parent
    dst_name = "http://purl.org/heals/food/Recipe"
    dst_node = part.lookup_node(dst_name)

    if dst_node is None:
        dst_node = Node(
            node_id = part.create_node_name(dst_name),
            name = dst_name,
            is_rdf = True,
            label_set = set(["top-level"]),
        )

        part.add_node(dst_node)

    # define an edge connecting src => dst for this inheritance
    edge = Edge(
        rel = part.get_edge_rel("http://www.w3.org/1999/02/22-rdf-syntax-ns#type", create=True),
        node_id = dst_node.node_id,
    )

    src_node.add_edge(edge)

    # serialize this partition to multiple formats
    part.save_file_parquet(
        cloudpathlib.AnyPath("foo.parq"),
    )

    part.save_file_csv(
        cloudpathlib.AnyPath("foo.csv"),
        sort = True,
    )

    part.save_file_rdf(
        cloudpathlib.AnyPath("foo.rdf"),
        rdf_format = "ttl",
    )

    # check the files "foo.*" to see what was constructed programmatically
    # also, here's a dataframe representation
    df = part.to_df()
    ic(df.head())

