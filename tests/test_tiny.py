#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Unit test coverage:

  * construct a partition programmatically
  * compare with a reference CSV file
"""

import tempfile

from icecream import ic
import cloudpathlib
import pytest

from pynock import Partition, Node, Edge


def test_tiny ():
    try:
        load_csv: str = "dat/tiny.csv"
        tmp_obs = tempfile.NamedTemporaryFile(mode="w+b", delete=True)

        # construct a Partition
        part: Partition = Partition(
            part_id = 0,
        )

        # lookup/create the src node for the recipe
        # NB: this node has properties, which RDF cannot query
        src_name: str = "https://www.food.com/recipe/327593"
        src_node: Node = part.find_or_create_node(src_name)

        src_node.is_rdf = True
        src_node.label_set = set(["Recipe"])
        src_node.prop_map = {
            "minutes": 8,
            "name": "anytime crepes",
        }

        # lookup/create a dst node for the "Egg" ingredient
        dst_name: str = "http://purl.org/heals/ingredient/ChickenEgg"
        dst_node: Node = part.find_or_create_node(dst_name)

        dst_node.is_rdf = True
        dst_node.label_set = set(["Ingredient"])

        # define an edge connecting src => dst for this ingredient
        part.create_edge(
            src_node,
            "http://purl.org/heals/food/uses_ingredient",
            dst_node,
        )

        # define a dst node for the "Milk" ingredient
        dst_name = "http://purl.org/heals/ingredient/CowMilk"
        dst_node = part.find_or_create_node(dst_name)

        dst_node.is_rdf = True
        dst_node.label_set = set(["Ingredient"])

        # define an edge connecting src => dst for this ingredient
        part.create_edge(
            src_node,
            "http://purl.org/heals/food/uses_ingredient",
            dst_node,
        )

        # define a dst node for the "Flour" ingredient
        # NB: this node has properties, which RDF cannot query
        dst_name = "http://purl.org/heals/ingredient/WholeWheatFlour"
        dst_node = part.find_or_create_node(dst_name)

        dst_node.is_rdf = True
        dst_node.label_set = set(["Ingredient"])
        dst_node.prop_map = {
            "vegan": True,
        }

        # define an edge connecting src => dst for this ingredient
        part.create_edge(
            src_node,
            "http://purl.org/heals/food/uses_ingredient",
            dst_node,
        )

        # define a dst node for the "wtm:Recipe" parent
        dst_name = "http://purl.org/heals/food/Recipe"
        dst_node = part.find_or_create_node(dst_name)

        dst_node.is_rdf = True
        dst_node.label_set = set(["top_level"])

        # define an edge connecting src => dst for this inheritance
        part.create_edge(
            src_node,
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
            dst_node,
        )

        # write the partition as a CSV file
        part.save_file_csv(
            cloudpathlib.AnyPath(tmp_obs.name),
            encoding = "utf-8",
            sort = True,
        )

        # compare the respective texts
        obs_text: str = cloudpathlib.AnyPath(tmp_obs.name).read_text()
        exp_text: str = cloudpathlib.AnyPath(load_csv).read_text()

        assert exp_text == obs_text

    except Exception as ex:
        ic(ex)

    finally:
        tmp_obs.close()
