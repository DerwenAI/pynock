#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Unit test coverage:

CSV => RDF

  * read a CSV file
  * construct a Partition internally
  * write an RDF file (TTL format)
"""

import tempfile

from icecream import ic
import cloudpathlib
import pyarrow.parquet as pq  # type: ignore
import pytest

from pynock import Partition


def test_parq_csv ():
    try:
        load_csv: str = "dat/tiny.csv"
        load_rdf: str = "dat/tiny.ttl"
        tmp_obs = tempfile.NamedTemporaryFile(mode="w+b", delete=True)

        # construct a Partition
        part: Partition = Partition(
            part_id = 0,
        )

        part.parse_rows(
            part.iter_load_csv(
                cloudpathlib.AnyPath(load_csv),
            ),
        )

        # write the partition as an RDF file
        part.save_file_rdf(
            cloudpathlib.AnyPath(tmp_obs.name),
            rdf_format = "ttl",
            sort = True,
        )

        # compare the respective texts
        obs_text: str = cloudpathlib.AnyPath(tmp_obs.name).read_text()
        exp_text: str = cloudpathlib.AnyPath(load_rdf).read_text()

        assert exp_text == obs_text

    except Exception as ex:
        ic(ex)

    finally:
        tmp_obs.close()
