#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Unit test coverage:

RDF => CSV

  * read an RDF file (TTL format)
  * construct a Partition internally
  * write a CSV file
"""

import tempfile

from icecream import ic
import cloudpathlib
import pyarrow.parquet as pq  # type: ignore
import pytest

from pynock import Partition


def test_parq_csv ():
    try:
        load_rdf: str = "dat/tiny.ttl"
        load_csv: str = "dat/tiny.csv"
        tmp_obs = tempfile.NamedTemporaryFile(mode="w+b", delete=True)

        # construct a Partition
        part: Partition = Partition(
            part_id = 0,
        )

        part.parse_rows(
            part.iter_load_rdf(
                cloudpathlib.AnyPath(load_rdf),
                rdf_format,
            ),
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
