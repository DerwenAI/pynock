#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Unit test coverage:

  * read a Parquet file
  * construct a Partition internally
  * write a CSV file
"""

import tempfile

from icecream import ic
import cloudpathlib
import pyarrow.parquet as pq  # type: ignore
import pytest

from pynock import Partition


def test_save_file_csv ():
    tmp_exp = tempfile.NamedTemporaryFile(mode="w+b", delete=True)
    tmp_obs = tempfile.NamedTemporaryFile(mode="w+b", delete=True)

    try:
        load_parq: str = "dat/recipes.parq"
        parq_file: pq.ParquetFile = pq.ParquetFile(load_parq)

        # leverage Arrow to convert the "exp" baseline
        for batch in parq_file.iter_batches():
            df = batch.to_pandas()
            df.to_csv(tmp_exp.name, index=False)
            break

        # construct a Partition internally
        part: Partition = Partition(
            part_id = 0,
        )

        part.load_rows_parquet(parq_file)

        # write the partition as a CSV file
        part.save_file_csv(cloudpathlib.AnyPath(tmp_obs.name))

        # compare the respective texts
        exp_text: str = cloudpathlib.AnyPath(tmp_exp.name).read_text()
        obs_text: str = cloudpathlib.AnyPath(tmp_obs.name).read_text()

        assert exp_text == obs_text

    except Exception as ex:
        ic(ex)

    finally:
        tmp_exp.close()
        tmp_obs.close()
