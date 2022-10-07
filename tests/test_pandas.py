#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Unit test coverage:

Pandas handling of missing values:

  * read a Parquet file
  * read a CSV file
"""

from icecream import ic
import cloudpathlib
import pandas as pd
import pytest

from pynock import Partition


def test_pandas ():
    df_csv = pd.read_csv(
        cloudpathlib.AnyPath("dat/tiny.csv"),
    ).fillna("").sort_values(Partition.SORT_COLUMNS).reset_index(drop=True)

    ic(df_csv.iloc[:, [2, 3, 7]])

    df_parq = pd.read_parquet(
        cloudpathlib.AnyPath("dat/tiny.parq"),
        use_nullable_dtypes = True,
    ).fillna("").sort_values(Partition.SORT_COLUMNS).reset_index(drop=True)

    ic(df_parq.iloc[:, [2, 3, 7]])

    # general diff
    ic(df_csv.compare(df_parq))
    assert len(df_csv.compare(df_parq)) == 0

if __name__ == "__main__":
    test_pandas()
