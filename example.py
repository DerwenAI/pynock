#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Examples code for using `pynock`
"""

from icecream import ic  # type: ignore
import cloudpathlib
import pyarrow.parquet as pq  # type: ignore
import typer

from pynock import Partition

APP = typer.Typer()


@APP.command()
def load_parquet (
    *,
    load_parq: str = typer.Option(..., "--file", "-f", help="input Parquet file"),
    save_csv: str = typer.Option(None, "--save-csv", help="output as CSV"),
    debug: bool = False,
    ) -> None:
    """
Load a Parquet file into a graph partition, optionally converting and
saving to different formats.
    """
    parq_file: pq.ParquetFile = pq.ParquetFile(load_parq)

    if debug:
        ic(parq_file.metadata)
        ic(parq_file.schema)
        ic(type(parq_file.schema))

    part: Partition = Partition(
        part_id = 0,
    )

    part.load_rows_parquet(parq_file)

    if debug:
        ic(part)

    # next, handle the output options
    if save_csv is not None:
        part.save_file_csv(cloudpathlib.AnyPath(save_csv))


@APP.command()
def load_csv (
    *,
    load_csv: str = typer.Option(..., "--file", "-f", help="input CSV file"),
    save_parq: str = typer.Option(None, "--save-parq", help="output Parquet"),
    debug: bool = False,
    ) -> None:
    """
Load a CSV file into a graph partition, optionally converting and
saving to different formats.
    """
    part: Partition = Partition(
        part_id = 0,
    )

    part.load_rows_csv(cloudpathlib.AnyPath(load_csv))

    if debug:
        ic(part)

    if save_parq is not None:
        part.save_file_parquet(cloudpathlib.AnyPath(save_parq))


if __name__ == "__main__":
    APP()
