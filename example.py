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
    save_parq: str = typer.Option(None, "--save-parq", help="output Parquet"),
    save_ttl: str = typer.Option(None, "--save-ttl", help="output TTL"),
    debug: bool = False,
    ) -> None:
    """
Load a Parquet file into a graph partition.
    """
    pq_file: pq.ParquetFile = pq.ParquetFile(load_parq)

    if debug:
        ic(pq_file.metadata)
        ic(pq_file.schema)
        ic(type(pq_file.schema))

    part: Partition = Partition(
        part_id = 0,
    )

    part.load_rows(pq_file)

    if debug:
        ic(part)

    # next, handle the output options
    if save_csv is not None:
        part.save_file_csv(cloudpathlib.AnyPath(save_csv))

    if save_parq is not None:
        part.save_file_parquet(cloudpathlib.AnyPath(save_parq))


@APP.command()
def load_csv ():
    """
Wherein we do stuff with CSV files.
    """
    pass


if __name__ == "__main__":
    APP()
