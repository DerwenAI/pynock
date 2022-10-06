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


@APP.command("load-parq")
def cli_load_parq (
    *,
    load_parq: str = typer.Option(..., "--file", "-f", help="input Parquet file"),
    save_csv: str = typer.Option(None, "--save-csv", help="output as CSV"),
    save_rdf: str = typer.Option(None, "--save-rdf", help="output as RDF"),
    rdf_format: str = typer.Option("ttl", "--format", help="RDF format: ttl, rdf, jsonld, etc."),
    encoding: str = typer.Option("utf-8", "--encoding", help="output encoding"),
    dump: bool = typer.Option(False, "--dump", help="dump the data, only"),
    sort: bool = typer.Option(False, "--sort", help="sort the output"),
    debug: bool = False,
    ) -> None:
    """
Load a Parquet file into a graph partition, optionally converting and
saving to different formats.
    """
    part: Partition = Partition(
        part_id = 0,
    )

    parq_file: pq.ParquetFile = pq.ParquetFile(load_parq)

    # in this case, only print what Parquet has parsed then quit
    if dump:
        part.dump_parquet(parq_file)
        return

    part.parse_rows(
        part.iter_load_parquet(
            parq_file,
            debug = debug,
        ),
        debug = debug,
    )

    if debug:
        ic(part)

    # next, handle the output options
    if save_csv is not None:
        part.save_file_csv(
            cloudpathlib.AnyPath(save_csv),
            encoding = encoding,
            sort = sort,
            debug = debug,
        )

    if save_rdf is not None:
        part.save_file_rdf(
            cloudpathlib.AnyPath(save_rdf),
            rdf_format = rdf_format,
            encoding = encoding,
            sort = sort,
            debug = debug,
        )


@APP.command("load-csv")
def cli_load_csv (
    *,
    load_csv: str = typer.Option(..., "--file", "-f", help="input CSV file"),
    save_parq: str = typer.Option(None, "--save-parq", help="output as Parquet"),
    save_rdf: str = typer.Option(None, "--save-rdf", help="output as RDF"),
    rdf_format: str = typer.Option("ttl", "--format", help="RDF format: ttl, rdf, jsonld, etc."),
    encoding: str = typer.Option("utf-8", "--encoding", help="output encoding"),
    sort: bool = typer.Option(False, "--sort", help="sort the output"),
    debug: bool = False,
    ) -> None:
    """
Load a CSV file into a graph partition, optionally converting and
saving to different formats.
    """
    part: Partition = Partition(
        part_id = 0,
    )

    part.parse_rows(
        part.iter_load_csv(
            cloudpathlib.AnyPath(load_csv),
            encoding = encoding,
            debug = debug,
        ),
        debug = debug,
    )

    if debug:
        ic(part)

    # next, handle the output options
    if save_parq is not None:
        part.save_file_parquet(
            cloudpathlib.AnyPath(save_parq),
            sort = sort,
            debug = debug,
        )

    if save_rdf is not None:
        part.save_file_rdf(
            cloudpathlib.AnyPath(save_rdf),
            rdf_format = rdf_format,
            encoding = encoding,
            sort = sort,
            debug = debug,
        )


@APP.command("load-rdf")
def cli_load_rdf (
    *,
    load_rdf: str = typer.Option(..., "--file", "-f", help="input RDF file"),
    rdf_format: str = typer.Option("ttl", "--format", help="RDF format: ttl, rdf, jsonld, etc."),
    save_parq: str = typer.Option(None, "--save-parq", help="output as Parquet"),
    save_csv: str = typer.Option(None, "--save-csv", help="output as CSV"),
    encoding: str = typer.Option("utf-8", "--encoding", help="output encoding"),
    sort: bool = typer.Option(False, "--sort", help="sort the output"),
    debug: bool = False,
    ) -> None:
    """
Load an RDF file into a graph partition, optionally converting and
saving to different formats.
    """
    part: Partition = Partition(
        part_id = 0,
    )

    part.parse_rows(
        part.iter_load_rdf(
            cloudpathlib.AnyPath(load_rdf),
            rdf_format = rdf_format,
            encoding = encoding,
            debug = debug,
        ),
    )

    if debug:
        ic(part)

    # next, handle the output options
    if save_parq is not None:
        part.save_file_parquet(
            cloudpathlib.AnyPath(save_parq),
            sort = sort,
            debug = debug,
        )

    if save_csv is not None:
        part.save_file_csv(
            cloudpathlib.AnyPath(save_csv),
            encoding = encoding,
            sort = sort,
            debug = debug,
        )


if __name__ == "__main__":
    APP()
