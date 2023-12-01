"""Rucio Transfer Manager - command line interface.

Keep track of a large number of rucio transfers

Usage:
    rtm define 
    rtm start
    rtm status
    rtm verify
    rtm delete
"""
import logging
import pathlib
import sys

import click
from click_loglevel import LogLevel
import yaml

from rtm import manager
from rtm import config
from rtm import utils


logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")

log = logging.getLogger()
cfg = config.get()


@click.group
@click.option(
    "-l",
    "--log-level",
    type=LogLevel(),
    default="INFO",
    help="Set logging level",
    show_default=True,
)
@click.option(
    "-c",
    "--config",
    "config_path",
    default=config.DEFAULT_CONFIG_PATH,
    type=click.Path(dir_okay=False, path_type=pathlib.Path),
    help="Configuration File",
    show_default=True,
)
def main(log_level, config_path: pathlib.Path):
    """Rucio Transfer Manager - command line interface."""
    log.setLevel(log_level)
    logging.getLogger("transitions.core").setLevel(logging.WARNING)
    cfg.load(config_path)
    if not utils.voms_proxy():
        log.fatal("No valid proxy")
        sys.exit(1)


@main.command
@click.argument("datasets", nargs=-1)
@click.option("--site")
@click.option(
    "--file",
    "yaml_path",
    type=click.Path(exists=True, dir_okay=False, path_type=pathlib.Path),
)
def define(site: str, datasets: list[str], yaml_path: pathlib.Path | None) -> None:
    """Define dataset in the DB"""
    mngr = manager.Manager()
    if yaml_path is None:
        mngr.add_datasets(datasets, site)
    else:
        with open(yaml_path, "r") as yaml_inp:
            data = yaml.safe_load(yaml_inp)
        mngr.add_datasets(data["datasets"], data["site"])


@main.command
@click.argument("pattern", default="")
def subscribe(pattern: str) -> None:
    """Start transfer."""
    mngr = manager.Manager()
    mngr.subscribe(pattern)
    mngr.list_datasets(pattern)


@main.command
@click.argument("pattern", default="")
@click.option("--update/--no-update", default=True, help="Update subscription status")
def status(pattern: str, update: bool) -> None:
    """List status of transfers."""
    mngr = manager.Manager()
    if update:
        mngr.update(pattern)
    mngr.list_datasets(pattern)


@main.command
@click.argument("pattern", default="")
def verify(pattern: str):
    """verify the transferred dataset."""
    mngr = manager.Manager()
    mngr.verify(pattern)
    mngr.list_datasets(pattern)


@main.command
def delete():
    """Delete subscription."""
    log.warning("Not implemented yet")


@main.command
def whoami():
    """Effective rucio client account."""
    mngr = manager.Manager()
    mngr.whoami()

@main.command
def summary():
    """Summarise the status of all datasets"""
    mngr = manager.Manager()
    mngr.summary()

