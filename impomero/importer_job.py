#!/usr/bin/env python

"""Auto import script from a user directory
"""
import os
import logging
import tempfile

from pathlib import Path

import csv
import yaml
import omero

from omero.util import import_candidates
from omero.cli import CLI
from .collector import create_import_table, get_configuration


log = logging.getLogger(__name__)
logfile = logging.FileHandler("auto_importer.log")
log.setLevel("INFO")
log.addHandler(logfile)


def auto_import(
    base_dir, dry_run=False, import_table=None, reset=True, clean=False, **kwargs
):
    """Automatically import image data from the directories bellow base_dir

    The process starts by walking those directories to find annotation files.
    An annotation file must contain a `username` and a `project` entry.

    """

    base_dir = Path(base_dir)
    conf = get_configuration()
    conf["base_dir"] = base_dir
    if (import_table is None) or reset:
        import_table = create_import_table(base_dir)

    if not "group" in import_table:
        import_table["group"] = ""

    for (user, group), sub_table in import_table.groupby(["user", "group"]):

        _, bulk_yml = tempfile.mkstemp(suffix=".yml", text=True)
        log.info(f"creating bulk yaml {bulk_yml}")

        _, tsv_file = tempfile.mkstemp(suffix=".tsv", text=True)
        log.info(f"creating tsv_file {tsv_file}")

        _, out_file = tempfile.mkstemp(suffix=".out", text=True)
        log.info(f"creating out_file {out_file}")

        conf["username"] = user
        conf["group"] = group
        conf["bulk_yml"] = bulk_yml
        conf["tsv_file"] = tsv_file
        conf["out_file"] = out_file

        create_bulk_yml(bulk_yml=bulk_yml, dry_run=dry_run, path=tsv_file)
        sub_table[["target", "fileset", "file_path"]].to_csv(
            tsv_file, sep="\t", index=False, header=False, quoting=csv.QUOTE_NONE
        )

        if dry_run:
            print({k, v for k, v in conf if k != 'admin_passwd'})

        perform_import(conf, **kwargs)
        if clean:
            for tmp in (bulk_yml, tsv_file, out_file):
                os.remove(tmp)
    return conf, import_table


def perform_import(conf, transfer="ln_s"):

    # see https://docs.openmicroscopy.org/omero/5.6.2/users/cli/sessions.html

    cmd = [
        "import",
        "-s",
        conf["server"],
        "-p",
        conf["port"],
        "--sudo",
        "root",
        "-w",
        conf["admin_passwd"],
        "-u",
        conf["username"],
        "--exclude",
        "clientpath",
        "--file",
        Path(conf["out_file"]).absolute().as_posix(),
        "--errs",
        Path("err.txt").absolute().as_posix(),
        "--bulk",
        conf["bulk_yml"],
    ]
    if conf["group"]:
        cmd.extend(["-g", conf["group"]])

    if transfer:
        cmd.extend(["--transfer", transfer])

    cli = CLI()
    cli.loadplugins()
    pwd_idx = cmd.index(conf["admin_passwd"])
    cmd_str = cmd.copy()
    cmd_str[pwd_idx] = "XXX"
    log.info("Invoking omero %s", " ".join(cmd_str))
    cli.invoke(cmd)


def create_bulk_yml(bulk_yml="bulk.yml", **kwargs):
    """Creates the bulk.yml file in the current directory

    Any keyword argument will update the default setting.

    See https://docs.openmicroscopy.org/omero/5.6.2/users/cli/import-bulk.html
    for more info

    Defaults
    --------

    "path": "files.tsv",
    "columns": ["target", "name", "path"],
    "continue": True,
    "exclude": "clientpath",
    "checksum_algorithm": "CRC-32",

    Example
    -------

    # create a bulk.yml file that will result in a dry-run
    >>> create_bulk_yml(dry_run=True)

    """

    bulk_opts = {
        "path": "files.tsv",
        "columns": ["target", "name", "path"],
        "continue": True,
        "exclude": "clientpath",
        "checksum_algorithm": "CRC-32",
    }
    bulk_opts.update(kwargs)

    log.info("bulk options: %s", bulk_opts)
    with open(bulk_yml, "w") as yml_file:
        yaml.dump(bulk_opts, yml_file)
