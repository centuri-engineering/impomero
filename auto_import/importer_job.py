#!/usr/bin/env python

"""Auto import script from a user directory
"""
import os
import logging
import tempfile
from datetime import date

from pathlib import Path

import yaml
import omero

from omero.util import import_candidates
from omero.cli import CLI

log = logging.getLogger(__name__)
logfile = logging.FileHandler("auto_importer.log")
log.setLevel("INFO")
log.addHandler(logfile)


def auto_import(base_dir, dry_run=False, reset=True, clean=False):

    base_dir = Path(base_dir)
    conf = get_configuration(base_dir)

    conf["base_dir"] = base_dir

    _, bulk_yml = tempfile.mkstemp(suffix=".yml", text=True)
    log.info(f"creating bulk yaml {bulk_yml}")

    _, tsv_file = tempfile.mkstemp(suffix=".tsv", text=True)
    log.info(f"creating tsv_file {tsv_file}")

    _, out_file = tempfile.mkstemp(suffix=".out", text=True)
    log.info(f"creating out_file {out_file}")

    conf["bulk_yml"] = bulk_yml
    conf["tsv_file"] = tsv_file
    conf["out_file"] = out_file

    log.info("\n".join(f"{k}: {v}" for k, v in conf.items()))

    create_bulk_yml(bulk_yml=bulk_yml, dry_run=dry_run, path=tsv_file)

    if not Path(tsv_file).is_file() or reset:
        candidates = import_candidates.as_dictionary(base_dir.as_posix())
        create_bulk_tsv(candidates, base_dir, conf)

    if dry_run:
        return conf

    perform_import(conf)
    if clean:
        for tmp in (bulk_yml, tsv_file, out_file):
            os.remove(tmp)

    return conf


def perform_import(conf):

    # TODO use `sudo` to login as root and not store the pswds
    # see https://docs.openmicroscopy.org/omero/5.6.2/users/cli/sessions.html

    cmd = [
        "import",
        "-s",
        conf["server"],
        "-p",
        conf["port"],
        "--transfer",
        "ln_s",
        "--sudo",
        "root",
        "-u",
        conf["username"],
        "-g",
        conf["group"],
        # "--exclude",
        # "clientpath",
        "--file",
        Path(conf["out_file"]).absolute().as_posix(),
        "--errs",
        Path("err.txt").absolute().as_posix(),
        "--bulk",
        conf["bulk_yml"],

    ]
    cli = CLI()
    cli.loadplugins()
    log.info(f"Invoking omero {' '.join(cmd)}")
    cli.invoke(cmd)


def get_configuration(pth):
    """walks up from the directory 'pth' until a file named omero_userconf.yml is found.

    Returns a dictionnary from the parsed file.

    Parameters
    ----------
    pth: string or Path
         the path from which to search the configuration file

    Returns
    -------
    conf: dict

    For example:

    .. code::
        {'base_dir': 'User data root directory',
         'group': 'Prof Prakash',
         'password': 'XXXXX',
         'port': '4064',
         'project': 'Default Project',
         'server': 'omero.server.example.com',
         'username': 'E.Erhenfest'}


    Raises
    ------
    FileNotFoundError if there is no omero_userconf.yml in the file system

    """
    pth = Path(pth)
    conf = pth / "omero_userconf.yml"
    if conf.is_file():
        print(f"using {conf.absolute().as_posix()}")
        with open(conf, "r") as f:
            return yaml.safe_load(f)

    for parent in pth.parents:
        conf = parent / "omero_userconf.yml"
        if conf.is_file():
            print(f"using {conf.absolute().as_posix()} configuration file")
            with open(conf, "r") as f:
                return yaml.safe_load(f)
    raise FileNotFoundError("User configuration file not found")


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
        # "exclude": "clientpath",
        "checksum_algorithm": "CRC-32",
    }
    bulk_opts.update(kwargs)

    log.info(f"bulk options: {bulk_opts}")
    with open(bulk_yml, "w") as yml_file:
        yaml.dump(bulk_opts, yml_file)


def create_bulk_tsv(candidates, base_dir, conf):

    lines = []
    last_project = ""

    # group candidates by directory
    paths = sorted(candidates, key=lambda p: Path(p).parent.as_posix())
    for fullpath in paths:
        parts = Path(fullpath).relative_to(base_dir).parts
        if len(parts) == 1:
            project = conf.get("project", "no_project")
            dataset = date.today().isoformat()
            name = parts[0].replace(" ", "_")

        elif len(parts) == 2:
            project = parts[0].replace(" ", "_")
            dataset = date.today().isoformat()
            name = parts[1].replace(" ", "_")

        elif len(parts) == 3:
            project = parts[0].replace(" ", "_")
            dataset = parts[1].replace(" ", "_")
            name = parts[2].replace(" ", "_")

        else:
            project = parts[0].replace(" ", "_")
            dataset = parts[1].replace(" ", "_")
            name = "-".join(parts[2:]).replace(" ", "_")

        if project == last_project:
            ## use the latest existing dataset
            target = f"Project:name:{project}/Dataset:+name:{dataset}"
        else:
            ## create a new dataset
            target = f"Project:name:{project}/Dataset:@name:{dataset}"
            last_project = project

        lines.append("\t".join((target, name, fullpath + "\n")))

    log.info(f"Preparing to import {len(lines)} files")
    out_file = conf.get("tsv_file", "files.tsv")

    with open(out_file, "w") as f:
        f.writelines(lines)
