#!/usr/bin/env python
"""Auto import script from a user directory
"""
import argparse
import os

from .monitor import start_toml_observer

parser = argparse.ArgumentParser()
parser.add_argument("path", help="path to the directory you want to import into omero")
parser.add_argument(
    "-d",
    "--dry_run",
    help="do not perform the import, just output the preparation files",
    action="store_true",
)

parser.add_argument(
    "-l",
    "--link",
    help="Use symbolic links to raw data",
    action="store_true",
)


args = parser.parse_args()
transfer = "ln_s" if args.link else None

db = os.environ.get("IMPOMERO_DB")
if db is None:
    db = os.path.join(os.environ.get("HOME", "impomero.sql"))

start_toml_observer(args.path, transfer=transfer, dry_run=args.dry_run, import_db=db)
