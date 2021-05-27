#!/usr/bin/env python

"""Auto import script from a user directory
"""


import argparse
from omero.gateway import BlitzGateway

from impomero.importer_job import auto_import
from impomero.annotation_job import auto_annotate


parser = argparse.ArgumentParser()
parser.add_argument("path", help="path to the directory you want to import into omero")
parser.add_argument(
    "-d",
    "--dry_run",
    help="do not perform the import, just output the preparation files",
    action="store_true",
)

parser.add_argument(
    "-c",
    "--use_cache",
    help="Do not parse the directory again, perform import "
    "from previously generated files",
    action="store_true",
)

parser.add_argument(
    "-a",
    "--annotate",
    help="automatic annotations from the toml file",
    action="store_true",
)

parser.add_argument(
    "-l",
    "--link",
    help="Use symbolic links to raw data",
    action="store_true",
)


args = parser.parse_args()

reset = not args.use_cache


transfer = "ln_s" if args.link else None

print("importing ... ")
conf, import_table = auto_import(
    base_dir=args.path,
    dry_run=args.dry_run,
    reset=reset,
    clean=not args.annotate,
    transfer=transfer,
)

conn = BlitzGateway(
    host=conf["host"],
    port=conf["port"],
    username="root",
    passwd=conf["admin_passwd"],
    secure=True,
)

if args.annotate:
    print("Annotating ... ")
    auto_annotate(conn, import_table)
