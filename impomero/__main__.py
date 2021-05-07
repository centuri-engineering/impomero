#!/usr/bin/env python

"""Auto import script from a user directory
"""


import argparse

from auto_import.importer_job import auto_import
from auto_import.annotation_job import auto_annotate


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
    help="automate annotations from yml files",
    action="store_true",
)

args = parser.parse_args()

reset = not args.use_cache

print("importing ... ")
conf = auto_import(args.path, args.dry_run, reset, clean=not args.annotate)

if args.annotate:
    print("Annotating ... ")
    auto_annotate(conf)
