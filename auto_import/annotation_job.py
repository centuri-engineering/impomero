"""Auto annotation of omero database files from .toml annotation files


Example annotation file
=======================

..code:



"""
# TODO finish module level doc


import os
import logging
import tempfile
from datetime import date

from pathlib import Path
from functools import wraps

import toml
import omero
from omero.gateway import (
    BlitzGateway,
    TagAnnotationWrapper,
    MapAnnotationWrapper,
    CommentAnnotationWrapper,
)


log = logging.getLogger(__name__)
logfile = logging.FileHandler("auto_importer.log")
log.setLevel("INFO")
log.addHandler(logfile)

def auto_reconnect(fun):

    # assumes the connection object is the first argument
    @wraps(fun)
    def decorrated(*args, **kwargs):
        conn = args[0]
        if not conn.isConnected():
            conn.connect()
        return fun(*args, **kwargs)

    return decorrated


def auto_annotate(conf, dry_run=False):
    """Parses a directory and adds annotation to imported
    files in omero.

    Parameters
    ----------
    conf: dictionary
        the following keys are expected:
        - "username"
        - "password"
        - "server"
        - "port"
        - "base_dir"
        - "tsv_file"
    dry_run: bool, default False

    `base_dir` is the path to the directory to be imported, and
    `tsv_file` is the tab separated file used to bulk-import the data

    See Also
    --------

    importer_job.create_bulk_tsv : the function used to generate the `tsv_file`
    """
    conn = BlitzGateway(
        username=conf["username"],
        passwd=conf["password"],
        host=conf["server"],
        port=conf["port"],
    )
    try:
        pairs = pair_annotation_to_datasets(conn, conf["base_dir"], conf["tsv_file"])
        log.info(f"Annotating {len(pairs)} dataset/annotation pairs")
        if dry_run:
            print("Would annotate:")
            print(pairs)
            return

        for dset_id, annotation_toml in pairs:
            annotate(conn, dset_id, annotation_toml, object_type="Dataset")
    finally:
        conn.close()


@auto_reconnect
def annotate(conn, object_id, annotation_toml, object_type="Dataset"):
    """Applies the annotations in `annotation_toml` to the dataset

    Parameters
    ----------
    conn: An `omero.gateway.BlitzGateway` connection
    dataset_id: int - the Id of the dataset to annotate
    annnotation_toml: str or `Path` a toml file containing the annotation

    """
    annotation_toml = Path(annotation_toml)
    annotated = conn.getObject(object_type, object_id)
    log.info(f"\n")
    log.info(f"Annotating {object_type} {object_id} with {annotation_toml.as_posix()}")

    with annotation_toml.open("r") as ann_toml:
        ann = toml.load(ann_toml)

    key_value_pairs = list(ann.get("kv_pairs", {}).items())
    if key_value_pairs:
        log.info("Map annotations: ")
        log.info("\n".join([f"{k}: {v}" for k, v in key_value_pairs]))
        map_ann = MapAnnotationWrapper(conn)
        namespace = omero.constants.metadata.NSCLIENTMAPANNOTATION
        map_ann.setNs(namespace)
        map_ann.setValue(key_value_pairs)
        map_ann.save()
        annotated.linkAnnotation(map_ann)

    for tag in ann.get("tags", []):
        log.info(f"Adding tag: {tag}")
        tag_ann = TagAnnotationWrapper(conn)
        tag_ann.setValue(tag)
        tag_ann.save()
        annotated.linkAnnotation(tag_ann)

    comment = ann.get("comment", "")
    if comment:
        log.info(f"Adding comment: {comment}")
        com_ann = CommentAnnotationWrapper(conn)
        com_ann.setValue(comment)
        com_ann.save()
        annotated.linkAnnotation(com_ann)


@auto_reconnect
def pair_annotation_to_datasets(conn, base_dir, tsv_file):
    """Parses the input tsv_file to find correspondances between imported datasets and
    annotation files within the directory.


    """
    base_dir = Path(base_dir)
    with open(tsv_file, "r") as tsvf:
        new_datasets = [
            _parse_tsv_line(line) for line in tsvf if "Dataset:@name:" in line
        ]

    annotation_tomls = collect_annotations(base_dir)
    log.info(annotation_tomls)
    pairs = []
    for dataset in new_datasets:
        dset_dir = dataset["directory"].relative_to(base_dir)
        dset_id = _find_dataset_id(conn, dataset)["dataset"]
        if dset_dir in annotation_tomls:
            pairs.append((dset_id, annotation_tomls[dset_dir]))
        else:
            for parent in list(dset_dir.parents)[::-1]:
                if parent in annotation_tomls:
                    pairs.append((dset_id, annotation_tomls[parent]))
                    break
            else:
                log.info(f"No annotation found above {dset_dir}")
    return pairs


@auto_reconnect
def _find_dataset_id(conn, dataset):
    """Query the omero db to find the dataset id based on its name"""
    dsets = conn.getObjects("Dataset", attributes={"name": dataset["dataset"]})
    for dset in dsets:
        projs = [p for p in dset.getAncestry() if p.name == dataset["project"]]
        if projs:
            log.info(
                f"Found dataset {dataset['dataset']} of project {dataset['project']}"
            )
            return {"dataset": dset.getId(), "project": projs[0].getId()}
    raise ValueError(f"No dataset {dataset} was found in the base")


def collect_annotations(base_dir):
    """Finds annotation files throughout the base_dir directory"""

    base_dir = Path(base_dir)
    all_tomls = base_dir.glob("**/*.toml")
    # This is relative to base_dir
    annotation_tomls = {
        toml.parent.relative_to(base_dir): base_dir / toml
        for toml in all_tomls
        if _is_annotation(base_dir / toml)
    }
    return annotation_tomls


def _parse_tsv_line(tsv_line):
    cols = tsv_line.split("\t")

    target = cols[0]
    project, dataset = target.split("/")
    directory = Path(cols[2]).parent
    return {
        "project": project.split(":")[-1],
        "dataset": dataset.split(":")[-1],
        "directory": directory,
    }


def _has_annotation(directory):
    tomls = Path(directory).glob("*.toml")
    if not tomls:
        return False
    for toml_file in tomls:
        if _is_annotation(toml):
            return toml_file
    return False


def _is_annotation(toml_file):
    with open(toml_file, "r") as fh:
        return "# omero annotation file" in fh.readline()
