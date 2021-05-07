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

from collector import create_import_table, get_configuration


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


def auto_annotate(conn, import_table, dry_run=False):

    for row in import_table.iterrows():
        dset_id = _find_dataset_id(conn, row["dataset"])["dataset"]
        if dry_run:
            print(f"would annotate dataset {dset_id} with {row['title']}")
            continue
        annotate(conn, dset_id, row, object_type="Dataset")


@auto_reconnect
def annotate(conn, object_id, ann, object_type="Dataset"):
    """Applies the annotations in `annotation_toml` to the dataset

    Parameters
    ----------
    conn: An `omero.gateway.BlitzGateway` connection
    object_id: int - the Id of the dataset to annotate
    annnotation_toml: str or `Path` a toml file containing the annotation
    oject_type: the omero object type to annotate (default Dataset)
    """
    log.info("\n")
    log.info("Annotating %s %d with %s", object_type, object_id, ann["title"])
    annotated = conn.getObject(object_type, object_id)

    user = ann["user"]
    user_conn = conn.suConn(user)

    key_value_pairs = list(ann.get("kv_pairs", {}).items())
    if key_value_pairs:
        log.info("Map annotations: ")
        log.info("\n".join([f"{k}: {v}" for k, v in key_value_pairs]))
        map_ann = MapAnnotationWrapper(user_conn)
        namespace = omero.constants.metadata.NSCLIENTMAPANNOTATION
        map_ann.setNs(namespace)
        map_ann.setValue(key_value_pairs)
        map_ann.save()
        annotated.linkAnnotation(map_ann)

    for tag in ann.get("tags", []):
        log.info("Adding tag: %s", tag)
        matches = list(
            user_conn.getObjects("TagAnnotation", attributes={"textValue": tag})
        )
        if matches:
            tag_ann = matches[0]
        else:
            tag_ann = TagAnnotationWrapper(user_conn)
            tag_ann.setValue(tag)
            tag_ann.save()
        annotated.linkAnnotation(tag_ann)

    comment = ann.get("comment", "")
    if comment:
        log.info(f"Adding comment: {comment}")
        com_ann = CommentAnnotationWrapper(user_conn)
        com_ann.setValue(comment)
        com_ann.save()
        annotated.linkAnnotation(com_ann)


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
