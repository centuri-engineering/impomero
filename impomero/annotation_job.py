"""Auto annotation of omero database files from .toml annotation files


Example annotation file
=======================

..code:



"""
# TODO finish module level doc


import logging
from functools import wraps

import omero
import pandas as pd
import toml
from omero.gateway import (
    CommentAnnotationWrapper,
    MapAnnotationWrapper,
    TagAnnotationWrapper,
)

log = logging.getLogger(__name__)
logfile = logging.FileHandler("auto_importer.log", encoding="utf-8")
log.setLevel("INFO")
log.addHandler(logfile)


def auto_reconnect(fun):
    """Auto reconnection decorator, assumes the connection object is the
    first argument of the decorated function"""
    # assumes the connection object is the first argument
    @wraps(fun)
    def decorrated(*args, **kwargs):
        conn = args[0]
        if not conn.isConnected():
            conn.connect()
        return fun(*args, **kwargs)

    return decorrated


@auto_reconnect
def auto_annotate(conn, import_table, dry_run=False):
    """Uses the import_table to annotate all the images
    from the imported dataset
    """
    # all images from a given dataset
    # are annotated by the same data
    dset_table = import_table.groupby("dataset").first()
    annotated = []
    for dset_name, row in dset_table.iterrows():
        user = row["user"]
        user_conn = conn.suConn(user)
        try:
            dset_id = _find_dataset_id(user_conn, dset_name, row["project"])["dataset"]
        except ValueError:
            # Try with quotes
            dset_id = _find_dataset_id(user_conn, f'"{dset_name}"', row["project"])[
                "dataset"
            ]

        dataset = user_conn.getObject("Dataset", dset_id)
        for image in dataset.listChildren():
            img_id = image.getId()
            if dry_run:
                print(f"would annotate image {img_id} with card {row['title']}")
                continue
            annotate(user_conn, img_id, row, object_type="Image")
            rec = dict(row)
            rec["id"] = img_id
            annotated.append(rec)
    return pd.DataFrame.from_records(rec)


@auto_reconnect
def annotate(conn, object_id, ann, object_type="Image"):
    """Applies the annotations in `ann` to the object

    Parameters
    ----------

    conn: An `omero.gateway.BlitzGateway` connection
    object_id: int - the Id of the dataset to annotate
    ann: dict containing the annotation
    oject_type: the omero object type to annotate (default "Image")


    """
    log.info("\n")
    log.info("Annotating %s %d with %s", object_type, object_id, ann["title"])
    annotated = conn.getObject(object_type, object_id)

    kv_pairs = ann.get("kv_pairs")
    if kv_pairs:
        update_mapannotation(conn, annotated, kv_pairs)

    for tag in ann.get("tags", []):
        log.info("Adding tag: %s", tag)
        matches = list(conn.getObjects("TagAnnotation", attributes={"textValue": tag}))
        if matches:
            tag_ann = matches[0]
        else:
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


def _find_dataset_id(conn, dataset, project):
    """Query the omero db to find the dataset id based on its name"""
    dsets = conn.getObjects("Dataset", attributes={"name": dataset})

    for dset in dsets:
        projs = [p for p in dset.getAncestry() if p.name in (f'"{project}"', project)]
        if projs:
            log.info(f"Found dataset {dataset} of project {project}")
            return {"dataset": dset.getId(), "project": projs[0].getId()}
        raise ValueError(f"No {dataset} associated with project {project}")
    raise ValueError(f"No dataset {dataset} was found in the base")


@auto_reconnect
def update_annotation(conn, object_id, annotation_path, object_type="Image"):
    with open(annotation_path, "r", encoding="utf-8") as fh:
        annotation = toml.load(fh)

    annotated = conn.getObject(object_type, object_id)
    for ann in annotated.listAnnotations():
        annotated.unlinkAnnotation(ann)
    annotate(conn, object_id, annotation, object_type)


@auto_reconnect
def update_mapannotation(conn, annotated, kv_pairs):
    """Search for MapAnnotations associated to the annotated object
    and updates **the first map it finds** (in the dictionnary update sense)
    or creates a new one if no map annotation was present.
    """
    log.info("Map annotations: ")
    log.info("\n".join([f"{k}: {v}" for k, v in kv_pairs.items()]))
    for map_ann in annotated.listAnnotations():
        if isinstance(map_ann, MapAnnotationWrapper):
            vals = dict(map_ann.getValue())
            vals.update(kv_pairs)
            map_ann.setValue(list(vals.items()))
            break
    else:
        map_ann = MapAnnotationWrapper(conn)
        namespace = omero.constants.metadata.NSCLIENTMAPANNOTATION
        map_ann.setNs(namespace)
        map_ann.setValue(list(kv_pairs.items()))

    map_ann.save()
    annotated.linkAnnotation(map_ann)
