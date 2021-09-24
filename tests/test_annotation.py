import datetime
import tempfile

import pytest
import toml
from omero.gateway import MapAnnotationWrapper, TagAnnotationWrapper

from impomero.annotation_job import annotate, auto_annotate, update_annotation

pytest_plugins = ["docker_compose"]


def test_annotate(get_connection):
    annotation = {
        "title": "Title 1",
        "created": datetime.datetime(2021, 4, 26, 7, 58, 50, 254758),
        "project": "Project Test 2",
        "user": "kathleen",
        "comment": "#test comment with #new tag",
        "tags": ["test", "new"],
        "accessed": "2021-04-26 10:23:09.698404",
        "kv_pairs": {
            "organism": "Python breitensteini",
            "method": "Laser Ablation",
            "channel_0": "Atb2-GFP",
        },
    }
    conn = get_connection
    annotate(conn, 1, annotation)
    for tag in ("test", "new"):
        tag_ann = conn.getObjects("TagAnnotation", attributes={"textValue": tag})
        assert len(list(tag_ann)) == 1
    img = conn.getObject("Image", 1)
    assert len(list(img.listAnnotations())) == 4


def test_update_annotation(get_connection):
    annotation = {
        "title": "Title 1",
        "created": datetime.datetime(2021, 4, 26, 7, 58, 50, 254758),
        "project": "Project Test 2",
        "user": "john",
        "comment": "#test comment without the new tag",
        "tags": ["test"],
        "accessed": "2021-04-26 10:23:09.698404",
        "kv_pairs": {
            "organism": "Python breitensteini",
            "method": "Laser Ablation",
            "channel_0": "myosin-RFP",
        },
    }
    tmptoml = tempfile.mktemp(suffix=".toml")
    with open(tmptoml, "w") as fh:
        toml.dump(annotation, fh)

    conn = get_connection
    update_annotation(conn, 1, tmptoml)
    tag_ann = conn.getObjects("TagAnnotation", attributes={"textValue": "test"})
    assert len(list(tag_ann)) == 1
    annotated = conn.getObject("Image", 1)
    for ann in annotated.listAnnotations():
        if isinstance(ann, TagAnnotationWrapper):
            assert ann.getValue() == "test"
        elif isinstance(ann, MapAnnotationWrapper):
            assert dict(ann.getValue())["channel_0"] == "myosin-RFP"


def test_auto_annotate(get_root_connection, import_table):
    conn = get_root_connection
    with pytest.raises(ValueError):
        auto_annotate(conn, import_table, dry_run=False)
