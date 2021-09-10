import datetime

from omero.gateway import BlitzGateway

from impomero.annotation_job import annotate

pytest_plugins = ["docker_compose"]


def test_annotate(populate_db):
    host, port = populate_db
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

    with BlitzGateway(
        host=host,
        port=int(port),
        username="john",
        passwd="nhoj",
        secure=True,
    ) as conn:
        imgs = list(conn.getObjects("Image"))
        assert len(imgs)

        im_id = imgs[0].getId()
        annotate(conn, im_id, annotation)
