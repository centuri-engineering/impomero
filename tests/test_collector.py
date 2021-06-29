import os
from pathlib import Path

from impomero import collector

DATA_PATH = Path(__file__).parent / "../test_data/"


def test_get_configuration():

    conf = collector.get_configuration()
    assert {"server", "port", "admin_passwd"}.issubset(conf)
    os.environ["OMERO_ROOT_PASSWORD"] = "omero"
    conf = collector.get_configuration()
    assert conf["admin_passwd"] == "omero"


def test_is_annotation():

    good = DATA_PATH / "dir0" / "file1.toml"
    assert collector.is_annotation(good)

    bad = DATA_PATH / "dir0" / "bad.toml"
    assert not collector.is_annotation(bad)

    bad = DATA_PATH / "dir0" / "bad2.toml"
    assert not collector.is_annotation(bad)


def test_collect_annotations():
    annotation_tomls = collector.collect_annotations(DATA_PATH)
    assert len(annotation_tomls) == 3


def test_collect_candidates():
    cands = collector.collect_candidates(DATA_PATH)
    assert len(cands) == 7

    img0 = DATA_PATH / "dir0" / "sub_dir1" / "img0.tif"
    img0 = img0.absolute().resolve()
    assert img0 in cands
    assert cands[img0].as_posix().endswith("file1.toml")

    img3 = DATA_PATH / "dir1" / "sub_dir1" / "subsub_dir" / "img0.tif"
    img3 = img3.absolute().resolve()
    assert img3 in cands
    assert cands[img3].as_posix().endswith("file2.toml")


def test_parse_pair():
    cands = collector.collect_candidates(DATA_PATH)
    img3 = DATA_PATH / "dir1" / "sub_dir1" / "subsub_dir" / "img0.tif"
    img3 = img3.absolute().resolve()
    ann = collector.parse_pair(img3, cands[img3], DATA_PATH.resolve())
    assert "user" in ann
    assert ann["fileset"].endswith("img0.tif")
    assert "Dataset:+name" in ann["target"]
    ann = collector.parse_pair(img3, cands[img3], DATA_PATH.resolve(), new_dataset=True)
    assert "Dataset:@name" in ann["target"]


def test_create_import_table():
    table = collector.create_import_table(DATA_PATH)
    assert table.shape == (7, 12)
    assert "Dataset:@name" in table.loc[0, "target"]
    assert "Dataset:+name" in table.loc[1, "target"]

    table = collector.create_import_table(DATA_PATH, update_dataset=True)
    assert "Dataset:+name" in table.loc[0, "target"]
