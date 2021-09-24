import os
from pathlib import Path

from impomero import collector

DATA_PATH = Path(__file__).parent.parent / "data/"
RAW = DATA_PATH / "raw"
TOMLS = DATA_PATH / "tomls"


def test_get_configuration():

    conf = collector.get_configuration()
    assert {"server", "port", "admin_passwd"}.issubset(conf)
    os.environ["OMERO_ROOT_PASSWORD"] = "omero"
    conf = collector.get_configuration()
    assert conf["admin_passwd"] == "omero"


def test_good_annotation():

    good = TOMLS / "file1.toml"
    assert collector.is_annotation(good)


def test_bad_header_annotation():

    bad = TOMLS / "bad.toml"
    assert not collector.is_annotation(bad)


def test_noo_project_annotation():

    bad = TOMLS / "bad2.toml"
    assert not collector.is_annotation(bad)


def test_collect_annotations(move_tomls):
    annotation_tomls = collector.collect_annotations(RAW)
    assert len(annotation_tomls) == 3


def test_collect_candidates(candidates):
    img0 = RAW / "dir0" / "sub_dir1" / "img0.tif"
    img0 = img0.absolute().resolve()
    assert img0 in candidates
    assert candidates[img0].as_posix().endswith("file0.toml")

    img3 = RAW / "dir1" / "sub_dir1" / "subsub_dir" / "img0.tif"
    img3 = img3.absolute().resolve()
    assert img3 in candidates
    assert candidates[img3].as_posix().endswith("file1.toml")


def test_parse_pair(candidates):
    img3 = RAW / "dir1" / "sub_dir1" / "subsub_dir" / "img0.tif"
    img3 = img3.absolute().resolve()
    ann = collector.parse_pair(img3, candidates[img3], RAW.resolve())
    assert "user" in ann
    assert ann["fileset"].endswith("img0.tif")
    assert "Dataset:+name" in ann["target"]
    ann = collector.parse_pair(img3, candidates[img3], RAW.resolve(), new_dataset=True)
    assert "Dataset:@name" in ann["target"]


def test_create_import_table(import_table):
    table = import_table
    assert table.shape == (7, 12)
    assert "Dataset:@name" in table.loc[0, "target"]
    assert "Dataset:+name" in table.loc[1, "target"]

    table = collector.create_import_table(RAW, update_dataset=True)
    assert "Dataset:+name" in table.loc[0, "target"]
