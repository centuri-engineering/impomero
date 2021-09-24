from pathlib import Path

from impomero.importer_job import auto_import

DATA_PATH = Path(__file__).parent.parent / "data/"
RAW = DATA_PATH / "raw"


pytest_plugins = ["docker_compose"]


def test_dry_auto_import(import_table):
    conf, import_table_ = auto_import(
        RAW, dry_run=True, import_table=import_table, reset=False, clean=False
    )
    with open(conf["tsv_file"]) as tsv:
        assert len(tsv.readlines()) == 1
    with open(conf["out_file"]) as out:
        assert not out.readlines()


def test_auto_import(import_table, get_connection):

    conn = get_connection
    conf, _ = auto_import(
        RAW, dry_run=False, import_table=import_table, reset=False, clean=False
    )
    assert conf["port"] == 14064
    with open(conf["out_file"]) as out:
        assert not out.readlines()  # WHY?

    assert len(list(conn.getObjects("Dataset"))) == 2
