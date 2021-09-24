import shutil
import subprocess
import time
from pathlib import Path

import pytest
from Ice import ConnectionLostException
from omero.gateway import BlitzGateway

from impomero import collector

pytest_plugins = ["docker_compose"]

DATA_PATH = Path(__file__).parent.parent / "data/"
RAW = DATA_PATH / "raw"
TOMLS = DATA_PATH / "tomls"


@pytest.fixture(scope="session")
def wait_for_server(session_scoped_container_getter):
    """Wait for the api from omeroserver to become responsive (try for up to 2 mins)"""

    service = session_scoped_container_getter.get("omeroserver").network_info[0]
    is_connected = False
    t = 0
    while not is_connected:
        try:
            with BlitzGateway(
                host=service.hostname,
                port=int(service.host_port),
                username="root",
                passwd="omero",
                secure=True,
            ) as con:
                is_connected = con.isConnected()
                print(is_connected)
        except Exception:
            pass

        time.sleep(5)
        t += 1
        if t > 24:
            raise ConnectionLostException("Unable to connect")

    print(f"Waited {(t * 5) // 60} min {(t * 5) % 60} s for connection")
    return service.hostname, service.host_port


@pytest.fixture(scope="session")
def populate_db(wait_for_server):
    host, port = wait_for_server
    subprocess.run(["tests/populate_db.sh"], check=True)
    return host, port


@pytest.fixture(scope="function")
def get_connection(populate_db):
    host, port = populate_db
    with BlitzGateway(
        host=host,
        port=int(port),
        username="john",
        passwd="nhoj",
        secure=True,
    ) as conn:
        yield conn


@pytest.fixture(scope="function")
def get_root_connection(populate_db):
    host, port = populate_db
    with BlitzGateway(
        host=host,
        port=int(port),
        username="root",
        passwd="omero",
        secure=True,
    ) as conn:
        yield conn


@pytest.fixture
def move_tomls():
    destinations = ["dir0", "dir1", "dir0/sub_dir2"]
    for i, dst in enumerate(destinations):
        shutil.copy(TOMLS / f"file{i}.toml", RAW / dst)
    yield
    for i, dst in enumerate(destinations):
        (RAW / dst / f"file{i}.toml").unlink()


@pytest.fixture
def candidates(move_tomls):
    cands = collector.collect_candidates(RAW)
    assert len(cands) == 7
    return cands


@pytest.fixture
def import_table(move_tomls):
    return collector.create_import_table(RAW)
