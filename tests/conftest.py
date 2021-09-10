import subprocess
import time
import warnings

import pytest
from Ice import ConnectionLostException
from omero.gateway import BlitzGateway

pytest_plugins = ["docker_compose"]


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

    warnings.warn(f"Waited {(t * 5) // 60} min {(t * 5) % 60} s for connection")
    return service.hostname, service.host_port


@pytest.fixture(scope="session")
def populate_db(wait_for_server):
    host, port = wait_for_server
    subprocess.run(["tests/populate_db.sh"], check=True)
    return host, port
