import threading

import pytest
from Node.node import NodeTCPServer
from Node.utils import get_any_open_port, in_green


@pytest.fixture(scope="module")
def host():
    return "127.0.0.1"


@pytest.fixture(scope="module")
def introducer_port():
    return 8080


@pytest.fixture
def open_port():
    return get_any_open_port()


@pytest.fixture(scope="module")
def introducer(host, introducer_port):
    introducer_node = NodeTCPServer(host, introducer_port, is_introducer=True)
    introducer_node.allow_reuse_address = True
    introducer_node.server_bind()
    introducer_node.server_activate()
    introducer_node.join_network()

    thread = threading.Thread(target=introducer_node.serve_forever, daemon=True)
    thread.start()

    yield introducer_node

    introducer_node.shutdown()


@pytest.fixture
def node(host, open_port, introducer_port):
    running_node = NodeTCPServer(host, open_port, is_introducer=False)
    running_node.allow_reuse_address = True
    running_node.server_bind()
    running_node.server_activate()
    running_node.join_network(introducer_host=host, introducer_port=introducer_port)
    thread = threading.Thread(target=running_node.serve_forever, daemon=True)
    thread.start()
    print(in_green(f"Node running on {host}:{open_port}"))

    yield running_node

    running_node.shutdown()
