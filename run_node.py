import argparse
import os
import threading
import socket

from Node.node import NodeTCPServer
from Node.nodetypes import VM1_URL
from Node.utils import get_any_open_port, in_green, run_node_command_menu
import logging

if __name__ == "__main__":
    # set logging config
    logging.basicConfig(
        level=logging.INFO,
    )
    formatter = logging.Formatter("%(name)s:[%(levelname)-8s] %(message)s")

    for handler in logging.root.handlers:
        handler.setFormatter(formatter)
        handler.setLevel(logging.INFO)
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument("--local", action="store_true", help="Run the client locally")

    args = parser.parse_args()
    if args.local:
        HOST, PORT = "127.0.0.1", get_any_open_port()

    else:
        # get the Node ID from the environment variable
        node_id = os.environ["NODE_ID"]

        # get self ip address
        self_ip = socket.gethostbyname(socket.gethostname())
        HOST, PORT = self_ip, 8080

    # create a udp server that resuses the address

    with NodeTCPServer(HOST, PORT, is_introducer=False) as node:
        node.allow_reuse_address = True
        node.server_bind()
        node.server_activate()
        node.join_network()
        thread = threading.Thread(target=node.serve_forever, daemon=True)
        thread.start()
        print(in_green(f"Node running on {HOST}:{PORT}"))

        run_node_command_menu(node)
