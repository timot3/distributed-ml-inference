import argparse
import socket
import threading

import logging

from Node.node import NodeTCPServer
from Node.nodetypes import VM1_URL
from Node.utils import in_green, run_node_command_menu, get_any_open_port

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--local", action="store_true", help="Run the introducer locally")

    args = parser.parse_args()

    # set logging config
    logging.basicConfig(
        level=logging.INFO,
        handlers=[logging.FileHandler("debug.log"), logging.StreamHandler()],
    )

    formatter = logging.Formatter(
        "[%(filename)s:%(lineno)d]:[%(levelname)-8s] %(message)s"
    )

    for handler in logging.root.handlers:
        handler.setFormatter(formatter)

    logging.basicConfig(level=logging.INFO)

    # get the IP of VM1_URL
    if args.local:
        HOST, PORT = "127.0.0.1", 8080
    else:
        HOST, PORT = socket.gethostbyname(VM1_URL), 8080

    # create  a tcp server that reuses the address
    with NodeTCPServer(HOST, PORT, is_introducer=True) as introducer:
        introducer.allow_reuse_address = True
        introducer.server_bind()
        introducer.server_activate()
        introducer.join_network()

        thread = threading.Thread(target=introducer.serve_forever, daemon=True)
        thread.start()

        scheduler = threading.Thread(target=introducer.start_scheduler, daemon=True)
        scheduler.start()

        print(in_green(f"Node running on {HOST}:{PORT}"))

        run_node_command_menu(introducer)
