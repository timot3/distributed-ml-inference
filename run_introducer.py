import threading

import logging

from Node.node import NodeTCPServer
from Node.utils import run_node_command_menu

if __name__ == "__main__":
    # set logging config
    logging.basicConfig(
        level=logging.INFO,
        handlers=[logging.FileHandler("debug.log"), logging.StreamHandler()],
    )

    formatter = logging.Formatter("%(name)s:[%(levelname)-8s] %(message)s")

    for handler in logging.root.handlers:
        handler.setFormatter(formatter)

    logging.basicConfig(level=logging.INFO)

    HOST, PORT = "127.0.0.1", 8080
    # create  a tcp server that reuses the address
    with NodeTCPServer(HOST, PORT, is_introducer=True) as introducer:
        introducer.allow_reuse_address = True
        introducer.server_bind()
        introducer.server_activate()
        introducer.join_network()

        thread = threading.Thread(target=introducer.serve_forever, daemon=True)
        thread.start()

        run_node_command_menu(introducer)
