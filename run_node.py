import threading

from Node.node import NodeUDPServer
from Node.utils import get_any_open_port, run_node_command_menu
import logging

if __name__ == "__main__":
    # set logging config
    logging.basicConfig(level=logging.INFO,)
    formatter = logging.Formatter("%(name)s:[%(levelname)-8s] %(message)s")

    for handler in logging.root.handlers:
        handler.setFormatter(formatter)
        handler.setLevel(logging.INFO)
    logging.basicConfig(level=logging.INFO)

    HOST, PORT = "127.0.0.1", get_any_open_port()
    INTRODUCER_HOST, INTRODUCER_PORT = "127.0.0.1", 8080
    # create a udp server that resuses the address

    with NodeUDPServer(
        HOST, PORT, INTRODUCER_HOST, INTRODUCER_PORT, is_introducer=False
    ) as node:
        node.allow_reuse_address = True
        node.server_bind()
        node.server_activate()
        node.join_network()
        thread = threading.Thread(target=node.serve_forever, daemon=True)
        thread.start()

        node.start_heartbeat_watchdog()

        run_node_command_menu(node)
