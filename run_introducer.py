import threading

from mp2.introducer import IntroducerServer
import logging

from mp2.utils import run_node_command_menu

if __name__ == "__main__":
    # set logging config
    logging.basicConfig(
        level=logging.INFO,
        handlers=[
            logging.FileHandler("debug.log"),
            logging.StreamHandler()
        ]
    )

    formatter = logging.Formatter("%(name)s:[%(levelname)-8s] %(message)s")

    for handler in logging.root.handlers:
        handler.setFormatter(formatter)

    logging.basicConfig(level=logging.INFO)

    HOST, PORT = "127.0.0.1", 8080
    # create  a tcp server that reuses the address
    with IntroducerServer(HOST, PORT) as server:
        server.allow_reuse_address = True
        server.server_bind()
        server.server_activate()
        server.start_node_server_thread()
        server_thread = threading.Thread(target=server.serve_forever, daemon=True)
        server_thread.start()
        server.node.start_heartbeat_watchdog()
        run_node_command_menu(server.node)
