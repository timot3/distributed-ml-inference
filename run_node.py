from mp2.node import NodeUDPServer
from mp2.utils import get_any_open_port
import logging
if __name__ == "__main__":
    # set logging config
    logging.basicConfig(
        level=logging.DEBUG,
        handlers=[
            logging.FileHandler("debug.log"),
            logging.StreamHandler()
        ]
    )
    formatter = logging.Formatter("%(name)s:[%(levelname)s] %(message)s")

    for handler in logging.root.handlers:
        handler.setFormatter(formatter)


    HOST, PORT = "127.0.0.1", get_any_open_port()
    INTRODUCER_HOST, INTRODUCER_PORT = "127.0.0.1", 8080
    # create a udp server that resuses the address
    with NodeUDPServer(HOST, PORT, INTRODUCER_HOST, INTRODUCER_PORT, is_introducer=False) as server:
        server.allow_reuse_address = True
        print("binding")
        server.server_bind()
        print("activating")
        server.server_activate()
        print("joining")
        server.join_network()
        print("serving")
        server.serve_forever()
