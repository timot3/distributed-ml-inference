from mp2.node import NodeUDPServer
from mp2.utils import get_any_open_port
if __name__ == "__main__":
    HOST, PORT = "127.0.0.1", get_any_open_port()
    INTRODUCER_HOST, INTRODUCER_PORT = "127.0.0.1", 8080
    # create a udp server that resuses the address
    with NodeUDPServer(HOST, PORT, INTRODUCER_HOST, INTRODUCER_PORT) as server:
        server.allow_reuse_address = True
        server.server_bind()
        server.server_activate()
        server.serve_forever()
