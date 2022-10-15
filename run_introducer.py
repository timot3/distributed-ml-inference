from mp2.introducer import IntroducerServer
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

    HOST, PORT = "127.0.0.1", 8080
    # create  a tcp server that reuses the address
    with IntroducerServer(HOST, PORT) as server:
        server.allow_reuse_address = True
        server.server_bind()
        server.server_activate()
        server.start_node_server_thread()
        server.serve_forever()
