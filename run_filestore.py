import socket

from FileStore.FileStoreNode import FileStoreNode
from FileStore.utils import add_len_prefix, trim_len_prefix

BUFF_SIZE = 4096


if __name__ == "__main__":
    with FileStoreNode(("localhost", 8082)) as server:
        server.allow_reuse_address = True
        server.serve_forever()
