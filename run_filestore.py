import socket

from FileStore.FileStoreNode import FileStoreNode
from FileStore.utils import add_len_prefix, trim_len_prefix

BUFF_SIZE = 4096


if __name__ == "__main__":
    # with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    #     s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    #
    #     s.bind(("localhost", 8082))
    #     s.listen(1)
    #     conn, addr = s.accept()
    #     print("Connected by", addr)
    #     data = recvall(conn)
    #     print("Received", len(data))
    #     resp = add_len_prefix(data)
    #     conn.sendall(resp)
    #     print("Sent", len(data))
    #     conn.close()

    with FileStoreNode(("localhost", 8082)) as server:
        server.allow_reuse_address = True
        server.serve_forever()
