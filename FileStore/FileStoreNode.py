import socket
import socketserver
from typing import Tuple

from FileStore.utils import trim_len_prefix, add_len_prefix

socketserver.TCPServer.allow_reuse_address = True
BUFF_SIZE = 4096


class FileStoreHandler(socketserver.BaseRequestHandler):
    def recvall(self, sock):
        # use popular method of recvall
        data = bytearray()
        msg_len, msg = trim_len_prefix(sock.recv(BUFF_SIZE))
        data.extend(msg)
        while len(data) < msg_len:
            msg = sock.recv(BUFF_SIZE)
            if not msg:
                break
            data.extend(msg)

        return data

    def sendall(self, data):
        data = add_len_prefix(data)
        self.request.sendall(data)

    def handle(self):
        # self.request is the TCP socket connected to the client
        # read ALL data from socket
        data = self.recvall(self.request)

        print("Received", len(data))
        self.sendall(data)
        self.request.close()


class FileStoreNode(socketserver.TCPServer):
    def __init__(self, addr: Tuple[str, int]):
        super().__init__(addr, FileStoreHandler)
        self.allow_reuse_address = True
        self.allow_reuse_port = True

        self.ip, self.port = self.server_address
