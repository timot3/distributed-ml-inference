import socket

from FileStore.utils import add_len_prefix, trim_len_prefix

BUFF_SIZE = 4096


def recvall(sock):
    data = bytearray()
    msg_len, msg = trim_len_prefix(sock.recv(BUFF_SIZE))
    data.extend(msg)
    while len(data) < msg_len:
        msg = sock.recv(BUFF_SIZE)
        if not msg:
            break
        data.extend(msg)

    return data

    # data = bytearray()
    # while True:
    #     packet = sock.recv(BUFF_SIZE)
    #     if not packet:
    #         break
    #     data.extend(packet)
    # return data


if __name__ == "__main__":
    # read all data from test file

    # data = open("test_file", "rb")
    data = b"a" * 10000
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect(("localhost", 8082))
        s.sendall(add_len_prefix(data))
        data = recvall(s)
        print("Received", len(data))
