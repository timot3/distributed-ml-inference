import socket

from Node.types import FileStoreMessage, MessageType
from Node.utils import trim_len_prefix, add_len_prefix

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


def make_file_message(
    file_data: bytes, message_type: MessageType, host: str, port: int
) -> FileStoreMessage:
    """
    Create a filestore message from the file data
    :param file_data: the file data in bytes
    :param message_type: The MessageType
    :param host: the host ip (where the message is sent from)
    :param port: the host port (where the message is sent from)
    :return: the FileStoreMessage with dummy values for ip, port, and timestamp
    """
    return FileStoreMessage(
        message_type,  # message type
        host,  # ip
        port,  # port
        0,  # timestamp
        "test.txt",  # file name
        0,  # version
        file_data,  # file data
    )


if __name__ == "__main__":
    # get this value from he command line when starting the introducer
    HOST, PORT = "127.0.0.1", 56845

    # get data from testfiles/letters.txt
    with open("testfiles/letters.txt", "rb") as f:
        file_data = f.readline()

    # create a filestore message

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        # bind the socket to any port
        s.connect((HOST, PORT))

        socket_host, socket_port = s.getsockname()
        print(f"Connected to {HOST}:{PORT} from {socket_host}:{socket_port}")
        file_message = make_file_message(
            file_data, MessageType.PUT, socket_host, socket_port
        )

        s.sendall(add_len_prefix(file_message.serialize()))
        data = recvall(s)
        file_message = FileStoreMessage.deserialize(data)
        print(f"Received {file_message}")
