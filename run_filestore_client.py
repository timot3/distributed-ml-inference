import socket
import time
from typing import List

from Node.types import FileMessage, Message, MessageType
from Node.utils import get_message_from_bytes, trim_len_prefix, add_len_prefix

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
    file_data: bytes, file_name: str, message_type: MessageType, host: str, port: int
) -> FileMessage:
    """
    Create a filestore message from the file data
    :param file_data: the file data in bytes
    :param message_type: The MessageType
    :param host: the host ip (where the message is sent from)
    :param port: the host port (where the message is sent from)
    :return: the FileStoreMessage with dummy values for ip, port, and timestamp
    """
    return FileMessage(
        message_type,  # message type
        host,  # ip
        port,  # port
        0,  # timestamp
        file_name,  # file name
        0,  # version
        file_data,  # file data
    )


def send_file_message(ip, port, file_message: FileMessage):
    """
    Sends a filestore message to the ip and port
    :param ip: the ip to send the message to
    :param port: the port to send the message to
    :param file_message: the filestore message
    :return: None
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((ip, port))
        sock.sendall(add_len_prefix(file_message.serialize()))
        data = recvall(sock)
    file_message = get_message_from_bytes(data)
    return file_message


def _get_filestore_command() -> List[str]:
    """
    Gets the filestore command as per the user input
    :return: the filestore command
    """

    command_menu = """Enter a command:
        put <localfilename> <sdfsfilename>
        get <sdfsfilename> <localfilename>
        delete <sdfsfilename>
    """
    command = input(command_menu)
    command = command.split(" ")
    return command


def get_filestore_command(host, port, command=None):
    command_message: Message
    if command is None:
        command = _get_filestore_command()
    if command[0] == "put":
        # read commands[1] and send the data to the introducer
        with open(command[1], "rb") as f:
            file_data = f.read()
        command_message = make_file_message(
            file_data, command[2], MessageType.PUT, host, port
        )

        response = send_file_message(host, port, command_message)
        print(response)

    elif command[0] == "get":
        command_message = make_file_message(b"", command[1], MessageType.GET, host, port)
        response = send_file_message(host, port, command_message)
        print(response)

        with open(command[2], "wb") as f:
            f.write(response.data)
        print("Done writing file")

    elif command[0] == "delete":
        command_message = make_file_message(b"", MessageType.DELETE, host, port)
        response = send_file_message(host, port, command_message)
        print(response)

    else:
        print("Invalid command")


if __name__ == "__main__":
    HOST, PORT = "127.0.0.1", 8080
    # while True:
    #     get_filestore_command(HOST, PORT)

    puts = []
    gets = []
    alphabet = "abcdefghijklmnopqrstuvwxyz".upper()
    for i in range(10):
        puts.append(["put", f"testfiles/{alphabet[i]}.txt", f"{alphabet[i]}.txt"])
        gets.append(["get", f"{alphabet[i]}.txt", f"testfiles/res_{alphabet[i]}.txt"])

    for get, put in zip(gets, puts):
        get_filestore_command(HOST, PORT, put)
        time.sleep(5)
        get_filestore_command(HOST, PORT, get)
        time.sleep(3)
