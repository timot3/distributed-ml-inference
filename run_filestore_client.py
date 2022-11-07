import argparse
import os
import socket
import time
from typing import List

from Node.nodetypes import FileMessage, Message, MessageType, VM1_URL, FileVersionMessage
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


def make_file_version_message(
    file_name: str,
    message_type: MessageType,
    host: str,
    port: int,
    versions: List[int] = [],
) -> FileVersionMessage:
    """
    Create a filestore message from the file data
    :param file_name: the file name
    :param message_type: The MessageType
    :param host: the host ip (where the message is sent from)
    :param port: the host port (where the message is sent from)
    :param versions: the versions to get. if empty, latest version is returned
    :return: the FileStoreMessage with dummy values for ip, port, and timestamp
    """
    return FileVersionMessage(
        message_type,  # message type
        host,  # ip
        port,  # port
        0,  # timestamp
        file_name,  # file name
        versions,  # versions
    )


def send_file_message(ip, port, file_message: Message):
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

        command_message = make_file_version_message(
            command[1], MessageType.GET, host, port
        )
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


def test_25_mb(host, port):
    """
    Test sending a 40 MB file
    :return: None
    """
    with open("testfiles/mb25.test", "rb") as f:
        file_data = f.read()
    command_message = make_file_message(
        file_data, "25mb.txt", MessageType.PUT, host, port
    )
    # time the next section
    start = time.time()
    response = send_file_message(host, port, command_message)
    end = time.time()

    print("INSERT TIME (s): \t\t", end - start)

    # read
    command_message = make_file_version_message("25mb.txt", MessageType.GET, host, port)
    start = time.time()
    response = send_file_message(host, port, command_message)
    end = time.time()
    print("READ TIME (s): \t\t", end - start)

    # update
    command_message = make_file_message(
        file_data, "25mb.txt", MessageType.PUT, host, port
    )
    start = time.time()
    response = send_file_message(host, port, command_message)
    end = time.time()
    print("DELETE TIME (s): \t\t", end - start)


def test_40_mb(host, port):
    # upload 40mb file
    with open("testfiles/mb40.test", "rb") as f:
        file_data = f.read()

    command_message = make_file_message(
        file_data, "40mb.txt", MessageType.PUT, host, port
    )

    # send the message
    response = send_file_message(host, port, command_message)
    print(response)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--local", action="store_true", help="Run the client locally")
    parser.add_argument("--menu", action="store_true", help="Run the client in menu mode")

    args = parser.parse_args()
    if args.local:
        HOST, PORT = "127.0.0.1", 8080
    else:
        leader_ip = socket.gethostbyname(VM1_URL)
        HOST, PORT = leader_ip, 8080

    if args.menu:
        while True:
            get_filestore_command(HOST, PORT)

    # test_25_mb(HOST, PORT)
    test_40_mb(HOST, PORT)

    # puts = []
    # gets = []
    # alphabet = "abcdefghijklmnopqrstuvwxyz".upper()
    # for i in range(10):
    #     puts.append(["put", f"testfiles/{alphabet[i]}.txt", f"{alphabet[i]}.txt"])
    #     gets.append(["get", f"{alphabet[i]}.txt", f"testfiles/res_{alphabet[i]}.txt"])
    #
    # for get, put in zip(gets, puts):
    #     get_filestore_command(HOST, PORT, put)
    #     time.sleep(5)
    #     get_filestore_command(HOST, PORT, get)
    #     time.sleep(3)
