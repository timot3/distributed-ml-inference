import socket
import struct
import textwrap
import time

from Node.types import bcolors, MessageType, Message, FileStoreMessage


def is_communication_message(message_type: int) -> bool:
    """
    Checks if the message is a communication message
    :param message_type: the message type
    :return: True if the message is a communication message, False otherwise
    """
    return (
        message_type == MessageType.JOIN
        or message_type == MessageType.LEAVE
        or message_type == MessageType.PING
        or message_type == MessageType.PONG
    )


def is_election_message(message_type: int) -> bool:
    """
    Checks if the message is an election message
    :param message_type: the message type
    :return: True if the message is an election message, False otherwise
    """
    raise NotImplementedError
    # future work
    # return (
    #     message_type == MessageType.ELECT_SEND.value
    #     or message_type == MessageType.CLAIM_LEADER.value
    # )


def is_filestore_message(message_type: int) -> bool:
    """
    Checks if the message is a filestore message
    :param message_type: the message type
    :return: True if the message is a filestore message, False otherwise
    """
    return (
        message_type == MessageType.PUT.value
        or message_type == MessageType.GET.value
        or message_type == MessageType.DELETE.value
    )


def in_red(text):
    return bcolors.FAIL + text + bcolors.ENDC


def in_green(text):
    return bcolors.OKGREEN + text + bcolors.ENDC


def in_blue(text):
    return bcolors.OKBLUE + text + bcolors.ENDC


def add_len_prefix(message: str) -> int:
    msg = struct.pack(">I", len(message)) + message
    return msg


def trim_len_prefix(message):
    msg_len = struct.unpack(">I", message[:4])[0]
    msg = message[4 : 4 + msg_len]
    return msg_len, msg


def get_self_ip_and_port(sock):
    """
    Gets the ip and port of the socket
    :param sock: the socket
    :return: the ip and port of the socket
    """
    ip = "127.0.0.1"  # TODO: get the ip of THIS machine
    port = sock.getsockname()[1]
    return ip, port


def get_any_open_port() -> int:
    """
    Gets a random open port
    :return: a random open port
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("", 0))
        port = sock.getsockname()[1]

    return port


def _get_command_option() -> int:
    commands = """Input a command:
    1 -> list_mem: list the membership list
    2 -> list_self: list self's id
    3 -> join: join the group
    4 -> leave: leave the group
    """
    # unindent the commands using textwrap
    commands = textwrap.dedent(commands)

    command = input(commands)
    try:
        command = int(command)
    except ValueError:
        command = -1

    return command


def _handle_command(node, command):
    if command == 1:
        membership_list = node.get_membership_list()
        print(bcolors.OKBLUE + str(membership_list) + bcolors.ENDC)
    elif command == 2:
        print(bcolors.OKBLUE + "NODE ID: " + str(node.get_self_id()) + bcolors.ENDC)
    elif command == 3:
        node.join_network()
    elif command == 4:
        node.leave_network()
    else:
        print("Invalid command")


def run_node_command_menu(node):
    while True:
        command = _get_command_option()
        _handle_command(node, command)


def timed_out(timestamp, timeout):
    return time.time() - timestamp > timeout


def get_message_from_bytes(data: bytes) -> Message:
    """
    Factory method to get either a Message, FileStoreMessage, or ElectionMessage
    from a byte array.

    :param data: the bytes received
    :return: the Message, FileStoreMessage, or ElectionMessage
    """
    # the first byte of the message is the message type
    # get it with struct.unpack

    if len(data) == 0:
        raise ValueError("Empty message")

    message_type = struct.unpack(">B", data[:1])[0]

    if is_communication_message(message_type):
        return Message.deserialize(data)
    elif is_filestore_message(message_type):
        return FileStoreMessage.deserialize(data)
    elif is_election_message(message_type):
        raise NotImplementedError
        # future work
        # return ElectionMessage.deserialize(data)
    else:
        raise ValueError("Invalid message type")
