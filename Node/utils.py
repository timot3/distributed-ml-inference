import logging
import socket
import struct
import textwrap
import time
from typing import Tuple

from Node.nodetypes import (
    LSMessage,
    bcolors,
    MessageType,
    Message,
    FileMessage,
    MembershipListMessage,
    FileReplicationMessage,
    FileVersionMessage,
    BUFF_SIZE,
)


def in_red(text):
    return bcolors.FAIL + text + bcolors.ENDC


def in_green(text):
    return bcolors.OKGREEN + text + bcolors.ENDC


def in_blue(text):
    return bcolors.OKBLUE + text + bcolors.ENDC


def add_len_prefix(message: bytes) -> bytes:
    msg = struct.pack(">I", len(message)) + message
    return msg


def trim_len_prefix(message: bytes) -> Tuple[int, bytes]:
    msg_len = struct.unpack(">I", message[:4])[0]
    msg = message[4 : 4 + msg_len]
    return msg_len, msg


def _send(msg: Message, addr: Tuple[str, int], logger: logging.Logger = None) -> bool:
    if logger is None:
        logger = logging.getLogger(__name__)
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            if s.connect_ex(addr) != 0:
                raise ConnectionError("Could not connect to {}".format(addr))
            msg = add_len_prefix(msg.serialize())
            s.sendall(msg)
        return True

    except Exception as e:
        logger.error(f"Error sending message: {e}")
    finally:
        return False


def _recvall(sock: socket.socket, logger: logging.Logger = None) -> bytes:
    if logger is None:
        logger = logging.getLogger(__name__)
    # use popular method of recvall
    data = bytearray()
    rec = sock.recv(BUFF_SIZE)
    # remove the length prefix
    msg_len, msg = trim_len_prefix(rec)
    data.extend(msg)
    # read the rest of the data, if any
    while len(data) < msg_len:
        msg = sock.recv(BUFF_SIZE)
        if not msg:
            break
        data.extend(msg)
    logger.debug(f"Received {len(data)} bytes from {sock.getpeername()}")
    return data


def _send_file_err(sock, member, file_name):
    # send error message
    error_message = FileMessage(
        MessageType.FILE_ERROR,
        member.host,
        member.port,
        member.timestamp,
        file_name,
        -1,
        b"",
    )
    sock.sendall(add_len_prefix(error_message.serialize()))


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
    5 -> ls all: list files in the filestore
    6 -> ls <file>: list specific files in filestore
    7 -> store: List the files currently stored in this node's filestore
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
    elif command == 5:
        node.send_ls()
    elif command == 6:
        raise NotImplementedError
    elif command == 7:
        file_store = node.get_file_store()
        filenames = file_store.get_file_names()
        print(bcolors.OKBLUE + ",".join(filenames) + bcolors.ENDC)

    else:
        print("Invalid command")


def run_node_command_menu(node):
    while True:
        command = _get_command_option()
        _handle_command(node, command)


def timed_out(timestamp, timeout):
    return time.time() - timestamp > timeout


def is_membership_message(message_type: int) -> bool:
    return message_type == MessageType.MEMBERSHIP_LIST


def is_communication_message(message_type: int) -> bool:
    """
    Checks if the message is a communication message
    :param message_type: the message type
    :return: True if the message is a communication message, False otherwise
    """
    return (
        message_type == MessageType.NEW_NODE
        or message_type == MessageType.JOIN
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
    # future work
    return (
        message_type == MessageType.ELECT_PING
        or message_type == MessageType.CLAIM_LEADER_PING
        or message_type == MessageType.CLAIM_LEADER_ACK
    )


def is_filestore_message(message_type: int) -> bool:
    """
    Checks if the message is a filestore message
    :param message_type: the message type
    :return: True if the message is a filestore message, False otherwise
    """
    return (
        message_type == MessageType.PUT
        or message_type == MessageType.GET
        or message_type == MessageType.DELETE
        or message_type == MessageType.FILE_ACK
        or message_type == MessageType.FILE_ERROR
    )


def is_ls_message(message_type: int) -> bool:
    """
    Checks if the message is a ls message
    :param message_type: the message type
    :return: True if the message is a ls message, False otherwise
    """
    return message_type == MessageType.LS


def is_file_replication_message(message_type: int) -> bool:
    """
    Checks if the message is a file replication message
    :param message_type: the message type
    :return: True if the message is a file replication message, False otherwise
    """
    return (
        message_type == MessageType.FILE_REPLICATION_REQUEST
        or message_type == MessageType.FILE_REPLICATION_ACK
    )


def is_fileversion_message(message_type: int) -> bool:
    return message_type == MessageType.GET_VERSIONS or message_type == MessageType.GET


def get_message_from_bytes(data: bytes) -> Message:
    """
    Factory method to get either a Message, FileStoreMessage, or ElectionMessage
    from a byte array.

    :param data: the bytes received
    :return: the Message, FileStoreMessage, or ElectionMessage
    """

    if len(data) == 0:
        raise ValueError("Empty message")

    # the first byte of the message is the message type
    # get it with struct.unpack

    message_type = struct.unpack(">I", data[:4])[0]

    if is_communication_message(message_type):
        return Message.deserialize(data)

    elif is_filestore_message(message_type):
        return FileMessage.deserialize(data)

    elif is_fileversion_message(message_type):
        return FileVersionMessage.deserialize(data)

    elif is_ls_message(message_type):
        return LSMessage.deserialize(data)

    elif is_membership_message(message_type):
        return MembershipListMessage.deserialize(data)

    elif is_election_message(message_type):
        raise NotImplementedError

    elif is_file_replication_message(message_type):
        return FileReplicationMessage.deserialize(data)

    else:
        raise ValueError("Invalid message type")


# Useful for displaying/debugging purposes, not used for functionality
ip_url_dict = {
    socket.gethostbyname(
        f"fa22-cs425-25{i:02}.cs.illinois.edu"
    ): f"fa22-cs425-25{i:02}.cs.illinois.edu"
    for i in range(1, 10)
}


def get_replication_level(num_nodes, replication_factor):
    """
    Gets the replication level for the filestore
    :param num_nodes: the number of nodes in the membership list
    :param replication_factor: the replication factor
    :return: the replication level
    """
    return min(num_nodes, replication_factor)
