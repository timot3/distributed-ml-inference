import socket
import struct
import textwrap

from Node.types import bcolors


def get_self_ip_and_port(sock) -> (str, int):
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
