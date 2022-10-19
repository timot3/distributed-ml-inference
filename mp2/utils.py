import socket
import struct


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
