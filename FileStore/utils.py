import struct


def add_len_prefix(message: str) -> int:
    msg = struct.pack(">I", len(message)) + message
    return msg


def trim_len_prefix(message):
    msg_len = struct.unpack(">I", message[:4])[0]
    msg = message[4 : 4 + msg_len]
    return msg_len, msg
