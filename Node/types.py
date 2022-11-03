import socket
import struct
from enum import IntEnum
from typing import List, Any, Optional
import logging
from threading import Lock

lock = Lock()

# timeout duration of heartbeat
HEARTBEAT_WATCHDOG_TIMEOUT = 5


class MessageType(IntEnum):
    # Communication messages
    JOIN = 0
    LEAVE = 1
    PING = 2
    PONG = 3
    DISCONNECTED = 4  # sent to node that is disconnected

    # Election messages
    # todo @zhuxuan: add election messages

    # FileStore messages
    PUT = 7
    GET = 8
    DELETE = 9


# https://stackoverflow.com/questions/287871/how-do-i-print-colored-text-to-the-terminal
class bcolors:
    HEADER = "\033[95m"
    OKBLUE = "\033[94m"
    OKCYAN = "\033[96m"
    OKGREEN = "\033[92m"
    WARNING = "\033[93m"
    FAIL = "\033[91m"
    ENDC = "\033[0m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"


# join message has the following fields:
# 1 byte for message type
# 4 bytes for my ip address
# 2 bytes for my port
# 4 bytes for my timestamp
JOIN_FORMAT = "!B4sHI"
join_struct = struct.Struct(JOIN_FORMAT)

# membership list message has the following fields
# 4 bytes for the ip address
# 2 bytes for the port
# 4 bytes for the timestamp
# this repeats for each machine in the membership list
MEMBERSHIP_LIST_FORMAT = "!4sHI"
membership_list_struct = struct.Struct(MEMBERSHIP_LIST_FORMAT)

# communication message has the following fields
# 1 byte for message type
# 4 bytes for the ip address of the sender
# 2 bytes for the port of the sender
# 4 bytes for the timestamp of the sender
COMMUNICATION_FORMAT = "!B4sHI"
communication_struct = struct.Struct(COMMUNICATION_FORMAT)


class Message:
    def __init__(self, message_type: MessageType, ip: str, port: int, timestamp: int):
        self.message_type: MessageType = message_type
        self.ip: str = ip
        self.port: int = port
        self.timestamp: int = timestamp

    def serialize(self):
        # convert the ip address to bytes
        ip_bytes = socket.inet_aton(self.ip)
        return communication_struct.pack(
            self.message_type, ip_bytes, self.port, self.timestamp
        )

    @classmethod
    def deserialize(cls, data: bytes):
        message_type, ip, port, timestamp = communication_struct.unpack(data)
        # convert the ip address to a string
        ip = socket.inet_ntoa(ip)
        return cls(message_type, ip, port, timestamp)

    def __str__(self):
        msg_type = MessageType(self.message_type).name
        return f"Message({msg_type}, ip={self.ip}, port={self.port}, timestamp={self.timestamp})"

    def __eq__(self, other):
        return (
            self.message_type == other.message_type
            and self.ip == other.ip
            and self.port == other.port
            and self.timestamp == other.timestamp
        )

    def __hash__(self):
        return hash((self.message_type, self.ip, self.port, self.timestamp))


class Member:
    def __init__(self, ip: str, port: int, timestamp: int, last_heartbeat: int = None):
        self.ip: str = ip
        self.port: int = port
        self.timestamp: int = timestamp

        if last_heartbeat is None:
            self.last_heartbeat = timestamp
        else:
            self.last_heartbeat = last_heartbeat

    def __str__(self):
        return f"Member(ip={self.ip}, port={self.port}, timestamp={self.timestamp})"

    def __repr__(self) -> str:
        return self.__str__()

    def __eq__(self, other):
        return (
            self.ip == other.ip
            and self.port == other.port
            and self.timestamp == other.timestamp
        )

    def __hash__(self):
        return hash((self.ip, self.port, self.timestamp))

    def to_tuple(self):
        return self.ip, self.port, self.timestamp

    def serialize(self):
        # convert the "ip:port:timestamp" to bytes
        bytes_msg = f"{self.ip}:{self.port}:{self.timestamp}".encode()
        return bytes_msg

    @classmethod
    def deserialize(cls, data: bytes):
        # convert the bytes to a string
        msg = data.decode()
        ip, port, timestamp = msg.split(":")
        last_heartbeat = timestamp
        return cls(ip, int(port), int(timestamp), last_heartbeat=int(last_heartbeat))

    @classmethod
    def from_tuple(cls, tup):
        ip, port, timestamp = tup
        try:
            # verify that the ip is valid
            socket.inet_aton(ip)
        except socket.error:
            raise ValueError(f"Invalid ip address: {ip}")
        # repeat with port
        try:
            port = int(port)
        except ValueError:
            raise ValueError(f"Invalid port: {port}")
        # repeat with timestamp
        try:
            timestamp = int(timestamp)
        except ValueError:
            raise ValueError(f"Invalid timestamp: {timestamp}")
        return Member(ip, port, timestamp)

    def is_same_machine_as(self, other) -> bool:
        return self.ip == other.ip and self.port == other.port


class MembershipList(list):
    # def __init__(self, membership_list: List[Member]):
    #     super().__init__()
    #     self.list = membership_list

    def has_machine(self, member: Member) -> bool:
        for m in self:
            if m.is_same_machine_as(member):
                return True
        return False

    def update_heartbeat(self, member, new_timestamp) -> bool:
        # find the member in the list
        for m in self:
            if m.is_same_machine_as(member):
                # update the timestamp
                # acquire a lock
                with lock:
                    m.last_heartbeat = new_timestamp
                return True

        logging.getLogger(__name__).info(f"Could not find {member} in membership list")
        return False

    def get_machine(self, member) -> Optional[Member]:
        for m in self:
            if m.is_same_machine_as(member):
                return m

        return None

    def __str__(self):
        members = [str(member) for member in self]
        return f"MembershipList({', '.join(members)})"

    def serialize(self):
        # ip address and port and timestamp are separated by a colon
        # different machines are separated by a comma
        # the membership list is a string

        bytes_str = b",".join([member.serialize() for member in self])
        return bytes_str

    @classmethod
    def deserialize(cls, data: bytes):
        # convert the bytes to a string
        membership_list_str = data.decode()
        # split the string into a list of strings
        membership_list_str_list = membership_list_str.split(",")
        # split each string into a list of ip and port

        membership_list = [tuple(m.split(":")) for m in membership_list_str_list]
        # convert to member objects
        membership_list = [Member.from_tuple(m) for m in membership_list]
        # verify that the ip and port are valid

        return cls(membership_list)
