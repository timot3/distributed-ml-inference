"""
This file contains most of the types used in the project, including the Message class and its subclasses.
These classes are used to serialize and deserialize messages.

It also contains useful magic number variables.
"""

import socket
import struct
from enum import Enum, IntEnum
from typing import List, Any, Optional, Set, Tuple
import logging
from threading import Lock

from FileStore.FileStore import File, FileStore

lock = Lock()

# timeout duration of heartbeat
HEARTBEAT_WATCHDOG_TIMEOUT = 5

BUFF_SIZE = 4096 * 4096

INTRODUCER_HOST = "localhost"
INTRODUCER_PORT = 8080

ELECT_LEADER_TIMEOUT = 3

REPLICATION_LEVEL = 4


class MessageType(IntEnum):
    # Communication messages
    JOIN = 1
    LEAVE = 2
    PING = 3
    PONG = 4
    DISCONNECTED = 5  # sent to node that is disconnected

    # Election messages
    # todo @zhuxuan: add election messages
    ELECT_PING = 6  # send to all nodes that are lower in id
    CLAIM_LEADER_PING = 8  # The sender claims to be the leader
    CLAIM_LEADER_ACK = 7

    # FileStore messages
    PUT = 9
    GET = 10
    GET_VERSIONS = 11
    DELETE = 12
    FILE_ACK = 13
    LS = 14
    FILE_ERROR = 15
    FILE_REPLICATION_REQUEST = 16
    FILE_REPLICATION_ACK = 17

    # Membership messages
    NEW_NODE = 18
    MEMBERSHIP_LIST = 19


# PORT IDs
class DnsDaemonPortID(IntEnum):
    ELECTION = 8787
    LEADER = 8788


INTRODUCER_PORT = 8789

VM1_URL = "fa22-cs425-2501.cs.illinois.edu"


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
# 4 byte for message type
# 4 bytes for my ip address
# 2 bytes for my port
# 4 bytes for my timestamp
JOIN_FORMAT = "!I4sHI"
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
COMMUNICATION_FORMAT = "!I4sHI"
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
        # focus on the first 14 bytes
        message_type, ip, port, timestamp = communication_struct.unpack(
            data[: communication_struct.size]
        )
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


class FileMessage(Message):
    def __init__(
        self,
        message_type: MessageType,
        ip: str,
        port: int,
        timestamp: int,
        file_name: str,
        version: int,
        data: bytes,
    ):
        super().__init__(message_type, ip, port, timestamp)
        self.file_name = file_name
        self.data = data
        self.version = version

    def serialize(self):
        """
        Serialize the message into bytes
        :return: the bytes representation of the message
        """
        base_message = super().serialize()
        # pack the filename into a 32 byte string using struct.pack
        file_name = struct.pack(">32s", self.file_name.encode("utf-8"))

        # pack the version into a 4 byte int using struct.pack
        version = struct.pack(">I", self.version)

        # finally, append all the bytes together
        return base_message + file_name + version + self.data

    @classmethod
    def deserialize(cls, data: bytes):
        base_size = communication_struct.size

        min_size = base_size + 36
        if len(data) < min_size:
            raise ValueError("Invalid message")

        # get the message type
        # the first 14 bytes are the same as the communication message
        base_message = Message.deserialize(data[: communication_struct.size])
        message_type = base_message.message_type
        ip = base_message.ip
        port = base_message.port
        timestamp = base_message.timestamp

        # get the filename
        file_name = struct.unpack(">32s", data[base_size : base_size + 32])[0]
        file_name = file_name.decode("utf-8").strip("\x00")

        # get the version
        version = struct.unpack(">I", data[base_size + 32 : base_size + 36])[0]

        # get the data using struct.unpack and the length of the data
        remaining_len = len(data) - base_size - 36
        data = struct.unpack(f">{remaining_len}s", data[base_size + 36 :])[0]

        return cls(message_type, ip, port, timestamp, file_name, version, data)

    def __str__(self):
        msg_type = MessageType(self.message_type).name
        data_first_10 = self.data[:10]
        if len(self.data) > 10:
            data_first_10 += b"..."
        return f"FileStoreMessage({msg_type}, file_name={self.file_name}, version={self.version}, data={data_first_10})"


class FileReplicationMessage(Message):
    """
    This message is used to replicate a file to another node
    The node that receives this message will send the file to the
    node at the `to_ip` and `to_port` fields

    Then, the node will reply to the node that sent this message
    with a FileReplicationMessage with the `message_type` set to
    `MessageType.FILE_REPLICATION_ACK` when the file has been sent
    """

    def __init__(
        self,
        message_type: MessageType,
        to_ip: str,  # the ip of the machine that will receive the file
        to_port: int,  # the port of the machine that will receive the file
        to_timestamp: int,  # the timestamp of the machine that will receive the file
        file_name: str,
        from_ip: str,  # The ip address of the node that has the file
        from_port: int,  # The port of the node that has the file
        from_timestamp: int,  # The timestamp of the node that has the file
    ):
        super().__init__(message_type, to_ip, to_port, to_timestamp)
        self.file_name = file_name
        self.from_ip = from_ip
        self.from_port = from_port
        self.from_timestamp = from_timestamp

    def serialize(self):
        base_message = super().serialize()
        # pack the filename into a 32 byte string using struct.pack
        file_name = struct.pack(">32s", self.file_name.encode("utf-8"))

        # pack the from_ip into a 4 byte string using struct.pack
        from_ip = socket.inet_aton(self.from_ip)

        # pack the from_port into a 2 byte int using struct.pack
        from_port = struct.pack(">H", self.from_port)

        # pack the from_timestamp into a 4 byte int using struct.pack
        from_timestamp = struct.pack(">I", self.from_timestamp)

        # finally, append all the bytes together
        return base_message + file_name + from_ip + from_port + from_timestamp

    @classmethod
    def deserialize(cls, data: bytes):
        base_size = communication_struct.size

        min_size = base_size + (32 + 4 + 2 + 4)
        if len(data) < min_size:
            raise ValueError("Invalid message")

        # get the message type
        # the first 14 bytes are the same as the communication message
        base_message = Message.deserialize(data[: communication_struct.size])
        message_type = base_message.message_type
        ip = base_message.ip
        port = base_message.port
        timestamp = base_message.timestamp

        # get the filename
        file_name = struct.unpack(">32s", data[base_size : base_size + 32])[0]
        file_name = file_name.decode("utf-8").strip("\x00")

        # get the from_ip
        from_ip = socket.inet_ntoa(data[base_size + 32 : base_size + 36])

        # get the from_port
        from_port = struct.unpack(">H", data[base_size + 36 : base_size + 38])[0]

        # get the from_timestamp
        from_timestamp = struct.unpack(">I", data[base_size + 38 : base_size + 42])[0]

        return cls(
            message_type,
            ip,
            port,
            timestamp,
            file_name,
            from_ip,
            from_port,
            from_timestamp,
        )

    def __str__(self):
        msg_type = MessageType(self.message_type).name
        return f"FileReplicationMessage({msg_type}, file_name={self.file_name}, from_ip={self.from_ip}, from_port={self.from_port}, from_timestamp={self.from_timestamp})"


class FileVersionMessage(Message):
    def __init__(
        self,
        message_type: MessageType,
        ip: str,
        port: int,
        timestamp: int,
        file_name: str,
        versions: List[int],
    ):
        super().__init__(message_type, ip, port, timestamp)
        self.file_name = file_name
        self.versions = versions

    def serialize(self):
        base_message = super().serialize()
        # pack the filename into a 32 byte string using struct.pack
        file_name = struct.pack(">32s", self.file_name.encode("utf-8"))

        # pack the number of versions into a 4 byte int using struct.pack
        num_versions = struct.pack(">I", len(self.versions))

        # pack the versions into a byte string using struct.pack
        versions = struct.pack(f">{len(self.versions)}I", *self.versions)

        # finally, append all the bytes together
        return base_message + file_name + num_versions + versions

    @classmethod
    def deserialize(cls, data: bytes):
        base_size = communication_struct.size

        min_size = base_size + (32 + 4)
        if len(data) < min_size:
            raise ValueError("Invalid message")

        # get the message type
        # the first 14 bytes are the same as the communication message
        base_message = Message.deserialize(data[: communication_struct.size])
        message_type = base_message.message_type
        ip = base_message.ip
        port = base_message.port
        timestamp = base_message.timestamp

        # get the filename
        file_name = struct.unpack(">32s", data[base_size : base_size + 32])[0]
        file_name = file_name.decode("utf-8").strip("\x00")

        # get the number of versions
        num_versions = struct.unpack(">I", data[base_size + 32 : base_size + 36])[0]

        # get the versions
        versions = struct.unpack(f">{num_versions}I", data[base_size + 36 :])[0]

        return cls(message_type, ip, port, timestamp, file_name, versions)


class LSMessage(Message):
    def __init__(
        self,
        message_type: MessageType,
        ip: str,
        port: int,
        timestamp: int,
        files: List[File],
    ):
        super().__init__(message_type, ip, port, timestamp)
        self.files = files

    def serialize(self):
        base_message = super().serialize()
        # serialize the files
        ls_files = b",".join(file.ls_serialize() for file in self.files)
        return base_message + ls_files

    @classmethod
    def deserialize(cls, data: bytes):
        base_message = Message.deserialize(data[: communication_struct.size])
        message_type = base_message.message_type
        ip = base_message.ip
        port = base_message.port
        timestamp = base_message.timestamp

        # get the file list
        if len(data) > communication_struct.size:
            files = data[communication_struct.size :].split(b",")
            files = [File.ls_deserialize(file) for file in files]
        else:
            files = []

        return cls(message_type, ip, port, timestamp, files)

    def __str__(self):
        msg_type = MessageType(self.message_type).name
        return f"LSMessage({msg_type}, files={self.files})"


class ElectionMessage(Message):
    def __init__(
        self,
        message_type: MessageType,
        ip: str,
        port: int,
        timestamp: int,
    ):
        super.__init__(self, message_type, ip, port, timestamp)


class Member:
    def __init__(self, ip: str, port: int, timestamp: int, last_heartbeat: int = None):
        self.ip: str = ip
        self.port: int = port
        self.timestamp: int = timestamp
        self.files: FileStore = FileStore()

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

    def __lt__(self, other):
        if self == other:
            logging.warn(
                f"Exactly the same elements detected when doing a less than comparison"
            )
        if self.timestamp == other.timestamp:
            # tiebreak
            return self.ip < other.ip
        return self.timestamp < other.timestamp

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
        return False

    def get_machine(self, member) -> Optional[Member]:
        for m in self:
            if m.is_same_machine_as(member):
                return m

        return None

    def find_machines_with_file(
        self, file_name: str, file_version: int = -1
    ) -> List[Member]:
        machines = []
        for m in self:
            if m.files.has_file_version(file_name, file_version):
                machines.append(m)
        return machines

    def find_machines_without_file(
        self, file_name: str, file_version: int = -1
    ) -> List[Member]:
        machines = []
        for m in self:
            if not m.files.has_file_version(file_name, file_version):
                machines.append(m)

        return machines

    def find_machine_with_latest_version(
        self, file_name: str
    ) -> Tuple[Optional[Member], int]:
        machines = self.find_machines_with_file(file_name)
        if len(machines) == 0:
            return None
        machines.sort(key=lambda m: m.files.get_file_version(file_name), reverse=True)
        return machines[0], machines[0].files.get_file_version(file_name)

    def __str__(self):
        members = [str(member) for member in self]
        return f"MembershipList({', '.join(members)})"

    def __sub__(self, other):
        # return a new membership list with the members that are not in the other
        return MembershipList([member for member in self if member not in other])

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

    def get_files_on_all_str(self) -> str:
        res = ""
        for m in self:
            files = ", ".join(m.files)
            res += f"{m}: {files}\n"

        return res


class MembershipListMessage(Message):
    def __init__(
        self,
        message_type: MessageType,
        ip: str,
        port: int,
        timestamp: int,
        members: MembershipList,
    ):
        super().__init__(message_type, ip, port, timestamp)
        self.membership_list = members

    def serialize(self):
        """
        Serialize the message into bytes
        :return: the bytes representation of the message
        """
        base_message = super().serialize()
        # pack the number of members into a 4 byte int using struct.pack
        membership_list_serialized = self.membership_list.serialize()

        return base_message + membership_list_serialized

    @classmethod
    def deserialize(cls, data: bytes):
        base_size = communication_struct.size

        if len(data) < base_size:
            raise ValueError("Invalid message")

        # get the message type
        # the first 14 bytes are the same as the communication message
        base_message = Message.deserialize(data[: communication_struct.size])
        message_type = base_message.message_type
        ip = base_message.ip
        port = base_message.port
        timestamp = base_message.timestamp

        # deserialize the remainder of the message using MembershipList.deserialize
        # the remainder of the message is the membership list
        membership_list = MembershipList.deserialize(data[base_size:])

        return cls(message_type, ip, port, timestamp, membership_list)

    def __str__(self):
        msg_type = MessageType(self.message_type).name
        return f"MembershipListMessage({msg_type}, members={self.members})"
