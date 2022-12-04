"""
This file contains most of the types used in the project, including the Message class and its subclasses.
These classes are used to serialize and deserialize messages.

It also contains useful magic number variables.
"""

import socket
import struct
import time
from enum import Enum, IntEnum
from typing import List, Any, Optional, Set, Tuple, Union
import logging
from threading import Lock

from FileStore.FileStore import File, FileStore
from ML.modeltypes import ModelType
from Node.LoadBalancer.Batch import Batch

lock = Lock()

# timeout duration of heartbeat
HEARTBEAT_WATCHDOG_TIMEOUT = 5

BUFF_SIZE = 4096 * 4096

INTRODUCER_HOST = "localhost"
INTRODUCER_PORT = 8080

ELECT_LEADER_TIMEOUT = 3

REPLICATION_LEVEL = 4

NUM_JOBS = 2  # resnet, alexnet


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


class LoadRepresentation:
    """
    Load is defined as the sum of time a node has spent processing batches over the last 10 seconds.
    """

    def __init__(self, model_type):
        self.load = 0
        self.model_type = model_type
        self.batches = []

    def add_batch(self, batch: Batch):
        self.batches.append(batch)

    def remove_batch(self, batch):
        self.batches.remove(batch)

    def get_load(self):
        """
        Determine the load over the last 10 seconds. If there are any batches older than 10 seconds,
        remove them from batches and update the load unless the batch is still active.

        Returns: the load over the last 10 seconds
        """
        current_time = int(time.time())
        for batch in self.batches:
            if current_time - batch.start_time > 10:
                self.remove_batch(batch)
        if len(self.batches) == 0:
            return 0
        return sum(batch.duration for batch in self.batches) / 10


class Member:
    def __init__(self, ip: str, port: int, timestamp: int, last_heartbeat: int = 0):
        self.ip: str = ip
        self.port: int = port
        self.timestamp: int = timestamp
        self.files: FileStore = FileStore()
        self.active_queries = List[int]

        # load representation
        self.loads = {}
        for model_type in range(NUM_JOBS):
            self.loads[ModelType(model_type)] = LoadRepresentation(ModelType(model_type))

        if last_heartbeat == 0:
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
            logging.warning(
                f"Exactly the same elements detected when doing a less than comparison"
            )
        if self.timestamp == other.timestamp:
            # tiebreak
            return self.ip < other.ip
        return self.timestamp < other.timestamp

    def __hash__(self):
        return hash((self.ip, self.port, self.timestamp))

    def get_load(self, model_type: ModelType) -> int:
        return self.loads[model_type].get_load()

    def get_least_loaded_model(self) -> ModelType:
        """Gets the model type that is least loaded on this machine

        Returns:
            ModelType: The model type that is least loaded on this machine
        """
        least_loaded_model = ModelType(0)
        least_load = self.loads[least_loaded_model].get_load()
        for model_type in range(1, NUM_JOBS):
            model_type = ModelType(model_type)
            load = self.loads[model_type].get_load()
            if load < least_load:
                least_loaded_model = model_type
                least_load = load
        return least_loaded_model

    def get_total_load(self):
        return sum(load.get_load() for load in self.loads.values())

    def add_batch(self, batch: Batch):
        self.loads[batch.model_type].add_batch(batch)

    def remove_batch(self, batch: Batch):
        self.loads[batch.model_type].remove_batch(batch)

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
        """Determines if this machine is the same as other machine

        Args:
            other (Member): The other member to compare to

        Returns:
            bool: True if the machines' ip and port are the same
        """
        return self.ip == other.ip and self.port == other.port


class MembershipList(list):
    def has_member(self, member: Member) -> bool:
        """Determines if the membership list has a machine

        Args:
            member (Member): The member to check for

        Returns:
            bool: True if the member is in the membership list
        """
        for m in self:
            if m.is_same_machine_as(member):
                return True
        return False

    def update_heartbeat(self, member: Member, new_timestamp: int) -> bool:
        """Update the heartbeat of the member if it exists in the membership list.
        Returns True if the member was updated, False otherwise (if the member was not found).

        Args:
            member (Member): The member to update
            new_timestamp (int): The new timestamp to update to

        Returns:
            bool: True if the member was updated, False otherwise
        """
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

    def find_members_without_file(
        self, file_name: str, file_version: int = -1
    ) -> List[Member]:
        machines = []
        for m in self:
            if not m.files.has_file_version(file_name, file_version):
                machines.append(m)

        return machines

    def find_machine_with_latest_version(
        self, file_name: str
    ) -> Tuple[Optional[Member], Optional[int]]:
        machines = self.find_machines_with_file(file_name)
        if len(machines) == 0:
            return None, None
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
    def deserialize(cls, data: Union[bytes, bytearray]):
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

    def get_least_loaded_member(self):
        least_loaded_member = None
        least_load = float("inf")
        for member in self:  # type: Member
            load = member.get_total_load()
            if load < least_load:
                least_loaded_member = member
                least_load = load
        return least_loaded_member

    def get_model_load(self, model_type: ModelType):
        load = 0
        for member in self:  # type: Member
            load += member.get_load(model_type)
        return load

    def get_least_loaded_node_for_model(self, model_type) -> Member:
        least_loaded_member = None
        least_load = float("inf")
        for member in self:  # type: Member
            load = member.get_load(model_type)
            if load < least_load:
                least_loaded_member = member
                least_load = load
        return least_loaded_member
