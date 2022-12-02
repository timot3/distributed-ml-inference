import concurrent.futures
import logging
import socketserver
import socket
import threading
import time
import traceback
from typing import Any, List, Optional, Tuple, Dict
from ML.modeltypes import ModelCollection

# from nodeelectinfo import NodeElectInfo

import random

from FileStore.FileStore import File, FileStore
from ML.modeltypes import ClassifierModel, DatasetType, DummyModel
from Node.handler import NodeHandler
from Node.messages import (
    FileReplicationMessage,
    LSMessage,
    MembershipListMessage,
    Message,
    MessageType,
    FileMessage,
)
from .LoadBalancer.LoadBalancer import LoadBalancer
from .nodetypes import (
    MembershipList,
    Member,
    HEARTBEAT_WATCHDOG_TIMEOUT,
    BUFF_SIZE,
    DnsDaemonPortID,
    VM1_URL,
)
from .utils import (
    add_len_prefix,
    in_blue,
    in_red,
    trim_len_prefix,
    get_message_from_bytes,
    in_green,
    _recvall,
)


# the node class is a subclass of UDPServer
# Its server bind method is called when the server is created
# and connects to the introducer server via a tcp socket
class NodeTCPServer(socketserver.ThreadingTCPServer):
    def __init__(
        self,
        host,
        port,
        is_introducer=False,
        slow_mode=False,
    ):
        # call the super class constructor
        super().__init__((host, port), NodeHandler, bind_and_activate=False)

        self.host = host
        self.port = port

        self.is_introducer = is_introducer
        self.slow_mode = slow_mode

        # initialized when the node joins the ring
        self.introducer_host = None
        self.introducer_port = None

        self.leader_host = self.host
        self.leader_port = self.port

        # initialized when the node joins the ring
        self.timestamp: int = 0
        self.member: Member = None

        self.logger = logging.getLogger("NodeServer")
        self.logger.setLevel(logging.INFO)

        # create a tcp socket to connect to the introducer
        # it will be initialized when the node joins the network
        self.introducer_socket = None

        self.in_ring = False

        self.membership_list = MembershipList([])

        self.file_store = FileStore()

        self.dnsdaemon_ip = socket.gethostbyname(host)
        # self.election_info = NodeElectInfo()

        self.model_collection = ModelCollection(self)

        # init the load balancer, if necessart
        if self.is_introducer:
            self.load_balancer = LoadBalancer(self)

        # init the models
        self.model1 = DummyModel(DatasetType.OXFORD_PETS)
        self.model2 = DummyModel(DatasetType.CIFAR10)

    def validate_request(self, request, message) -> bool:
        data = request[0]
        if len(data) == 0:
            return False
        if not self.in_ring:
            self.logger.info("Not in ring yet. Ignoring message: {}".format(message))
            return False

        return True

    def join_network(self, introducer_host="localhost", introducer_port=8080) -> bool:
        """
        Join the network by connecting to the introducer
        and processing the received membership list.
        :return: If the connection was successful
        """

        if self.in_ring:
            self.logger.error("Already in ring")
            return False

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as introducer_sock:

            introducer_sock.settimeout(HEARTBEAT_WATCHDOG_TIMEOUT)
            self.timestamp = int(time.time())

            self.member = Member(self.host, self.port, self.timestamp)

            if self.is_introducer:
                self.in_ring = True
                self.leader_host = self.host
                self.leader_port = self.port
                self.logger.info("I am the introducer. I am the leader!")
                # set the membership list to contain only the introducer
                self.membership_list = MembershipList([self.member])
                # TODO: What if the leader leaves and rejoins?

                self.start_heartbeat_watchdog()

                return True

            # store the introducer ip and port
            self.introducer_host = introducer_host
            self.introducer_port = introducer_port

            try:
                if introducer_sock.connect_ex((introducer_host, introducer_port)) != 0:
                    self.logger.error("Could not connect to introducer")
                    return False

                # first, construct self's member
                join_message = Message(
                    MessageType.NEW_NODE, self.host, self.port, self.timestamp
                )

                # send the new node message to the introducer
                introducer_sock.sendall(add_len_prefix(join_message.serialize()))

                # get the response from the introducer
                response = introducer_sock.recv(BUFF_SIZE)
                _, response = trim_len_prefix(response)  # don't care about the length
                # can close the socket now
            except socket.timeout:
                self.logger.error("Could not connect to introducer")
                return False

        # deserialize the response
        # this response should be a MEMBERSHIP LIST
        recvd_message = MembershipListMessage.deserialize(response)

        if recvd_message.message_type != MessageType.MEMBERSHIP_LIST:
            raise ValueError(
                "Expected MEMBERSHIP_LIST message type, got {}".format(
                    recvd_message.message_type
                )
            )

        # update the membership list
        self.membership_list = recvd_message.membership_list
        self.logger.info(f"Received membership list: {self.membership_list}")

        self.in_ring = True

        # set the leader
        self.leader_host = self.membership_list[0].ip
        self.leader_port = self.membership_list[0].port

        # start the heartbeat watchdog
        self.start_heartbeat_watchdog()

        return True

    def rejoin(self):
        """
        Rejoin the network by connecting to the introducer
        and processing the received membership list.
        :return:
        """
        self.membership_list = MembershipList([])
        self.join_network()

    def leave_network(self) -> bool:
        """
        Leave the network by broadcasting a LEAVE message to all neighbors
        :return: True if the node left the network successfully
        """
        # send leave message to all neighbors
        self.logger.info("Sending LEAVE message to neighbors")

        # clear the membership list
        # broadcast the leave message
        leave_message = Message(
            MessageType.LEAVE, self.member.ip, self.member.port, self.member.timestamp
        )
        self.broadcast_to_neighbors(leave_message)
        self.membership_list = MembershipList([])

        # delete all files in file store
        self.file_store = FileStore()

        # disconnect the introducer socket
        self.in_ring = False
        return True

    def _heartbeat_watchdog(self):
        """
        This method periodically checks to see if any members have failed
        It does this by sending pings to neighbors
        If a neighbor does not respond to a ping, it is removed from the membership list
        And a LEAVE message is broadcast to all neighbors
        :return: Never returns
        """
        while True:
            time.sleep(HEARTBEAT_WATCHDOG_TIMEOUT)
            if not self.in_ring or self.member is None:
                continue

            # send a ping to all neighbors
            self.logger.debug("Sending Heartbeat PING to neighbors")
            ping_message = Message(
                MessageType.PING,
                self.member.ip,
                self.member.port,
                self.member.timestamp,
            )
            responses = self.broadcast_to_neighbors(ping_message)
            # sleep for HEARTBEAT_WATCHDOG_TIMEOUT seconds
            # check if any neighbors have failed
            # if they have, remove them from the membership list
            # and broadcast a LEAVE message to all neighbors

            for member, response in responses.items():
                if not response:
                    self.logger.info(
                        f"Member {member} has failed! Attempting to notify them...."
                    )

                    # send a disconnect mesage to the failed member
                    disconnect_message = Message(
                        MessageType.DISCONNECTED,
                        member.ip,
                        member.port,
                        member.timestamp,
                    )
                    if self._send(disconnect_message, member):
                        self.logger.info(
                            f"Successfully notified {member} of their failure, meaning they are still alive"
                        )
                        continue

                    # broadcast a LEAVE message to all neighbors with the failed member's info
                    self.logger.info(
                        f"Failed to notify {member} of their failure, meaning they are dead. Notifying neighbors..."
                    )
                    # remove the failed member from the membership list
                    if self.is_introducer:
                        # replicate files
                        self._rereplicate_files(member)
                    self.membership_list.remove(member)
                    leave_message = Message(
                        MessageType.LEAVE, member.ip, member.port, member.timestamp
                    )
                    self.broadcast_to_neighbors(leave_message)

                # TODO: If leader is detected as failed, send messages to initiate elections.

    def start_heartbeat_watchdog(self):
        """
        This method starts the heartbeat watchdog thread
        :return: the thread object
        """
        self.logger.debug("Starting heartbeat watchdog")
        thread = threading.Thread(target=self._heartbeat_watchdog, daemon=True)
        thread.start()
        return thread

    def get_neighbors(self) -> List[Member]:
        # return a list of neighbors
        # treat neighbors as a circular list
        # find self in the membership list and return the two nodes before it
        # if self is the first node, return the last two nodes
        # if self is the second node, return the last node and the first node
        # if there are less than three nodes, return the nodes that are not self
        if len(self.membership_list) < 2:
            return []  # no neighbors?? (meme here)

        if len(self.membership_list) <= 3:
            return [
                member
                for member in self.membership_list
                if not member.is_same_machine_as(self.member)
            ]

        idx = self.membership_list.index(self.member)
        # return the two nodes before self
        return [self.membership_list[idx - 1], self.membership_list[idx - 2]]

    def _send(self, msg: Message, member: Member, recv=False) -> Any:
        """
        Send a message to a member
        :param msg: The message to send
        :param member: The member to send the message to
        :return: if send was successful
        """
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                # s.settimeout(HEARTBEAT_WATCHDOG_TIMEOUT)
                s.connect((member.ip, member.port))
                msg_bytes = add_len_prefix(msg.serialize())
                s.sendall(msg_bytes)
                if recv:
                    data = _recvall(s, logger=self.logger)
                    return get_message_from_bytes(data)
                else:
                    return True
        except Exception as e:
            self.logger.error(
                in_red("Failed to send message to member {}: {}".format(member, e))
            )
            # print the stack trace
            traceback.print_exc()
            return None

    def broadcast_to(
        self, message: Message, members: List[Member], recv=False
    ) -> Dict[Member, Any]:
        """
        Broadcast a message to all `members`
        :param message: the message to broadcast
        :param members: The members to send to
        :param recv: Whether or not to receive a response from the members
        :return: a dict of neighbors and whether the message was sent successfully
        """
        member_to_response = {}

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            # Start the broadcast operations and get whether send was successful for each neighbor
            future_to_member = {
                executor.submit(self._send, message, neighbor, recv=recv): neighbor
                for neighbor in members
            }
            # for future in concurrent.futures.as_completed(future_to_member, timeout=HEARTBEAT_WATCHDOG_TIMEOUT):
            for future in concurrent.futures.as_completed(future_to_member):
                member = future_to_member[future]
                try:
                    data = future.result()
                    member_to_response[member] = data
                    self.logger.debug(f"Received resp {data} from {member}")
                except Exception as exc:
                    self.logger.critical(
                        in_red(f"{member} generated an exception: {exc}")
                    )

        return member_to_response

    def broadcast_to_neighbors(self, message) -> Dict[Member, bool]:
        """
        Broadcast a message to all neighbors
        :param message: the message to broadcast
        :return: a dict of neighbors and whether the message was sent successfully
        """

        neighbors = self.get_neighbors()
        return self.broadcast_to(message, neighbors)

    def add_new_member(self, member) -> None:
        # add the new member to the membership list
        if not self.membership_list.update_heartbeat(member, member.timestamp):
            self.membership_list.append(member)

    def send_ls(self, display_res=True) -> Dict[Member, Any]:
        """
        Send a LS message to all neighbors
        :return: None
        """
        ls_message = LSMessage(
            MessageType.LS, self.member.ip, self.member.port, self.member.timestamp, []
        )
        res = self.broadcast_to(ls_message, self.membership_list, recv=True)
        if display_res:
            for member, message in res.items():
                files_str = ", ".join(str(file) for file in message.files)
                print(f"{member}: {files_str}")

        return res

    def get_file_store(self) -> FileStore:
        """
        Get the local files
        :return: a list of files
        """
        return self.file_store

    def get_membership_list(self) -> MembershipList:
        """
        Get the membership list
        :return: membership_list as a MembershipList object
        """
        return self.membership_list

    def _rereplicate_files(self, member: Member) -> None:
        """If the node is the introducer, then we want to
        Rereplicate the files that the node had onto the other nodes

        Args:
            member (Member): The member that left

        """
        # we need tof ind the files in the membership lsit
        # and then rereplicate them
        start_time = time.time()
        adjusted_membership_list = self.membership_list - [member] - [self.member]
        leaving_member = self.membership_list.get_machine(member)
        if leaving_member is None:
            self.logger.warning(f"Leaving member {member} not found in membership list")
            return
        files_on_member = leaving_member.files
        print(f"Files on member {member}: {files_on_member}")

        # print al the files
        # get the nodes that have the file
        leaving_member_files = files_on_member.get_file_names()
        for file_name in leaving_member_files:
            # get the latest version the leaving member had
            file_version = files_on_member.get_file_version(file_name)
            # choose two unique nodes from membership list
            # one that has the file and one that does not
            nodes_with_file = adjusted_membership_list.find_machines_with_file(
                file_name, file_version=file_version
            )
            machines_without_file = adjusted_membership_list.find_machines_without_file(
                file_name, file_version=file_version
            )
            if len(machines_without_file) == 0 or len(nodes_with_file) == 0:
                # we cannot rereplicate the file
                continue

            member_with_file = random.choice(nodes_with_file)
            member_without_file = random.choice(machines_without_file)

            print(
                f"Chose {member_with_file} to send {file_name} to {member_without_file}"
            )

            # send a FileReplicationMessage to the node that has the file
            # so that it can send the file to the node that does not have the file
            file_replication_message = FileReplicationMessage(
                MessageType.FILE_REPLICATION_REQUEST,
                member_without_file.ip,
                member_without_file.port,
                member_without_file.timestamp,
                file_name,
                member_with_file.ip,
                member_with_file.port,
                member_with_file.timestamp,
            )

            print("Broadcasting file replication message")

            resp = self.broadcast_to(
                file_replication_message, [member_with_file], recv=True
            )
            new_file = File(file_name, b"", version=file_version)
            # store the file in the file store
            member_without_file.files.put_file(new_file, b"")
            print(resp)

        end_time = time.time()
        print(
            in_green(f"Rereplicated {leaving_member_files} in {end_time - start_time}s")
        )

    def process_leave(self, message, leaving_member: Member) -> None:
        """
        Process a leave message
        :param member: the member that left
        :return: None
        """

        self.logger.info(in_blue(f"Received LEAVE message from {leaving_member}"))
        if leaving_member not in self.membership_list:
            self.logger.info(
                "Machine {} not found in membership list".format(leaving_member)
            )
            return
        if self.is_introducer:
            print(in_red("REREPLICATING FILES"))
            self._rereplicate_files(leaving_member)

        if leaving_member in self.membership_list:
            self.membership_list.remove(leaving_member)
        self.broadcast_to_neighbors(message)

    def get_self_id(self) -> int:
        """
        Get the id of this node
        :return: The location of this node in the membership list
        """
        idx = -1
        try:
            idx = self.membership_list.index(self.member)
        except ValueError:
            self.logger.error("I am not in membership list!")
        return idx

    def get_self_id_tuple(self) -> Tuple[str, str, int]:
        return (self.host, self.port, self.timestamp)

    def get_alleged_introducer_ip(self):
        """
        Get the alleged IP of the introducer/leader. This gets what DNS Daemon
        is claiming. It is not guaranteed to be correct. A process should double
        check if this is valid.
        We have no guarantees on when the introducer will crash,
        so this is necessary.
        :return: The supposed id of the introducer/leader
        """
        self.dnsdaemon_ip = socket.gethostbyname(VM1_URL)
        daemon_ip = self.dnsdaemon_ip
        alleged_introducer_ip = None
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            print(f"dnsdaemon_ip: {daemon_ip}")
            try:
                # sleep(0.1)
                sock.connect((daemon_ip, DnsDaemonPortID.LEADER))
                sock.sendall(b"JOIN")
                data = sock.recv(1024)
                print(f"Received data from server: {data}")
                if data.decode():
                    sock.sendall(b"ACK")
                    alleged_introducer_ip = data.decode()

            except:
                print("Exception when trying to contact DNS Daemon, trying again")
        return alleged_introducer_ip
