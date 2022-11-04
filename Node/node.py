import concurrent.futures
import logging
import socketserver
import socket
import threading
import time
import traceback
from typing import Any, List, Optional, Tuple, Dict

import random

from FileStore.FileStore import File, FileStore
from Node.handler import NodeHandler
from .types import (
    LSMessage,
    MessageType,
    Message,
    MembershipList,
    Member,
    HEARTBEAT_WATCHDOG_TIMEOUT,
    BUFF_SIZE,
    DnsDaemonPortID,
    VM1_URL,
    MembershipListMessage,
)
from .utils import (
    add_len_prefix,
    get_self_ip_and_port,
    in_red,
    trim_len_prefix,
    get_message_from_bytes,
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
        super().__init__((host, port), None, bind_and_activate=False)
        self.host = host
        self.port = port

        self.is_introducer = is_introducer
        self.slow_mode = slow_mode

        # initialized when the node joins the ring
        self.leader_host = None
        self.leader_port = None

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

        # set the handler class
        self.RequestHandlerClass = NodeHandler
        self.dnsdaemon_ip = socket.gethostbyname(VM1_URL)

    def validate_request(self, request, message) -> bool:
        data = request[0]
        if len(data) == 0:
            return False
        if not self.in_ring:
            self.logger.info("Not in ring yet. Ignoring message: {}".format(message))
            return False

        super().validate_request(request, message)

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
            host, port = get_self_ip_and_port(self.socket)
            self.timestamp = int(time.time())

            self.member = Member(host, port, self.timestamp)

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

            try:
                if introducer_sock.connect_ex((introducer_host, introducer_port)) != 0:
                    self.logger.error("Could not connect to introducer")
                    return False

                # first, construct self's member
                join_message = Message(MessageType.NEW_NODE, host, port, self.timestamp)

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

        # disconnect the introducer socket
        self.in_ring = False
        return True

    def _heartbeat_watchdog(self):
        """
        This method periodically checks to see if any members have failed
        It does this by sending pings to neighbors
        If a neighbor does not respond to a ping, it is removed from the membership list
        And a LEAVE message is broadcast to all neighbors
        :return: Never returnse
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
                s.settimeout(1)
                s.connect((member.ip, member.port))
                msg = add_len_prefix(msg.serialize())
                s.sendall(msg)
                if recv:
                    _, data = trim_len_prefix(s.recv(BUFF_SIZE))
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
    ) -> Dict[Member, bool]:
        """
        Broadcast a message to all `members`
        :param message: the message to broadcast
        :return: a dict of neighbors and whether the message was sent successfully
        """
        member_to_response = {}
        members = MembershipList(self.get_neighbors())

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            # Start the broadcast operations and get whether send was successful for each neighbor
            future_to_member = {
                executor.submit(self._send, message, neighbor, recv=recv): neighbor
                for neighbor in members
            }
            for future in concurrent.futures.as_completed(future_to_member):
                member = future_to_member[future]
                try:
                    data = future.result()
                    member_to_response[member] = data
                    self.logger.debug(f"Sent message to {member}")
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

    def send_ls(self) -> None:
        """
        Send a LS message to all neighbors
        :return: None
        """
        ls_message = LSMessage(
            MessageType.LS, self.member.ip, self.member.port, self.member.timestamp, []
        )
        res = self.broadcast_to(ls_message, self.membership_list, recv=True)
        for member, message in res.items():
            files_str = ", ".join(str(file) for file in message.files)
            print(f"{member}: {files_str}")

    def get_membership_list(self) -> MembershipList:
        """
        Get the membership list
        :return: membership_list as a MembershipList object
        """
        return self.membership_list

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
