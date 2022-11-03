import concurrent.futures
import logging
import socketserver
import socket
import threading
import time
from typing import List, Tuple, Dict

from FileStore.FileStore import FileStore
from .types import (
    MessageType,
    Message,
    MembershipList,
    Member,
    HEARTBEAT_WATCHDOG_TIMEOUT,
    BUFF_SIZE,
    bcolors,
)
from .utils import (
    add_len_prefix,
    get_self_ip_and_port,
    in_red,
    in_blue,
    timed_out,
    trim_len_prefix,
    get_message_from_bytes,
)


class NodeHandler(socketserver.BaseRequestHandler):
    def _send(self, msg: Message, addr: Tuple[str, int]) -> bool:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                if s.connect_ex(addr) != 0:
                    raise ConnectionError("Could not connect to {}".format(addr))
                msg = add_len_prefix(msg.serialize())
                s.sendall(msg)
            return True

        except Exception as e:
            self.server.logger.error(f"Error sending message: {e}")
        finally:
            return False

    def _recvall(self, sock: socket.socket) -> bytes:
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
        self.server.logger.debug(f"Received {len(data)} bytes from {sock.getpeername()}")
        return data

    def _process_ack(self, message: Message) -> None:
        ack_machine = Member(message.ip, message.port, message.timestamp)
        now = int(time.time())
        if self.server.membership_list.update_heartbeat(ack_machine, now):
            self.server.logger.debug("Updated heartbeat of {}".format(ack_machine))
        else:
            self.server.logger.warning(
                "Machine {} not found in membership list".format(ack_machine)
            )

    def _process_message(self, message) -> None:
        """
        Process the message and take the appropriate action
        :param message: The received message
        :param sender: The machine that sent the message
        :return: None
        """
        self.server.logger.debug("Processing message: {}".format(message))
        # vary the behavior based on the message type

        # handle JOIN
        if message.message_type == MessageType.JOIN:
            new_member = Member(message.ip, message.port, message.timestamp)
            self.server.logger.info(in_blue(f"Received JOIN from {new_member}"))
            if self.server.membership_list.has_machine(new_member):
                self.server.logger.debug(
                    "Machine {} already exists in the membership list".format(new_member)
                )
                return
            self.server.broadcast_to_neighbors(message)
            # add the member after broadcasting
            # in order to not include the member in neighbors
            self.server.add_new_member(new_member)

        # handle PING
        elif message.message_type == MessageType.PING:
            ack_message = Message(
                MessageType.PONG,
                self.server.host,
                self.server.port,
                self.server.timestamp,
            )
            addr = (message.ip, message.port)
            self._send(ack_message, addr)

        # handle PONG (ack)
        elif message.message_type == MessageType.PONG:
            self._process_ack(message)

        # handle LEAVE
        elif message.message_type == MessageType.LEAVE:
            leaving_member = Member(message.ip, message.port, message.timestamp)

            self.server.logger.info(
                in_blue(f"Received LEAVE message from {leaving_member}")
            )
            if leaving_member not in self.server.membership_list:
                self.server.logger.info(
                    "Machine {} not found in membership list".format(leaving_member)
                )
                return
            self.server.membership_list.remove(leaving_member)
            self.server.broadcast_to_neighbors(message)

        # handle DISCONNECTED
        elif message.message_type == MessageType.DISCONNECTED:
            # print in red that the node is disconnected
            fail_str = f"{'-' * 10}\nI HAVE BEEN DISCONNECTED. CLEARING MEMBERSHIP LIST AND REJOINING!!!!!"
            self.server.logger.critical(in_red(fail_str))
            self.server.in_ring = False
            time.sleep(HEARTBEAT_WATCHDOG_TIMEOUT)
            self.server.rejoin()

        # handle PUT for file store
        elif message.message_type == MessageType.PUT:
            raise NotImplementedError

        # handle GET for file store
        elif message.message_type == MessageType.GET:
            raise NotImplementedError

        # handle DELETE for file store
        elif message.message_type == MessageType.DELETE:
            raise NotImplementedError

        else:
            raise ValueError("Unknown message type! Received Message: ".format(message))

    def handle(self):
        self.server.logger.debug("Handling request from {}".format(self.client_address))
        data = self._recvall(self.request)
        data = data.strip()

        if len(data) == 0:
            return

        if self.server.slow_mode:
            time.sleep(1)

        # get the machine that sent the message
        ip_of_sender = self.client_address[0]
        port_of_sender = self.client_address[1]
        timestamp_of_sender = int(time.time())

        machine_of_sender = Member(ip_of_sender, port_of_sender, timestamp_of_sender)

        # deserialize the message
        received_message = get_message_from_bytes(data)
        self._process_message(received_message)


# the node class is a subclass of UDPServer
# Its server bind method is called when the server is created
# and connects to the introducer server via a tcp socket
class NodeTCPServer(socketserver.ThreadingTCPServer):
    def __init__(
        self,
        host,
        port,
        introducer_host,
        introducer_port,
        is_introducer=False,
        slow_mode=False,
    ):
        # call the super class constructor
        super().__init__((host, port), None, bind_and_activate=False)
        self.host = host
        self.port = port

        self.is_introducer = is_introducer
        self.slow_mode = slow_mode

        self.introducer_host = introducer_host
        self.introducer_port = introducer_port

        # initialized when the node joins the network
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

    def validate_request(self, request, message) -> bool:
        data = request[0]
        if len(data) == 0:
            return False
        if not self.in_ring:
            self.logger.info("Not in ring yet. Ignoring message: {}".format(message))
            return False

        super().validate_request(request, message)

    def join_network(self) -> bool:
        """
        Join the network by connecting to the introducer
        and processing the received membership list.
        :return: If the connection was successful
        """

        if self.introducer_socket is not None:
            self.logger.error("Already connected to the introducer")
            return False
        self.introducer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # send a message to the introducer to register this node

        # find the ip of this machine
        # send the ip and port of this machine to the introducer
        ip, port = get_self_ip_and_port(self.socket)
        self.timestamp = int(time.time())
        self.member = Member(ip, port, self.timestamp)

        join_message = Message(MessageType.JOIN, ip, port, self.timestamp)
        try:
            self.introducer_socket.connect((self.introducer_host, self.introducer_port))
        except OSError:
            self.logger.error("Already connected to introducer")
            return False

        self.introducer_socket.sendall(join_message.serialize())
        # get the response from the introducer
        response = self.introducer_socket.recv(1024)
        # print the response for now
        membership_list = MembershipList.deserialize(response)
        self.logger.info("Received membership list: {}".format(membership_list))
        self.membership_list = membership_list
        self.in_ring = True
        return True

    def rejoin(self):
        """
        Rejoin the network by connecting to the introducer
        and processing the received membership list.
        :return:
        """
        self.membership_list = MembershipList([])
        self.introducer_socket.close()
        self.introducer_socket = None
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
        self.introducer_socket.close()
        self.introducer_socket = None
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
                    self.logger.info("Member {} has failed".format(member))
                    self.membership_list.remove(member)

                    # send a disconnect mesage to the failed member
                    disconnect_message = Message(
                        MessageType.DISCONNECTED,
                        member.ip,
                        member.port,
                        member.timestamp,
                    )
                    self._send(disconnect_message, member)

                    # broadcast a LEAVE message to all neighbors with the failed member's info

                    leave_message = Message(
                        MessageType.LEAVE, member.ip, member.port, member.timestamp
                    )
                    self.broadcast_to_neighbors(leave_message)

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

    def _send(self, msg: Message, member: Member) -> bool:
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
        except Exception as e:
            self.logger.error(
                in_red("Failed to send message to member {}: {}".format(member, e))
            )
            return False

        return True

    def broadcast_to_neighbors(self, message) -> Dict[Member, bool]:
        """
        Broadcast a message to all neighbors
        :param message: the message to broadcast
        :return: a dict of neighbors and whether the message was sent successfully
        """
        neighbors_to_successes = {}
        neighbors = MembershipList(self.get_neighbors())
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            # Start the broadcast operations and get whether send was successful for each neighbor
            future_to_member = {
                executor.submit(self._send, message, neighbor): neighbor
                for neighbor in neighbors
            }
            for future in concurrent.futures.as_completed(future_to_member):
                member = future_to_member[future]
                try:
                    data = future.result()
                    neighbors_to_successes[member] = data
                    self.logger.debug(f"Sent message to {member}")
                except Exception as exc:
                    self.logger.critical(
                        in_red(f"{member} generated an exception: {exc}")
                    )

        return neighbors_to_successes

    def add_new_member(self, member) -> None:
        # add the new member to the membership list
        if not self.membership_list.update_heartbeat(member, member.timestamp):
            self.membership_list.append(member)

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
