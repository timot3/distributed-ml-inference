import concurrent.futures
import logging
import socketserver
import socket
import threading
import time
from typing import List, Tuple, Dict

import random

from FileStore.FileStore import FileStore
from .types import (
    REPLICATION_LEVEL,
    MessageType,
    Message,
    MembershipList,
    Member,
    HEARTBEAT_WATCHDOG_TIMEOUT,
    BUFF_SIZE,
    bcolors,
    DnsDaemonPortID,
    VM1_URL,
    FileStoreMessage,
    MembershipListMessage,
)
from .utils import (
    add_len_prefix,
    get_replication_level,
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

    def _process_join(self, message, new_member):
        if self.server.membership_list.has_machine(new_member):
            self.server.logger.debug(
                "Machine {} already exists in the membership list".format(new_member)
            )
            return
        self.server.broadcast_to_neighbors(message)
        # add the member after broadcasting
        # in order to not include the member in neighbors
        self.server.add_new_member(new_member)

    def _process_put(self, message: FileStoreMessage) -> None:
        """
        Process a PUT message and store the file
        :param message: The received message
        :return: None
        """

        if self.server.is_introducer:
            # choose REPLICATION_LEVEL nodese
            # store the file on those nodes
            membership_list_size = len(self.server.membership_list)
            replication_factor = get_replication_level(
                membership_list_size, REPLICATION_LEVEL
            )
            nodes_to_store = random.sample(
                self.server.membership_list, replication_factor
            )
            tried_nodes = []

            while replication_factor > 0:
                # send the file to the nodes
                # first, check if self.server.member in nodes_to_store
                # if so, store the file locally
                if self.server.member in nodes_to_store:
                    self.server.file_store.put_file(message.file_name, message.data)
                    nodes_to_store.remove(self.server.member)

                # send the file to the nodes
                results: dict = self.server.broadcast_to(message, nodes_to_store)
                # count the number of failures
                num_successes = list(results.values()).count(True)
                replication_factor -= num_successes

                if replication_factor > 0:
                    self.server.logger.warning(
                        f"Failed to store file on {replication_factor} nodes"
                    )
                    self.server.logger.warning(
                        f"Trying again with {replication_factor} nodes"
                    )
                    # choose REPLICATION_LEVEL - num_failures nodes
                    # that are not in tried_nodes
                    chosen_node_cnt = replication_factor
                    # clear nodes_to_store
                    nodes_to_store.clear()

                    while chosen_node_cnt > 0:
                        node = random.choice(self.server.membership_list)
                        if node not in tried_nodes:
                            nodes_to_store.append(node)
                            tried_nodes.append(node)
                            chosen_node_cnt -= 1

            # send a response to the client
            client_ack_message = FileStoreMessage(
                MessageType.FILE_ACK,
                self.server.host,
                self.server.port,
                self.server.timestamp,
                message.file_name,
                message.version,
                b"",  # no data -- this is an ack
            )

            self.server.logger.info(f"Replying with {client_ack_message}")
            self.request.sendall(add_len_prefix(client_ack_message.serialize()))

        else:
            # store the file locally
            self.server.file_store.put_file(message.file_name, message.data)

    def _process_get(self, message: FileStoreMessage) -> None:
        """
        Process a GET message and send the file
        :param message: The received message
        :return: None
        """
        pass

    def _process_delete(self, message: FileStoreMessage) -> None:
        """
        Process a DELETE message and delete the file
        :param message: The received message
        :return: None
        """
        pass

    def _process_ls(self, message: FileStoreMessage) -> None:
        """
        Process a LS message and send the list of files
        :param message: The received message
        :return: None
        """
        pass

    def _process_message(self, message) -> None:
        """
        Process the message and take the appropriate action
        :param message: The received message
        :param sender: The machine that sent the message
        :return: None
        """
        self.server.logger.debug("Processing message: {}".format(message))
        # vary the behavior based on the message type
        new_member_machine = Member(
            message.ip,
            message.port,
            message.timestamp,
        )
        if message.message_type == MessageType.NEW_NODE:
            if not self.server.is_introducer:
                self.server.logger.warning(
                    f"Received a NEW_NODE message from {new_member_machine} but I am not the introducer"
                )
                return

            print(self.server.membership_list)
            self.server.logger.info(f"New member {new_member_machine} joined")
            new_membership_list = self.server.membership_list + [new_member_machine]
            new_membership_list = MembershipList(new_membership_list)

            membership_list_msg = MembershipListMessage(
                MessageType.MEMBERSHIP_LIST,
                self.server.host,
                self.server.port,
                int(time.time()),
                new_membership_list,
            )
            # send the membership list to the node via the tcp socket
            self.request.sendall(add_len_prefix(membership_list_msg.serialize()))
            # convert message to a join message
            message.message_type = MessageType.JOIN
            self._process_join(message, new_member_machine)
            print(self.server.membership_list)

        # handle JOIN
        elif message.message_type == MessageType.JOIN:
            new_member = Member(message.ip, message.port, message.timestamp)
            self.server.logger.info(in_blue(f"Received JOIN from {new_member}"))

            self._process_join(message, new_member)

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

        elif message.message_type == MessageType.ELECT_PING:
            # No need for a different type of message to initiate election compared to
            # sending election messages to lower id nodes (action is exactly the same in
            # Bully algorithm for elections)
            # Find everyone in membership list
            # Send to all others in membership list with lower id
            # Wait for n seconds e.g. 5 seconds
            # No replies - declare leader
            pass

        elif message.message_type == MessageType.CLAIM_LEADER_ACK:
            pass

        elif message.message_type == MessageType.CLAIM_LEADER_PING:
            # Check if another node made a claim of being a leader in previous 5s
            # If so, initiate another election.
            # Otherwise, acknowledge sender as leader
            pass

        # handle PUT for file store
        elif message.message_type == MessageType.PUT:
            self.server.logger.info(f"Received PUT request for {message.file_name}")
            self._process_put(message)

        # handle GET for file store
        elif message.message_type == MessageType.GET:
            raise NotImplementedError

        # handle DELETE for file store
        elif message.message_type == MessageType.DELETE:
            raise NotImplementedError

        elif message.message_type == MessageType.FILE_ACK:
            # construct member from message
            ack_member = Member(message.ip, message.port, message.timestamp)
            self.server.logger.info(f"Received FILE_ACK from {ack_member}")

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

        # self.introducer_host = introducer_host
        # self.introducer_port = introducer_port

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

    def broadcast_to(self, message: Message, who: List[Member]) -> Dict[Member, bool]:
        """
        Broadcast a message to all `who`
        :param message: the message to broadcast
        :return: a dict of neighbors and whether the message was sent successfully
        """
        who_to_successes = {}
        who = MembershipList(self.get_neighbors())

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            # Start the broadcast operations and get whether send was successful for each neighbor
            future_to_member = {
                executor.submit(self._send, message, neighbor): neighbor
                for neighbor in who
            }
            for future in concurrent.futures.as_completed(future_to_member):
                member = future_to_member[future]
                try:
                    data = future.result()
                    who_to_successes[member] = data
                    self.logger.debug(f"Sent message to {member}")
                except Exception as exc:
                    self.logger.critical(
                        in_red(f"{member} generated an exception: {exc}")
                    )

        return who_to_successes

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
