import socketserver
import socket
import time
from typing import Any, List, Optional, Tuple, Dict

import random

from FileStore.FileStore import File
from .types import (
    REPLICATION_LEVEL,
    LSMessage,
    MessageType,
    Message,
    MembershipList,
    Member,
    HEARTBEAT_WATCHDOG_TIMEOUT,
    BUFF_SIZE,
    FileMessage,
    MembershipListMessage,
)
from .utils import (
    add_len_prefix,
    get_replication_level,
    in_red,
    in_blue,
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

    def _process_put(self, message: FileMessage) -> None:
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
                num_successes = 0

                if self.server.member in nodes_to_store:
                    self.server.logger.info("Storing file locally")
                    self.server.file_store.put_file(message.file_name, message.data)
                    nodes_to_store.remove(self.server.member)
                    replication_factor -= 1
                    num_successes += 1

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
            # get the file that was inserted
            new_file = self.server.file_store.get_file(message.file_name)
            client_ack_message = FileMessage(
                MessageType.FILE_ACK,
                self.server.host,
                self.server.port,
                self.server.timestamp,
                new_file.file_name,
                new_file.version,
                b"",  # no data -- this is an ack
            )

            self.server.logger.info(f"Replying with {client_ack_message}")
            self.request.sendall(add_len_prefix(client_ack_message.serialize()))

        else:
            # store the file locally
            self.server.file_store.put_file(message.file_name, message.data)

    def _process_get(self, message: FileMessage) -> None:
        """
        Process a GET message and send the file
        :param message: The received message
        :return: None
        """
        pass

    def _process_delete(self, message: FileMessage) -> None:
        """
        Process a DELETE message and delete the file
        :param message: The received message
        :return: None
        """
        pass

    def _process_ls(self, message: FileMessage) -> None:
        """
        Process a LS message and send the list of files
        :param message: The received message
        :return: None
        """
        # reply with everything in the filestore
        files = self.server.file_store.get_latest_versions()

        file_list_message = LSMessage(
            MessageType.LS,
            self.server.host,
            self.server.port,
            self.server.timestamp,
            files,
        )

        self.server.logger.info(f"Replying with {file_list_message}")
        self.request.sendall(add_len_prefix(file_list_message.serialize()))

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

        elif message.message_type == MessageType.LS:
            self._process_ls(message)

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
