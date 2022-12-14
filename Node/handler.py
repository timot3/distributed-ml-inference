"""This file does most of the heavy lifting for the entirety of the
SDFS system. It is responsible for handling all the messages that are
sent to the node.

Much of this code is divided based on whether or not the node is an
Introducer(leader). The introducer is responsible for mainaining the
file system in a central way. That way, if it detects a node failure,
it can reassign the files that were stored on that node to other nodes.

Communication with other nodes is handled with the `broadcast_to(message, nodes)`
function, which lives in the `Node` class. This function is responsible for
sending a message to all the nodes in the `nodes` list.

Raises:
    ConnectionError: If the node cannot connect to who it is trying to send a message to.
    NotImplementedError: If this component of the handler is a WIP
    ValueError: If the message type is not recognized
"""
import logging
import socketserver
import socket
import time
from typing import Any, List, Optional, Tuple, Dict, TYPE_CHECKING

import random
from threading import Lock

from FileStore.FileStore import File
from ML.messages import MLClientInferenceResponse
from Node.messages import (
    FileMessage,
    FileReplicationMessage,
    FileVersionMessage,
    LSMessage,
    MembershipListMessage,
    Message,
    MessageType,
)
from .nodetypes import (
    REPLICATION_LEVEL,
    MembershipList,
    Member,
    HEARTBEAT_WATCHDOG_TIMEOUT,
    BUFF_SIZE,
    ELECT_LEADER_TIMEOUT,
)
from .utils import (
    _send_file_err,
    add_len_prefix,
    get_replication_level,
    in_red,
    in_blue,
    trim_len_prefix,
    get_message_from_bytes,
    _recvall,
    _send,
)

if TYPE_CHECKING:
    from .node import NodeTCPServer


class NodeHandler(socketserver.BaseRequestHandler):
    """This class is responsible for handling all the messages that are
    sent to the node.

    Much of this code is divided based on whether or not the node is an
    Introducer(leader).
    """

    def __init__(self, request, client_address, server: "NodeTCPServer"):
        self.request = request
        self.client_address = client_address
        self.server: "NodeTCPServer" = server

        self.election_timestamp = time.time()
        self.election_lock = Lock()
        self.claim_leader_timestamp = 0
        self.claim_leader_lock = Lock()

        self.setup()
        try:
            self.handle()
        finally:
            self.finish()

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
        if self.server.membership_list.has_member(new_member):
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

        if not self.server.is_introducer:
            # store the file locally if not the introducer
            self.server.file_store.put_file(message.file_name, message.data)
            return

        # if the introducer, choose REP_LEVEL nodes to store the file
        # and send the file to those nodes
        # choose REPLICATION_LEVEL nodese
        # store the file on those nodes
        membership_list_size = len(self.server.membership_list)
        replication_factor = get_replication_level(
            membership_list_size - 1, REPLICATION_LEVEL  # don't count self
        )
        list_without_self = self.server.membership_list - [self.server.member]
        nodes_to_store = random.sample(list_without_self, replication_factor)
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

                # update self's files in the membership list
                self.server.member.files.put_file(message.file_name, b"")

                replication_factor -= 1
                num_successes += 1

            # send the file to the nodes
            results: Dict[Member, Any] = self.server.broadcast_to(message, nodes_to_store)
            # count the number of failures
            # num_successes = list(results.values()).count(True)

            # get the nodes that were successful and add the file to them
            for node, success in results.items():
                if success:
                    self.server.logger.debug(f"Storing file on {node.ip}:{node.port}")
                    # find the node in the membership list
                    # and update the files that it has
                    node_member = self.server.membership_list.get_machine(node)
                    if node_member is None:
                        self.server.logger.error(
                            f"PROCESS_PUT: Could not find {node} in membership list"
                        )
                        continue
                    node_member.files.put_file(message.file_name, b"")
                    replication_factor -= 1

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
        client_ack_message = FileMessage(
            MessageType.FILE_ACK,
            self.server.host,
            self.server.port,
            self.server.timestamp,
            message.file_name,
            0,  # version does not matter - this is an ack
            b"",  # no data -- this is an ack
        )

        self.server.logger.debug(f"Replying with {client_ack_message}")
        self.request.sendall(add_len_prefix(client_ack_message.serialize()))

        # DEBUGGING PURPOSES: Print the membership list --> files that each node has
        # for member in self.server.membership_list:
        #     self.server.logger.info(f"{member} has {member.files}")

    def _process_delete(self, message: FileMessage) -> None:
        """
        Process a DELETE message and delete the file
        :param message: The received message
        :return: None
        """
        if self.server.is_introducer:
            # get the nodes that have the file
            nodes_to_delete = self.server.membership_list.find_machines_with_file(
                message.file_name
            )
            if self.server.member in nodes_to_delete:
                self.server.logger.info("Deleting file locally")
                self.server.file_store.delete_file(message.file_name)
                nodes_to_delete.remove(self.server.member)

                # update self's files in the membership list
                self.server.member.files.delete_file(message.file_name)
            # broadcast the delete message to the nodes
            self.server.broadcast_to(message, nodes_to_delete)
        else:
            # delete the file locally
            self.server.file_store.delete_file(message.file_name)

    def _process_ls(self, message: LSMessage) -> None:
        """
        Process a LS message and send the list of files
        :param message: The received message
        :return: None
        """
        # reply with everything in the filestore
        files = self.server.file_store.get_latest_versions()
        file_names_stored = [file.file_name for file in files]
        file_names_requested = [str(file.file_name) for file in message.files]

        files_to_send = []

        # parse the files requested in the LSmessage. If no files are requested, send all files
        if len(file_names_requested) > 0:
            for requested_file in file_names_requested:
                if requested_file in file_names_stored:
                    # get the file from the filestore
                    file = self.server.file_store.get_file(requested_file)
                    files_to_send.append(file)

            if len(files_to_send) == 0:
                self.server.logger.info("No files found")
                # send a file_error message
                _send_file_err(
                    self.request, self.server.member, message.files[0].file_name
                )
                return
        else:
            files_to_send = files

        file_list_message = LSMessage(
            MessageType.LS,
            self.server.host,
            self.server.port,
            self.server.timestamp,
            files_to_send,
        )

        self.server.logger.debug(f"Replying with {file_list_message}")
        self.request.sendall(add_len_prefix(file_list_message.serialize()))

    def _process_get(self, message, versions=None) -> None:
        """
        Process a GET message and send the file
        :param message: The received message
        :return: None
        """
        # Contact all the nodes that have the file
        # then request that they send the file
        # then reply with the latest version of the file
        # if the file is not found, reply with an error message

        if versions is None:
            versions = []

        if not self.server.is_introducer:
            # if we are not the introducer, we reply only if we have the file
            # if len(versions) == 0, then get the latest version
            # otherwise, get the version specified
            files_to_send = []
            if len(versions) == 0:
                file = self.server.file_store.get_file(message.file_name)
                files_to_send.append(file)
            else:
                files_to_send = self.server.file_store.get_file_versions(
                    message.file_name, versions
                )

            for my_file in files_to_send:
                if my_file is not None:
                    # reply with the file
                    file_message = FileMessage(
                        MessageType.FILE_ACK,
                        self.server.host,
                        self.server.port,
                        self.server.timestamp,
                        message.file_name,
                        my_file.version,
                        my_file.file_content,
                    )
                    # print(f"Replying with {file_message}")
                    self.request.sendall(add_len_prefix(file_message.serialize()))
            return

        # if we are the introducer, we need to contact all the nodes
        # that have the file
        # get the nodes that have the file
        nodes_with_file = self.server.membership_list.find_machines_with_file(
            message.file_name
        )
        if len(nodes_with_file) == 0:
            _send_file_err(self.request, self.server.member, message.file_name)
            return

        # find the member with the latest version
        (
            member_with_latest_version,
            version,
        ) = self.server.membership_list.find_machine_with_latest_version(
            message.file_name
        )
        if member_with_latest_version is None:
            _send_file_err(self.request, self.server.member, message.file_name)
            return

        # if we have the latest version, send it
        if self.server.member.is_same_machine_as(member_with_latest_version):
            # get the file
            latest_file = self.server.file_store.get_file(message.file_name)
            if latest_file is None:
                _send_file_err(self.request, self.server.member, message.file_name)
                return

            file_message = FileMessage(
                MessageType.FILE_ACK,
                self.server.host,
                self.server.port,
                self.server.timestamp,
                message.file_name,
                latest_file.version,
                latest_file.file_content,
            )
            self.request.sendall(add_len_prefix(file_message.serialize()))
            return

        # request the latest version
        if version is None:
            version = -1

        get_message = FileVersionMessage(
            MessageType.GET,
            self.server.host,
            self.server.port,
            self.server.timestamp,
            message.file_name,
            [version],
        )

        resp = self.server.broadcast_to(get_message, nodes_with_file, recv=True)
        if resp is None:
            _send_file_err(self.request, self.server.member, message.file_name)
            return

        # reply with the file
        latest_file = resp[member_with_latest_version]
        if latest_file is not None:
            file_message = FileMessage(
                MessageType.FILE_ACK,
                self.server.host,
                self.server.port,
                self.server.timestamp,
                message.file_name,
                latest_file.version,
                latest_file.data,
            )
            self.request.sendall(add_len_prefix(file_message.serialize()))
        else:
            _send_file_err(self.request, self.server.member, message.file_name)

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
            _send(ack_message, addr, logger=self.server.logger)

        # handle PONG (ack)
        elif message.message_type == MessageType.PONG:
            self._process_ack(message)

        # handle LEAVE
        elif message.message_type == MessageType.LEAVE:
            member = Member(message.ip, message.port, message.timestamp)
            self.server.process_leave(message, member)

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
            source = Member(message.ip, message.port, message.timestamp)
            self.server.logger.info(f"Received ELECT_PING message from {source}")
            raise NotImplementedError
            # Find everyone in membership list with lower id

            # IMPORTANT: PREVENT SELF PINGS
            smaller_members = [
                x.get_self_id_tuple()
                for x in self.server.get_membership_list()
                if x < self
            ]

            # Send to all others in membership list with lower id
            claim_message = Message(
                MessageType.ELECT_PING,
                self.server.host,
                self.server.port,
                self.server.timestamp,
            )
            self.server.broadcast_to(claim_message, self.server.get_membership_list())

            # Wait for a short while
            self.server.election_info.update_election_timestamp()
            time.sleep(ELECT_LEADER_TIMEOUT)
            # No replies - declare leader
            if (
                time.time() - self.server.election_info.get_election_timestamp()
                >= ELECT_LEADER_TIMEOUT
            ):
                claim_message = Message(
                    MessageType.CLAIM_LEADER_PING,
                    self.server.host,
                    self.server.port,
                    self.server.timestamp,
                )
                self.server.broadcast_to(claim_message, smaller_members)

        elif message.message_type == MessageType.CLAIM_LEADER_ACK:
            source = Member(message.ip, message.port, message.timestamp)
            self.server.logger.info(f"Received CLAIM_LEADER_ACK message from {source}")
            raise NotImplementedError
            # If another leader claimed to be the leader within the timeout
            # and still thinks it is the leader, initiate another election

        elif message.message_type == MessageType.CLAIM_LEADER_PING:
            source = Member(message.ip, message.port, message.timestamp)
            self.server.logger.info(f"Received CLAIM_LEADER_PING message from {source}")
            raise NotImplementedError
            # Check if another node made a claim of being a leader in previous 5s
            # If so, initiate another election.
            # Otherwise, acknowledge sender as leader

        # handle PUT for file store
        elif message.message_type == MessageType.PUT:
            self.server.logger.debug(f"Received PUT request for {message.file_name}")
            self._process_put(message)

        # handle GET for file store
        elif message.message_type == MessageType.GET:
            self.server.logger.debug(f"Received GET request for {message.file_name}")
            self._process_get(message)

        elif message.message_type == MessageType.GET_VERSIONS:
            self.server.logger.debug(
                f"Received GET_VERSIONS request for {message.file_name}"
            )
            self._process_get(message, versions=message.versions)

        # handle DELETE for file store
        elif message.message_type == MessageType.DELETE:
            self.server.logger.info(f"Received DELETE request for {message.file_name}")
            self._process_delete(message)

        elif message.message_type == MessageType.FILE_ACK:
            # construct member from message
            ack_member = Member(message.ip, message.port, message.timestamp)
            self.server.logger.debug(f"Received FILE_ACK from {ack_member}")

        elif message.message_type == MessageType.LS:
            self._process_ls(message)

        elif message.message_type == MessageType.FILE_REPLICATION_REQUEST:
            self._process_file_replication_request(message)

        elif message.message_type == MessageType.SET_BATCH_SIZE:
            model = self.server.model_collection.select_model(message.model_type)
            model.set_batch_size(message.batch_size)

        elif message.message_type == MessageType.SCHEDULE_BATCH:
            if self.server.is_introducer:
                return
            self.server.logger.info(
                f"Scheduling batch {message.batch_id} for model {message.model_type}"
            )

            self.server.model_collection.insert_batch(
                message.model_type, message.batch_id, message.file_names
            )

        elif message.message_type == MessageType.BATCH_COMPLETE:
            self.server.logger.debug(f"Batch {message.batch_id} complete. Prediction: {message.results}")
            self.server.load_balancer.complete_batch(message.batch_id, message.results)
            # send back to client
            # make MLClientInferenceResponse message
            response = MLClientInferenceResponse(
                self.server.host,
                self.server.port,
                self.server.timestamp,
                message.model_type,
                message.file_names,
                message.results,
            )

            client_member = Member(self.server.client_ip, self.server.client_port, 0)
            self.server._send(response, client_member)

        elif message.message_type == MessageType.BATCH_FAILED:
            # requeue the batch
            self.server.logger.info(
                f"Batch {message.batch_id} failed. Requeuing for model {message.model_type}"
            )

        elif message.message_type == MessageType.INVALIDATE_BATCH:
            raise NotImplementedError

        elif message.message_type == MessageType.INVALIDATE_ALL_IN_NODE:
            raise NotImplementedError
            # Clear everything. If we do not have a queue, remove all
            # code related to this.
            # Right now, all in node == 1 batch, so this is entirely unnecessary

        elif message.message_type == MessageType.CLIENT_INFERERNCE_REQUEST:
            self.server._process_client_inference_request(message)
            self.server.inference_started = True

        elif message.message_type == MessageType.CLIENT_INFERERNCE_RESPONSE:
            raise NotImplementedError

        else:
            raise ValueError("Unknown message type! Received Message: ".format(message))

    def handle(self):
        self.server.logger.debug("Handling request from {}".format(self.client_address))
        data = _recvall(self.request, logger=self.server.logger)
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

    def get_election_timestamp(self) -> int:
        with self.election_lock:
            return self.election_timestamp

    def update_election_timestamp(self) -> None:
        with self.election_lock:
            self.election_timestamp = time.time()

    def _process_file_replication_request(self, message: FileReplicationMessage) -> None:
        # get the file from the file store
        file = self.server.file_store.get_file(message.file_name)
        if file is None:
            self.server.logger.critical(
                f"File {message.file_name} not found in file store"
            )
            return

        # send the file to the node in the to_ip and to_port
        self.server.logger.info(
            f"Sending file {message.file_name} to {message.ip}:{message.port}"
        )
        # construct PUT message
        put_message = FileMessage(
            MessageType.PUT,
            self.server.host,
            self.server.port,
            self.server.timestamp,
            file.file_name,
            file.version,
            file.file_content,
        )
        # send the message
        message_addr = (message.ip, message.port)
        _send(put_message, message_addr, logger=self.server.logger)
        # reply with FILE_REPLICATION_ACK
        ack_message = FileReplicationMessage(
            MessageType.FILE_REPLICATION_ACK,
            self.server.host,
            self.server.port,
            self.server.timestamp,
            message.file_name,
            message.ip,
            message.port,
            message.timestamp,
        )

        self.request.sendall(add_len_prefix(ack_message.serialize()))
