import logging
import socketserver
import socket
import threading
import time
from typing import List

from .types import MessageType, Message, MembershipList, Member, HEARTBEAT_WATCHDOG_TIMEOUT, bcolors
from .utils import get_self_ip_and_port


# the handler class is responsible for handling the request
class NodeHandler(socketserver.DatagramRequestHandler):
    def _send_ack(self):
        ack_message = Message(
            MessageType.PONG, self.server.host, self.server.port, self.server.timestamp
        )
        self.socket.sendto(ack_message.serialize(), self.client_address)

    def _process_ack(self, message):
        ack_machine = Member(message.ip, message.port, message.timestamp)
        now = int(time.time())
        if self.server.membership_list.update_heartbeat(ack_machine, now):
            self.server.logger.debug("Updated heartbeat of {}".format(ack_machine))
        else:
            self.server.logger.warning(
                "Machine {} not found in membership list".format(ack_machine)
            )

    def _process_message(self, message, sender=None) -> None:
        """
        Process the message and take the appropriate action
        :param message: The received message
        :param sender: The machine that sent the message
        :return: None
        """
        # vary the behavior based on the message type
        if message.message_type == MessageType.JOIN:
            # self.server.process_join(message, sender)
            new_member = Member(message.ip, message.port, message.timestamp)
            if self.server.membership_list.has_machine(new_member):
                self.server.logger.debug(
                    "Machine {} already exists in the membership list".format(
                        new_member
                    )
                )
                return
            self.server.broadcast_to_neighbors(message)
            # add the member after broadcasting
            # in order to not include the member in neighbors
            self.server.add_new_member(new_member)

        elif message.message_type == MessageType.PING:
            self.server.logger.info("Sending ACK to {}".format(self.client_address))
            self._send_ack()

        elif message.message_type == MessageType.PONG:
            self.server.logger.info("Received ACK from {}".format(self.client_address))
            self._process_ack(message)

        elif message.message_type == MessageType.LEAVE:
            self.server.logger.info(
                "Received LEAVE from {}".format(self.client_address)
            )
            leaving_member = Member(message.ip, message.port, message.timestamp)
            if leaving_member not in self.server.membership_list:
                self.server.logger.info(
                    "Machine {} not found in membership list".format(leaving_member)
                )
                return
            self.server.membership_list.remove(leaving_member)
            self.server.broadcast_to_neighbors(message)

        else:
            raise ValueError("Unknown message type! Received Message: ".format(message))

    def handle(self):
        self.server.logger.debug("Handling request from {}".format(self.client_address))
        # data, sock = self.request
        data = self.request[0]
        data = data.strip()
        sock = self.request[1]

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
        received_message = Message.deserialize(data)
        self.server.logger.info("RECEIVED: " + str(received_message))
        self._process_message(received_message, sender=machine_of_sender)

    def finish(self):
        # determine if self wfile has any data
        # if it does, send it to the client
        # if len(self.wfile.getvalue()) > 0:
        #     self.socket.sendto(self.wfile.getvalue(), self.client_address)
        self.wfile.flush()


# the node class is a subclass of UDPServer
# Its server bind method is called when the server is created
# and connects to the introducer server via a tcp scket
class NodeUDPServer(socketserver.UDPServer):
    def __init__(
            self, host, port, introducer_host, introducer_port, is_introducer=False, slow_mode=False
    ):
        # call the super class constructor
        super().__init__((host, port), None, bind_and_activate=False)
        self.host = host
        self.port = port

        self.is_introducer = is_introducer
        self.slow_mode = slow_mode

        self.introducer_host = introducer_host
        self.introducer_port = introducer_port

        # initalized when the node joins the network
        self.timestamp: int = 0
        self.member: Member = None

        self.logger = logging.getLogger("NodeServer")
        self.logger.setLevel(logging.INFO)

        # create a tcp socket to connect to the introducer
        # it will be initialized when the node joins the network
        self.introducer_socket = None

        self.membership_list = MembershipList([])

        # set the handler class
        self.RequestHandlerClass = NodeHandler
        # do all the other heavy lifting in the server_bind method

    def validate_request(self, request, message) -> bool:
        data = request[0]
        if len(data) == 0:
            return False
        super().validate_request(request, message)

    def join_network(self) -> bool:
        """
        Join the network by connecting to the introducer
        and processing the received membership list.
        :return:
        """

        if self.introducer_socket is not None:
            self.logger.info("Already connected to the introducer")
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
        return True

    def leave_network(self) -> bool:
        """
        Leave the network by broadcasting a LEAVE message to all neighbors
        :return: True if the node left the network successfully
        """
        # send leave message to all neighbors
        self.logger.info("Sending LEAVE message to neighbors")

        # clear the membership list
        # broadcast the leave message
        leave_message = Message(MessageType.LEAVE, self.member.ip, self.member.port, self.member.timestamp)
        self.broadcast_to_neighbors(leave_message)
        self.membership_list = MembershipList([])

        # disconnect the introducer socket
        self.introducer_socket.close()
        self.introducer_socket = None
        return True

    def _heartbeat_watchdog(self):
        """
        This method periodically checks to see if any members have failed
        It does this by sending pings to neighbors
        If a neighbor does not respond to a ping, it is removed from the membership list
        And a LEAVE message is broadcast to all neighbors
        :return:
        """
        while True:
            # send a ping to all neighbors
            self.logger.debug("Sending Heartbeat PING to neighbors")
            ping_message = Message(MessageType.PING, self.member.ip, self.member.port, self.member.timestamp)
            self.broadcast_to_neighbors(ping_message)
            # sleep for HEARTBEAT_WATCHDOG_TIMEOUT seconds
            time.sleep(HEARTBEAT_WATCHDOG_TIMEOUT)
            # check if any neighbors have failed
            # if they have, remove them from the membership list
            # and broadcast a LEAVE message to all neighbors
            failed_members = []
            neighbors = self.get_neighbors()
            for member in neighbors:
                if member.last_heartbeat < int(time.time()) - HEARTBEAT_WATCHDOG_TIMEOUT:
                    self.logger.warning("Member {} has timed out ping/ack. Marking failed!".format(member))
                    failed_members.append(member)
            for member in failed_members:
                # self.logger.warning("Member {} has timed out ping/ack. Marking failed!".format(member))
                self.membership_list.remove(member)
                leave_message = Message(MessageType.LEAVE, member.ip, member.port, member.timestamp)
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

    def server_bind(self) -> None:
        # call the super class server_bind method
        super().server_bind()

    def multicast(self, message) -> None:
        # send the message to all other nodes
        raise NotImplementedError()
        pass

    def broadcast_to_neighbors(self, message):
        neighbors = MembershipList(self.get_neighbors())
        for neighbor in neighbors:
            if neighbor == self.member:
                continue
            self.logger.info("Sending {} to {}".format(message, neighbor))
            self.socket.sendto(message.serialize(), (neighbor.ip, neighbor.port))

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
            return [member for member in self.membership_list if not member.is_same_machine_as(self.member)]

        idx = self.membership_list.index(self.member)
        # return the two nodes before self
        return [self.membership_list[idx - 1], self.membership_list[idx - 2]]

    def add_new_member(self, member) -> None:
        # add a new member to the membership list
        if member not in self.membership_list:
            self.membership_list.append(member)
        # otherwise, update the timestamp of the member
        if not self.membership_list.update_heartbeat(member, member.timestamp):
            self.membership_list.append(member)

    def get_membership_list(self):
        """
        Get the membership list
        :return: membership_list as a MembershipList object
        """
        return self.membership_list

    def get_self_id(self):
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

    def process_leave(self, message, sender) -> None:
        # remove the node from the membership list
        raise NotImplementedError()
        pass
