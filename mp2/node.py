import logging
import socketserver
import socket
import time
from typing import List

from .types import MessageType, Message, MembershipList, Member
from .utils import get_self_ip_and_port


# the handler class is responsible for handling the request
class NodeHandler(socketserver.DatagramRequestHandler):

    def _send_ack(self):
        ack_message = Message(MessageType.PONG, self.server.host, self.server.port, self.server.timestamp)
        self.socket.sendto(ack_message.serialize(), self.client_address)

    def _broadcast_to_neighbors(self, message):
        neighbors = MembershipList(self.server.get_neighbors())
        for neighbor in neighbors:
            if neighbor == self.server.member:
                continue
            self.server.logger.info("Sending {} to {}".format(message, neighbor))
            self.socket.sendto(message.serialize(), (neighbor.ip, neighbor.port))

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
                self.server.logger.info("Machine {} already exists in the membership list".format(new_member))
                return

            self._broadcast_to_neighbors(message)
            self.server.add_new_member(new_member)
        elif message.message_type == MessageType.PING:
            # self.server.process_ping(message, sender)
            self.server.logger.info("Sending ACK to {}".format(self.client_address))
            self._send_ack()
        elif message.message_type == MessageType.PONG:
            self.server.logger.info("Received ACK from {}".format(self.client_address))
            raise NotImplementedError
        elif message.message_type == MessageType.LEAVE:
            self.server.logger.info("Received LEAVE from {}".format(self.client_address))
            raise NotImplementedError

        else:
            raise ValueError("Unknown message type: {}".format(message.message_type))

    def handle(self):
        self.server.logger.info("Handling request from {}".format(self.client_address))
        # data, sock = self.request
        data = self.request[0]
        data = data.strip()
        sock = self.request[1]

        time.sleep(1)
        if len(data) == 0:
            return
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
    def __init__(self, host, port, introducer_host, introducer_port, is_introducer=False):
        # call the super class constructor
        super().__init__((host, port), None, bind_and_activate=False)
        self.host = host
        self.port = port

        self.is_introducer = is_introducer

        self.introducer_host = introducer_host
        self.introducer_port = introducer_port

        # initalized when the node joins the network
        self.timestamp: int = 0
        self.member: Member = None

        self.logger = logging.getLogger("NodeServer")
        self.logger.setLevel(logging.DEBUG)

        # create a tcp socket to connect to the introducer
        self.introducer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

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
        # send a message to the introducer to register this node

        # find the ip of this machine
        # send the ip and port of this machine to the introducer
        ip, port = get_self_ip_and_port(self.socket)
        self.timestamp = int(time.time())
        self.member = Member(ip, port, self.timestamp)

        if self.is_introducer:
            self.membership_list = MembershipList([self.member])
            self.logger.info("Added self to membership list")
            return True

        join_message = Message(MessageType.JOIN, ip, port, self.timestamp)
        self.introducer_socket.connect((self.introducer_host, self.introducer_port))
        self.introducer_socket.sendall(join_message.serialize())
        # get the response from the introducer
        response = self.introducer_socket.recv(1024)
        # print the response for now
        membership_list = MembershipList.deserialize(response)
        self.logger.info("Received membership list: {}".format(membership_list))
        self.membership_list = membership_list
        return True

    def server_bind(self) -> None:
        # call the super class server_bind method
        super().server_bind()

    def multicast(self, message) -> None:
        # send the message to all other nodes
        raise NotImplementedError()
        pass

    def send_to_neighbors(self, message) -> None:
        # send the message to all neighbors
        raise NotImplementedError()
        pass

    def get_neighbors(self) -> List[Member]:
        # return a list of neighbors
        # treat neighbors as a circular list
        # find self in the membership list and return the two nodes before it
        # if self is the first node, return the last two nodes
        # if self is the second node, return the last node and the first node
        # if there are less than three nodes, return the nodes that are not self
        if len(self.membership_list) == 0:
            return []  # no neighbors?? (meme here)

        idx = self.membership_list.index(self.member)
        # find the index of the member in the membership list

        if len(self.membership_list) == 1:
            return []
        elif len(self.membership_list) == 2:
            # return the other node
            return [self.membership_list[1 - idx]]
        elif len(self.membership_list) == 3:
            # return the other two nodes
            return [self.membership_list[1 - idx], self.membership_list[2 - idx]]

        # return the two nodes before self
        return [self.membership_list[idx - 1], self.membership_list[idx - 2]]

    def add_new_member(self, member) -> None:
        # add a new member to the membership list
        if member not in self.membership_list:
            self.membership_list.append(member)
        # otherwise, update the timestamp of the member
        if not self.membership_list.update_heartbeat(member, member.timestamp):
            self.membership_list.append(member)

    def process_leave(self, message, sender) -> None:
        # remove the node from the membership list
        raise NotImplementedError()
        pass
