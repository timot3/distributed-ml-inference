import logging
import socketserver
import socket
import time
from typing import List

from .types import MessageType, Message, MembershipList, Member
from .utils import get_self_ip_and_port


# the handler class is responsible for handling the request
class NodeHandler(socketserver.DatagramRequestHandler):

    def _send_ack(self, sock):
        ack_message = Message(MessageType.PONG, self.server.host, self.server.port, self.server.timestamp)
        sock.sendto(ack_message.serialize(), self.client_address)

    def _process_message(self, sock, message):
        # vary the behavior based on the message type
        if message.message_type == MessageType.JOIN:
            self.server.process_join(message)
        elif message.message_type == MessageType.PING:
            self.server.process_ping(message)
            self._send_ack(sock)
        elif message.message_type == MessageType.PONG:
            self.server.process_pong(message)
        elif message.message_type == MessageType.LEAVE:
            self.server.process_leave(message)

        else:
            raise ValueError("Unknown message type: {}".format(message.message_type))

    def handle(self):
        data, sock = self.request
        data = data.strip()
        # deserialize the message
        received_message = Message.deserialize(data)
        self.server.logger.info("RECEIVED: " + str(received_message))
        self._process_message(sock, received_message)



# the node class is a subclass of UDPServer
# Its server bind method is called when the server is created
# and connects to the introducer server via a tcp scket
class NodeUDPServer(socketserver.UDPServer):
    def __init__(self, host, port,introducer_host, introducer_port, is_introducer=False):
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

    def join_network(self):
        self.introducer_socket.connect((self.introducer_host, self.introducer_port))
        # send a message to the introducer to register this node

        # find the ip of this machine
        # send the ip and port of this machine to the introducer
        ip, port = get_self_ip_and_port(self.socket)
        self.timestamp = int(time.time())
        self.member = Member(ip, port, self.timestamp)

        join_message = Message(MessageType.JOIN, ip, port, self.timestamp)

        self.introducer_socket.sendall(join_message.serialize())
        # get the response from the introducer
        response = self.introducer_socket.recv(1024)
        # print the response for now
        membership_list = MembershipList.deserialize(response)
        self.membership_list = membership_list
        return membership_list


    def server_bind(self) -> None:
        # call the super class server_bind method
        super().server_bind()

        # connect to the introducer
        self.join_network()

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

    def process_join(self, message) -> None:
        # add the node to the membership list
        # send a membership list to the node
        neighbors = self.get_neighbors()
        for neighbor in neighbors:
            # send the message to the neighbor
            self.logger.info("Sending join message to neighbor: {}".format(neighbor))
            self.socket.sendto(message.serialize(), (neighbor.ip, neighbor.port))



    def process_ping(self, message) -> None:
        # send a pong message to the node
        pong_message = Message(MessageType.PONG, self.host, self.port, self.timestamp)
        self.socket.sendto(pong_message.serialize(), (message.ip, message.port))


    def process_pong(self, message) -> None:
        # update the timestamp of the node
        raise NotImplementedError()
        pass

    def process_leave(self, message) -> None:
        # remove the node from the membership list
        raise NotImplementedError()
        pass



