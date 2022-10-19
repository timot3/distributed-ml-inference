import logging
import socketserver
import socket
import time
from typing import List
import sys
import os
import traceback

from .types import MessageType, Message, MembershipList, Member
from .utils import get_self_ip_and_port


# class MySocket(socket.socket):
#     def __init__(self, sock):
#         self.sock = sock
#
#     def sendto(self, msg, addr):
#         # print the call stack
#         traceback.print_stack(file=sys.stdout)
#         self.sock.sendto(msg, addr)


# the handler class is responsible for handling the request
class NodeHandler(socketserver.DatagramRequestHandler):

    def _send_ack(self):
        ack_message = Message(MessageType.PONG, self.server.host, self.server.port, self.server.timestamp)
        self.socket.sendto(ack_message.serialize(), self.client_address)

    def _process_join(self, message, sender):

        new_member = Member(message.ip, message.port, message.timestamp)
        print("Processing join from {}".format(new_member))

        print(f"SELF = {self.server.member}")

        neighbors = MembershipList(self.server.get_neighbors())
        print(f"NEIGHBORS = {neighbors}")
        for neighbor in neighbors:
            # send the message to the neighbor
            # do not send the message to the sender or self
            # If the new member is the most recently added machine, do not send to it
            if neighbor == new_member:
                print("Not sending to most recently added member")
                continue
            if neighbor.is_same_machine_as(sender) or neighbor == self.server.member:
                print(f"Not sending to {neighbor}")
                continue
            print("Sending join message to neighbor: {}".format(neighbor))
            # sock = MySocket(self.socket)
            self.socket.sendto(message.serialize(), (neighbor.ip, neighbor.port))

        self.server.add_new_member(new_member)

    def _process_message(self, client_sock, message, sender=None):
        # vary the behavior based on the message type
        if message.message_type == MessageType.JOIN:
            # self.server.process_join(message, sender)
            print("Processing join")
            self._process_join(message, sender)
        elif message.message_type == MessageType.PING:
            # self.server.process_ping(message, sender)
            self.server.logger.info("Sending ACK to {}".format(self.client_address))
            self._send_ack()
        elif message.message_type == MessageType.PONG:
            self.server.process_pong(message, sender)
        elif message.message_type == MessageType.LEAVE:
            self.server.process_leave(message, sender)

        else:
            raise ValueError("Unknown message type: {}".format(message.message_type))

    def handle(self):
        print("Handling request from {}".format(self.client_address))
        # data, sock = self.request
        print(self.request[0])
        data = self.request[0]
        data = data.strip()
        sock = self.request[1]

        # time.sleep(5)
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
        self._process_message(self.client_address, received_message, sender=machine_of_sender)

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

    def join_network(self):
        # send a message to the introducer to register this node

        # find the ip of this machine
        # send the ip and port of this machine to the introducer
        ip, port = get_self_ip_and_port(self.socket)
        self.timestamp = int(time.time())
        self.member = Member(ip, port, self.timestamp)

        if self.is_introducer:
            self.membership_list = MembershipList([self.member])
            self.logger.info("Added self to membership list")
            return self.membership_list

        join_message = Message(MessageType.JOIN, ip, port, self.timestamp)
        self.introducer_socket.connect((self.introducer_host, self.introducer_port))
        self.introducer_socket.sendall(join_message.serialize())
        # get the response from the introducer
        response = self.introducer_socket.recv(1024)
        # print the response for now
        membership_list = MembershipList.deserialize(response)
        self.logger.info("Received membership list: {}".format(membership_list))
        self.membership_list = membership_list
        return membership_list

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
            print("b")
            return []
        elif len(self.membership_list) == 2:
            print("c")
            # return the other node
            return [self.membership_list[1 - idx]]
        elif len(self.membership_list) == 3:
            print("d")
            # return the other two nodes
            return [self.membership_list[1 - idx], self.membership_list[2 - idx]]

        print("e")
        # return the two nodes before self
        return [self.membership_list[idx - 1], self.membership_list[idx - 2]]

    def add_new_member(self, member) -> None:
        # add a new member to the membership list
        if member not in self.membership_list:
            self.membership_list.append(member)
        # otherwise, update the timestamp of the member
        if not self.membership_list.update_heartbeat(member, member.timestamp):
            self.membership_list.append(member)

    def process_join(self, message, sender) -> None:
        pass

    def process_ping(self, message, sender) -> None:
        # send a pong message to the node
        # pong_message = Message(MessageType.PONG, self.host, self.port, self.timestamp)
        # self.socket.sendto(pong_message.serialize(), (message.ip, message.port))
        pass

    def process_pong(self, message, sender) -> None:
        # update the timestamp of the node
        member = Member(message.ip, message.port, message.timestamp)
        now = int(time.time())
        self.membership_list.update_heartbeat(member, now)

    def process_leave(self, message, sender) -> None:
        # remove the node from the membership list
        raise NotImplementedError()
        pass
