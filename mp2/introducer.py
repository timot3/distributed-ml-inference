import socketserver
import socket
import threading
import time
import sys
from .types import Message, MessageType, MembershipList, Member
from .node import NodeUDPServer
from .utils import get_any_open_port, get_self_ip_and_port
import logging


class IntroducerHandler(socketserver.BaseRequestHandler):
    def handle(self):
        # get the data and socket that sent the data
        data = self.request.recv(1024).strip()
        sock = self.request

        # get the ip and port of the tcp client that just connected
        # deserialize the join message
        received_join_message = Message.deserialize(data)
        self.server.logger.info(str(received_join_message))


        # add the node to the membership list
        new_member = Member(received_join_message.ip, received_join_message.port, received_join_message.timestamp)

        self.server.add_to_membership_list(new_member)

        # send the membership list to the node
        membership_list_msg = self.server.get_membership_list().serialize()
        # send the membership list to the node via the tcp socket
        sock.sendall(membership_list_msg)

        # ping the node to check if it is alive
        # create a PING message
        ping_msg = Message(MessageType.PING, received_join_message.ip, received_join_message.port, received_join_message.timestamp)

        successful_join = False
        # make a udp socket to send the ping message
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as udp_socket:
            # set timeout to 1 second
            udp_socket.settimeout(1)
            # send the ping message
            udp_socket.sendto(ping_msg.serialize(), (received_join_message.ip, received_join_message.port))
            # wait for a response
            try:
                data, address = udp_socket.recvfrom(1024)
                self.server.logger.info("Received ping response from {}".format(address))
                successful_join = True
            except socket.timeout:
                self.server.logger.error("No response from {}".format(address))
                # remove the node from the membership list
                self.server.remove_from_membership_list(received_join_message.ip, received_join_message.port)


        if not successful_join:
            return

        # send a ping message to all the nodes in the membership list
        # create a machine to represent the newly joined node
        new_member_machine = Member(received_join_message.ip, received_join_message.port, received_join_message.timestamp)
        # broadcast the ping message to all the nodes in the membership list
        self.server.broadcast_join(new_member_machine)










class IntroducerServer(socketserver.TCPServer):
    def __init__(self, host, port):
        super().__init__((host, port), IntroducerHandler, bind_and_activate=False)
        self.node_thread = None
        self.host = host
        self.port = port

        # self.membership_list = MembershipList([])

        self.logger = logging.getLogger("IntroducerServer")
        self.logger.setLevel(logging.DEBUG)


        open_port = get_any_open_port()

        self.node = NodeUDPServer(host, open_port, host, port)
        

        
    def _start_node_server(self):
        with self.node:
            self.node.allow_reuse_address = True
            self.node.server_bind()
            self.node.server_activate()
            self.node.serve_forever()
        
    def start_node_server_thread(self):
        self.node_thread = threading.Thread(target=self._start_node_server)
        self.logger.info(f"Starting node server thread on port {self.node.port}")
        self.node_thread.start()

    def server_bind(self):
        super().server_bind()
        self.logger.info("Starting server on {}:{}".format(self.host, self.port))

        self.socket.listen(5)

    def add_to_membership_list(self, member):
        self.logger.info(f"Adding {str(member)} to membership list")
        self.node.membership_list.append(member)
        self.print_membership_list()

    def print_membership_list(self):
        self.logger.info(f"Membership list:\n\t{self.node.membership_list}")

    def remove_from_membership_list(self, machine):
        self.logger.info(f"Removing {str(machine)} from membership list")
        self.node.membership_list.remove(machine)

    def get_membership_list(self):
        return self.node.membership_list

    def broadcast_join(self, machine):
        self.logger.info(f"Broadcasting join message to {str(machine)}")
        # create a join message
        join_msg = Message(MessageType.JOIN, machine.ip, machine.port, machine.timestamp)
        self.node.process_join(join_msg)
