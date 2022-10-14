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
        membership_list_msg = self.server.membership_list.serialize()
        # send the membership list to the node via the tcp socket
        sock.sendall(membership_list_msg)

        # ping the node to check if it is alive
        # create a PING message
        ping_msg = Message(MessageType.PING, received_join_message.ip, received_join_message.port, received_join_message.timestamp)

        # make a udp socket to send the ping message
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as udp_socket:
            # send the ping message
            udp_socket.sendto(ping_msg.serialize(), (received_join_message.ip, received_join_message.port))
            # wait for a response
            try:
                data, address = udp_socket.recvfrom(1024)
                self.server.logger.info("Received ping response from {}".format(address))
            except socket.timeout:
                self.server.logger.error("No response from {}".format(address))
                # remove the node from the membership list
                self.server.remove_from_membership_list(received_join_message.ip, received_join_message.port)







class IntroducerServer(socketserver.TCPServer):
    def __init__(self, host, port):
        super().__init__((host, port), IntroducerHandler, bind_and_activate=False)
        self.node_thread = None
        self.host = host
        self.port = port

        self.membership_list = MembershipList([])

        self.logger = logging.getLogger("IntroducerServer")
        self.logger.setLevel(logging.DEBUG)
        self.logger.addHandler(logging.StreamHandler(sys.stdout))


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
        self.membership_list.add(member)

        self.print_membership_list()

    def print_membership_list(self):
        self.logger.info(f"Membership list:\n{self.membership_list}")

    def remove_from_membership_list(self, machine):
        self.logger.info(f"Removing {str(machine)} from membership list")
        self.membership_list.remove(machine)

    def get_membership_list(self):
        return self.membership_list





