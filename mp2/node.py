import socketserver
import socket
import time
from .types import MessageType, Message, MembershipList
from .utils import get_self_ip_and_port


# the handler class is responsible for handling the request
class NodeHandler(socketserver.DatagramRequestHandler):

    def _process_message(self, message):
        # vary the behavior based on the message type
        if message.message_type == MessageType.JOIN:
            self.server._process_join(message)
        elif message.message_type == MessageType.PING:
            self.server._process_ping(message)
        elif message.message_type == MessageType.PONG:
            self.server._process_pong(message)
        elif message.message_type == MessageType.LEAVE:
            self.server._process_leave(message)

        else:
            raise ValueError("Unknown message type: {}".format(message.message_type))

    def handle(self):
        data, sock = self.request
        data = data.strip()
        # deserialize the message
        received_message = Message.deserialize(data)
        print("RECEIVED: " + str(received_message))



        sock.sendto(b"Hello from the node", self.client_address)


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

        self.timestamp: int = 0 # initalized when the node joins the network

        # create a tcp socket to connect to the introducer
        self.introducer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        self.membership_list = []

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
        join_message = Message(MessageType.JOIN, ip, port, self.timestamp)

        self.introducer_socket.sendall(join_message.serialize())
        # get the response from the introducer
        response = self.introducer_socket.recv(1024)
        # print the response for now
        membership_list = MembershipList.deserialize(response)
        self.membership_list = membership_list.membership_list
        print(membership_list)

    def server_bind(self):
        # call the super class server_bind method
        super().server_bind()

        # connect to the introducer
        self.join_network()

    def multicast(self, message):
        # send the message to all other nodes
        raise NotImplementedError()
        pass

    def send_to_neighbors(self, message):
        # send the message to all neighbors
        raise NotImplementedError()
        pass

    def get_neighbors(self):
        # return a list of neighbors
        # treat neighbors as a circular list

        for neighbor in self.membership_list:
            pass
        raise NotImplementedError()

    def process_join(self, message):
        # add the node to the membership list
        # send a membership list to the node
        raise NotImplementedError()
        pass

    def process_ping(self, message):
        # send a pong message to the node
        pong_message = Message(MessageType.PONG, self.host, self.port, self.timestamp)
        self.socket.sendto(pong_message.serialize(), (message.ip, message.port))


    def process_pong(self, message):
        # update the timestamp of the node
        raise NotImplementedError()
        pass

    def process_leave(self, message):
        # remove the node from the membership list
        raise NotImplementedError()
        pass



