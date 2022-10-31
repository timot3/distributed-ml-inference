import socket
# import socketserver
import threading
import logging
from time import sleep

# TODO: Make this part of a utils file that contains all magic numbers, variables etc.
ELECTION_PORT = 8787
LEADER_PORT = 8788
VM1_URL = "fa22-cs425-2501.cs.illinois.edu"

# Useful for displaying purposes, not used for functionality
ip_url_dict = {
    socket.gethostbyname(f"fa22-cs425-25{i:02}.cs.illinois.edu"): f"fa22-cs425-25{i:02}.cs.illinois.edu" 
    for i in range(1, 10)
}

'''
The class can return a dead leader, so nodes checking for the leader still 
need to check if the so-called leader is valid.
'''
# class DNSEmulator:
class DNSDaemon:
    introducer_ip = socket.gethostbyname(VM1_URL)
    introducer_ip = "127.0.0.1"
    lock = threading.Lock()
    electionSocket = None
    processJoinSocket = None
    def __init__(self) -> None:
        # self.electionThread = threading.Thread(target=self.electionRoutine, daemon=True)
        self.processJoinThread = threading.Thread(target=self.getLeaderRoutine, daemon=True)
        # self.electionThread.start()
        self.processJoinThread.start()
        print("Created 2 special threads!")

    def getIntroducerIPSerialized(self):
        return self.introducer_ip.encode()

    def electionRoutine(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind((self.introducer_ip, ELECTION_PORT))
            print(f"Election socket binded to election port {ELECTION_PORT} !")
            while True:
               self.electionRoutine_inner(sock)
    
    def electionRoutine_inner(self, sock: socket.SocketType):
        # set up socket, wait for new leader announced.    
            sock.listen()
            conn, addr = sock.accept()
            with conn:
                print(f"Connected by {addr}, corresponding to URL {ip_url_dict[addr]}")
                while True:
                    data = conn.recv(1024)
                    print(f"Server received data: {data}")
                    if not data:
                        break 
                    # TODO: if data says new leader announced:
                    if (data.decode() == "NEW LEADER"):
                        self.update_introducer(str(addr[0]))
                        conn.sendall(b"INTRODUCER UPDATED")
                        break
                    else:
                        print(f"[ERROR] Decoded data is {data.decode()} instead of 'NEW LEADER'. Exiting.")
                        exit(1)

    def getLeaderRoutine(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind((self.introducer_ip, LEADER_PORT))
            print(f"Join socket binded to introducer port {LEADER_PORT} !")
            while True:
                self.getLeaderRoutine_inner(sock)
    
    def getLeaderRoutine_inner(self, sock: socket.SocketType):
        # set up socket, wait for new leader announced.
        sock.listen()
        conn, addr = sock.accept()
        with conn:
            print(f"Connected by {addr}")
            self.handleOneJoinPacket(conn, "JOIN", self.get_introducer())
            self.handleOneJoinPacket(conn, "ACK", None)
            print("New process has been informed of the introducer to contact!")

    def get_introducer(self) -> bytes:
        with self.lock:
            return self.getIntroducerIPSerialized()

    def update_introducer(self, new_introducer_addr: int):
        with self.lock:
            self.introducer_ip = new_introducer_addr
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock_out:
                sock_out.bind((new_introducer_addr, LEADER_PORT))
                sock_out.sendall(b'ACK')
                # release lock
                print(f"Introducer updated to {self.introducer_ip}, corresponding to address {ip_url_dict}")

    def handleOneJoinPacket(self, conn, desiredPacket, ackPacket=None):
        # desiredPacket should be decoded
        # ackPacket should be encoded
        while True:
            data = conn.recv(1024)
            print(f"Server received data: {data}")
            if not data:
                return 
            if data.decode() == desiredPacket:
                if ackPacket:
                    conn.sendall(ackPacket)
                return 
            else:
                print(f"[ERROR] Decoded data is {data.decode()} instead of {desiredPacket}. Exiting.")
                exit(1)


def main():
    # translator = DNSEmulator()
    translator = DNSDaemon()
    introducer_ip = socket.gethostbyname(VM1_URL)
    introducer_ip = "127.0.0.1"
    for i in range(100):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            print(f"introducer_ip: {introducer_ip}")
            try:
                # sleep(0.1)
                sock.connect((introducer_ip, LEADER_PORT))
                sock.sendall(b"JOIN")
                data = sock.recv(1024)
                print(f"Received data from server: {data}")
                if (data.decode()):
                    sock.sendall(b"ACK")
                # TODO: Join server implied by data (IP addr in str). This server may be incorrect, so waiting for 
                # the correct value to quiesce is necessary (by sleeping)
            except:
                print("Exception when trying to join, trying again")
                i -= 1
                sleep(1.0)
                continue
    print("Test program done")
    return




# Just before election ends, update file

# Election end, unlock file

if __name__ == "__main__":
    main()
