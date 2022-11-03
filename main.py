import argparse
import os
import sys
import shlex
import subprocess
import time
from typing import List

from concurrent.futures import ThreadPoolExecutor


def start_on_docker(num_nodes=10, num_introducers=1, num_replicas=3):
    """
    Start the introducer in the first container
    Start the nodes in the rest of the containers
    """
    # os.system("docker exec -it ece428_mp1_introducer_1 bash -c 'python3 run_introducer.py'")
    server_host_config = """
    services:
    """

    for introducer in range(num_introducers):
        server_host_config += f"""
        introducer_{introducer}:
            container_name: introducer{introducer}
            command: python3 run_introducer.py
            networks:
              static-network:
                ipv4_address: 172.19.0.{introducer + 2}"""

    for host in range(num_introducers, num_nodes):
        server_host_config += f"""
          node{host}:
            build: .
            command: python3 run_node.py
            container_name: server{host}
            environment:
              ID: "{host}" 
            networks:
              static-network:
                ipv4_address: 172.19.0.{host + 2}"""

    network_str = """
    networks:
      static-network:
        ipam:
          config:
            - subnet: 172.19.0.0/16
              ip_range: 172.19.0.0/24
              gateway: 172.19.0.254
    """

    with open("docker-compose.yml", "w") as f:
        f.write(server_host_config)


def start_on_remote():
    """
    Run the introducer and nodes on the remote machines
    """
    pass


def start_on_terminals(num_nodes=10):
    """
    Open terminals for each vm
    In the first terminal, start the introducer
    In the rest of the terminals, start the nodes
    """

    introducer_cmd = 'gnome-terminal -- bash -c "python3 run_introducer.py"'
    node_cmd = 'gnome-terminal -- bash -c "python3 run_node.py"'

    # start the introducer in the first terminal, and the nodes in the rest
    if num_nodes >= 1:
        os.system(introducer_cmd)
    for i in range(1, num_nodes):
        time.sleep(1)
        os.system(node_cmd)


if __name__ == "__main__":
    # 3 options:
    # 1. start all nodes with docker
    # 2. start all nodes in separate terminal
    # 3. Start all nodes on remote machines

    parser = argparse.ArgumentParser()
    # parser.add_argument("-n", "--netID", type=str, required=True, help="netID, will be checked for correctness")
    parser.add_argument("-i", "--pub", type=str, default=None, help="Path to publickey")
    parser.add_argument(
        "-d", "--docker", action="store_true", help="Use docker to start nodes"
    )
    parser.add_argument(
        "-r", "--remote", action="store_true", help="Start nodes on remote machines"
    )
    parser.add_argument(
        "-s", "--separate", action="store_true", help="Start nodes in separate terminals",
    )
    parser.add_argument(
        "-p", "--port", type=int, default=8080, help="Port to start introducer on"
    )

    args = parser.parse_args()

    # if args.netID != "tvitkin2" and args.netID != "zhuxuan2":
    #     print("Invalid netID specified! Terminating!")
    #     sys.exit(-1)

    if args.docker:
        start_on_docker()
    if args.remote:
        start_on_remote()
    if args.separate:
        start_on_terminals(num_nodes=5)
