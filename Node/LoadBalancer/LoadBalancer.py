"""Load balancer for the ML queries"""
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from Node.node import NodeTCPServer


class LoadBalancer:
    """Load balancer for the ML queries"""

    def __init__(self, node: "Node"):
        self.node = node
        self.node.load_balancer = self

    def get_best_node(self, query):
        """Returns the best node for the query"""
        return self.node
