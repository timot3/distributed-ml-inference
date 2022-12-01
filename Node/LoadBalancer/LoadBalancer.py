"""Load balancer for the ML queries"""
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from Node.node import NodeTCPServer
<<<<<<< HEAD
    from Node.nodetypes import Member, MembershipList
=======
>>>>>>> acd14b3a0fd707a0de2329242d3be18b9ad8d881


class LoadBalancer:
    """Load balancer for the ML queries"""

<<<<<<< HEAD
    def __init__(self, node: "NodeTCPServer"):
        self.node = node
        self.node.load_balancer = self

    def get_best_node(self, query) -> "Member":
        """Returns the best node for the query"""
        # get the node with the least amount of active queries
=======
    def __init__(self, node: "Node"):
        self.node = node
        self.node.load_balancer = self

    def get_best_node(self, query):
        """Returns the best node for the query"""
        return self.node
>>>>>>> acd14b3a0fd707a0de2329242d3be18b9ad8d881
