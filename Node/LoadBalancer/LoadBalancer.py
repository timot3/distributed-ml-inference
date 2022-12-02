"""Load balancer for the ML queries"""
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from Node.node import NodeTCPServer
    from Node.nodetypes import Member, MembershipList


class LoadBalancer:
    """Load balancer for the ML queries"""

    def __init__(self, node: "NodeTCPServer"):
        self.node = node
        self.node.load_balancer = self

    def get_best_node(self, query) -> "Member":
        """Returns the best node for the query"""
        # get the node with the least amount of active queries
        # if there is a tie, choose the node with the least amount of total queries
        pass
