"""Load balancer for the ML queries"""
from typing import TYPE_CHECKING

from ML.modeltypes import ModelType
from Node.LoadBalancer.Scheduler import Scheduler
from Node.LoadBalancer.Batch import Batch, BatchResult

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

    def get_best_model(self) -> ModelType:
        """Get the least loaded (by time of inference) model.
        We want model inference times to be within 20% of each other.
        So, take the model that currently has the least inference time
        and return that model.
        """

    async def dispatch(self, batch: Batch) -> BatchResult:
        """Dispatch a job to a node"""

        if batch.model == ModelType.ALEXNET:
            # schedule on least-loaded alexnet node
            return self.dispatch_to_model(ModelType.ALEXNET, batch)
        elif batch.model == ModelType.RESNET:
            # schedule on least-loaded resnet node
            return self.dispatch_to_model(ModelType.RESNET, batch)
        else:
            # pick the less-loaded model and schedule on that
            pass

    async def dispatch_to_model(self, model: ModelType, job: Batch) -> BatchResult:
        """Dispatch a job to a model"""
