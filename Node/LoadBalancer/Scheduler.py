from typing import TYPE_CHECKING, Optional
from Node.LoadBalancer.Batch import Batch
from Node.nodetypes import Member, MembershipList

if TYPE_CHECKING:
    from Node.LoadBalancer.LoadBalancer import LoadBalancer

    from ML.modeltypes import MLModelType
    from Node.node import NodeTCPServer


class Scheduler:
    def __init__(self, node: "NodeTCPServer"):
        self.batches = []
        self.node = node

    def schedule(self, batch):
        self.batches.append(batch)

    def schedule_on(self, node: Member, batch: Batch):
        """Schedule a batch on a specific model"""
        batch.node_scheduled_on = node
        self.schedule(batch)

    def get_next_batch(self) -> Optional[Batch]:
        """Get the next batch in the queue"""
        if len(self.batches) == 0:
            return None
        return self.batches[0]

    def pop_next_batch(self) -> Optional[Batch]:
        """Get the next batch in the queue"""
        if len(self.batches) == 0:
            return None
        return self.batches.pop(0)

    def get_next_batch_on(self, node: Member) -> Optional[Batch]:
        """Get the next batch for the node"""
        # get the next batch for the node
        # if there are no batchs, return None
        for batch in self.batches:
            if batch.node_scheduled_on == node:
                return batch
        return None

    async def dispatch(self):
        """Dispatch a batch to a node"""
        # get the next batch, and send the batch to the node
        # if there are no batchs, return
        batch = self.pop_next_batch()
        if batch is None:
            # no batchs to dispatch
            return

        # send the batch to the node
        results = await self.node.load_balancer.dispatch(batch)
        if results is None:
            # insert the batch back into the queue
            self.schedule(batch)

        return results
