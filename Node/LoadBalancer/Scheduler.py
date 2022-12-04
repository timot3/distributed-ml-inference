import asyncio
from typing import TYPE_CHECKING, Optional, Any, Coroutine
from Node.LoadBalancer.Batch import Batch
from Node.nodetypes import Member, MembershipList

if TYPE_CHECKING:
    from Node.LoadBalancer.LoadBalancer import LoadBalancer

    from ML.modeltypes import MLModelType
    from Node.node import NodeTCPServer


class Scheduler:
    def __init__(self, node: "NodeTCPServer"):
        self.batches = asyncio.Queue()
        self.node = node

    def schedule(self, batch):
        self.batches.put_nowait(batch)

    def schedule_on(self, node: Member, batch: Batch):
        """Schedule a batch on a specific model"""
        batch.node_scheduled_on = node
        self.schedule(batch)

    async def pop_next_batch(self) -> Coroutine[Any, Any, Optional[Batch]]:
        """Get the next batch in the queue"""
        if self.batches.qsize() == 0:
            return None
        return self.batches.get()

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
