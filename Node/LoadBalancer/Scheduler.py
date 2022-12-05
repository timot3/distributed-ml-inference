import asyncio
from typing import TYPE_CHECKING, Optional, Any, Coroutine
from Node.LoadBalancer.Batch import Batch, BatchResult
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
        self.node.logger.debug("Scheduling batch")
        self.batches.put_nowait(batch)

    def requeue(self, batch: Batch):
        """Requeue a batch"""
        self.schedule(batch)

    def schedule_on(self, node: Member, batch: Batch):
        """Schedule a batch on a specific model"""
        batch.node_scheduled_on = node
        self.schedule(batch)

    async def pop_next_batch(self) -> Coroutine[Any, Any, Optional[Batch]]:
        """Get the next batch in the queue"""
        if self.batches.qsize() == 0:
            return None
        return await self.batches.get()

    async def dispatch(self, batch: Batch):
        """Dispatch a batch to a node"""
        # get the next batch, and send the batch to the node
        # if there are no batchs, return
        if batch is None:
            # no batchs to dispatch
            return

        # send the batch to the node
        results = await self.node.load_balancer.dispatch(batch)
        if results is None:
            # insert the batch back into the queue
            self.schedule(batch)

        return results

    async def run(self):
        """Run the scheduler"""
        while True:
            await asyncio.sleep(0.2)
            # print("Scheduler running")
            batch = await self.pop_next_batch()
            if batch is None:
                continue

            elif type(batch) is Batch:
                await self.dispatch(batch)

            elif type(batch) is BatchResult:
                # send response to client
                print(f"GOT BATCH RESULT: {batch.results}")

    def get_query_rate(self) -> float:
        """Get the query rate of the scheduler"""
        return self.node.load_balancer.get_query_rate()
