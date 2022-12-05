"""Load balancer for the ML queries"""
from typing import TYPE_CHECKING, Optional

from ML.modeltypes import ModelType
from Node.LoadBalancer.Scheduler import Scheduler
from Node.LoadBalancer.Batch import Batch, BatchResult
from Node.messages import MessageType

if TYPE_CHECKING:
    from Node.node import NodeTCPServer
    from Node.nodetypes import Member, MembershipList


class LoadBalancer:
    """Load balancer for the ML queries"""

    def __init__(self, node: "NodeTCPServer"):
        self.node = node
        self.node.load_balancer = self
        self.previous_model = ModelType.ALEXNET
        self.queries_started = 0

        self.active_batches = {}

        self.recent_batches = []

        self.total_query_count = 0
        self.query_counts_by_model = {
            model: 0 for model in ModelType if model != ModelType.UNSPECIFIED
        }

    def get_best_model(self) -> ModelType:
        """Get the least loaded (by time of inference) model.
        We want model inference times to be within 20% of each other.
        So, take the model that currently has the least inference time
        and return that model.
        """

        if self.previous_model == ModelType.RESNET:
            self.previous_model = ModelType.ALEXNET
            return ModelType.ALEXNET
        else:
            self.previous_model = ModelType.RESNET
            return ModelType.RESNET

        # model_loads = {}
        # for model in ModelType:
        #     if model == ModelType.UNSPECIFIED:
        #         continue
        #     model_loads[model] = self.node.membership_list.get_model_load(model)

        # # print the difference between the models' loads, in percent
        # print("Model loads:")
        # for model in ModelType:
        #     if model == ModelType.UNSPECIFIED:
        #         continue
        #     print(f"{model.name}: {model_loads[model]}")

        # # get the model with the least load
        return min(model_loads, key=model_loads.get)

    async def dispatch(self, batch: Batch) -> BatchResult:
        """Dispatch a job to a node. Return the result of the job
        Either dispatch to the model specified, or to the best model
        if the model is not specified.
        """
        if batch.model_type != ModelType.UNSPECIFIED:
            # schedule on least-loaded alexnet node
            return await self.dispatch_to_model(batch.model_type, batch)
        else:
            best_model = self.get_best_model()
            batch.model_type = best_model

            print("Best model:", best_model)
            return await self.dispatch_to_model(best_model, batch)

    async def dispatch_to_model(self, model_type: ModelType, batch: Batch) -> BatchResult:
        """Dispatch a job to a model. First, get the least loaded node for the model,
        Then dispatch the job to that node

        :param model_type: The model to dispatch the job to
        :param batch: The job to dispatch
        :return: The result of the batch
        """

        # get the least loaded node for the model
        node = self.node.membership_list.get_least_loaded_node_for_model(model_type)

        # dispatch the job to the node
        return await self.dispatch_to_node(node, batch)

    async def dispatch_to_node(
        self, node: "Member", batch: Batch
    ) -> Optional[BatchResult]:
        """Dispatch a job to a node

        :param node: The node to dispatch the job to
        :param batch: The job to dispatch
        :return: The result of the batch
        """

        batch.schedule(node)

        self.active_batches[batch.id] = batch

        # dispatch the job to the node

        if self.node.is_introducer:
            print(f"Dispatching batch {batch.id} to node {node}")
            broadcast_result = self.node.broadcast_to(
                batch.get_job_message(), [node], recv=False
            )
            result = broadcast_result[node]
            if not result:
                print(f"Batch {batch.id} failed on node {node}")
                return None

            # return the result
            return BatchResult(batch, result)
        elif self.node.is_backup:
            return BatchResult(batch, None)

    def complete_batch(self, batch_id: int, result: list):
        """Complete a batch"""
        completed_batch = self.active_batches[batch_id]
        batch_size = self.node.model_collection.get_batch_size(completed_batch.model_type)
        self.total_query_count += batch_size
        self.query_counts_by_model[completed_batch.model_type] += batch_size
        completed_batch.complete(result)

        completion_time = completed_batch.get_completion_time()
        if completion_time is not None:
            self.recent_batches.append(completed_batch)

        del self.active_batches[batch_id]

    def get_query_rate(self) -> float:
        """Get the query rate of the load balancer"""
        # iterate over the recent batches.
        # if the batch is older than 10 seconds, remove it
        # otherwise, count the active time spent on the batch
        # and reeturn the sum of the active times
        # divided by the number of batches
        return self.get_query_rate_model(ModelType.RESNET) + self.get_query_rate_model(
            ModelType.ALEXNET
        )

    def get_query_rate_model(self, model) -> float:
        """Get the query rate of the load balancer"""
        # iterate over the recent batches.
        # if the batch is older than 10 seconds, remove it
        # otherwise, count the active time spent on the batch
        # and reeturn the sum of the active times
        # divided by the number of batches
        active_time = 0
        for batch in self.recent_batches:
            if batch.get_completion_time() is None:
                continue
            if batch.get_completion_time() > 10:
                self.recent_batches.remove(batch)
            elif batch.model_type == model:
                active_time += batch.get_completion_time()

        if len(self.recent_batches) == 0:
            return 0
        return active_time / len(self.recent_batches)

    def get_vm_job_mapping(self) -> dict:
        """Get the mapping of VMs to jobs"""
        vm_job_mapping = {}
        for batch in self.active_batches.values():
            vm_job_mapping[batch.node] = batch.id
        return vm_job_mapping
