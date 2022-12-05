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
        self.previous_model = ModelType.RESNET

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
        # dispatch the job to the node
        print(f"Dispatching batch {batch.id} to node {node}")
        broadcast_result = self.node.broadcast_to(
            batch.get_job_message(), [node], recv=True
        )
        result = broadcast_result[node]
        if result is None or result.message_type == MessageType.BATCH_FAILED:
            return None
        print(result)

        # update the query counts
        batch_size = self.node.model_collection.get_batch_size(result.model_type)
        self.total_query_count += batch_size
        self.query_counts_by_model[result.model_type] += batch_size

        # return the result
        return BatchResult(batch, result)
